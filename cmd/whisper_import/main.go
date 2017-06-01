//
// Copyright 2017 Gregory Trubetskoy. All Rights Reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/tgres/tgres/daemon"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

var (
	totalSqlOps, totalCount, totalPoints int
)

// Random notes on whisper files.
//
// This performs an operation that should never be done - it updates
// data points in the past. The idea is that we ought to be able to
// start sending stuff to tgres, and after that migrate graphite data
// (which can take a lot of time), and all should be well, you don't
// have a gap in your data.
//
// There are often gaps between slots because back-filling skipped
// slots is a thing in whisper. This means that we cannot just take
// whisper data and save the array as our RRD data.
//
// There is no notion of lastUpdate because every data point has a
// timestamp in it.
//
// Whisper timestamps mark the beginning of a slot, we mark the
// end. Though really it is more confusing than that because Whisper
// timestamps are adjusted towards the past.
//
// Whisper files can have "ghost" data points. When new points are
// written, whisper lib makes no attempt to clear out the previous
// incarnation of the round-robin that are there, so if the new data
// skips a point, that slot will still contain the old point. Thus,
// you have always check that the points are within the archive range.

func main() {

	var (
		whisperDir, root, dbConnect       string
		namePrefix, specStr, skipTo       string
		batchSize, staleDays, rraSpecStep int
		heartBeat                         int
		createOnly                        bool
		dsSpec                            *rrd.DSSpec
	)

	flag.StringVar(&whisperDir, "whisper-dir", "/opt/graphite/storage/whisper/", "location where all whisper files are stored")
	flag.StringVar(&root, "root", "", "location of files to be imported, should be subdirectory of whisperDir, defaults to whisperDir")
	flag.StringVar(&dbConnect, "dbconnect", "host=/var/run/postgresql dbname=tgres sslmode=disable", "db connect string")
	flag.StringVar(&namePrefix, "prefix", "", "series name prefix")
	flag.IntVar(&batchSize, "batch", 200, "batch size - should equal to segment width")
	flag.IntVar(&staleDays, "stale-days", 0, "Max days since last update before we ignore this DS (0 = process all)")
	flag.StringVar(&specStr, "spec", "", "Spec (config file format, comma-separated) to use for new DSs (Blank = infer from whisper file)")
	flag.IntVar(&rraSpecStep, "step", 10, "Step to be used with spec parameter (seconds)")
	flag.StringVar(&skipTo, "skip-to", "", "Skip to this series. (Assumes they're always in the same order, which depends on your filesystem).")
	flag.BoolVar(&createOnly, "create-only", false, "Do not save data, just create the DS and RRAs")
	flag.IntVar(&heartBeat, "hb", 1800, "Heartbeat (seconds).")

	flag.Parse()

	if root == "" {
		root = whisperDir
	}

	if specStr != "" {
		var err error
		if dsSpec, err = specFromStr(specStr, rraSpecStep, heartBeat); err != nil {
			fmt.Printf("Error parsing spec: %v\n", err)
			return
		}
		fmt.Printf("Aaa newly created DSs will follow this spec (step %ds) : %q\n", rraSpecStep, specStr)
	}

	var db serde.SerDe

	prefix := os.Getenv("TGRES_DB_PREFIX")
	db, err := serde.InitDb(dbConnect, prefix)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}

	var wg sync.WaitGroup
	ch := make(chan verticalCache)
	wg.Add(1)
	go vcacheFlusher(ch, db.VerticalFlusher(), &wg)
	wg.Add(1)
	go vcacheFlusher(ch, db.VerticalFlusher(), &wg)

	vcache := make(verticalCache)

	count := 0

	var skipping bool
	if skipTo != "" {
		skipping = true
	}

	filepath.Walk(
		root,
		func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(path, ".wsp") {
				return nil
			}

			if count >= batchSize {
				fmt.Printf("+++ Batch size reached: %v\n", batchSize)
				ch <- vcache
				vcache = make(verticalCache)
				totalCount += count
				count = 0
			}

			name := nameFromPath(path, whisperDir, namePrefix)
			if skipping {
				if name == skipTo {
					skipping = false
				}
				fmt.Printf("Skipping: %v\n", name)
				return nil
			}

			fmt.Printf("Processing: %v as %q \n", path, name)

			wsp, err := newWhisper(path)
			if err != nil {
				fmt.Printf("Skipping %v due to error: %v\n", path, err)
				return nil
			}

			if staleDays > 0 {
				ts := findMostRecentTS(wsp)
				if time.Now().Sub(ts) > time.Duration(staleDays*24)*time.Hour {
					fmt.Printf("Most recent update %v older than %v days, ignoring %q.\n", ts, staleDays, name)
					return nil
				}
			}

			// We need a spec
			var spec *rrd.DSSpec
			if dsSpec != nil {
				spec = dsSpec
			} else {
				spec = specFromHeader(wsp.header, heartBeat)
			}

			// NB: If the DS exists, our spec is ignored
			ds, err := db.Fetcher().FetchOrCreateDataSource(serde.Ident{"name": name}, spec)
			if err != nil {
				fmt.Printf("Database error: %v\n", err)
				return err
			}

			if createOnly {
				return nil
			}

			// This trickery replaces the internal DataSource and
			// RoundRobinArchive's with a fresh copy, which does
			// not have a LastUpdated or Latest, thereby
			// permitting us to update points in the past.
			// We need to save the original latest
			var latests []time.Time

			// NB: We must match the ds spec, not ours, but we don't want XFF
			dssp := ds.Spec()
			for i, _ := range dssp.RRAs {
				dssp.RRAs[i].Xff = 0
			}

			newDs := rrd.NewDataSource(dssp)
			rras := ds.RRAs()
			for i, rra := range newDs.RRAs() {
				latests = append(latests, rras[i].Latest())
				dbrra := rras[i].(*serde.DbRoundRobinArchive)
				dbrra.RoundRobinArchiver = rra
			}
			dbds := ds.(*serde.DbDataSource)
			oldDs := dbds.DataSourcer
			dbds.DataSourcer = newDs
			dbds.SetRRAs(rras)

			processAllPoints(ds, wsp)

			for i, rra := range ds.RRAs() {
				vcache.update(rra.(serde.DbRoundRobinArchiver), latests[i])
			}

			// Only flush the DS if LastUpdate has advanced,
			// otherwise leave as is.
			if dbds.LastUpdate().After(oldDs.LastUpdate()) {
				fmt.Printf("Saving DS LastUpdated for %v\n", dbds.Ident())
				db.Flusher().FlushDataSource(dbds)
			}

			count++
			return nil
		},
	)

	// final flush
	if len(vcache) > 0 {
		ch <- vcache
	}
	close(ch)
	wg.Wait() // Wait for flusher to exit.

	fmt.Printf("DONE: GRAND TOTAL %d points across %d series in %d SQL ops.\n", totalPoints, totalCount, totalSqlOps)
}

func vcacheFlusher(ch chan verticalCache, db serde.VerticalFlusher, wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		vcache, ok := <-ch
		if !ok {
			fmt.Printf("Flusher channel closed, exiting.\n")
			return
		}
		vcache.flush(db)
	}
}

func nameFromPath(path, whisperDir, prefix string) string {

	withSlash := whisperDir
	if !strings.HasSuffix(withSlash, "/") {
		withSlash += "/"
	}

	basename := strings.TrimSuffix(path[len(withSlash):], ".wsp")
	name := strings.Replace(basename, "/", ".", -1)
	if prefix != "" {
		name = prefix + "." + name
	}
	return name
}

func findMostRecentTS(wsp *whisper) time.Time {
	archs := wsp.header.archives

	latest := uint32(0)
	for i, _ := range archs {
		points, _ := wsp.dumpArchive(i)

		if len(points) > 0 {
			sort.Sort(archive(points))

			last := points[len(points)-1].TimeStamp
			if latest < last {
				latest = last
			}
		}
	}
	return time.Unix(int64(latest), 0)
}

func processAllPoints(ds rrd.DataSourcer, wsp *whisper) {

	var allPoints archive
	archs := wsp.header.archives

	start, end := uint32(0), uint32(0)

	var msgs []string
	for i, arch := range archs {

		step := time.Duration(arch.Step) * time.Second
		span := time.Duration(arch.Size) * step

		msgs = append(msgs, fmt.Sprintf("(%d) step: %v span: %v CF: %d", i, step, span, wsp.header.metadata.CF))

		points, _ := wsp.dumpArchive(i)

		if len(points) > 0 {

			sort.Sort(archive(points))

			last := points[len(points)-1].TimeStamp
			if last == 0 {
				continue // empty archive
			}

			last += arch.Step // Tgres tracks end of slots

			start = last - uint32(span.Seconds())

			if end == 0 {
				end = last
			} // else end is the last start

			// select points > start
			for _, p := range points {
				p.TimeStamp += arch.Step // Tgres tracks end of slots
				if p.TimeStamp >= start && p.TimeStamp < end {
					allPoints = append(allPoints, p)
				}
			}
			end = start
		}
	}
	fmt.Printf("  Archives: %s\n", strings.Join(msgs, ", "))

	processArchivePoints(ds, allPoints)
}

func processArchivePoints(ds rrd.DataSourcer, points archive) {
	n := 0
	sort.Sort(points)
	var begin, end time.Time
	for _, p := range points {
		if p.TimeStamp != 0 {
			ts := time.Unix(int64(p.TimeStamp), 0)
			if ts.After(ds.LastUpdate()) {
				ds.ProcessDataPoint(p.Value, ts)
				n++

				if begin.IsZero() {
					begin = ts
				}
				end = ts
			}
		}
	}
	fmt.Printf("Processed %d points between %v and %v\n", n, begin, end)
}

func specFromHeader(h *header, hb int) *rrd.DSSpec {

	// Archives are stored in order of precision, so first archive
	// step is the DS step. (TODO: it should be gcd of all
	// archives).
	dsStep := h.archives[0].Step

	spec := rrd.DSSpec{
		Step:      time.Duration(dsStep) * time.Second,
		Heartbeat: time.Duration(hb) * time.Second,
	}

	for _, arch := range h.archives {
		spec.RRAs = append(spec.RRAs, rrd.RRASpec{
			Function: rrd.WMEAN, // TODO: can we support others?
			Step:     time.Duration(arch.Step) * time.Second,
			Span:     time.Duration(arch.Size) * time.Duration(arch.Step) * time.Second,
		})
	}

	return &spec
}

func specFromStr(text string, step, hb int) (*rrd.DSSpec, error) {

	var cfgSpecs []*daemon.ConfigRRASpec

	parts := strings.Split(text, ",")
	for _, part := range parts {
		cfgSpec := &daemon.ConfigRRASpec{}
		if err := cfgSpec.UnmarshalText([]byte(part)); err != nil {
			return nil, err
		}
		cfgSpecs = append(cfgSpecs, cfgSpec)
	}

	spec := &rrd.DSSpec{
		Step:      time.Duration(step) * time.Second,
		Heartbeat: time.Duration(hb) * time.Second,
		RRAs:      make([]rrd.RRASpec, len(cfgSpecs)),
	}
	for i, r := range cfgSpecs {
		spec.RRAs[i] = rrd.RRASpec{
			Function: r.Function,
			Step:     r.Step,
			Span:     r.Span,
			Xff:      float32(r.Xff),
		}
	}
	return spec, nil
}
