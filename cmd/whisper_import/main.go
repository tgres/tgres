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
	"time"

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
		whisperDir, root, dbConnect, namePrefix string
		batchSize                               int
	)

	flag.StringVar(&whisperDir, "whisperDir", "/opt/graphite/storage/whisper/", "location where all whisper files are stored")
	flag.StringVar(&root, "root", "", "location of files to be imported, should be subdirectory of whisperDir, defaults to whisperDir")
	flag.StringVar(&dbConnect, "dbconnect", "host=/var/run/postgresql dbname=tgres sslmode=disable", "db connect string")
	flag.StringVar(&namePrefix, "prefix", "", "series name prefix")
	flag.IntVar(&batchSize, "batch", 200, "batch size - should equal to segment width")

	flag.Parse()

	if root == "" {
		root = whisperDir
	}

	var db serde.SerDe

	prefix := os.Getenv("TGRES_DB_PREFIX")
	db, err := serde.InitDb(dbConnect, prefix)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}

	vcache := make(verticalCache)

	count := 0

	filepath.Walk(
		root,
		func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(path, ".wsp") {
				return nil
			}

			if count >= batchSize {
				fmt.Printf("+++ Batch size reached: %v\n", batchSize)
				vcache.flush(db.VerticalFlusher())
				vcache = make(verticalCache)
				totalCount += count
				count = 0
			}

			wsp, err := newWhisper(path)
			if err != nil {
				fmt.Printf("Skipping %v due to error: %v\n", path, err)
				return nil
			}

			fmt.Printf("Processing: %v\n", path)

			name := nameFromPath(path, whisperDir, namePrefix)

			// So at this point we can create/fetch an RRD. First, we will need a database.

			spec := specFromHeader(wsp.header) // We need a spec

			ds, err := db.Fetcher().FetchOrCreateDataSource(serde.Ident{"name": name}, &spec)
			if err != nil {
				fmt.Printf("Database error: %v\n", err)
				return err
			}

			// This trickery replaces the internal DataSource and
			// RoundRobinArchive's with a fresh copy, which does
			// not have a LastUpdated or Latest, thereby
			// permitting us to update points in the past.
			// We need to save the original latest
			var latests []time.Time
			newDs := rrd.NewDataSource(spec)
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

	if len(vcache) > 0 {
		vcache.flush(db.VerticalFlusher())
	}

	fmt.Printf("DONE: GRAND TOTAL %d points across %d series in %d SQL ops.\n", totalPoints, totalCount, totalSqlOps)
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

func processAllPoints(ds rrd.DataSourcer, wsp *whisper) {

	var allPoints archive
	archs := wsp.header.archives

	start, end := uint32(0), uint32(0)

	for i, arch := range archs {

		step := time.Duration(arch.Step) * time.Second
		span := time.Duration(arch.Size) * step

		fmt.Printf("  archive: %d step: %v span: %v CF: %d\n", i, step, span, wsp.header.metadata.CF)

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

	processArchivePoints(ds, allPoints)
}

func processArchivePoints(ds rrd.DataSourcer, points archive) {
	n := 0
	sort.Sort(points)
	for _, p := range points {
		if p.TimeStamp != 0 {
			ts := time.Unix(int64(p.TimeStamp), 0)
			if ts.After(ds.LastUpdate()) {
				ds.ProcessDataPoint(p.Value, ts)
				n++
			}
		}
	}
	fmt.Printf("Processed %d points\n", n)
}

func specFromHeader(h *header) rrd.DSSpec {

	// Archives are stored in order of precision, so first archive
	// step is the DS step. (TODO: it should be gcd of all
	// archives).
	dsStep := h.archives[0].Step

	spec := rrd.DSSpec{
		Step: time.Duration(dsStep) * time.Second,
	}

	for _, arch := range h.archives {
		spec.RRAs = append(spec.RRAs, rrd.RRASpec{
			Function: rrd.WMEAN, // TODO: can we support others?
			Step:     time.Duration(arch.Step) * time.Second,
			Span:     time.Duration(arch.Size) * time.Duration(arch.Step) * time.Second,
		})
	}

	return spec
}
