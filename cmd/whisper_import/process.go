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
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	_ "github.com/lib/pq"
	"github.com/tgres/tgres/daemon"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

func fetchDataSources(db serde.SerDe) (map[string]serde.DbDataSourcer, error) {
	start := time.Now()
	dss, err := db.Fetcher().FetchDataSources()
	if err != nil {
		return nil, err
	}

	byName := make(map[string]serde.DbDataSourcer)

	for _, ds := range dss {
		dbds, ok := ds.(serde.DbDataSourcer)
		if !ok {
			return nil, fmt.Errorf("preLoad: ds must be a serde.DbDataSourcer")
		}
		if name := dbds.Ident()["name"]; name != "" {
			byName[name] = dbds
		}
	}
	fmt.Printf("Loaded %d DSs in %v\n", len(byName), time.Now().Sub(start))
	return byName, nil
}

func mapFilesToDSs(db serde.SerDe, cfg *Config) map[int64]map[string]int64 {

	fmt.Printf("Pre-caching existing DSs from DB...\n")
	byName, err := fetchDataSources(db)
	if err != nil {
		fmt.Printf("ERROR: %v\n", err)
		return nil
	}

	start, count := time.Now(), 0
	bySeg := make(map[int64]map[string]int64)

	fmt.Printf("Cross-referencing with files in %v\n", cfg.root)
	filepath.Walk(
		cfg.root,
		func(path string, info os.FileInfo, err error) error {
			if !strings.HasSuffix(path, ".wsp") {
				return nil
			}

			count++

			name := nameFromPath(path, cfg.whisperDir, cfg.namePrefix)
			if _, err := os.Stat(path); err != nil {
				return nil // file does not exist
			}

			if count%1000 == 0 {
				fmt.Printf("Checked %d files, currently on: %v\n", count, path)
			}

			ds := byName[name]

			if ds == nil {
				// This means the series is not in our database, so we
				// will assign it to the -1 segment, to be processed last.
				if bySeg[-1] == nil {
					bySeg[-1] = make(map[string]int64)
				}
				bySeg[-1][path] = 0
			} else {
				dbds := ds.(*serde.DbDataSource)
				for _, rra := range dbds.RRAs() {
					dbrra := rra.(*serde.DbRoundRobinArchive)
					seg := dbrra.Seg()
					id := dbds.Id()

					if bySeg[seg] == nil {
						bySeg[seg] = make(map[string]int64)
					}
					bySeg[seg][path] = id
				}
			}

			return nil
		},
	)

	fmt.Printf("Checked a total of %d files in %v.\n", count, time.Now().Sub(start))

	return bySeg
}

func processSegments(db serde.SerDe, ch chan *verticalCache, bySeg map[int64]map[string]int64, cfg *Config) {

	var wg sync.WaitGroup

	finished, err := cfg.sdb.finishedSegments()
	if err != nil {
		fmt.Printf("ERROR getting finishedSegments(): %v\n", err)
		return
	}

	if cfg.mode == "populate" {
		fmt.Printf("Processing whisper files by segments.\n")

		n := 0
		for seg, paths := range bySeg {
			if seg == -1 {
				continue // process last
			}

			n++

			if _, ok := finished[seg]; ok {
				fmt.Printf("Segment %d is done according to db, skipping.\n", seg)
				continue // this segment is done already
			}

			// We do not submit these in parallel, because usually we
			// can far outpace the database here (unlike create mode
			// below).

			fmt.Printf("Reading series for segment %d (%d of %d)...\n", seg, n, len(bySeg))
			wg.Add(1)
			cfg.sdb.setSegmentStarted(seg)
			processSegment(db, ch, seg, paths, cfg, &wg)
		}

		// When we are populating data, do NOT attempt to process
		// seg[-1] (new entries) because they will be a bunch of
		// sparse 1-wide entries that will bug down the DB for no good
		// reason. We should already have those DPs anyway.
		return
	} else {
		if len(bySeg[-1]) > 0 {
			fmt.Printf("Because of create mode, processing series that were not found in the DB.\n")
		} else {
			fmt.Printf("Warning: create mode specified, but there was nothing to create.\n")
		}
	}

	// Now process all new DSs in chunks of width If all we're doing
	// is creating them, we can process this in parallel to make it
	// faster.
	toProcess, n := make(map[string]int64), 0
	fn, submitted, maxProc := 1, 0, 1
	if cfg.mode == "create" {
		maxProc = 10
	}
	for k, v := range bySeg[-1] {
		toProcess[k] = v
		n++
		if n > serde.PgSegmentWidth {
			fmt.Printf("Segment: -1 flush (part %d of %d)\n", fn, len(bySeg[-1])/serde.PgSegmentWidth)
			wg.Add(1)
			submitted++
			go processSegment(db, ch, -1, toProcess, cfg, &wg)
			toProcess, n, fn = make(map[string]int64), 0, fn+1
		}
		if submitted >= maxProc {
			wg.Wait()
			submitted = 0
		}
	}
	if len(toProcess) > 0 {
		fmt.Printf("Segment: -1 flush (part %d of %d)\n", fn, len(bySeg[-1])/serde.PgSegmentWidth)
		wg.Add(1)
		processSegment(db, ch, -1, toProcess, cfg, &wg)
	}
}

var seq int

func processSegment(db serde.SerDe, ch chan *verticalCache, seg int64, paths map[string]int64, cfg *Config, wg *sync.WaitGroup) {
	defer wg.Done()

	vcache := &verticalCache{
		ts:  seq,
		dps: make(map[bundleKey]*verticalCacheSegment),
		dss: make(map[int64]map[int64]interface{}),
	}
	seq++

	stale := 0
	for path, _ := range paths {

		name := nameFromPath(path, cfg.whisperDir, cfg.namePrefix)

		wsp, err := newWhisper(path)
		if err != nil {
			fmt.Printf("Skipping %v due to error: %v\n", path, err)
			continue
		}

		if cfg.staleDays > 0 {
			ts := findMostRecentTS(wsp)
			if time.Now().Sub(ts) > time.Duration(cfg.staleDays*24)*time.Hour {
				stale++
				continue
			}
		}

		// We need a spec
		var spec *rrd.DSSpec
		if cfg.dsSpec != nil {
			spec = cfg.dsSpec
		} else {
			spec = specFromHeader(wsp.header, cfg.heartbeat)
		}

		// NB: If the DS exists, our spec is ignored
		ds, err := db.Fetcher().FetchOrCreateDataSource(serde.Ident{"name": name}, spec)
		if err != nil {
			fmt.Printf("Database error: %v\n", err)
			wsp.Close()
			continue
		}

		stats.Lock()
		stats.totalSqlOps++ // sort of right
		stats.Unlock()

		if cfg.mode == "create" {
			wsp.Close()
			continue
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
		wsp.Close()

		for i, rra := range ds.RRAs() {
			vcache.updateDps(rra.(serde.DbRoundRobinArchiver), latests[i])
		}

		// Only flush the DS if LastUpdate has advanced,
		// otherwise leave as is.
		if dbds.Created() || dbds.LastUpdate().After(oldDs.LastUpdate()) {
			vcache.updateDss(dbds)
		}
	}

	if cfg.mode == "populate" {
		fmt.Printf("+++ Sending vcache [%v] to flusher.\n", vcache.ts)
		vcache.seg = seg
		ch <- vcache
	} else {
		// NB: We still skip when creating, we just keep it quiet
		fmt.Printf("  -- skipped %d series older than %v days\n", stale, cfg.staleDays)
	}

	stats.Lock()
	stats.totalCount += len(paths)
	stats.Unlock()
}

func vcacheFlusher(ch chan *verticalCache, db serde.Flusher, wg *sync.WaitGroup, cfg *Config) {
	defer wg.Done()
	for {
		vcache, ok := <-ch
		if !ok {
			fmt.Printf("Flusher channel closed, exiting.\n")
			return
		}
		vcache.flush(db)
		cfg.sdb.setSegmentFinished(vcache.seg)
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
	if false {
		fmt.Printf("Processed %d points between %v and %v\n", n, begin, end)
	}
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

// DB stuff

type statusDb struct {
	conn   *sql.DB
	prefix string
}

func initStatusDb(connect_string, prefix string) (*statusDb, error) {
	if conn, err := sql.Open("postgres", connect_string); err != nil {
		return nil, err
	} else {
		if err := createTableIfNotExist(conn, prefix); err != nil {
			return nil, fmt.Errorf("createTablesIfNotExist: %v", err)
		}
		return &statusDb{conn: conn, prefix: prefix}, nil
	}
}

func createTableIfNotExist(conn *sql.DB, prefix string) error {
	stmt := `
CREATE TABLE IF NOT EXISTS %[1]swhisper_import_status (
  seg int not null primary key,
  started timestamptz,
  finished timestamptz);
`
	if _, err := conn.Exec(fmt.Sprintf(stmt, prefix)); err != nil {
		log.Printf("ERROR: whisper status CREATE TABLE failed: %v", err)
		return err
	}
	return nil
}

func (sdb *statusDb) setSegmentStarted(seg int64) error {
	stmt := fmt.Sprintf("INSERT INTO %[1]swhisper_import_status (seg, started) "+
		"VALUES ($1, $2) "+
		"ON CONFLICT(seg) "+
		"DO UPDATE SET started = $2", sdb.prefix)
	_, err := sdb.conn.Exec(stmt, seg, time.Now())
	return err
}

func (sdb *statusDb) setSegmentFinished(seg int64) error {
	stmt := fmt.Sprintf("INSERT INTO %[1]swhisper_import_status (seg, finished) "+
		"VALUES ($1, $2) "+
		"ON CONFLICT(seg) "+
		"DO UPDATE SET finished = $2", sdb.prefix)
	_, err := sdb.conn.Exec(stmt, seg, time.Now())
	return err
}

func (sdb *statusDb) finishedSegments() (map[int64]bool, error) {
	stmt := fmt.Sprintf("SELECT seg FROM %[1]swhisper_import_status "+
		"WHERE finished IS NOT NULL", sdb.prefix)

	rows, err := sdb.conn.Query(stmt)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[int64]bool)
	for rows.Next() {
		var seg int64
		if err = rows.Scan(&seg); err != nil {
			return nil, err
		}
		result[seg] = true
	}
	return result, nil
}
