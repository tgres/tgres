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
	"sync"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
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

var stats struct {
	sync.Mutex
	totalSqlOps, totalCount, totalPoints int
}

type Config struct {
	mode        string // create or populate
	dbConnect   string
	root        string
	whisperDir  string
	namePrefix  string
	specStr     string
	rraSpecStep int
	staleDays   int
	heartbeat   int
	dsSpec      *rrd.DSSpec
	workers     int
	width       int
	sdb         *statusDb
}

func main() {

	var cfg Config

	flag.StringVar(&cfg.whisperDir, "whisper-dir", "/opt/graphite/storage/whisper/", "location where all whisper files are stored")
	flag.StringVar(&cfg.root, "root", "", "location of files to be imported, should be subdirectory of whisperDir, defaults to whisperDir")
	flag.StringVar(&cfg.dbConnect, "dbconnect", "postgresql:///tgres?host=/var/run/postgresql", "db connect string")
	flag.StringVar(&cfg.namePrefix, "prefix", "", "series name prefix (no trailing dot)")
	flag.IntVar(&cfg.staleDays, "stale-days", 0, "Max days since last update before we ignore this DS (0 = process all)")
	flag.StringVar(&cfg.specStr, "spec", "", "Spec (config file format, comma-separated) to use for new DSs (Blank = infer from whisper file)")
	flag.IntVar(&cfg.rraSpecStep, "step", 10, "Step to be used with spec parameter (seconds)")
	flag.StringVar(&cfg.mode, "mode", "", "Must be create or populate")
	flag.IntVar(&cfg.heartbeat, "hb", 1800, "Heartbeat (seconds)")
	flag.IntVar(&cfg.workers, "workers", 4, "Number of concurrent db workers")
	flag.IntVar(&cfg.width, "width", serde.PgSegmentWidth, "Segment width (experimental/advanced)")

	flag.Parse()

	if cfg.mode != "create" && cfg.mode != "populate" {
		fmt.Printf("Please specify -mode create or populate\n")
		return
	}

	if cfg.root == "" {
		cfg.root = cfg.whisperDir
	}

	if cfg.specStr != "" {
		var err error
		if cfg.dsSpec, err = specFromStr(cfg.specStr, cfg.rraSpecStep, cfg.heartbeat); err != nil {
			fmt.Printf("Error parsing spec: %v\n", err)
			return
		}
		fmt.Printf("All newly created DSs will follow this spec (step %ds) : %q\n", cfg.rraSpecStep, cfg.specStr)
	}

	if cfg.width != serde.PgSegmentWidth {
		fmt.Printf("Setting segment width to %d\n", cfg.width)
		serde.PgSegmentWidth = cfg.width
	}

	var db serde.SerDe

	prefix := os.Getenv("TGRES_DB_PREFIX")
	db, err := serde.InitDb(cfg.dbConnect, prefix)
	if err != nil {
		fmt.Printf("Error connecting to database: %v\n", err)
		return
	}

	cfg.sdb, err = initStatusDb(cfg.dbConnect, prefix)
	if err != nil {
		fmt.Printf("Error connecting to database (2): %v\n", err)
		return
	}

	bySeg := mapFilesToDSs(db, &cfg)

	var wg sync.WaitGroup
	ch := make(chan *verticalCache)

	for i := 0; i < cfg.workers; i++ {
		wg.Add(1)
		go vcacheFlusher(ch, db.Flusher(), &wg, &cfg)
	}

	processSegments(db, ch, bySeg, &cfg)

	close(ch)
	wg.Wait() // Wait for flusher to exit.

	fmt.Printf("DONE: GRAND TOTAL %d points across %d series in %d SQL ops.\n", stats.totalPoints, stats.totalCount, stats.totalSqlOps)
}
