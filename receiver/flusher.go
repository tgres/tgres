//
// Copyright 2016 Gregory Trubetskoy. All Rights Reserved.
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

package receiver

import (
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/tgres/tgres/serde"
)

type dsFlusher struct {
	db     serde.Flusher
	vcache *verticalCache
	sr     statReporter
	dbCh   chan *vDpFlushRequest
}

// There are 3 types of flush requests:
// 1. Data Points (DPS), requires bundle_id, seg, dps and vers
// 2. RRA State, requires bundle_id, seg, latests, duration, value
// 3. DS State (DSS), requires seg, lastupdate, duration, value
type vDpFlushRequest struct {
	bundleId, seg, i            int64
	dps, vers                   map[int64]interface{} // DPS
	latests                     map[int64]interface{} // Latests
	lastupdate, duration, value map[int64]interface{} // DSS
}

func (f *dsFlusher) start(flusherWg, startWg *sync.WaitGroup, minStep time.Duration, n int) {

	// It's not clear what the size of this channel should be, but
	// we know we do not want it to be infinite. When it blocks,
	// it means the db most definitely cannot keep up, and it's
	// okay for whatever upstream to be blocked by it.
	f.dbCh = make(chan *vDpFlushRequest, 10240)
	f.vcache = &verticalCache{
		Mutex:   &sync.Mutex{},
		dps:     make(map[bundleKey]*verticalCacheSegment),
		dss:     make(map[int64]*dsStateSegment),
		minStep: minStep,
	}

	log.Printf(" -- vertical db flusher...")
	for i := 0; i < n; i++ {
		startWg.Add(1)
		go dbFlusher(&wrkCtl{wg: flusherWg, startWg: startWg, id: fmt.Sprintf("vdbflusher_%d", i)}, f.db, f.dbCh, f.sr)
	}
	go vcacheFlusher(f.vcache, f.dbCh, minStep, f.sr)

	if tdb, ok := f.db.(tsTableSizer); ok {
		log.Printf(" -- ts table size reporter")
		go reportTsTableSize(tdb, f.sr)
	}
}

func (f *dsFlusher) stop() {
	log.Printf("flusher.stop(): performing full vcache flush...")
	f.vcache.flush(f.dbCh, true)
	log.Printf("flusher.stop(): performing full vcache flush done.")

	if f.db != nil {
		close(f.dbCh)
	}
}

func (f *dsFlusher) verticalFlush(ds serde.DbDataSourcer) {

	if _ds, ok := ds.(*serde.DbDataSource); ok {
		f.vcache.updateDss(_ds)
	} else {
		log.Printf("verticalFlush: ERROR: ds not a *serde.DbDataSource!")
	}

	for _, rra := range ds.RRAs() {
		if _rra, ok := rra.(*serde.DbRoundRobinArchive); ok {
			f.vcache.updateDps(_rra)
		} else {
			log.Printf("verticalFlush: ERROR: rra not a *serde.DbRoundRobinArchive!")
		}
	}
}

func (f *dsFlusher) flushToVCache(ds serde.DbDataSourcer) {
	if f.db != nil {
		// These operations do not write to the db, but only move
		// stuff to another cache.
		f.verticalFlush(ds)
		ds.ClearRRAs()
	}
	return
}

func (f *dsFlusher) statReporter() statReporter {
	return f.sr
}

type dsFlusherBlocking interface {
	flushToVCache(serde.DbDataSourcer)
	statReporter() statReporter
	start(flusherWg, startWg *sync.WaitGroup, minStep time.Duration, n int)
	stop()
}

var dbFlusher = func(wc wController, db serde.Flusher, ch chan *vDpFlushRequest, sr statReporter) {
	wc.onEnter()
	defer wc.onExit()

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	type stats struct {
		dpsDur, dsDur, rraDur             time.Duration
		dpsCount, dsCount, rraCount       int
		dpsFlushes, dsFlushes, rraFlushes int
		dpsSqlOps, dsSqlOps, rraSqlOps    int
		chMaxLen, chGets                  int
		start                             time.Time
	}

	st := &stats{start: time.Now()}

	for {
		dpr, ok := <-ch
		if !ok {
			log.Printf("%s: exiting", wc.ident())
			return
		}

		st.chGets += 1
		if l := len(ch); st.chMaxLen < l {
			st.chMaxLen = l
		}

		if len(dpr.lastupdate) > 0 {
			// DS state Flush
			start := time.Now()
			sqlOps, err := db.FlushDSStates(dpr.seg, dpr.lastupdate, dpr.value, dpr.duration)
			if err != nil {
				log.Printf("vdbflusher: ERROR in VerticalFlushDSs: %v", err)
			}
			st.dsDur += time.Now().Sub(start)
			st.dsCount += len(dpr.lastupdate)
			st.dsSqlOps += sqlOps
			st.dsFlushes++

		} else if len(dpr.dps) > 0 {
			// Datapoints flush
			start := time.Now()
			sqlOps, err := db.FlushDataPoints(dpr.bundleId, dpr.seg, dpr.i, dpr.dps, dpr.vers)
			if err != nil {
				log.Printf("vdbflusher: ERROR in VerticalFlushDps: %v", err)
			}
			st.dpsDur += time.Now().Sub(start)
			st.dpsCount += len(dpr.dps)
			st.dpsSqlOps += sqlOps
			st.dpsFlushes++

		} else if len(dpr.latests) > 0 {
			// RRA State flush
			start := time.Now()
			sqlOps, err := db.FlushRRAStates(dpr.bundleId, dpr.seg, dpr.latests, dpr.value, dpr.duration)
			if err != nil {
				log.Printf("verticalCache: ERROR in VerticalFlushRRAs: %v", err)
			}
			st.rraDur += time.Now().Sub(start)
			st.rraCount += len(dpr.latests)
			st.rraSqlOps += sqlOps
			st.rraFlushes++
		}

		if st.start.Before(time.Now().Add(-time.Second)) {
			dpsDur := st.dpsDur.Seconds()
			if st.dpsFlushes > 0 {
				sr.reportStatGauge("serde.flush_dps.duration_ms", dpsDur*1000/float64(st.dpsFlushes))
			}
			sr.reportStatGauge("serde.flush_dps.speed", float64(st.dpsCount)/dpsDur)
			sr.reportStatCount("serde.flush_dps.count", float64(st.dpsCount))
			sr.reportStatCount("serde.flush_dps.sql_ops", float64(st.dpsSqlOps))

			rraDur := st.rraDur.Seconds()
			if st.rraFlushes > 0 {
				sr.reportStatGauge("serde.flush_rra_state.duration_ms", rraDur*1000/float64(st.rraFlushes))
			}
			sr.reportStatGauge("serde.flush_rra_state.speed", float64(st.rraCount)/rraDur)
			sr.reportStatCount("serde.flush_rra_state.count", float64(st.rraCount))
			sr.reportStatCount("serde.flush_rra_state.sql_ops", float64(st.rraSqlOps))

			dsDur := st.dsDur.Seconds()
			if st.dsFlushes > 0 {
				sr.reportStatGauge("serde.flush_ds_state.duration_ms", dsDur*1000/float64(st.dsFlushes))
			}
			sr.reportStatGauge("serde.flush_ds_state.speed", float64(st.dsCount)/dsDur)
			sr.reportStatCount("serde.flush_ds_state.count", float64(st.dsCount))
			sr.reportStatCount("serde.flush_ds_state.sql_ops", float64(st.dsSqlOps))

			sr.reportStatCount("serde.flush_channel.gets", float64(st.chGets))
			sr.reportStatGauge("serde.flush_channel.len", float64(st.chMaxLen))

			st = &stats{start: time.Now()}
		}
	}
}

var vcacheFlusher = func(vcache *verticalCache, dbCh chan *vDpFlushRequest, nap time.Duration, sr statReporter) {
	for {
		time.Sleep(nap)

		// NB: All this does is create flush requests, no DB I/O happens here.
		st := vcache.flush(dbCh, false)

		sr.reportStatGauge("receiver.vcache.segments", float64(st.dpSegments))
		sr.reportStatGauge("receiver.vcache.segment_rows", float64(st.dpRows))
		sr.reportStatGauge("receiver.vcache.points", float64(st.dpPoints))
		sr.reportStatGauge("receiver.vcache.ds_segments", float64(st.dsSegments))

		sr.reportStatCount("serde.flush_channel.dp_pushes", float64(st.dpFlushes))
		sr.reportStatCount("serde.flush_channel.rs_pushes", float64(st.rsFlushes))
		sr.reportStatCount("serde.flush_channel.ds_pushes", float64(st.dsFlushes))
		sr.reportStatCount("serde.flush_channel.points", float64(st.dpFlushedPoints))
		sr.reportStatCount("serde.flush_channel.blocked", float64(st.dpFlushBlocked))
	}
}

// Periodically report the size of the TS table - this can be used to
// detect table bloat when autovacuum is not keeping up

type tsTableSizer interface {
	TsTableSize() (size, count int64, err error)
}

func reportTsTableSize(ts tsTableSizer, sr statReporter) {
	for {
		time.Sleep(15 * time.Second)
		sz, cnt, _ := ts.TsTableSize()
		sr.reportStatGauge("serde.ts_table.bytes", float64(sz))
		sr.reportStatGauge("serde.ts_table.rows", float64(cnt))
		// 447 bytes overhead per row was determined by way of experimentation, it's probably wrong
		bloat := float64(sz)/(float64(cnt)*float64(serde.PgSegmentWidth*8+447)) - 1.0
		sr.reportStatGauge("serde.ts_table.bloat_factor", bloat)
	}
}
