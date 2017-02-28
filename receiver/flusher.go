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

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"golang.org/x/time/rate"
)

type dsFlusher struct {
	flusherCh    flusherChannel
	flushLimiter *rate.Limiter
	db           serde.Flusher
	vdb          serde.VerticalFlusher
	vcache       *verticalCache
	sr           statReporter
	vdbCh        chan *vDpFlushRequest
}

type vDpFlushRequest struct {
	bundleId, seg, i int64
	dps              crossRRAPoints
	latests          map[int64]time.Time
}

func (f *dsFlusher) start(flusherWg, startWg *sync.WaitGroup, mfs int, minStep time.Duration, n int) {
	if mfs > 0 {
		f.flushLimiter = rate.NewLimiter(rate.Limit(mfs), mfs)
	}

	// It's not clear what the size of this channel should be, but
	// we know we do not want it to be infinite. When it blocks,
	// it means the db most definitely cannot keep up, and it's
	// okay for whatever upstream to be blocked by it.
	f.vdbCh = make(chan *vDpFlushRequest, 10240)
	f.vcache = &verticalCache{
		Mutex:   &sync.Mutex{},
		m:       make(map[bundleKey]*verticalCacheSegment),
		minStep: minStep,
	}

	log.Printf(" -- vertical db flusher...")
	for i := 0; i < n; i++ {
		startWg.Add(1)
		go vdbflusher(&wrkCtl{wg: flusherWg, startWg: startWg, id: fmt.Sprintf("vdbflusher_%d", i)}, f.vdb, f.vdbCh, f.sr)
	}
	go vcacheFlusher(f.vcache, f.vdbCh, f.vdb, minStep, f.sr)

	log.Printf(" -- ds flusher...")
	startWg.Add(1)
	f.flusherCh = make(chan *dsFlushRequest, 1024) // TODO why 1024?
	go dsUpdater(&wrkCtl{wg: flusherWg, startWg: startWg, id: "flusher"}, f, f.flusherCh, f.sr)

	if tdb, ok := f.db.(tsTableSizer); ok {
		log.Printf(" -- ts table size reporter")
		go reportTsTableSize(tdb, f.sr)
	}
}

func (f *dsFlusher) stop() {
	log.Printf("flusher.stop(): performing full vcache flush...")
	f.vcache.flush(f.vdbCh, true)
	log.Printf("flusher.stop(): performing full vcache flush done.")

	if f.vdb != nil {
		close(f.vdbCh)
	}

	close(f.flusherCh)
}

func (f *dsFlusher) verticalFlush(ds serde.DbDataSourcer) {
	for _, rra := range ds.RRAs() {
		if _rra, ok := rra.(*serde.DbRoundRobinArchive); ok {
			f.vcache.update(_rra)
		} else {
			log.Printf("verticalFlush: ERROR: rra not a *serde.DbRoundRobinArchive!")
		}
	}
}

func (f *dsFlusher) horizontalFlush(ds serde.DbDataSourcer, block bool) {
	f.flusherCh.queueBlocking(ds, block)
}

func (f *dsFlusher) flushDs(ds serde.DbDataSourcer, block bool) {
	if f.db == nil {
		return
	}
	if f.vdb != nil {
		// These operations do not write to the db, but only move
		// stuff to another cache.
		f.verticalFlush(ds)
		ds.ClearRRAs(false)
		f.horizontalFlush(ds, block)
	}
	return
}

func (f *dsFlusher) enabled() bool {
	return f.db != nil
}

func (f *dsFlusher) statReporter() statReporter {
	return f.sr
}

func (f *dsFlusher) flusher() serde.Flusher {
	return f.db
}

type dsFlusherBlocking interface {
	flushDs(serde.DbDataSourcer, bool)
	enabled() bool
	statReporter() statReporter
	flusher() serde.Flusher
	start(flusherWg, startWg *sync.WaitGroup, mfs int, minStep time.Duration, n int)
	stop()
}

type dsFlushRequest struct {
	ds   rrd.DataSourcer
	resp chan bool
}

type flusherChannel chan *dsFlushRequest

func (f flusherChannel) queueBlocking(ds serde.DbDataSourcer, block bool) {
	defer func() { recover() }() // if we're writing to a closed channel below
	fr := &dsFlushRequest{ds: ds.Copy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	f <- fr
	if block {
		<-fr.resp
	}
}

var dsUpdater = func(wc wController, dsf dsFlusherBlocking, ch chan *dsFlushRequest, sr statReporter) {
	wc.onEnter()
	defer wc.onExit()

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	toFlush := make(map[int64]*serde.DbDataSource)

	limiter := rate.NewLimiter(rate.Limit(10), 10) // TODO: Why 10?

	for {

		var (
			fr *dsFlushRequest
			ok bool
		)

		fr, ok = <-ch
		if !ok {
			log.Printf("%s: channel closed, exiting", wc.ident())
			return
		}

		if fr != nil {
			ds := fr.ds.(*serde.DbDataSource)
			if fr.resp != nil { // blocking flush requested
				start := time.Now()
				err := dsf.flusher().FlushDataSource(ds)
				if err != nil {
					log.Printf("%s: error flushing data source %v: %v", wc.ident(), ds, err)
				}
				dur := time.Now().Sub(start).Seconds()
				delete(toFlush, ds.Id())
				fr.resp <- (err == nil)
				if err == nil {
					sr.reportStatGauge("serde.flush_ds.duration_ms", dur*1000)
					sr.reportStatCount("serde.flush_ds.count", 1)
					sr.reportStatCount("serde.flush_ds.sql_ops", 1)
				}
			} else {
				toFlush[ds.Id()] = ds
			}
		}

		// At this point we're not obligated to do anything, but
		// we can try to flush a few DSs at a very low rate, just
		// in case.
		if !limiter.Allow() {
			continue
		}
		for id, ds := range toFlush { // flush a data source
			start := time.Now()
			err := dsf.flusher().FlushDataSource(ds)
			if err != nil {
				log.Printf("%s: error (background) flushing data source %v: %v", wc.ident(), ds, err)
			}
			dur := time.Now().Sub(start).Seconds()
			delete(toFlush, id)
			if err == nil {
				sr.reportStatGauge("serde.flush_ds.duration_ms", dur*1000)
				sr.reportStatCount("serde.flush_ds.count", 1)
				sr.reportStatCount("serde.flush_ds.sql_ops", 1)
			}
			break
		}
	}
}

var vdbflusher = func(wc wController, db serde.VerticalFlusher, ch chan *vDpFlushRequest, sr statReporter) {
	wc.onEnter()
	defer wc.onExit()

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	type stats struct {
		dpsDur, latDur         time.Duration
		dpsCount, latCount     int
		dpsFlushes, latFlushes int
		dpsSqlOps, latSqlOps   int
		chMaxLen, chGets       int
		start                  time.Time
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

		if len(dpr.dps) > 0 {
			start := time.Now()
			sqlOps, err := db.VerticalFlushDPs(dpr.bundleId, dpr.seg, dpr.i, dpr.dps)
			if err != nil {
				log.Printf("vdbflusher: ERROR in VerticalFlushDps: %v", err)
			}
			st.dpsDur += time.Now().Sub(start)
			st.dpsCount += len(dpr.dps)
			st.dpsSqlOps += sqlOps
			st.dpsFlushes++
		}

		if len(dpr.latests) > 0 {
			start := time.Now()
			sqlOps, err := db.VerticalFlushLatests(dpr.bundleId, dpr.seg, dpr.latests)
			if err != nil {
				log.Printf("verticalCache: ERROR in VerticalFlushLatests: %v", err)
			}
			st.latDur += time.Now().Sub(start)
			st.latCount += len(dpr.latests)
			st.latSqlOps += sqlOps
			st.latFlushes++
		}

		if st.start.Before(time.Now().Add(-time.Second)) {
			dpsDur := st.dpsDur.Seconds()
			if st.dpsFlushes > 0 {
				sr.reportStatGauge("serde.flush_dps.duration_ms", dpsDur*1000/float64(st.dpsFlushes))
			}
			sr.reportStatGauge("serde.flush_dps.speed", float64(st.dpsCount)/dpsDur)
			sr.reportStatCount("serde.flush_dps.count", float64(st.dpsCount))
			sr.reportStatCount("serde.flush_dps.sql_ops", float64(st.dpsSqlOps))
			latDur := st.latDur.Seconds()
			if st.latFlushes > 0 {
				sr.reportStatGauge("serde.flush_latests.duration_ms", latDur*1000/float64(st.latFlushes))
			}
			sr.reportStatGauge("serde.flush_latests.speed", float64(st.latCount)/latDur)
			sr.reportStatCount("serde.flush_latests.count", float64(st.latCount))
			sr.reportStatCount("serde.flush_latests.sql_ops", float64(st.latSqlOps))

			sr.reportStatCount("serde.flush_channel.gets", float64(st.chGets))
			sr.reportStatGauge("serde.flush_channel.len", float64(st.chMaxLen))

			st = &stats{start: time.Now()}
		}
	}
}

var vcacheFlusher = func(vcache *verticalCache, vdbCh chan *vDpFlushRequest, vdb serde.VerticalFlusher, nap time.Duration, sr statReporter) {
	for {
		time.Sleep(nap)
		st := vcache.flush(vdbCh, false)

		sr.reportStatCount("receiver.vcache.points_flushed", float64(st.flushedPoints))
		sr.reportStatGauge("receiver.vcache.points", float64(st.points))
		sr.reportStatGauge("receiver.vcache.segments", float64(st.segments))
		sr.reportStatGauge("receiver.vcache.segment_rows", float64(st.rows))
		sr.reportStatCount("serde.flush_channel.pushes", float64(st.flushes))
		sr.reportStatCount("serde.flush_channel.blocked", float64(st.flushBlocked))
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
