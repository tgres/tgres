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
	"context"
	"log"
	"sync"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"golang.org/x/time/rate"
)

type dsFlusher struct {
	flusherChs   flusherChannels
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

func (f *dsFlusher) start(flusherWg, startWg *sync.WaitGroup, mfs int, minStep time.Duration) {
	if mfs > 0 {
		f.flushLimiter = rate.NewLimiter(rate.Limit(mfs), mfs)
	}

	// It's not clear what the size of this channel should be, but
	// we know we do not want it to be infinite. When it blocks,
	// it means the db most definitely cannot keep up, and it's
	// okay for whatever upstream to be blocked by it.
	f.vdbCh = make(chan *vDpFlushRequest, 1024)
	f.vcache = &verticalCache{
		Mutex:   &sync.Mutex{},
		m:       make(map[bundleKey]*verticalCacheSegment),
		minStep: minStep,
		sr:      f.sr,
	}

	log.Printf(" -- 1 vertical db flusher...")
	startWg.Add(1)
	go vdbflusher(&wrkCtl{wg: flusherWg, startWg: startWg, id: "vdbflusher2"}, f.vdb, f.vdbCh, f.sr)
	go vcacheFlusher(f.vcache, f.vdbCh, f.vdb, minStep, f.sr)

	log.Printf(" -- 1 ds flusher...")
	startWg.Add(1)
	f.flusherChs = make(flusherChannels, 1)
	f.flusherChs[0] = make(chan *dsFlushRequest, 1024) // TODO why 1024?
	go dsUpdater(&wrkCtl{wg: flusherWg, startWg: startWg, id: "flusher"}, f, f.flusherChs[0])

	if tdb, ok := f.db.(tsTableSizer); ok {
		log.Printf(" -- ts table size reporter")
		go reportTsTableSize(tdb, f.sr)
		_ = tdb
	}
}

func (f *dsFlusher) stop() {
	log.Printf("flusher.stop(): performing full vcache flush...")
	f.vcache.flush(f.vdbCh, f.vdb, true)
	log.Printf("flusher.stop(): performing full vcache flush done.")

	if f.vdb != nil {
		close(f.vdbCh)
	}

	for _, ch := range f.flusherChs {
		close(ch)
	}

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
	f.flusherChs.queueBlocking(ds, block)
}

func (f *dsFlusher) flushDs(ds serde.DbDataSourcer, block bool) bool {
	if f.db == nil {
		return true
	}
	if f.vdb != nil {
		// These operations do not write to the db, but only move
		// stuff to another cache.
		f.verticalFlush(ds)
		ds.ClearRRAs(false)
		f.horizontalFlush(ds, block)
	}
	return true // ZZZ
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

func (f *dsFlusher) channels() flusherChannels {
	return f.flusherChs
}

type dsFlusherBlocking interface {
	flushDs(serde.DbDataSourcer, bool) bool
	enabled() bool
	statReporter() statReporter
	flusher() serde.Flusher
	channels() flusherChannels
	start(flusherWg, startWg *sync.WaitGroup, mfs int, minStep time.Duration)
	stop()
}

type dsFlushRequest struct {
	ds   rrd.DataSourcer
	resp chan bool
}

type flusherChannels []chan *dsFlushRequest

func (f flusherChannels) queueBlocking(ds serde.DbDataSourcer, block bool) {
	defer func() { recover() }() // if we're writing to a closed channel below
	fr := &dsFlushRequest{ds: ds.Copy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	f[ds.Id()%int64(len(f))] <- fr
	if block {
		<-fr.resp
	}
}

var dsUpdater = func(wc wController, dsf dsFlusherBlocking, ch chan *dsFlushRequest) {
	wc.onEnter()
	defer wc.onExit()

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	toFlush := make(map[int64]*serde.DbDataSource)

	ctx := context.TODO()
	limiter := rate.NewLimiter(rate.Limit(10), 10) // TODO: Why 10?

	for {

		var (
			fr *dsFlushRequest
			ok bool
		)

		select {
		case fr, ok = <-ch:
			if !ok {
				log.Printf("%s: channel closed, exiting", wc.ident())
				return
			}
		default:
		}

		if fr != nil {
			ds := fr.ds.(*serde.DbDataSource)
			if fr.resp != nil { // blocking flush requested
				err := dsf.flusher().FlushDataSource(ds)
				if err != nil {
					log.Printf("%s: error flushing data source %v: %v", wc.ident(), ds, err)
				}
				delete(toFlush, ds.Id())
				fr.resp <- (err == nil)
			} else {
				toFlush[ds.Id()] = ds
			}

		} else {
			// At this point we're not obligated to do anything, but
			// we can try to flush a few DSs at a very low rate, just
			// in case.
			limiter.Wait(ctx)
			for id, ds := range toFlush { // flush a data source
				err := dsf.flusher().FlushDataSource(ds)
				if err != nil {
					log.Printf("%s: error (background) flushing data source %v: %v", wc.ident(), ds, err)
				}
				delete(toFlush, id)
				break
			}
		}
	}
}

var vdbflusher = func(wc wController, db serde.VerticalFlusher, ch chan *vDpFlushRequest, sr statReporter) {
	wc.onEnter()
	defer wc.onExit()

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	for {
		dpr, ok := <-ch
		if !ok {
			log.Printf("%s: exiting", wc.ident())
			return
		}

		if len(dpr.dps) > 0 {
			start := time.Now()
			if err := db.VerticalFlushDPs(dpr.bundleId, dpr.seg, dpr.i, dpr.dps); err != nil {
				log.Printf("vdbflusher: ERROR in VerticalFlushDps: %v", err)
			}
			dur := time.Now().Sub(start).Seconds() * 1000
			sr.reportStatGauge("serde.vertical.flush_duration_ms", dur)
			sr.reportStatGauge("serde.vertical.flush_speed", float64(len(dpr.dps))/dur)
			sr.reportStatCount("serde.vertical.flushes", 1)
		}

		if len(dpr.latests) > 0 {
			if err := db.VerticalFlushLatests(dpr.bundleId, dpr.seg, dpr.latests); err != nil {
				log.Printf("verticalCache: ERROR in VerticalFlushLatests: %v", err)
			}
			sr.reportStatCount("serde.vertical.flush_latests", 1)
		}
		sr.reportStatCount("serde.vertical.channel_gets", 1)
	}
}

var vcacheFlusher = func(vcache *verticalCache, vdbCh chan *vDpFlushRequest, vdb serde.VerticalFlusher, nap time.Duration, sr statReporter) {
	for {
		time.Sleep(nap)
		fc := vcache.flush(vdbCh, vdb, false) // This may block/slow
		sr.reportStatCount("serde.vertical.channel_pushes", float64(fc))
		sr.reportStatCount("serde.flushes", 1)
	}
}

// Periodically report the size of the TS table - this can be used to
// detect table bloat when autovacuum is not keeping up

type tsTableSizer interface {
	TsTableSize() (size int64, err error)
}

func reportTsTableSize(ts tsTableSizer, sr statReporter) {
	for {
		time.Sleep(15 * time.Second)
		sz, _ := ts.TsTableSize()
		sr.reportStatGauge("serde.ts_table_size", float64(sz))
	}
}
