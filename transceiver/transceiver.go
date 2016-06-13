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

package transceiver

import (
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/statsd"
	"log"
	"math"
	"math/rand"
	"sync"
	"time"
)

type MatchingDSSpecFinder interface {
	FindMatchingDSSpec(name string) *rrd.DSSpec
}

type Transceiver struct {
	serde                              rrd.SerDe
	NWorkers                           int
	MaxCacheDuration, MinCacheDuration time.Duration
	MaxCachedPoints                    int
	StatFlushDuration                  time.Duration
	StatsNamePrefix                    string
	DSSpecs                            MatchingDSSpecFinder
	dss                                *rrd.DataSources
	dpCh                               chan *rrd.DataPoint    // incoming data point
	workerChs                          []chan *rrd.DataPoint  // incoming data point with ds
	flusherChs                         []chan *rrd.DataSource // ds to flush
	dsCopyChs                          []chan *dsCopyRequest  // request a copy of a DS (used by HTTP)
	stCh                               chan *statsd.Stat      // incoming statd stats
	workerWg                           sync.WaitGroup
	flusherWg                          sync.WaitGroup
	statWg                             sync.WaitGroup
	dispatcherWg                       sync.WaitGroup
	startWg                            sync.WaitGroup
}

type dsCopyRequest struct {
	dsId int64
	resp chan *rrd.DataSource
}

type dftDSFinder struct{}

func (_ *dftDSFinder) FindMatchingDSSpec(name string) *rrd.DSSpec {
	return &rrd.DSSpec{
		Step:      10 * time.Second,
		Heartbeat: 2 * time.Hour,
		RRAs: []*rrd.RRASpec{
			&rrd.RRASpec{Function: "AVERAGE",
				Step: 10 * time.Second,
				Size: 6 * time.Hour,
				Xff:  0.5,
			},
			&rrd.RRASpec{Function: "AVERAGE",
				Step: 1 * time.Minute,
				Size: 24 * time.Hour,
				Xff:  0.5,
			},
			&rrd.RRASpec{Function: "AVERAGE",
				Step: 10 * time.Minute,
				Size: 93 * 24 * time.Hour,
				Xff:  0.5,
			},
			&rrd.RRASpec{Function: "AVERAGE",
				Step: 24 * time.Hour,
				Size: 1825 * 24 * time.Hour,
				Xff:  1,
			},
		},
	}
}

func New(serde rrd.SerDe) *Transceiver {
	return &Transceiver{
		serde:             serde,
		NWorkers:          4,
		MaxCacheDuration:  5 * time.Second,
		MinCacheDuration:  1 * time.Second,
		MaxCachedPoints:   256,
		StatFlushDuration: 10 * time.Second,
		StatsNamePrefix:   "stats",
		DSSpecs:           &dftDSFinder{},
		dss:               &rrd.DataSources{},
		dpCh:              make(chan *rrd.DataPoint, 65536), // so we can survive a graceful restart
		stCh:              make(chan *statsd.Stat, 65536),   // ditto
	}
}

func (t *Transceiver) Start() error {
	log.Printf("Transceiver: Loading data from serde.")
	t.dss.Reload(t.serde)

	t.startWorkers()
	t.startFlushers()
	t.startStatWorker()

	// Wait for workers/flushers to start correctly
	t.startWg.Wait()
	log.Printf("Transceiver: All workers running, starting dispatcher.")

	go t.dispatcher()
	log.Printf("Transceiver: Ready.")

	return nil
}

func (t *Transceiver) Stop() {

	log.Printf("Closing dispatcher channel...")
	close(t.dpCh)
	t.dispatcherWg.Wait()
	log.Printf("Dispatcher finished.")
}

func (t *Transceiver) stopWorkers() {
	log.Printf("stopWorkers(): waiting for worker channels to empty...")
	empty := false
	for !empty {
		empty = true
		for _, c := range t.workerChs {
			if len(c) > 0 {
				empty = false
				break
			}
		}
		if !empty {
			time.Sleep(100 * time.Millisecond)
		}
	}

	log.Printf("stopWorkers(): closing all worker channels...")
	for _, ch := range t.workerChs {
		close(ch)
	}
	log.Printf("stopWorkers(): waiting for workers to finish...")
	t.workerWg.Wait()
	log.Printf("stopWorkers(): all workers finished.")
}

func (t *Transceiver) stopFlushers() {
	log.Printf("stopFlushers(): closing all flusher channels...")
	for _, ch := range t.flusherChs {
		close(ch)
	}
	log.Printf("stopFlushers(): waiting for flushers to finish...")
	t.flusherWg.Wait()
	log.Printf("stopFlushers(): all flushers finished.")
}

func (t *Transceiver) stopStatWorker() {

	log.Printf("stopStatWorker(): waiting for stat channel to empty...")
	for len(t.stCh) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("stopStatWorker(): closing stat channel...")
	close(t.stCh)
	log.Printf("stopStatWorker(): waiting for stat worker to finish...")
	t.statWg.Wait()
	log.Printf("stopStatWorker(): stat worker finished.")
}

func (t *Transceiver) dispatcher() {
	t.dispatcherWg.Add(1)
	defer t.dispatcherWg.Done()

	for {
		dp, ok := <-t.dpCh

		if !ok {
			log.Printf("dispatcher(): channel closed, shutting down")
			t.stopStatWorker()
			t.stopWorkers()
			t.stopFlushers()
			break
		}

		if dp.DS = t.dss.GetByName(dp.Name); dp.DS == nil {
			// DS does not exist, can we create it?
			if dsSpec := t.DSSpecs.FindMatchingDSSpec(dp.Name); dsSpec != nil {
				if ds, err := t.serde.CreateDataSource(dp.Name, dsSpec); err == nil {
					t.dss.Insert(ds)
					dp.DS = ds
				} else {
					log.Printf("dispatcher(): createDataSource() error: %v", err)
					continue
				}
			}
		}

		if dp.DS != nil {
			t.workerChs[dp.DS.Id%int64(t.NWorkers)] <- dp
		}
	}
}

func (t *Transceiver) QueueDataPoint(dp *rrd.DataPoint) {
	t.dpCh <- dp
}

func (t *Transceiver) QueueStat(st *statsd.Stat) {
	t.stCh <- st
}

func (t *Transceiver) requestDsCopy(id int64) *rrd.DataSource {
	req := &dsCopyRequest{id, make(chan *rrd.DataSource)}
	t.dsCopyChs[id%int64(t.NWorkers)] <- req
	return <-req.resp
}

// Satisfy DSGetter interface in tgres/dsl
func (t *Transceiver) GetDSById(id int64) *rrd.DataSource {
	return t.requestDsCopy(id)
}
func (t *Transceiver) DsIdsFromIdent(ident string) map[string]int64 {
	return t.dss.DsIdsFromIdent(ident)
}
func (t *Transceiver) SeriesQuery(ds *rrd.DataSource, from, to time.Time, maxPoints int64) (rrd.Series, error) {
	return t.serde.SeriesQuery(ds, from, to, maxPoints)
}

// End DSGetter interface

func (t *Transceiver) worker(id int64) {

	t.workerWg.Add(1)
	defer t.workerWg.Done()

	var (
		ds              *rrd.DataSource
		flushEverything bool
		recent          = make(map[int64]bool)
	)

	var periodicFlushCheck = make(chan int)
	go func() {
		for {
			// Sleep randomly between min and max cache durations (is this wise?)
			i := int(t.MaxCacheDuration.Nanoseconds()-t.MinCacheDuration.Nanoseconds()) / 1000
			time.Sleep(time.Duration(rand.Intn(i))*time.Millisecond + t.MinCacheDuration)
			periodicFlushCheck <- 1
		}
	}()

	log.Printf("  - worker(%d) started.", id)
	t.startWg.Done()

	for {
		ds, flushEverything = nil, false

		select {
		case <-periodicFlushCheck:
			// Nothing to do here
		case dp, ok := <-t.workerChs[id]:
			if ok {
				ds = dp.DS // at this point dp.ds has to be already set
				if err := dp.Process(); err == nil {
					recent[ds.Id] = true
				} else {
					log.Printf("worker(%d): dp.process() error: %v", id, err)
				}
			} else {
				flushEverything = true
			}
		case r := <-t.dsCopyChs[id]:
			ds = t.dss.GetById(r.dsId)
			if ds == nil {
				log.Printf("worker(%d): WAT? cannot lookup ds id (%d) sent for copy?", id, r.dsId)
			} else {
				r.resp <- ds.MostlyCopy()
				close(r.resp)
			}
			continue
		}

		if ds == nil {
			// flushEverything or periodic
			for dsId, _ := range recent {
				ds = t.dss.GetById(dsId)
				if ds == nil {
					log.Printf("worker(%d): WAT? cannot lookup ds id (%d) to flush?", id, dsId)
					continue
				} else if flushEverything || ds.ShouldBeFlushed(t.MaxCachedPoints,
					t.MinCacheDuration, t.MaxCacheDuration) {
					t.flushDs(ds)
					delete(recent, ds.Id)
				}
			}
		} else if ds.ShouldBeFlushed(t.MaxCachedPoints, t.MinCacheDuration, t.MaxCacheDuration) {
			// flush just this one ds
			t.flushDs(ds)
			delete(recent, ds.Id)
		}

		if flushEverything {
			break
		}
	}
}

func (t *Transceiver) flushDs(ds *rrd.DataSource) {
	t.flusherChs[ds.Id%int64(t.NWorkers)] <- ds.MostlyCopy()
	ds.LastFlushRT = time.Now()
	ds.ClearRRAs()
}

func (t *Transceiver) startWorkers() {

	t.workerChs = make([]chan *rrd.DataPoint, t.NWorkers)
	t.dsCopyChs = make([]chan *dsCopyRequest, t.NWorkers)

	log.Printf("Starting %d workers...", t.NWorkers)
	t.startWg.Add(t.NWorkers)
	for i := 0; i < t.NWorkers; i++ {
		t.workerChs[i] = make(chan *rrd.DataPoint, 1024)
		t.dsCopyChs[i] = make(chan *dsCopyRequest, 1024)

		go t.worker(int64(i))
	}

}

func (t *Transceiver) flusher(id int64) {
	t.flusherWg.Add(1)
	defer t.flusherWg.Done()

	log.Printf("  - flusher(%d) started.", id)
	t.startWg.Done()

	for {
		ds, ok := <-t.flusherChs[id]
		if ok {
			if err := t.serde.FlushDataSource(ds); err != nil {
				log.Printf("flusher(%d): error flushing data source %v: %v", id, ds, err)
			}
		} else {
			log.Printf("flusher(%d): channel closed, exiting", id)
			break
		}
	}

}

func (t *Transceiver) startFlushers() {

	t.flusherChs = make([]chan *rrd.DataSource, t.NWorkers)

	log.Printf("Starting %d flushers...", t.NWorkers)
	t.startWg.Add(t.NWorkers)
	for i := 0; i < t.NWorkers; i++ {
		t.flusherChs[i] = make(chan *rrd.DataSource)
		go t.flusher(int64(i))
	}
}

func (t *Transceiver) startStatWorker() {
	log.Printf("Starting statWorker...")
	t.startWg.Add(1)
	go t.statWorker()
}

func (t *Transceiver) statWorker() {

	t.statWg.Add(1)
	defer t.statWg.Done()

	var flushCh = make(chan int, 1)
	go func() {
		for {
			// NB: We do not use a time.Ticker here because my simple
			// experiments show that it will not stay aligned on a
			// multiple of durationif the system clock is
			// adjusted. This thing will mostly remain aligned.
			clock := time.Now()
			time.Sleep(clock.Truncate(t.StatFlushDuration).Add(t.StatFlushDuration).Sub(clock))
			if len(flushCh) == 0 {
				flushCh <- 1
			} else {
				log.Printf("statWorker(): dropping stat flush timer on the floor - busy system?")
			}
		}
	}()

	log.Printf("  - statWorker() started.")
	t.startWg.Done()

	counts := make(map[string]int64)
	gauges := make(map[string]float64)
	timers := make(map[string][]float64)

	prefix := t.StatsNamePrefix

	var flushStats = func() {
		for name, count := range counts {
			perSec := float64(count) / t.StatFlushDuration.Seconds()
			t.QueueDataPoint(&rrd.DataPoint{Name: prefix + "." + name, TimeStamp: time.Now(), Value: perSec})
		}
		for name, gauge := range gauges {
			t.QueueDataPoint(&rrd.DataPoint{Name: prefix + ".gauges." + name, TimeStamp: time.Now(), Value: gauge})
		}
		for name, times := range timers {
			// count
			t.QueueDataPoint(&rrd.DataPoint{Name: prefix + ".timers." + name + ".count", TimeStamp: time.Now(), Value: float64(len(times))})

			// lower, upper, sum, mean
			if len(times) > 0 {
				var (
					lower, upper = times[0], times[0]
					sum          float64
				)

				for _, v := range times[1:] {
					lower = math.Min(lower, v)
					upper = math.Max(upper, v)
					sum += v
				}
				t.QueueDataPoint(&rrd.DataPoint{Name: prefix + ".timers." + name + ".lower", TimeStamp: time.Now(), Value: lower})
				t.QueueDataPoint(&rrd.DataPoint{Name: prefix + ".timers." + name + ".upper", TimeStamp: time.Now(), Value: upper})
				t.QueueDataPoint(&rrd.DataPoint{Name: prefix + ".timers." + name + ".sum", TimeStamp: time.Now(), Value: sum})
				t.QueueDataPoint(&rrd.DataPoint{Name: prefix + ".timers." + name + ".mean", TimeStamp: time.Now(), Value: sum / float64(len(times))})
			}

			// TODO - these will require sorting:
			// count_ps ?
			// mean_90
			// median
			// std
			// sum_90
			// upper_90

		}
		// clear the maps
		counts = make(map[string]int64)
		gauges = make(map[string]float64)
		timers = make(map[string][]float64)
	}

	for {
		// It's important to flush stats at as precise time as
		// possible. This non-blocking select will guarantee that we
		// process flushCh even if there is stuff in the stCh.
		select {
		case <-flushCh:
			flushStats()
		default:
		}

		select {
		case <-flushCh:
			flushStats()
		case st, ok := <-t.stCh:
			if !ok {
				flushStats() // Final flush
				return
			}
			if st.Metric == "c" {
				if _, ok := counts[st.Name]; !ok {
					counts[st.Name] = 0
				}
				counts[st.Name] += int64(st.Value)
			} else if st.Metric == "g" {
				gauges[st.Name] = st.Value
			} else if st.Metric == "ms" {
				if _, ok := timers[st.Name]; !ok {
					timers[st.Name] = make([]float64, 4)
				}
				timers[st.Name] = append(timers[st.Name], st.Value)
			} else {
				log.Printf("statWorker(): invalid metric type: %q, ignoring.", st.Metric)
			}
		}
	}
}

func (t *Transceiver) FsFind(pattern string) []*rrd.FsFindNode {
	return t.dss.FsFind(pattern)
}
