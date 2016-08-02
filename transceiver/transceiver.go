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
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/statsd"
	"log"
	"math/rand"
	"sync"
	"time"
)

type MatchingDSSpecFinder interface {
	FindMatchingDSSpec(name string) *rrd.DSSpec
}

type Transceiver struct {
	cluster                            *cluster.Cluster
	serde                              rrd.SerDe
	NWorkers                           int
	MaxCacheDuration, MinCacheDuration time.Duration
	MaxCachedPoints                    int
	StatFlushDuration                  time.Duration
	StatsNamePrefix                    string
	DSSpecs                            MatchingDSSpecFinder
	dss                                *rrd.DataSources
	Rcache                             *ReadCache
	dpCh                               chan *rrd.DataPoint    // incoming data point
	workerChs                          []chan *rrd.DataPoint  // incoming data point with ds
	flusherChs                         []chan *dsFlushRequest // ds to flush
	stCh                               chan *statsd.Stat      // incoming statd stats
	workerWg                           sync.WaitGroup
	flusherWg                          sync.WaitGroup
	statWg                             sync.WaitGroup
	dispatcherWg                       sync.WaitGroup
	startWg                            sync.WaitGroup
}

type dsFlushRequest struct {
	ds   *rrd.DataSource
	resp chan bool
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

func New(clstr *cluster.Cluster, serde rrd.SerDe) *Transceiver {
	return &Transceiver{
		cluster:           clstr,
		serde:             serde,
		NWorkers:          4,
		MaxCacheDuration:  5 * time.Second,
		MinCacheDuration:  1 * time.Second,
		MaxCachedPoints:   256,
		StatFlushDuration: 10 * time.Second,
		StatsNamePrefix:   "stats",
		DSSpecs:           &dftDSFinder{},
		dss:               &rrd.DataSources{},
		Rcache:            &ReadCache{serde: serde, dsns: &rrd.DataSourceNames{}},
		dpCh:              make(chan *rrd.DataPoint, 65536), // so we can survive a graceful restart
		stCh:              make(chan *statsd.Stat, 65536),   // ditto
	}
}

func (t *Transceiver) Start() error {

	log.Printf("Transceiver: Loading data from serde.")
	if err := t.dss.Reload(t.serde); err != nil {
		log.Printf("transceiver.Start(): dss.Reload() error: %v", err)
		return err
	}

	// ZZZ
	if err := t.Rcache.Reload(); err != nil {
		log.Printf("transceiver.Start(): dss.Reload() error: %v", err)
		return err
	}

	// Let the cluster know about our data
	t.cluster.LoadDistData(func() ([]cluster.DistDatum, error) {
		dss := t.dss.List()
		result := make([]cluster.DistDatum, len(dss))
		for n, ds := range t.dss.List() {
			result[n] = &distDatumDataSource{t, ds}
		}
		return result, nil
	})

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

	log.Printf("Leaving cluster.")
	t.cluster.Leave(1 * time.Second)
	t.cluster.Shutdown()
}

func (t *Transceiver) ClusterReady(ready bool) {
	t.cluster.Ready(ready)
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

func (t *Transceiver) createOrLoadDS(dp *rrd.DataPoint) error {
	if dsSpec := t.DSSpecs.FindMatchingDSSpec(dp.Name); dsSpec != nil {
		if ds, err := t.serde.CreateOrReturnDataSource(dp.Name, dsSpec); err == nil {
			t.dss.Insert(ds)
			// tell the cluster about it (TODO should Insert() do this?)
			t.cluster.LoadDistData(func() ([]cluster.DistDatum, error) {
				return []cluster.DistDatum{&distDatumDataSource{t, ds}}, nil
			})
			dp.DS = ds
		} else {
			return err
		}
	}
	return nil
}

func (t *Transceiver) dispatcher() {
	t.dispatcherWg.Add(1)
	defer t.dispatcherWg.Done()

	// Monitor Cluster changes
	clusterChgCh := t.cluster.NotifyClusterChanges()

	// Channel for event forwards to other nodes and us
	snd, rcv := t.cluster.RegisterMsgType()
	go func() {
		defer func() { recover() }() // if we're writing to a closed channel below

		for {
			m := <-rcv

			// To get an event back:
			var dp rrd.DataPoint
			if err := m.Decode(&dp); err != nil {
				log.Printf("dispatcher(): msg <- rcv data point decoding FAILED, ignoring this data point.")
				continue
			}

			var maxHops = t.cluster.NumMembers() * 2 // This is kind of arbitrary
			if dp.Hops > maxHops {
				log.Printf("dispatcher(): dropping data point, max hops (%d) reached", maxHops)
				continue
			}

			t.dpCh <- &dp // See recover above
		}
	}()

	log.Printf("dispatcher(): marking cluster node as Ready.")
	t.cluster.Ready(true)

	for {

		var dp *rrd.DataPoint
		var ok bool
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := t.cluster.Transition(45 * time.Second); err != nil {
					log.Printf("dispatcher(): Transition error: %v", err)
				}
			}
			continue
		case dp, ok = <-t.dpCh:
		}

		if !ok {
			log.Printf("dispatcher(): channel closed, shutting down")
			t.stopStatWorker()
			t.stopWorkers()
			t.stopFlushers()
			break
		}

		if dp.DS = t.dss.GetByName(dp.Name); dp.DS == nil {
			if err := t.createOrLoadDS(dp); err != nil {
				log.Printf("dispatcher(): createDataSource() error: %v", err)
				continue
			}
		}

		for _, node := range t.cluster.NodesForDistDatum(&distDatumDataSource{t, dp.DS}) {
			if node.Name() == t.cluster.LocalNode().Name() {
				t.workerChs[dp.DS.Id%int64(t.NWorkers)] <- dp // This dp is for us
			} else if dp.Hops == 0 { // we do not forward more than once
				if node.Ready() {
					dp.Hops++
					if msg, err := cluster.NewMsgGob(node, dp); err == nil {
						snd <- msg
						t.QueueStatCount("tgres.dispatcher_forward", 1)
					}
				} else {
					// This should be a very rare thing
					log.Printf("dispatcher(): Returning the data point to dispatcher!")
					time.Sleep(100 * time.Millisecond)
					t.dpCh <- dp
				}
			}
		}
	}
}

func (t *Transceiver) QueueDataPoint(name string, ts time.Time, v float64) {
	t.dpCh <- &rrd.DataPoint{Name: name, TimeStamp: ts, Value: v}
}

func (t *Transceiver) QueueStat(st *statsd.Stat) {
	t.stCh <- st
}

func (t *Transceiver) QueueStatCount(name string, n int) {
	if t != nil {
		t.QueueStat(&statsd.Stat{Name: name, Value: float64(n), Metric: "c"})
	}
}

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
					log.Printf("worker(%d): dp.process(%s) error: %v", id, dp.DS.Name, err)
				}
			} else {
				flushEverything = true
			}
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
					t.flushDs(ds, false)
					delete(recent, ds.Id)
				}
			}
		} else if ds.ShouldBeFlushed(t.MaxCachedPoints, t.MinCacheDuration, t.MaxCacheDuration) {
			// flush just this one ds
			t.flushDs(ds, false)
			delete(recent, ds.Id)
		}

		if flushEverything {
			break
		}
	}
}

func (t *Transceiver) flushDs(ds *rrd.DataSource, block bool) {
	if ds.LastUpdate == time.Unix(0, 0) {
		// Do not flush empty DSs
		return
	}
	fr := &dsFlushRequest{ds: ds.MostlyCopy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	t.flusherChs[ds.Id%int64(t.NWorkers)] <- fr
	if block {
		<-fr.resp
	}
	ds.LastFlushRT = time.Now()
	ds.ClearRRAs(block) // block = clearLU in this case (see rrd.go)
}

func (t *Transceiver) startWorkers() {

	t.workerChs = make([]chan *rrd.DataPoint, t.NWorkers)

	log.Printf("Starting %d workers...", t.NWorkers)
	t.startWg.Add(t.NWorkers)
	for i := 0; i < t.NWorkers; i++ {
		t.workerChs[i] = make(chan *rrd.DataPoint, 1024)

		go t.worker(int64(i))
	}

}

func (t *Transceiver) flusher(id int64) {
	t.flusherWg.Add(1)
	defer t.flusherWg.Done()

	log.Printf("  - flusher(%d) started.", id)
	t.startWg.Done()

	for {
		fr, ok := <-t.flusherChs[id]
		if ok {
			if err := t.serde.FlushDataSource(fr.ds); err != nil {
				log.Printf("flusher(%d): error flushing data source %v: %v", id, fr.ds, err)
				if fr.resp != nil {
					fr.resp <- false
				}
			} else if fr.resp != nil {
				fr.resp <- true
			}
		} else {
			log.Printf("flusher(%d): channel closed, exiting", id)
			break
		}
	}

}

func (t *Transceiver) startFlushers() {

	t.flusherChs = make([]chan *dsFlushRequest, t.NWorkers)

	log.Printf("Starting %d flushers...", t.NWorkers)
	t.startWg.Add(t.NWorkers)
	for i := 0; i < t.NWorkers; i++ {
		t.flusherChs[i] = make(chan *dsFlushRequest)
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

	// Channel for event forwards to other nodes and us
	snd, rcv := t.cluster.RegisterMsgType()
	go func() {
		defer func() { recover() }() // if we're writing to a closed channel below

		for {
			m := <-rcv

			// To get an event back:
			var st statsd.Stat
			if err := m.Decode(&st); err != nil {
				log.Printf("statWorker(): msg <- rcv statsd.Stat decoding FAILED, ignoring this stat.")
				continue
			}

			var maxHops = t.cluster.NumMembers() * 2 // This is kind of arbitrary
			if st.Hops > maxHops {
				log.Printf("statWorker(): dropping stat, max hops (%d) reached", maxHops)
				continue
			}

			t.stCh <- &st // See recover above
		}
	}()

	var flushCh = make(chan int, 1)
	go func() {
		for {
			// NB: We do not use a time.Ticker here because my simple
			// experiments show that it will not stay aligned on a
			// multiple of duration if the system clock is
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

	log.Printf("statWorker(): started.")
	t.startWg.Done()

	agg := statsd.NewAggregator(t, t.StatsNamePrefix)
	aggDd := &distDatumAggregator{agg}
	t.cluster.LoadDistData(func() ([]cluster.DistDatum, error) {
		log.Printf("statWorker(): adding the statsd.Aggregator DistDatum to the cluster")
		return []cluster.DistDatum{aggDd}, nil
	})

	for {
		// It's nice to flush stats at as precise time as
		// possible. This non-blocking select trick guarantees that we
		// always process flushCh even if there is stuff in the stCh.
		select {
		case <-flushCh:
			agg.Flush()
		default:
		}

		select {
		case <-flushCh:
			agg.Flush()
		case st, ok := <-t.stCh:
			if !ok {
				agg.Flush()
				return
			}

			// Is this stat for us?
			for _, node := range t.cluster.NodesForDistDatum(aggDd) {
				if node.Name() == t.cluster.LocalNode().Name() {
					if err := agg.Process(st); err != nil {
						log.Printf("statWorker(): a.Process() error %v", err)
					}
				} else if st.Hops == 0 { // we do not forward more than once
					if node.Ready() {
						st.Hops++
						if msg, err := cluster.NewMsgGob(node, st); err == nil {
							snd <- msg
						}
					} else {
						// Drop it
						log.Printf("statWorker(): dropping stat on the floor because no node is Ready!")
					}
				}
			}
		}
	}
}

func (t *Transceiver) FsFind(pattern string) []*rrd.FsFindNode {
	return t.Rcache.FsFind(pattern)
}

// Implement cluster.DistDatum for data sources

type distDatumDataSource struct {
	t  *Transceiver
	ds *rrd.DataSource
}

func (d *distDatumDataSource) Relinquish() error {
	d.t.flushDs(d.ds, true)
	return nil
}

func (d *distDatumDataSource) Id() int64 { return d.ds.Id }

func (d *distDatumDataSource) Type() string { return "DataSource" }

func (d *distDatumDataSource) GetName() string {
	return d.ds.Name
}

// Implement cluster.DistDatum for stats

type distDatumAggregator struct {
	a *statsd.Aggregator
}

func (d *distDatumAggregator) Id() int64       { return 1 }
func (d *distDatumAggregator) Type() string    { return "statsd.Aggregator" }
func (d *distDatumAggregator) GetName() string { return "TheStatsAggregator" }
func (d *distDatumAggregator) Relinquish() error {
	d.a.Flush()
	return nil
}
