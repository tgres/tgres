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
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/statsd"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

var debug bool

func init() {
	debug = os.Getenv("TGRES_RCVR_DEBUG") != ""
}

type MatchingDSSpecFinder interface {
	FindMatchingDSSpec(name string) *rrd.DSSpec
}

type Receiver struct {
	cluster                            *cluster.Cluster
	serde                              serde.SerDe
	NWorkers                           int
	MaxCacheDuration, MinCacheDuration time.Duration
	MaxCachedPoints                    int
	StatFlushDuration                  time.Duration
	StatsNamePrefix                    string
	DSSpecs                            MatchingDSSpecFinder
	dss                                *rrd.DataSources
	Rcache                             *dsl.ReadCache
	dpCh                               chan *rrd.IncomingDP     // incoming data point
	workerChs                          []chan *rrd.IncomingDP   // incoming data point with ds
	flusherChs                         []chan *dsFlushRequest   // ds to flush
	aggCh                              chan *aggregator.Command // aggregator commands (for statsd type stuff)
	workerWg                           sync.WaitGroup
	flusherWg                          sync.WaitGroup
	aggWg                              sync.WaitGroup
	dispatcherWg                       sync.WaitGroup
	startWg                            sync.WaitGroup
	ReportStats                        bool
	ReportStatsPrefix                  string
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

func New(clstr *cluster.Cluster, serde serde.SerDe) *Receiver {
	return &Receiver{
		cluster:           clstr,
		serde:             serde,
		NWorkers:          4,
		MaxCacheDuration:  5 * time.Second,
		MinCacheDuration:  1 * time.Second,
		MaxCachedPoints:   256,
		StatFlushDuration: 10 * time.Second,
		StatsNamePrefix:   "stats",
		DSSpecs:           &dftDSFinder{},
		dss:               rrd.NewDataSources(false),
		Rcache:            dsl.NewReadCache(serde),
		dpCh:              make(chan *rrd.IncomingDP, 65536),     // so we can survive a graceful restart
		aggCh:             make(chan *aggregator.Command, 65536), // ditto
		ReportStats:       true,
		ReportStatsPrefix: "tgres",
	}
}

func (r *Receiver) Start() error {
	log.Printf("Receiver: starting...")

	r.startWorkers()
	r.startFlushers()
	r.startAggWorker()

	// Wait for workers/flushers to start correctly
	r.startWg.Wait()
	log.Printf("Receiver: All workers running, starting dispatcher.")

	go r.dispatcher()
	log.Printf("Receiver: Ready.")

	return nil
}

func (r *Receiver) Stop() {

	log.Printf("Closing dispatcher channel...")
	close(r.dpCh)
	r.dispatcherWg.Wait()
	log.Printf("Dispatcher finished.")

	log.Printf("Leaving cluster.")
	r.cluster.Leave(1 * time.Second)
	r.cluster.Shutdown()
}

func (r *Receiver) ClusterReady(ready bool) {
	r.cluster.Ready(ready)
}

func (r *Receiver) stopWorkers() {
	log.Printf("stopWorkers(): waiting for worker channels to empty...")
	empty := false
	for !empty {
		empty = true
		for _, c := range r.workerChs {
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
	for _, ch := range r.workerChs {
		close(ch)
	}
	log.Printf("stopWorkers(): waiting for workers to finish...")
	r.workerWg.Wait()
	log.Printf("stopWorkers(): all workers finished.")
}

func (r *Receiver) stopFlushers() {
	log.Printf("stopFlushers(): closing all flusher channels...")
	for _, ch := range r.flusherChs {
		close(ch)
	}
	log.Printf("stopFlushers(): waiting for flushers to finish...")
	r.flusherWg.Wait()
	log.Printf("stopFlushers(): all flushers finished.")
}

func (r *Receiver) stopAggWorker() {

	log.Printf("stopAggWorker(): waiting for stat channel to empty...")
	for len(r.aggCh) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("stopAggWorker(): closing stat channel...")
	close(r.aggCh)
	log.Printf("stopAggWorker(): waiting for stat worker to finish...")
	r.aggWg.Wait()
	log.Printf("stopAggWorker(): stat worker finished.")
}

func (r *Receiver) createOrLoadDS(dp *rrd.IncomingDP) error {
	if dsSpec := r.DSSpecs.FindMatchingDSSpec(dp.Name); dsSpec != nil {
		if ds, err := r.serde.CreateOrReturnDataSource(dp.Name, dsSpec); err == nil {
			r.dss.Insert(ds)
			// tell the cluster about it (TODO should Insert() do this?)
			r.cluster.LoadDistData(func() ([]cluster.DistDatum, error) {
				return []cluster.DistDatum{&distDatumDataSource{r, ds}}, nil
			})
			dp.DS = ds
		} else {
			return err
		}
	}
	return nil
}

func (r *Receiver) dispatcher() {
	r.dispatcherWg.Add(1)
	defer r.dispatcherWg.Done()

	// Monitor Cluster changes
	clusterChgCh := r.cluster.NotifyClusterChanges()

	// Channel for event forwards to other nodes and us
	snd, rcv := r.cluster.RegisterMsgType()
	go func() {
		defer func() { recover() }() // if we're writing to a closed channel below

		for {
			m := <-rcv

			// To get an event back:
			var dp rrd.IncomingDP
			if err := m.Decode(&dp); err != nil {
				log.Printf("dispatcher(): msg <- rcv data point decoding FAILED, ignoring this data point.")
				continue
			}

			var maxHops = r.cluster.NumMembers() * 2 // This is kind of arbitrary
			if dp.Hops > maxHops {
				log.Printf("dispatcher(): dropping data point, max hops (%d) reached", maxHops)
				continue
			}

			r.dpCh <- &dp // See recover above
		}
	}()

	log.Printf("dispatcher(): marking cluster node as Ready.")
	r.cluster.Ready(true)

	for {

		var dp *rrd.IncomingDP
		var ok bool
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := r.cluster.Transition(45 * time.Second); err != nil {
					log.Printf("dispatcher(): Transition error: %v", err)
				}
			}
			continue
		case dp, ok = <-r.dpCh:
		}

		if !ok {
			log.Printf("dispatcher(): channel closed, shutting down")
			r.stopAggWorker()
			r.stopWorkers()
			r.stopFlushers()
			break
		}

		if dp.DS = r.dss.GetByName(dp.Name); dp.DS == nil {
			if err := r.createOrLoadDS(dp); err != nil {
				log.Printf("dispatcher(): createDataSource() error: %v", err)
				continue
			}
		}

		for _, node := range r.cluster.NodesForDistDatum(&distDatumDataSource{r, dp.DS}) {
			if node.Name() == r.cluster.LocalNode().Name() {
				r.workerChs[dp.DS.Id%int64(r.NWorkers)] <- dp // This dp is for us
			} else if dp.Hops == 0 { // we do not forward more than once
				if node.Ready() {
					dp.Hops++
					if msg, err := cluster.NewMsg(node, dp); err == nil {
						snd <- msg
						r.reportStatCount("receiver.datapoints_forwarded", 1)
					}
				} else {
					// This should be a very rare thing
					log.Printf("dispatcher(): Returning the data point to dispatcher!")
					time.Sleep(100 * time.Millisecond)
					r.dpCh <- dp
				}
			}
		}
	}
}

func (r *Receiver) QueueDataPoint(name string, ts time.Time, v float64) {
	r.dpCh <- &rrd.IncomingDP{Name: name, TimeStamp: ts, Value: v}
}

// TODO we could have shorthands such as:
// QueueGauge()
// QueueGaugeDelta()
// QueueAppendValue()
// ... but for now QueueAggregatorCommand seems sufficient
func (r *Receiver) QueueAggregatorCommand(agg *aggregator.Command) {
	r.aggCh <- agg
}

func (r *Receiver) reportStatCount(name string, f float64) {
	if r != nil && r.ReportStats {
		r.QueueAggregatorCommand(aggregator.NewCommand(aggregator.CmdAdd, r.ReportStatsPrefix+"."+name, f))
	}
}

func (r *Receiver) worker(id int64) {
	r.workerWg.Add(1)
	defer r.workerWg.Done()

	recent := make(map[int64]bool)

	periodicFlushCheck := make(chan int)
	go func() {
		for {
			// Sleep randomly between min and max cache durations (is this wise?)
			i := int(r.MaxCacheDuration.Nanoseconds()-r.MinCacheDuration.Nanoseconds()) / 1000
			dur := time.Duration(rand.Intn(i))*time.Millisecond + r.MinCacheDuration
			time.Sleep(dur)
			periodicFlushCheck <- 1
			if debug {
				log.Printf("worker(%d): Periodic flush after sleep: %v", id, dur)
			}
		}
	}()

	log.Printf("  - worker(%d) started.", id)
	r.startWg.Done()

	for {
		var (
			ds            *rrd.DataSource
			channelClosed bool
		)

		select {
		case <-periodicFlushCheck:
			// Nothing to do here
		case dp, ok := <-r.workerChs[id]:
			if ok {
				ds = dp.DS // at this point dp.ds has to be already set
				if err := dp.Process(); err == nil {
					recent[ds.Id] = true
				} else {
					log.Printf("worker(%d): dp.process(%s) error: %v", id, dp.DS.Name, err)
				}
			} else {
				channelClosed = true
			}
		}

		if ds == nil {
			// periodic flush - check recent
			for dsId, _ := range recent {
				ds = r.dss.GetById(dsId)
				if ds == nil {
					log.Printf("worker(%d): WAT? cannot lookup ds id (%d) to flush?", id, dsId)
					continue
				}
				if ds.ShouldBeFlushed(r.MaxCachedPoints, r.MinCacheDuration, r.MaxCacheDuration) {
					if debug {
						log.Printf("worker(%d): Requesting (periodic) flush of ds id: %d", id, ds.Id)
					}
					r.flushDs(ds, false)
					delete(recent, ds.Id)
				}
			}
		} else if ds.ShouldBeFlushed(r.MaxCachedPoints, r.MinCacheDuration, r.MaxCacheDuration) {
			if debug {
				log.Printf("worker(%d): Requesting flush of ds id: %d", id, ds.Id)
			}
			// flush just this one ds
			r.flushDs(ds, false)
			delete(recent, ds.Id)
		}

		if channelClosed {
			break
		}
	}
}

func (r *Receiver) flushDs(ds *rrd.DataSource, block bool) {
	fr := &dsFlushRequest{ds: ds.MostlyCopy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	r.flusherChs[ds.Id%int64(r.NWorkers)] <- fr
	if block {
		<-fr.resp
	}
	ds.LastFlushRT = time.Now()
	ds.ClearRRAs(block) // block = clearLU in this case (see rrd.go)
}

func (r *Receiver) startWorkers() {

	r.workerChs = make([]chan *rrd.IncomingDP, r.NWorkers)

	log.Printf("Starting %d workers...", r.NWorkers)
	r.startWg.Add(r.NWorkers)
	for i := 0; i < r.NWorkers; i++ {
		r.workerChs[i] = make(chan *rrd.IncomingDP, 1024)

		go r.worker(int64(i))
	}

}

func (r *Receiver) flusher(id int64) {
	r.flusherWg.Add(1)
	defer r.flusherWg.Done()

	log.Printf("  - flusher(%d) started.", id)
	r.startWg.Done()

	for {
		fr, ok := <-r.flusherChs[id]
		if ok {
			if err := r.serde.FlushDataSource(fr.ds); err != nil {
				log.Printf("flusher(%d): error flushing data source %v: %v", id, fr.ds, err)
				if fr.resp != nil {
					fr.resp <- false
				}
			} else if fr.resp != nil {
				fr.resp <- true
			}
			r.reportStatCount("serde.datapoints_flushed", float64(fr.ds.PointCount()))
		} else {
			log.Printf("flusher(%d): channel closed, exiting", id)
			break
		}
	}

}

func (r *Receiver) startFlushers() {

	r.flusherChs = make([]chan *dsFlushRequest, r.NWorkers)

	log.Printf("Starting %d flushers...", r.NWorkers)
	r.startWg.Add(r.NWorkers)
	for i := 0; i < r.NWorkers; i++ {
		r.flusherChs[i] = make(chan *dsFlushRequest)
		go r.flusher(int64(i))
	}
}

func (r *Receiver) startAggWorker() {
	log.Printf("Starting aggWorker...")
	r.startWg.Add(1)
	go r.aggWorker()
}

func (r *Receiver) aggWorker() {

	r.aggWg.Add(1)
	defer r.aggWg.Done()

	// Channel for event forwards to other nodes and us
	snd, rcv := r.cluster.RegisterMsgType()
	go func() {
		defer func() { recover() }() // if we're writing to a closed channel below

		for {
			m := <-rcv

			// To get an event back:
			var ac aggregator.Command
			if err := m.Decode(&ac); err != nil {
				log.Printf("aggWorker(): msg <- rcv aggreagator.Command decoding FAILED, ignoring this command.")
				continue
			}

			var maxHops = r.cluster.NumMembers() * 2 // This is kind of arbitrary
			if ac.Hops > maxHops {
				log.Printf("aggWorker(): dropping command, max hops (%d) reached", maxHops)
				continue
			}

			r.aggCh <- &ac // See recover above
		}
	}()

	var flushCh = make(chan time.Time, 1)
	go func() {
		for {
			// NB: We do not use a time.Ticker here because my simple
			// experiments show that it will not stay aligned on a
			// multiple of duration if the system clock is
			// adjusted. This thing will mostly remain aligned.
			clock := time.Now()
			time.Sleep(clock.Truncate(r.StatFlushDuration).Add(r.StatFlushDuration).Sub(clock))
			if len(flushCh) == 0 {
				flushCh <- time.Now()
			} else {
				log.Printf("aggWorker(): dropping aggreagator flush timer on the floor - busy system?")
			}
		}
	}()

	log.Printf("aggWorker(): started.")
	r.startWg.Done()

	statsd.Prefix = r.StatsNamePrefix

	agg := aggregator.NewAggregator(r)
	aggDd := &distDatumAggregator{agg}
	r.cluster.LoadDistData(func() ([]cluster.DistDatum, error) {
		log.Printf("aggWorker(): adding the aggregator.Aggregator DistDatum to the cluster")
		return []cluster.DistDatum{aggDd}, nil
	})

	for {
		// It's nice to flush stats at as precise time as
		// possible. This non-blocking select trick guarantees that we
		// always process flushCh even if there is stuff in the stCh.
		select {
		case now := <-flushCh:
			agg.Flush(now)
		default:
		}

		select {
		case now := <-flushCh:
			agg.Flush(now)
		case ac, ok := <-r.aggCh:
			if !ok {
				log.Printf("aggWorker(): channel closed, performing last flush")
				agg.Flush(time.Now())
				return
			}

			for _, node := range r.cluster.NodesForDistDatum(aggDd) {
				if node.Name() == r.cluster.LocalNode().Name() {
					agg.ProcessCmd(ac)
				} else if ac.Hops == 0 { // we do not forward more than once
					if node.Ready() {
						ac.Hops++
						if msg, err := cluster.NewMsg(node, ac); err == nil {
							snd <- msg
							r.reportStatCount("receiver.aggregationss_forwarded", 1)
						}
					} else {
						// Drop it
						log.Printf("aggWorker(): dropping command on the floor because no node is Ready!")
					}
				}
			}
		}
	}
}

// Implement cluster.DistDatum for data sources

type distDatumDataSource struct {
	r  *Receiver
	ds *rrd.DataSource
}

func (d *distDatumDataSource) Relinquish() error {
	if d.ds.LastUpdate != time.Unix(0, 0) {
		d.r.flushDs(d.ds, true)
		d.r.dss.Delete(d.ds)
	}
	return nil
}

func (d *distDatumDataSource) Acquire() error {
	d.r.dss.Delete(d.ds) // it will get loaded afresh when needed
	return nil
}

func (d *distDatumDataSource) Id() int64 { return d.ds.Id }

func (d *distDatumDataSource) Type() string { return "DataSource" }

func (d *distDatumDataSource) GetName() string {
	return d.ds.Name
}

// Implement cluster.DistDatum for stats

type distDatumAggregator struct {
	a *aggregator.Aggregator
}

func (d *distDatumAggregator) Id() int64       { return 1 }
func (d *distDatumAggregator) Type() string    { return "aggregator.Aggregator" }
func (d *distDatumAggregator) GetName() string { return "TheAggregator" }
func (d *distDatumAggregator) Relinquish() error {
	d.a.Flush(time.Now())
	return nil
}
func (d *distDatumAggregator) Acquire() error { return nil }
