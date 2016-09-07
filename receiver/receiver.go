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

// Package receiver manages the receiving end of the data. All of the
// queueing, caching, perioding flushing and cluster forwarding logic
// is here.
package receiver

import (
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/statsd"
	"log"
	"math"
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
	FindMatchingDSSpec(name string) *serde.DSSpec
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
	dss                                *dataSources
	Rcache                             *dsl.ReadCache
	dpCh                               chan *IncomingDP         // incoming data point
	workerChs                          []chan *incomingDpWithDs // incoming data point with ds
	flusherChs                         []chan *dsFlushRequest   // ds to flush
	aggCh                              chan *aggregator.Command // aggregator commands (for statsd type stuff)
	workerWg                           sync.WaitGroup
	flusherWg                          sync.WaitGroup
	aggWg                              sync.WaitGroup
	dispatcherWg                       sync.WaitGroup
	startWg                            sync.WaitGroup
	pacedMetricWg                      sync.WaitGroup
	ReportStats                        bool
	ReportStatsPrefix                  string
	pacedMetricCh                      chan *pacedMetric
}

// IncomingDP is incoming data, i.e. this is the form in which input
// data is expected. This is not an internal representation of a data
// point, it's the format in which they are expected to arrive and is
// easy to convert to from most ant data point representation out
// there. This data point representation has no notion of duration and
// therefore must rely on some kind of an externally stored "last
// update" time.
type IncomingDP struct {
	Name      string
	TimeStamp time.Time
	Value     float64
	Hops      int
}

type incomingDpWithDs struct {
	dp  *IncomingDP
	rds *receiverDs
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

func (_ *dftDSFinder) FindMatchingDSSpec(name string) *serde.DSSpec {
	return &serde.DSSpec{
		Step:      10 * time.Second,
		Heartbeat: 2 * time.Hour,
		RRAs: []*serde.RRASpec{
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 10 * time.Second,
				Size: 6 * time.Hour,
			},
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 1 * time.Minute,
				Size: 24 * time.Hour,
			},
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 10 * time.Minute,
				Size: 93 * 24 * time.Hour,
			},
			&serde.RRASpec{Function: rrd.WMEAN,
				Step: 24 * time.Hour,
				Size: 1825 * 24 * time.Hour,
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
		dss:               newDataSources(false),
		Rcache:            dsl.NewReadCache(serde),
		dpCh:              make(chan *IncomingDP, 65536),         // so we can survive a graceful restart
		aggCh:             make(chan *aggregator.Command, 65536), // ditto
		ReportStats:       true,
		ReportStatsPrefix: "tgres",
		pacedMetricCh:     make(chan *pacedMetric, 256),
	}
}

func (r *Receiver) Start() error {
	log.Printf("Receiver: starting...")

	r.startWorkers()
	r.startFlushers()
	r.startAggWorker()
	r.startPacedMetricWorker()

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

	log.Printf("stopAggWorker(): waiting for agg channel to empty...")
	for len(r.aggCh) > 0 {
		time.Sleep(100 * time.Millisecond)
	}

	log.Printf("stopAggWorker(): closing agg channel...")
	close(r.aggCh)
	log.Printf("stopAggWorker(): waiting for agg worker to finish...")
	r.aggWg.Wait()
	log.Printf("stopAggWorker(): agg worker finished.")
}

func (r *Receiver) stopPacedMetricWorker() {
	log.Printf("stopPacedMetricWorker(): closing paced metric channel...")
	close(r.pacedMetricCh)
	log.Printf("stopPacedMetricWorker(): waiting for paced metric worker to finish...")
	r.pacedMetricWg.Wait()
	log.Printf("stopPacedMetricWorker(): paced metric worker finished.")
}

func (r *Receiver) createOrLoadDS(name string) (*receiverDs, error) {
	if dsSpec := r.DSSpecs.FindMatchingDSSpec(name); dsSpec != nil {
		ds, err := r.serde.CreateOrReturnDataSource(name, dsSpec)
		var rds *receiverDs
		if err == nil {
			rds = r.dss.Insert(ds)
			// tell the cluster about it (TODO should Insert() do this?)
			r.cluster.LoadDistData(func() ([]cluster.DistDatum, error) {
				return []cluster.DistDatum{&distDatumDataSource{r, rds}}, nil
			})
		}
		return rds, err
	}
	return nil, nil // We couldn't find anything, which is not an error
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
			var dp IncomingDP
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

		var dp *IncomingDP
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
			r.stopPacedMetricWorker()
			r.stopAggWorker()
			r.stopWorkers()
			r.stopFlushers()
			break
		}

		r.reportStatCount("receiver.dispatcher.datapoints.total", 1)

		if math.IsNaN(dp.Value) {
			// NaN is meaningless, e.g. "the thermometer is
			// registering a NaN". Or it means that "for certain it is
			// offline", but that is not part of our scope. You can
			// only get a NaN by exceeding HB. Silently ignore it.
			continue
		}

		var rds *receiverDs = r.dss.GetByName(dp.Name)
		if rds == nil {
			var err error
			if rds, err = r.createOrLoadDS(dp.Name); err != nil {
				log.Printf("dispatcher(): createOrLoadDS() error: %v", err)
				continue
			}
			if rds == nil {
				log.Printf("dispatcher(): No spec matched name: %q, ignoring data point", dp.Name)
				continue
			}
		}

		for _, node := range r.cluster.NodesForDistDatum(&distDatumDataSource{r, rds}) {
			if node.Name() == r.cluster.LocalNode().Name() {
				r.workerChs[rds.ds.Id()%int64(r.NWorkers)] <- &incomingDpWithDs{dp, rds} // This dp is for us
			} else {
				if dp.Hops == 0 { // we do not forward more than once
					if node.Ready() {
						dp.Hops++
						if msg, err := cluster.NewMsg(node, dp); err == nil {
							snd <- msg
							r.reportStatCount("receiver.dispatcher.datapoints.forwarded", 1)
						} else {
							log.Printf("dispatcher(): Encoding error forwarding a data point: %v", err)
						}
					} else {
						// This should be a very rare thing
						log.Printf("dispatcher(): Returning the data point to dispatcher!")
						time.Sleep(100 * time.Millisecond)
						r.dpCh <- dp
					}
				}
				// Always clear RRAs to prevent it from being saved
				if pc := rds.ds.PointCount(); pc > 0 {
					log.Printf("dispatcher(): WARNING: Clearing DS with PointCount > 0: %v", pc)
				}
				rds.ds.ClearRRAs()
			}
		}
	}
}

func (r *Receiver) QueueDataPoint(name string, ts time.Time, v float64) {
	r.dpCh <- &IncomingDP{Name: name, TimeStamp: ts, Value: v}
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
		r.QueueSum(r.ReportStatsPrefix+"."+name, f)
	}
}

func (r *Receiver) QueueSum(name string, v float64) {
	r.pacedMetricCh <- &pacedMetric{pacedSum, name, v}
}

func (r *Receiver) QueueGauge(name string, v float64) {
	r.pacedMetricCh <- &pacedMetric{pacedGauge, name, v}
}

func (r *Receiver) worker(id int64) {
	r.workerWg.Add(1)
	defer r.workerWg.Done()

	recent := make(map[int64]bool)

	periodicFlushCheck := make(chan int)
	go func() {
		for {
			// Sleep randomly between min and max cache durations (is this wise?)
			i := int(r.MaxCacheDuration.Nanoseconds()-r.MinCacheDuration.Nanoseconds()) / 1000000
			dur := time.Duration(rand.Intn(i))*time.Millisecond + r.MinCacheDuration
			time.Sleep(dur)
			periodicFlushCheck <- 1
		}
	}()

	log.Printf("  - worker(%d) started.", id)
	r.startWg.Done()

	for {
		var (
			rds           *receiverDs
			channelClosed bool
		)

		select {
		case <-periodicFlushCheck:
			// Nothing to do here
		case dpds, ok := <-r.workerChs[id]:
			if ok {
				rds = dpds.rds // at this point ds has to be already set
				if err := rds.ds.ProcessIncomingDataPoint(dpds.dp.Value, dpds.dp.TimeStamp); err == nil {
					recent[rds.ds.Id()] = true
				} else {
					log.Printf("worker(%d): dp.process(%s) error: %v", id, rds.ds.Name(), err)
				}
			} else {
				channelClosed = true
			}
		}

		if rds == nil {
			// periodic flush - check recent
			if len(recent) > 0 {
				if debug {
					log.Printf("worker(%d): Periodic flush.", id)
				}
				for dsId, _ := range recent {
					rds = r.dss.GetById(dsId)
					if rds == nil {
						log.Printf("worker(%d): Cannot lookup ds id (%d) to flush (possible if it moved to another node).", id, dsId)
						delete(recent, dsId)
						continue
					}
					if rds.shouldBeFlushed(r.MaxCachedPoints, r.MinCacheDuration, r.MaxCacheDuration) {
						if debug {
							log.Printf("worker(%d): Requesting (periodic) flush of ds id: %d", id, rds.ds.Id())
						}
						r.flushDs(rds, false)
						delete(recent, rds.ds.Id())
					}
				}
			}
		} else if rds.shouldBeFlushed(r.MaxCachedPoints, r.MinCacheDuration, r.MaxCacheDuration) {
			// flush just this one ds
			if debug {
				log.Printf("worker(%d): Requesting flush of ds id: %d", id, rds.ds.Id())
			}
			r.flushDs(rds, false)
			delete(recent, rds.ds.Id())
		}

		if channelClosed {
			break
		}
	}
}

func (r *Receiver) flushDs(rds *receiverDs, block bool) {
	fr := &dsFlushRequest{ds: rds.ds.MostlyCopy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	r.flusherChs[rds.ds.Id()%int64(r.NWorkers)] <- fr
	if block {
		<-fr.resp
	}
	rds.ds.ClearRRAs()
	rds.lastFlushRT = time.Now()
}

func (r *Receiver) startWorkers() {

	r.workerChs = make([]chan *incomingDpWithDs, r.NWorkers)

	log.Printf("Starting %d workers...", r.NWorkers)
	r.startWg.Add(r.NWorkers)
	for i := 0; i < r.NWorkers; i++ {
		r.workerChs[i] = make(chan *incomingDpWithDs, 1024)

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

type pacedMetricType int

const (
	pacedSum pacedMetricType = iota
	pacedGauge
)

type pacedMetric struct {
	kind  pacedMetricType
	name  string
	value float64
}

func (r *Receiver) startPacedMetricWorker() {
	log.Printf("Starting pacedMetricWorker...")
	r.startWg.Add(1)
	go r.pacedMetricWorker(time.Second)
}

func (r *Receiver) pacedMetricWorker(frequency time.Duration) {
	r.pacedMetricWg.Add(1)
	defer r.pacedMetricWg.Done()

	sums := make(map[string]float64)
	gauges := make(map[string]*rrd.ClockPdp)

	flush := func() {
		for name, sum := range sums {
			r.QueueAggregatorCommand(aggregator.NewCommand(aggregator.CmdAdd, name, sum))
		}
		for name, gauge := range gauges {
			r.QueueDataPoint(name, gauge.End, gauge.Reset())
		}
		sums = make(map[string]float64)
		// NB: We do not reset gauges, they need to live on
	}

	var flushCh = make(chan bool, 1)
	go func() {
		for {
			time.Sleep(frequency)
			if len(flushCh) == 0 {
				flushCh <- true
			} else {
				log.Printf("pacedMetricWorker(): dropping flush timer on the floor - busy system?")
			}
		}
	}()

	log.Printf("pacedMetricWorker(): started.")
	r.startWg.Done()

	for {
		select {
		case <-flushCh:
			flush()
		case ps, ok := <-r.pacedMetricCh:
			if !ok {
				flush()
				return
			} else {
				switch ps.kind {
				case pacedSum:
					sums[ps.name] += ps.value
				case pacedGauge:
					if _, ok := gauges[ps.name]; !ok {
						gauges[ps.name] = &rrd.ClockPdp{}
					}
					gauges[ps.name].AddValue(ps.value)
				}
			}
		}
	}
}

// Implement cluster.DistDatum for data sources

type distDatumDataSource struct {
	r   *Receiver
	rds *receiverDs
}

func (d *distDatumDataSource) Relinquish() error {
	if !d.rds.ds.LastUpdate().IsZero() {
		d.r.flushDs(d.rds, true)
		d.r.dss.Delete(d.rds)
	}
	return nil
}

func (d *distDatumDataSource) Acquire() error {
	d.r.dss.Delete(d.rds) // it will get loaded afresh when needed
	return nil
}

func (d *distDatumDataSource) Id() int64       { return d.rds.ds.Id() }
func (d *distDatumDataSource) Type() string    { return "DataSource" }
func (d *distDatumDataSource) GetName() string { return d.rds.ds.Name() }

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
