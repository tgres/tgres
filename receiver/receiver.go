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
	"fmt"
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

	go dispatcher(&wrkCtl{wg: &r.dispatcherWg, startWg: &r.startWg, id: "dispatcher"}, r.dpCh, r.cluster, r, r, r, r.dss, r.workerChs, r.NWorkers, r)
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

func (r *Receiver) stopAllWorkers() {
	r.stopPacedMetricWorker()
	r.stopAggWorker()
	r.stopWorkers()
	r.stopFlushers()
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
				return []cluster.DistDatum{&distDatumDataSource{rds, r.dss, r}}, nil
			})
		}
		return rds, err
	}
	return nil, nil // We couldn't find anything, which is not an error
}

func dispatcherIncomingDPMessages(rcv chan *cluster.Msg, clstr clusterer, dpCh chan *IncomingDP) {
	defer func() { recover() }() // if we're writing to a closed channel below

	for {
		m := <-rcv

		// To get an event back:
		var dp IncomingDP
		if err := m.Decode(&dp); err != nil {
			log.Printf("dispatcher(): msg <- rcv data point decoding FAILED, ignoring this data point.")
			continue
		}

		var maxHops = clstr.NumMembers() * 2 // This is kind of arbitrary
		if dp.Hops > maxHops {
			log.Printf("dispatcher(): dropping data point, max hops (%d) reached", maxHops)
			continue
		}

		dpCh <- &dp // See recover above
	}
}

func dispatcherProcessOrForward(rds *receiverDs, clstr clusterer, dss *dataSources, workerChs []chan *incomingDpWithDs, dsf dsFlusherBlocking,
	dp *IncomingDP, NWorkers int, scr statCountReporter, dpCh chan *IncomingDP, snd chan *cluster.Msg) {
	for _, node := range clstr.NodesForDistDatum(&distDatumDataSource{rds, dss, dsf}) {
		if node.Name() == clstr.LocalNode().Name() {
			workerChs[rds.ds.Id()%int64(NWorkers)] <- &incomingDpWithDs{dp, rds} // This dp is for us
		} else {
			if dp.Hops == 0 { // we do not forward more than once
				if node.Ready() {
					dp.Hops++
					if msg, err := cluster.NewMsg(node, dp); err == nil {
						snd <- msg
						scr.reportStatCount("receiver.dispatcher.datapoints.forwarded", 1)
					} else {
						log.Printf("dispatcher(): Encoding error forwarding a data point: %v", err)
					}
				} else {
					// This should be a very rare thing
					log.Printf("dispatcher(): Returning the data point to dispatcher!")
					time.Sleep(100 * time.Millisecond)
					dpCh <- dp
				}
			}
			// Always clear RRAs to prevent it from being saved
			if pc := rds.ds.PointCount(); pc > 0 {
				log.Printf("dispatcher(): WARNING: Clearing DS with PointCount > 0: %v", pc)
			}
			rds.ds.ClearRRAs(true)
		}
	}
}

func dispatcherLookupDsByName(name string, dss *dataSources, dscl dsCreateOrLoader) *receiverDs {
	var rds *receiverDs = dss.GetByName(name)
	if rds == nil {
		var err error
		if rds, err = dscl.createOrLoadDS(name); err != nil {
			log.Printf("dispatcher(): createOrLoadDS() error: %v", err)
			return nil
		}
		if rds == nil {
			log.Printf("dispatcher(): No spec matched name: %q, ignoring data point", name)
			return nil
		}
	}
	return rds
}

func dispatcher(wc wController, dpCh chan *IncomingDP, clstr clusterer, wstp workerStopper, scr statCountReporter,
	dscl dsCreateOrLoader, dss *dataSources, workerChs []chan *incomingDpWithDs, NWorkers int, dsf dsFlusherBlocking) {
	wc.onEnter()
	defer wc.onExit()

	// Monitor Cluster changes
	clusterChgCh := clstr.NotifyClusterChanges()

	// Channel for event forwards to other nodes and us
	snd, rcv := clstr.RegisterMsgType()
	go dispatcherIncomingDPMessages(rcv, clstr, dpCh)

	log.Printf("dispatcher(): marking cluster node as Ready.")
	clstr.Ready(true)

	for {

		var dp *IncomingDP
		var ok bool
		select {
		case _, ok = <-clusterChgCh:
			if ok {
				if err := clstr.Transition(45 * time.Second); err != nil {
					log.Printf("dispatcher(): Transition error: %v", err)
				}
			}
			continue
		case dp, ok = <-dpCh:
		}

		if !ok {
			log.Printf("dispatcher(): channel closed, shutting down")
			wstp.stopAllWorkers()
			break
		}

		scr.reportStatCount("receiver.dispatcher.datapoints.total", 1)

		if math.IsNaN(dp.Value) {
			// NaN is meaningless, e.g. "the thermometer is
			// registering a NaN". Or it means that "for certain it is
			// offline", but that is not part of our scope. You can
			// only get a NaN by exceeding HB. Silently ignore it.
			continue
		}

		if rds := dispatcherLookupDsByName(dp.Name, dss, dscl); rds != nil {
			dispatcherProcessOrForward(rds, clstr, dss, workerChs, dsf, dp, NWorkers, scr, dpCh, snd)
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

func workerPeriodicFlushSignal(periodicFlushCheck chan bool, minCacheDur, maxCacheDur time.Duration) {
	// Sleep randomly between min and max cache durations (is this wise?)
	i := int(maxCacheDur.Nanoseconds()-minCacheDur.Nanoseconds()) / 1000000
	dur := time.Duration(rand.Intn(i))*time.Millisecond + minCacheDur
	time.Sleep(dur)
	periodicFlushCheck <- true
}

func workerPeriodicFlush(wc wController, dsf dsFlusherBlocking, recent map[int64]bool, dss *dataSources, minCacheDur, maxCacheDur time.Duration, maxPoints int) {
	for dsId, _ := range recent {
		rds := dss.GetById(dsId)
		if rds == nil {
			log.Printf("%s: Cannot lookup ds id (%d) to flush (possible if it moved to another node).", wc.ident(), dsId)
			delete(recent, dsId)
			continue
		}
		if rds.shouldBeFlushed(maxPoints, minCacheDur, minCacheDur) {
			if debug {
				log.Printf("%s: Requesting (periodic) flush of ds id: %d", wc.ident(), rds.ds.Id())
			}
			dsf.flushDs(rds, false)
			delete(recent, rds.ds.Id())
		}
	}
}

func worker(wc wController, dsf dsFlusherBlocking, workerCh chan *incomingDpWithDs, dss *dataSources, minCacheDur, maxCacheDur time.Duration, maxPoints int) {
	wc.onEnter()
	defer wc.onExit()

	recent := make(map[int64]bool)

	periodicFlushCheck := make(chan bool)
	go workerPeriodicFlushSignal(periodicFlushCheck, minCacheDur, maxCacheDur)

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	for {
		var rds *receiverDs

		select {
		case <-periodicFlushCheck:
			workerPeriodicFlush(wc, dsf, recent, dss, minCacheDur, maxCacheDur, maxPoints)
		case dpds, ok := <-workerCh:
			if !ok {
				break
			}
			rds = dpds.rds // at this point ds has to be already set
			if err := rds.ds.ProcessIncomingDataPoint(dpds.dp.Value, dpds.dp.TimeStamp); err == nil {
				if rds.shouldBeFlushed(maxPoints, minCacheDur, maxCacheDur) {
					// flush just this one ds
					if debug {
						log.Printf("%s: Requesting flush of ds id: %d", wc.ident(), rds.ds.Id())
					}
					dsf.flushDs(rds, false)
					delete(recent, rds.ds.Id())
				} else {
					recent[rds.ds.Id()] = true
				}
			} else {
				log.Printf("%s: dp.process(%s) error: %v", wc.ident(), rds.ds.Name(), err)
			}
		}

	}
}

func (r *Receiver) flushDs(rds *receiverDs, block bool) {
	fr := &dsFlushRequest{ds: rds.ds.Copy()}
	if block {
		fr.resp = make(chan bool, 1)
	}
	r.flusherChs[rds.ds.Id()%int64(r.NWorkers)] <- fr
	if block {
		<-fr.resp
	}
	rds.ds.ClearRRAs(false)
	rds.lastFlushRT = time.Now()
}

func (r *Receiver) startWorkers() {

	r.workerChs = make([]chan *incomingDpWithDs, r.NWorkers)

	log.Printf("Starting %d workers...", r.NWorkers)
	r.startWg.Add(r.NWorkers)
	for i := 0; i < r.NWorkers; i++ {
		r.workerChs[i] = make(chan *incomingDpWithDs, 1024)
		go worker(&wrkCtl{wg: &r.flusherWg, startWg: &r.startWg, id: fmt.Sprintf("worker(%d)", i)}, r, r.workerChs[i], r.dss, r.MinCacheDuration, r.MaxCacheDuration, r.MaxCachedPoints)
	}
}

type dsFlusher interface {
	FlushDataSource(*rrd.DataSource) error
}

type dsFlusherBlocking interface { // TODO same as above?
	flushDs(*receiverDs, bool)
}

type statReporter interface {
	reportStatCount(string, float64)
}

type dataPointQueuer interface {
	QueueDataPoint(string, time.Time, float64)
}

type aggregatorCommandQueuer interface {
	QueueAggregatorCommand(*aggregator.Command)
}

type statCountReporter interface {
	reportStatCount(string, float64)
}

type dsCreateOrLoader interface {
	createOrLoadDS(string) (*receiverDs, error)
}

type wrkCtl struct {
	wg, startWg *sync.WaitGroup
	id          string
}

func (w *wrkCtl) ident() string { return w.id }
func (w *wrkCtl) onEnter()      { w.wg.Add(1) }
func (w *wrkCtl) onExit()       { w.wg.Done() }
func (w *wrkCtl) onStarted()    { w.startWg.Done() }

type wController interface {
	ident() string
	onEnter()
	onExit()
	onStarted()
}

type clusterer interface {
	RegisterMsgType() (chan *cluster.Msg, chan *cluster.Msg)
	NumMembers() int
	LoadDistData(func() ([]cluster.DistDatum, error)) error
	NodesForDistDatum(cluster.DistDatum) []*cluster.Node
	LocalNode() *cluster.Node
	NotifyClusterChanges() chan bool
	Transition(time.Duration) error
	Ready(bool) error
	//NewMsg(*cluster.Node, interface{}) (*cluster.Msg, error)
}

type workerStopper interface {
	stopAllWorkers()
}

func flusher(wc wController, df dsFlusher, sr statReporter, flusherCh chan *dsFlushRequest) {
	wc.onEnter()
	defer wc.onExit()

	log.Printf("  - %s started.", wc.ident())
	wc.onStarted()

	for {
		fr, ok := <-flusherCh
		if !ok {
			log.Printf("%s: channel closed, exiting", wc.ident())
			return
		}
		err := df.FlushDataSource(fr.ds)
		if err != nil {
			log.Printf("%s: error flushing data source %v: %v", wc.ident(), fr.ds, err)
		}
		if fr.resp != nil {
			fr.resp <- (err == nil)
		}
		sr.reportStatCount("serde.datapoints_flushed", float64(fr.ds.PointCount()))
	}
}

func (r *Receiver) startFlushers() {

	r.flusherChs = make([]chan *dsFlushRequest, r.NWorkers)

	log.Printf("Starting %d flushers...", r.NWorkers)
	r.startWg.Add(r.NWorkers)
	for i := 0; i < r.NWorkers; i++ {
		r.flusherChs[i] = make(chan *dsFlushRequest)
		go flusher(&wrkCtl{wg: &r.flusherWg, startWg: &r.startWg, id: fmt.Sprintf("flusher(%d)", i)}, r.serde, r, r.flusherChs[i])
	}
}

func (r *Receiver) startAggWorker() {
	log.Printf("Starting aggWorker...")
	r.startWg.Add(1)
	go aggWorker(&wrkCtl{wg: &r.aggWg, startWg: &r.startWg, id: "aggWorker"}, r.aggCh, r.cluster, r.StatFlushDuration, r.StatsNamePrefix, r, r)
}

func aggWorker(wc wController, aggCh chan *aggregator.Command, clstr clusterer, statFlushDuration time.Duration, statsNamePrefix string, sr statReporter, dpq *Receiver) {

	wc.onEnter()
	defer wc.onExit()

	// Channel for event forwards to other nodes and us
	snd, rcv := clstr.RegisterMsgType()
	go func() {
		defer func() { recover() }() // if we're writing to a closed channel below

		for {
			m := <-rcv

			// To get an event back:
			var ac aggregator.Command
			if err := m.Decode(&ac); err != nil {
				log.Printf("%s: msg <- rcv aggreagator.Command decoding FAILED, ignoring this command.", wc.ident())
				continue
			}

			var maxHops = clstr.NumMembers() * 2 // This is kind of arbitrary
			if ac.Hops > maxHops {
				log.Printf("%s: dropping command, max hops (%d) reached", wc.ident(), maxHops)
				continue
			}

			aggCh <- &ac // See recover above
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
			time.Sleep(clock.Truncate(statFlushDuration).Add(statFlushDuration).Sub(clock))
			if len(flushCh) == 0 {
				flushCh <- time.Now()
			} else {
				log.Printf("%s: dropping aggreagator flush timer on the floor - busy system?", wc.ident())
			}
		}
	}()

	log.Printf("%s: started.", wc.ident())
	wc.onStarted()

	statsd.Prefix = statsNamePrefix

	agg := aggregator.NewAggregator(dpq) // aggregator.dataPointQueuer
	aggDd := &distDatumAggregator{agg}
	clstr.LoadDistData(func() ([]cluster.DistDatum, error) {
		log.Printf("%s: adding the aggregator.Aggregator DistDatum to the cluster", wc.ident())
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
		case ac, ok := <-aggCh:
			if !ok {
				log.Printf("%s: channel closed, performing last flush", wc.ident())
				agg.Flush(time.Now())
				return
			}

			for _, node := range clstr.NodesForDistDatum(aggDd) {
				if node.Name() == clstr.LocalNode().Name() {
					agg.ProcessCmd(ac)
				} else if ac.Hops == 0 { // we do not forward more than once
					if node.Ready() {
						ac.Hops++
						if msg, err := cluster.NewMsg(node, ac); err == nil {
							snd <- msg
							sr.reportStatCount("receiver.aggregationss_forwarded", 1)
						}
					} else {
						// Drop it
						log.Printf("%s: dropping command on the floor because no node is Ready!", wc.ident())
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
	go pacedMetricWorker(&wrkCtl{wg: &r.pacedMetricWg, startWg: &r.startWg, id: "pacedMetricWorker"}, r.pacedMetricCh, r, r, time.Second)
}

func pacedMetricWorker(wc wController, pacedMetricCh chan *pacedMetric, acq aggregatorCommandQueuer, dpq dataPointQueuer, frequency time.Duration) {
	wc.onEnter()
	defer wc.onExit()

	sums := make(map[string]float64)
	gauges := make(map[string]*rrd.ClockPdp)

	flush := func() {
		for name, sum := range sums {
			acq.QueueAggregatorCommand(aggregator.NewCommand(aggregator.CmdAdd, name, sum))
		}
		for name, gauge := range gauges {
			dpq.QueueDataPoint(name, gauge.End, gauge.Reset())
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
				log.Printf("%s: dropping flush timer on the floor - busy system?", wc.ident())
			}
		}
	}()

	log.Printf("%s: started.", wc.ident())
	wc.onStarted()

	for {
		select {
		case <-flushCh:
			flush()
		case ps, ok := <-pacedMetricCh:
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
	rds *receiverDs
	dss *dataSources
	dsf dsFlusherBlocking
}

func (d *distDatumDataSource) Relinquish() error {
	if !d.rds.ds.LastUpdate().IsZero() {
		d.dsf.flushDs(d.rds, true)
		d.dss.Delete(d.rds)
	}
	return nil
}

func (d *distDatumDataSource) Acquire() error {
	d.dss.Delete(d.rds) // it will get loaded afresh when needed
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
