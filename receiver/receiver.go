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
	"github.com/tgres/tgres/serde"
	"os"
	"sync"
	"time"
)

var debug bool

func init() {
	debug = os.Getenv("TGRES_RCVR_DEBUG") != ""
}

type Receiver struct {
	cluster                            clusterer
	serde                              serde.SerDe
	NWorkers                           int
	MaxCacheDuration, MinCacheDuration time.Duration
	MaxCachedPoints                    int
	StatFlushDuration                  time.Duration
	StatsNamePrefix                    string
	dsc                                *dsCache
	Rcache                             *dsl.ReadCache
	dpCh                               chan *IncomingDP         // incoming data point
	workerChs                          workerChannels           // incoming data point with ds
	flusherChs                         flusherChannels          // ds to flush
	aggCh                              chan *aggregator.Command // aggregator commands (for statsd type stuff)
	workerWg                           sync.WaitGroup
	flusherWg                          sync.WaitGroup
	aggWg                              sync.WaitGroup
	dispatcherWg                       sync.WaitGroup
	pacedMetricWg                      sync.WaitGroup
	ReportStats                        bool
	ReportStatsPrefix                  string
	pacedMetricCh                      chan *pacedMetric
}

// IncomingDP is incoming data, i.e. this is the form in which input
// data is expected. This is not an internal representation of a data
// point, it's the format in which they are expected to arrive and is
// easy to convert to from most any data point representation out
// there. This data point representation has no notion of duration and
// therefore must rely on some kind of an externally stored "last
// update" time.
type IncomingDP struct {
	Name      string
	TimeStamp time.Time
	Value     float64
	Hops      int
}

type workerChannels []chan *incomingDpWithDs

func (w workerChannels) queue(dp *IncomingDP, rds *receiverDs) {
	w[rds.Id()%int64(len(w))] <- &incomingDpWithDs{dp, rds}
}

type incomingDpWithDs struct {
	dp  *IncomingDP
	rds *receiverDs
}

func New(clstr *cluster.Cluster, serde serde.SerDe, finder MatchingDSSpecFinder) *Receiver {
	if finder == nil {
		finder = &dftDSFinder{}
	}
	r := &Receiver{
		cluster:           clstr,
		serde:             serde,
		NWorkers:          4,
		MaxCacheDuration:  5 * time.Second,
		MinCacheDuration:  1 * time.Second,
		MaxCachedPoints:   256,
		StatFlushDuration: 10 * time.Second,
		StatsNamePrefix:   "stats",
		Rcache:            dsl.NewReadCache(serde),
		dpCh:              make(chan *IncomingDP, 65536),         // so we can survive a graceful restart
		aggCh:             make(chan *aggregator.Command, 65536), // ditto
		ReportStats:       true,
		ReportStatsPrefix: "tgres",
		pacedMetricCh:     make(chan *pacedMetric, 256),
	}
	r.dsc = newDsCache(serde, finder, clstr, r, false)
	return r
}

func (r *Receiver) Start() {
	doStart(r)
}

func (r *Receiver) Stop() {
	doStop(r, r.cluster)
}

func (r *Receiver) ClusterReady(ready bool) {
	r.cluster.Ready(ready)
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
	if r != nil && r.ReportStats && f != 0 {
		r.QueueSum(r.ReportStatsPrefix+"."+name, f)
	}
}

func (r *Receiver) QueueSum(name string, v float64) {
	r.pacedMetricCh <- &pacedMetric{pacedSum, name, v}
}

func (r *Receiver) QueueGauge(name string, v float64) {
	r.pacedMetricCh <- &pacedMetric{pacedGauge, name, v}
}

func (r *Receiver) flushDs(rds *receiverDs, block bool) {
	r.flusherChs.queueBlocking(rds, block)
	rds.ClearRRAs(false)
	rds.lastFlushRT = time.Now()
}

type dsFlusherBlocking interface {
	flushDs(*receiverDs, bool)
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

type clusterer interface {
	RegisterMsgType() (chan *cluster.Msg, chan *cluster.Msg)
	NumMembers() int
	LoadDistData(func() ([]cluster.DistDatum, error)) error
	NodesForDistDatum(cluster.DistDatum) []*cluster.Node
	LocalNode() *cluster.Node
	NotifyClusterChanges() chan bool
	Transition(time.Duration) error
	Ready(bool) error
	Leave(timeout time.Duration) error
	Shutdown() error
	//NewMsg(*cluster.Node, interface{}) (*cluster.Msg, error)
}
