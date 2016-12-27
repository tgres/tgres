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
	"bytes"
	"encoding/gob"
	"fmt"
	"os"
	"reflect"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
)

// init sets debug
func Test_init(t *testing.T) {
	if os.Getenv("TGRES_RCVR_DEBUG") == "" && debug {
		t.Errorf("debug is set when TGRES_RCVR_DEBUG isn't")
	}
}

// func Test_workerChannels_queue(t *testing.T) {
// 	var wcs workerChannels = make([]chan *incomingDpWithDs, 2)
// 	wcs[0] = make(chan *incomingDpWithDs)
// 	wcs[1] = make(chan *incomingDpWithDs)

// 	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
// 	rds := &receiverDs{DataSource: ds}
// 	called := 0
// 	go func() {
// 		<-wcs[0]
// 		called++
// 	}()
// 	wcs.queue(nil, rds)
// 	if called != 1 {
// 		t.Errorf("id 0 should be send to worker 0")
// 	}
// }

// func Test_New(t *testing.T) {
// 	c := &fakeCluster{}
// 	r := New(c, nil, nil)
// 	if r.NWorkers != 4 || r.ReportStatsPrefix != "tgres.nil" {
// 		t.Errorf(`New: r.NWorkers != 4 (%d) || r.ReportStatsPrefix != "tgres" (%s)`, r.NWorkers, r.ReportStatsPrefix)
// 	}
// }

func Test_Recevier_Start(t *testing.T) {
	save := doStart
	called := 0
	doStart = func(_ *Receiver) { called++ }
	(*Receiver)(nil).Start()
	if called != 1 {
		t.Errorf("Receiver.Start: called != 1")
	}
	doStart = save
}

func Test_Recevier_Stop(t *testing.T) {
	save := doStop
	called := 0
	doStop = func(_ *Receiver, _ clusterer) { called++ }
	r := &Receiver{}
	r.Stop()
	if called != 1 {
		t.Errorf("Receiver.Stop: called != 1")
	}
	doStop = save
}

func Test_Receiver_ClusterReady(t *testing.T) {
	c := &fakeCluster{}
	r := &Receiver{cluster: c}
	r.ClusterReady(true)
	if c.nReady != 1 {
		t.Errorf("ClusterReady: c.nReady != 1 - didn't call Ready()?")
	}
}

// func Test_Receiver_SetMaxFlushRate(t *testing.T) {
// 	r := &Receiver{}
// 	r.SetMaxFlushRate(100)
// 	if r.flushLimiter == nil {
// 		t.Errorf("r.flushLimiter == nil")
// 	}
// }

func Test_Receiver_SetCluster(t *testing.T) {
	c := &fakeCluster{}
	name := "1ibegin_with_a_digit"
	c.ln = &cluster.Node{Node: &memberlist.Node{Name: name}}
	dsc := &dsCache{}
	r := &Receiver{dsc: dsc}
	r.SetCluster(c)
	if r.ReportStatsPrefix != "_"+name {
		t.Errorf("r.ReportStatsPrefix != \"_\"+name")
	}
	r.ReportStatsPrefix = "foo"
	r.SetCluster(c)
	if r.ReportStatsPrefix != "foo._"+name {
		t.Errorf("r.ReportStatsPrefix != foo.name")
	}
	c = &fakeCluster{}
	r.SetCluster(c)
	if r.ReportStatsPrefix != "foo._"+name+".nil" {
		t.Errorf(`r.ReportStatsPrefix != "foo._"+name+".nil"`)
	}
}

func Test_Receiver_QueueDataPoint(t *testing.T) {
	r := &Receiver{dpCh: make(chan *IncomingDP)}
	called := 0
	go func() {
		<-r.dpCh
		called++
	}()
	r.QueueDataPoint("", time.Time{}, 0)
	if called != 1 {
		t.Errorf("QueueDataPoint didn't sent to dpCh?")
	}
}

func Test_Receiver_QueueAggregatorCommand(t *testing.T) {
	r := &Receiver{aggCh: make(chan *aggregator.Command)}
	called := 0
	go func() {
		<-r.aggCh
		called++
	}()
	r.QueueAggregatorCommand(nil)
	if called != 1 {
		t.Errorf("QueueAggregatorCommand didn't sent to aggCh?")
	}
}

func Test_Receiver_reportStatCount(t *testing.T) {
	// Also tests QueueSum and QueueGauge
	r := &Receiver{ReportStats: true, ReportStatsPrefix: "foo", pacedMetricCh: make(chan *pacedMetric)}
	called := 0
	go func() {
		for {
			<-r.pacedMetricCh
			called++
		}
	}()
	(*Receiver)(nil).reportStatCount("", 0) // noop
	r.reportStatCount("", 0)                // noop (f == 0)
	r.reportStatCount("", 1)                // called++
	r.reportStatGauge("", 1)                // called++
	r.ReportStats = false
	r.reportStatCount("", 1) // noop (ReportStats false)
	r.QueueSum("", 0)        // called++
	r.QueueGauge("", 0)      // called++
	if called != 3 {
		t.Errorf("reportStatCount call count not 3 but %d", called)
	}
}

// func Test_Receiver_flushDs(t *testing.T) {
// 	// So we need to test that this calls queueblocking...
// 	r := &Receiver{flusherChs: make([]chan *dsFlushRequest, 1), flushLimiter: rate.NewLimiter(10, 10)}
// 	r.flusherChs[0] = make(chan *dsFlushRequest)
// 	called := 0
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			if _, ok := <-r.flusherChs[0]; !ok {
// 				break
// 			}
// 			called++
// 		}
// 	}()
// 	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
// 	rra, _ := rrd.NewRoundRobinArchive(0, 0, "WMEAN", time.Second, 10, 10, 0, time.Time{})
// 	ds.SetRRAs([]*rrd.RoundRobinArchive{rra})
// 	ds.ProcessDataPoint(10, time.Unix(100, 0))
// 	ds.ProcessDataPoint(10, time.Unix(101, 0))
// 	rds := &receiverDs{DataSource: ds}
// 	r.SetMaxFlushRate(1)
// 	r.flushDs(rds, false)
// 	r.flushDs(rds, false)
// 	close(r.flusherChs[0])
// 	wg.Wait()
// 	if called != 1 {
// 		t.Errorf("flushDs call count not 1: %d", called)
// 	}
// 	if ds.PointCount() != 0 {
// 		t.Errorf("ClearRRAs was not called by flushDs")
// 	}
// }

// fake cluster
type fakeCluster struct {
	n, nLeave, nShutdown, nReady int
	nReg, nTrans                 int
	nodesForDd                   []*cluster.Node
	ln                           *cluster.Node
	cChange                      chan bool
	tErr                         bool
}

func (c *fakeCluster) RegisterMsgType() (chan *cluster.Msg, chan *cluster.Msg) {
	c.nReg++
	return nil, nil
}
func (_ *fakeCluster) NumMembers() int                                          { return 0 }
func (_ *fakeCluster) LoadDistData(f func() ([]cluster.DistDatum, error)) error { f(); return nil }
func (c *fakeCluster) NodesForDistDatum(cluster.DistDatum) []*cluster.Node      { return c.nodesForDd }
func (c *fakeCluster) LocalNode() *cluster.Node                                 { return c.ln }
func (c *fakeCluster) NotifyClusterChanges() chan bool {
	return c.cChange
}
func (c *fakeCluster) Transition(time.Duration) error {
	c.nTrans++
	if c.tErr {
		return fmt.Errorf("some error")
	}
	return nil
}
func (c *fakeCluster) Ready(bool) error {
	c.n++
	c.nReady = c.n
	return nil
}
func (c *fakeCluster) Leave(timeout time.Duration) error {
	c.n++
	c.nLeave = c.n
	return nil
}
func (c *fakeCluster) Shutdown() error {
	c.n++
	c.nShutdown = c.n
	return nil
}

// IncomingDP must be gob encodable
func TestIncomingDP_gobEncodable(t *testing.T) {
	now := time.Now()
	dp1 := &IncomingDP{
		Name:      "foo.bar",
		TimeStamp: now,
		Value:     1.2345,
		Hops:      7,
	}

	var bb bytes.Buffer
	enc := gob.NewEncoder(&bb)
	dec := gob.NewDecoder(&bb)

	err := enc.Encode(dp1)
	if err != nil {
		t.Errorf("gob encode error:", err)
	}

	var dp2 *IncomingDP
	dec.Decode(&dp2)

	if !reflect.DeepEqual(dp1, dp2) {
		t.Errorf("dp1 != dp2 after gob encode/decode")
	}
}
