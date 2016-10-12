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
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/rrd"
	"os"
	"reflect"
	"testing"
	"time"
)

// init sets debug
func Test_init(t *testing.T) {
	if os.Getenv("TGRES_RCVR_DEBUG") == "" && debug {
		t.Errorf("debug is set when TGRES_RCVR_DEBUG isn't")
	}
}

func Test_workerChannels_queue(t *testing.T) {
	var wcs workerChannels = make([]chan *incomingDpWithDs, 2)
	wcs[0] = make(chan *incomingDpWithDs)
	wcs[1] = make(chan *incomingDpWithDs)

	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
	rds := &receiverDs{DataSource: ds}
	called := 0
	go func() {
		<-wcs[0]
		called++
	}()
	wcs.queue(nil, rds)
	if called != 1 {
		t.Errorf("id 0 should be send to worker 0")
	}
}

func Test_New(t *testing.T) {
	r := New(nil, nil, nil)
	if r.NWorkers != 4 || r.ReportStatsPrefix != "tgres" {
		t.Errorf(`New: r.NWorkers != 4 || r.ReportStatsPrefix != "tgres"`)
	}
}

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
	r.ReportStats = false
	r.reportStatCount("", 1) // noop (ReportStats false)
	r.QueueSum("", 0)        // called++
	r.QueueGauge("", 0)      // called++
	if called != 3 {
		t.Errorf("reportStatCount call count not 3 but %d", called)
	}
}

func Test_Receiver_flushDs(t *testing.T) {
	// So we need to test that this calls queueblocking...
	r := &Receiver{flusherChs: make([]chan *dsFlushRequest, 1)}
	r.flusherChs[0] = make(chan *dsFlushRequest)
	called := 0
	go func() {
		for {
			<-r.flusherChs[0]
			called++
		}
	}()
	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
	rds := &receiverDs{DataSource: ds}
	// TODO testing that ClearRRAs was called is tricky...
	r.flushDs(rds, false)
	if called != 1 {
		t.Errorf("flushDs call count not 1: %d", called)
	}
}

// fake cluster
type fakeCluster struct {
	n, nLeave, nShutdown, nReady int
}

func (_ *fakeCluster) RegisterMsgType() (chan *cluster.Msg, chan *cluster.Msg)  { return nil, nil }
func (_ *fakeCluster) NumMembers() int                                          { return 0 }
func (_ *fakeCluster) LoadDistData(f func() ([]cluster.DistDatum, error)) error { return nil }
func (_ *fakeCluster) NodesForDistDatum(cluster.DistDatum) []*cluster.Node      { return nil }
func (_ *fakeCluster) LocalNode() *cluster.Node                                 { return nil }
func (_ *fakeCluster) NotifyClusterChanges() chan bool                          { return nil }
func (_ *fakeCluster) Transition(time.Duration) error                           { return nil }
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
