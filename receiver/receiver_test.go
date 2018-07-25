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
	"net"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/memberlist"
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/serde"
)

// init sets debug
func Test_Receiver_init(t *testing.T) {
	if os.Getenv("TGRES_RCVR_DEBUG") == "" && debug {
		t.Errorf("debug is set when TGRES_RCVR_DEBUG isn't")
	}
}

func Test_Receiver_New(t *testing.T) {
	db := &fakeSerde{}
	r := New(db, nil)
	if r.NWorkers != 1 || r.ReportStatsPrefix != "tgres" {
		t.Errorf(`New: r.NWorkers != 1 (%d) || r.ReportStatsPrefix != "tgres" (%s)`, r.NWorkers, r.ReportStatsPrefix)
	}
}

func Test_Receiver_Start(t *testing.T) {
	save := doStart
	called := 0
	doStart = func(_ *Receiver) { called++ }
	(*Receiver)(nil).Start()
	if called != 1 {
		t.Errorf("Receiver.Start: called != 1")
	}
	doStart = save
}

func Test_Receiver_Stop(t *testing.T) {
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

func Test_Receiver_SetCluster(t *testing.T) {
	c := &fakeCluster{}
	c.ln = &cluster.Node{Node: &memberlist.Node{Addr: net.ParseIP("10.10.10.10")}}
	addr := strings.Replace(c.ln.Addr.String(), ".", "_", -1)
	dsc := &dsCache{}
	r := &Receiver{dsc: dsc}
	r.SetCluster(c)
	if r.ReportStatsPrefix != addr {
		t.Errorf("r.ReportStatsPrefix != addr: %v", r.ReportStatsPrefix)
	}
	r.ReportStatsPrefix = "foo"
	r.SetCluster(c)
	if r.ReportStatsPrefix != "foo."+addr {
		t.Errorf("r.ReportStatsPrefix != foo.addr: %v", r.ReportStatsPrefix)
	}
}

func Test_Receiver_QueueDataPoint(t *testing.T) {
	ch := make(chan interface{})
	r := &Receiver{dpChIn: ch, dpChOut: ch}
	called := 0
	go func() {
		<-r.dpChOut
		called++
	}()
	r.QueueDataPoint(serde.Ident{}, time.Time{}, 0)
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
	r.reportStatCount("", 0)                // called++ (f == 0)
	r.reportStatCount("", 1)                // called++
	r.reportStatGauge("", 1)                // called++
	r.ReportStats = false
	r.reportStatCount("", 1)       // noop (ReportStats false)
	r.QueueSum(serde.Ident{}, 0)   // called++
	r.QueueGauge(serde.Ident{}, 0) // called++
	if called != 5 {
		t.Errorf("reportStatCount call count not 5 but %d", called)
	}
}

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
	now := time.Now().Round(0)
	dp1 := &incomingDP{
		cachedIdent: newCachedIdent(serde.Ident{"name": "foo.bar"}),
		timeStamp:   now,
		value:       1.2345,
		Hops:        7,
	}

	var bb bytes.Buffer
	enc := gob.NewEncoder(&bb)
	dec := gob.NewDecoder(&bb)

	err := enc.Encode(dp1)
	if err != nil {
		t.Errorf("gob encode error: %v", err)
	}

	var dp2 *incomingDP
	dec.Decode(&dp2)

	if !reflect.DeepEqual(dp1, dp2) {
		t.Errorf("dp1 != dp2 after gob encode/decode")
	}
}
