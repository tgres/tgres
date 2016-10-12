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
	"os"
	"reflect"
	"sync"
	"testing"
	"time"
)

// init sets debug
func Test_init(t *testing.T) {
	if os.Getenv("TGRES_RCVR_DEBUG") == "" && debug {
		t.Errorf("debug is set when TGRES_RCVR_DEBUG isn't")
	}
}

// dftDSFinder
func Test_FindMatchingDSSpec(t *testing.T) {

	df := &dftDSFinder{}
	d := df.FindMatchingDSSpec("whatever")
	if d.Step != 10*time.Second || len(d.RRAs) == 0 {
		t.Errorf("FindMatchingDSSpec: d.Step != 10s || len(d.RRAs) == 0")
	}
}

func Test_New(t *testing.T) {
	r := New(nil, nil, nil)
	if r.NWorkers != 4 || r.ReportStatsPrefix != "tgres" {
		t.Errorf(`New: r.NWorkers != 4 || r.ReportStatsPrefix != "tgres"`)
	}
}

func Test_startAllWorkers(t *testing.T) {
	// Save and replace the start funcs
	f1, f2, f3, f4 := startWorkers, startFlushers, startAggWorker, startPacedMetricWorker
	called := 0
	f := func(r *Receiver, wg *sync.WaitGroup) { called++ }
	startWorkers, startFlushers, startAggWorker, startPacedMetricWorker = f, f, f, f
	startAllWorkers(nil, &sync.WaitGroup{})
	if called != 4 {
		t.Errorf("startAllWorkers: called != 4")
	}
	// Restore
	startWorkers, startFlushers, startAggWorker, startPacedMetricWorker = f1, f2, f3, f4
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

func Test_doStart(t *testing.T) {
	delay := 100 * time.Millisecond
	r := &Receiver{}
	saveDisp := dispatcher
	called := 0
	dispatcher = func(wc wController, dpCh chan *IncomingDP, clstr clusterer, scr statCountReporter, dss *dsCache, workerChs workerChannels) {
		called++
		wc.onStarted()
	}
	calledSAW := 0
	startAllWorkers = func(r *Receiver, startWg *sync.WaitGroup) {
		calledSAW++
		startWg.Add(1)
		go func() {
			time.Sleep(delay)
			startWg.Done()
		}()
	}
	started := time.Now()
	doStart(r)
	if called == 0 {
		t.Errorf("doStart: didn't call dispatcher()?")
	}
	if calledSAW == 0 {
		t.Errorf("doStart: calledSAW == 0, didn't call startAllWorkers()?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("doStart: not enough time passed, didn't call startAllWorkers()?")
	}
	dispatcher = saveDisp
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

func Test_Receiver_doStop(t *testing.T) {
	f1, f2 := stopDispatcher, stopAllWorkers
	called, calledSAW := 0, 0
	stopDispatcher = func(_ *Receiver) { called++ }
	stopAllWorkers = func(_ *Receiver) { calledSAW++ }
	r := &Receiver{}
	c := &fakeCluster{}
	doStop(r, c)
	if c.nLeave != 1 {
		t.Errorf("doStop: never called cluster.Leave, or not first: %d", c.nLeave)
	}
	if c.nShutdown != 2 {
		t.Errorf("doStop: never called cluster.Shutdown, or not second: %d", c.nShutdown)
	}
	stopDispatcher, stopAllWorkers = f1, f2
}

func Test_Receiver_ClusterReady(t *testing.T) {
	c := &fakeCluster{}
	r := &Receiver{cluster: c}
	r.ClusterReady(true)
	if c.nReady != 1 {
		t.Errorf("ClusterReady: c.nReady != 1 - didn't call Ready()?")
	}
}

func Test_stopWorkers(t *testing.T) {
	workerChs := make([]chan *incomingDpWithDs, 0)
	workerChs = append(workerChs, make(chan *incomingDpWithDs))
	closed := 0
	var closeWatchWg sync.WaitGroup
	closeWatchWg.Add(1)
	go func() {
		defer closeWatchWg.Done()
		_, ok := <-workerChs[0]
		if !ok {
			closed++
		}
	}()
	var workerWg sync.WaitGroup
	workerWg.Add(1)
	delay := time.Millisecond * 100
	started := time.Now()
	go func() {
		defer workerWg.Done()
		time.Sleep(delay)
	}()
	stopWorkers(workerChs, &workerWg)
	closeWatchWg.Wait()
	if closed != 1 {
		t.Errorf("stopWorkers: closed != 1 didn't close channel?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("stopWorkers: not enough time passed, didn't wait on the WaitGroup?")
	}
}

func Test_stopFlushers(t *testing.T) {
	workerChs := make([]chan *dsFlushRequest, 0)
	workerChs = append(workerChs, make(chan *dsFlushRequest))
	closed := 0
	var closeWatchWg sync.WaitGroup
	closeWatchWg.Add(1)
	go func() {
		defer closeWatchWg.Done()
		_, ok := <-workerChs[0]
		if !ok {
			closed++
		}
	}()
	var workerWg sync.WaitGroup
	workerWg.Add(1)
	delay := time.Millisecond * 100
	started := time.Now()
	go func() {
		defer workerWg.Done()
		time.Sleep(delay)
	}()
	stopFlushers(workerChs, &workerWg)
	closeWatchWg.Wait()
	if closed != 1 {
		t.Errorf("stopFlushers: closed != 1 didn't close channel?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("stopFlushers: not enough time passed, didn't wait on the WaitGroup?")
	}
}

func Test_stopPacedMetricWorker(t *testing.T) {
	workerCh := make(chan *pacedMetric)
	closed := 0
	var closeWatchWg sync.WaitGroup
	closeWatchWg.Add(1)
	go func() {
		defer closeWatchWg.Done()
		_, ok := <-workerCh
		if !ok {
			closed++
		}
	}()
	var workerWg sync.WaitGroup
	workerWg.Add(1)
	delay := time.Millisecond * 100
	started := time.Now()
	go func() {
		defer workerWg.Done()
		time.Sleep(delay)
	}()
	stopPacedMetricWorker(workerCh, &workerWg)
	closeWatchWg.Wait()
	if closed != 1 {
		t.Errorf("stopPacedMetricWorker: closed != 1 didn't close channel?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("stopPacedMetricWorker: not enough time passed, didn't wait on the WaitGroup?")
	}
}

func Test_stopAggWorker(t *testing.T) {
	workerCh := make(chan *aggregator.Command)
	closed := 0
	var closeWatchWg sync.WaitGroup
	closeWatchWg.Add(1)
	go func() {
		defer closeWatchWg.Done()
		_, ok := <-workerCh
		if !ok {
			closed++
		}
	}()
	var workerWg sync.WaitGroup
	workerWg.Add(1)
	delay := time.Millisecond * 100
	started := time.Now()
	go func() {
		defer workerWg.Done()
		time.Sleep(delay)
	}()
	stopAggWorker(workerCh, &workerWg)
	closeWatchWg.Wait()
	if closed != 1 {
		t.Errorf("stopAggWorker: closed != 1 didn't close channel?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("stopAggWorker: not enough time passed, didn't wait on the WaitGroup?")
	}
}

func Test_stopAllWorkers(t *testing.T) {
	// Save
	f1, f2, f3, f4 := stopWorkers, stopFlushers, stopAggWorker, stopPacedMetricWorker
	called := 0
	stopWorkers = func(workerChs []chan *incomingDpWithDs, workerWg *sync.WaitGroup) { called++ }
	stopFlushers = func(flusherChs []chan *dsFlushRequest, flusherWg *sync.WaitGroup) { called++ }
	stopAggWorker = func(aggCh chan *aggregator.Command, aggWg *sync.WaitGroup) { called++ }
	stopPacedMetricWorker = func(pacedMetricCh chan *pacedMetric, pacedMetricWg *sync.WaitGroup) { called++ }
	stopAllWorkers(&Receiver{})
	if called != 4 {
		t.Errorf("stopAllWorkers: called != 4")
	}
	// Restore
	stopWorkers, stopFlushers, stopAggWorker, stopPacedMetricWorker = f1, f2, f3, f4
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
