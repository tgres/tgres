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
	r := New(nil, nil)
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
	(*Receiver)(nil).startAllWorkers(&sync.WaitGroup{})
	if called != 4 {
		t.Errorf("startAllWorkers: called != 4")
	}
	// Restore
	startWorkers, startFlushers, startAggWorker, startPacedMetricWorker = f1, f2, f3, f4
}

func Test_Recevier_Start(t *testing.T) {
	save := doStart
	called := 0
	doStart = func(ws workerStarter, r *Receiver) { called++ }
	(*Receiver)(nil).Start()
	if called != 1 {
		t.Errorf("Receiver.Start: called != 1")
	}
	doStart = save
}

type fakeWorkerStarter struct {
	startAllWorkersCalled int
	delay                 time.Duration
}

func (ws *fakeWorkerStarter) startAllWorkers(startWg *sync.WaitGroup) {
	ws.startAllWorkersCalled++
	startWg.Add(1)
	go func() {
		time.Sleep(ws.delay)
		startWg.Done()
	}()
}

func Test_doStart(t *testing.T) {
	delay := 100 * time.Millisecond
	ws := &fakeWorkerStarter{delay: delay}
	r := &Receiver{}
	saveDisp := dispatcher
	called := 0
	dispatcher = func(wc wController, dpCh chan *IncomingDP, clstr clusterer, wstp workerStopper, scr statCountReporter,
		dscl dsCreateOrLoader, dss *dataSources, workerChs []chan *incomingDpWithDs, NWorkers int, dsf dsFlusherBlocking) {
		called++
		wc.onStarted()
	}
	started := time.Now()
	doStart(ws, r)
	if called == 0 {
		t.Errorf("doStart: didn't call dispatcher()?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("doStart: not enough time passed, didn't call startAllWorkers()?")
	}
	dispatcher = saveDisp
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
