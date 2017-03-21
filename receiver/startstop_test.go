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
	"sync"
	"testing"
	"time"

	"github.com/tgres/tgres/aggregator"
)

func Test_startstop_wrkCtl(t *testing.T) {
	wc := &wrkCtl{id: "foo"}
	if wc.ident() != "foo" {
		t.Errorf(`wc.ident() != "foo"`)
	}
}

func Test_startstop_startAllWorkers(t *testing.T) {
	// Save and replace the start funcs
	f1, f2, f3 := startFlushers, startAggWorker, startPacedMetricWorker
	called := 0
	f := func(r *Receiver, wg *sync.WaitGroup) { called++ }
	startFlushers, startAggWorker, startPacedMetricWorker = f, f, f
	startAllWorkers(nil, &sync.WaitGroup{})
	if called != 3 {
		t.Errorf("startAllWorkers: called != 3: %d", called)
	}
	// Restore
	startFlushers, startAggWorker, startPacedMetricWorker = f1, f2, f3
}

func Test_startstop_doStart(t *testing.T) {
	delay := 100 * time.Millisecond

	//clstr := &fakeCluster{cChange: make(chan bool)}
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	sr := &fakeSr{}
	fl := &dsFlusher{db: db, sr: sr}
	dsc := newDsCache(db, df, fl)

	r := &Receiver{dpCh: make(chan interface{}), dsc: dsc}

	saveDisp := director
	saveSaw := startAllWorkers
	called := 0
	stopped := false
	director = func(wc wController, dpCh chan interface{}, nWorkers int, clstr clusterer, sr statReporter, dsc *dsCache, dsf dsFlusherBlocking, maxQLen int) {
		wc.onEnter()
		defer wc.onExit()
		called++
		wc.onStarted()
		if dp := <-dpCh; dp == nil {
			stopped = true
		}
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
		t.Errorf("doStart: didn't call director()?")
	}
	if calledSAW == 0 {
		t.Errorf("doStart: calledSAW == 0, didn't call startAllWorkers()?")
	}
	if time.Now().Sub(started) < delay {
		t.Errorf("doStart: not enough time passed, didn't call startAllWorkers()?")
	}

	// test stopDirector here too
	stopDirector(r)
	if !stopped {
		t.Errorf("stopDirector didn't stop dispatcher")
	}

	director = saveDisp
	startAllWorkers = saveSaw
}

func Test_startstop_Receiver_doStop(t *testing.T) {
	f1, f2, f3, f4 := stopPacedMetricWorker, stopAggWorker, stopDirector, stopFlushers
	called := 0
	stopPacedMetricWorker = func(_ chan *pacedMetric, _ *sync.WaitGroup) { called++ }
	stopAggWorker = func(_ chan *aggregator.Command, _ *sync.WaitGroup) { called++ }
	stopDirector = func(_ *Receiver) { called++ }
	stopFlushers = func(_ dsFlusherBlocking, _ *sync.WaitGroup) { called++ }
	r := &Receiver{}
	c := &fakeCluster{}
	doStop(r, c)
	if c.nLeave != 1 {
		t.Errorf("doStop: never called cluster.Leave, or not first: %d", c.nLeave)
	}
	if c.nShutdown != 2 {
		t.Errorf("doStop: never called cluster.Shutdown, or not second: %d", c.nShutdown)
	}
	stopPacedMetricWorker, stopAggWorker, stopDirector, stopFlushers = f1, f2, f3, f4
}

func Test_startstop_stopPacedMetricWorker(t *testing.T) {
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

func Test_startstop_stopAggWorker(t *testing.T) {
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

func Test_startstop_startAggWorker(t *testing.T) {
	started := 0
	saveAW := aggWorker
	aggWorker = func(wc wController, aggCh chan *aggregator.Command, clstr clusterer, statFlushDuration time.Duration, statsNamePrefix string, scr statReporter, dpq *Receiver) {
		wc.onEnter()
		defer wc.onExit()
		started++
		wc.onStarted()
	}
	var startWg sync.WaitGroup
	r := &Receiver{}
	startAggWorker(r, &startWg)
	startWg.Wait()

	if started == 0 {
		t.Errorf("startAggWorker: no aggWorker started")
	}
	aggWorker = saveAW
}

func Test_startstop_startPacedMetricWorker(t *testing.T) {
	started := 0
	savePMW := pacedMetricWorker
	pacedMetricWorker = func(wc wController, pacedMetricCh chan *pacedMetric, acq aggregatorCommandQueuer, dpq dataPointQueuer, frequency time.Duration, sr statReporter) {
		wc.onEnter()
		defer wc.onExit()
		started++
		wc.onStarted()
	}
	var startWg sync.WaitGroup
	r := &Receiver{}
	startPacedMetricWorker(r, &startWg)
	startWg.Wait()

	if started == 0 {
		t.Errorf("startPAcedMetricWorker: no pacedMetricWorker started")
	}
	pacedMetricWorker = savePMW
}
