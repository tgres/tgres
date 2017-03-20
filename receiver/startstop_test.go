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

// func Test_startstop_startAllWorkers(t *testing.T) {
// 	// Save and replace the start funcs
// 	f1, f2, f3, f4 := startWorkers, startFlushers, startAggWorker, startPacedMetricWorker
// 	called := 0
// 	f := func(r *Receiver, wg *sync.WaitGroup) { called++ }
// 	startWorkers, startFlushers, startAggWorker, startPacedMetricWorker = f, f, f, f
// 	startAllWorkers(nil, &sync.WaitGroup{})
// 	if called != 4 {
// 		t.Errorf("startAllWorkers: called != 4: %d", called)
// 	}
// 	// Restore
// 	startWorkers, startFlushers, startAggWorker, startPacedMetricWorker = f1, f2, f3, f4
// }

// func Test_startstop_doStart(t *testing.T) {
// 	delay := 100 * time.Millisecond

// 	//clstr := &fakeCluster{cChange: make(chan bool)}
// 	db := &fakeSerde{}
// 	df := &SimpleDSFinder{DftDSSPec}
// 	sr := &fakeSr{}
// 	fl := &dsFlusher{db: db, sr: sr}
// 	dsc := newDsCache(db, df, fl)

// 	r := &Receiver{dpCh: make(chan *IncomingDP), dsc: dsc}

// 	saveDisp := director
// 	saveSaw := startAllWorkers
// 	called := 0
// 	stopped := false
// 	director = func(wc wController, dpCh chan *IncomingDP, clstr clusterer, scr statReporter, dss *dsCache, workerChs workerChannels) {
// 		wc.onEnter()
// 		defer wc.onExit()
// 		called++
// 		wc.onStarted()
// 		if _, ok := <-dpCh; !ok {
// 			stopped = true
// 		}
// 	}
// 	calledSAW := 0
// 	startAllWorkers = func(r *Receiver, startWg *sync.WaitGroup) {
// 		calledSAW++
// 		startWg.Add(1)
// 		go func() {
// 			time.Sleep(delay)
// 			startWg.Done()
// 		}()
// 	}
// 	started := time.Now()
// 	doStart(r)
// 	if called == 0 {
// 		t.Errorf("doStart: didn't call director()?")
// 	}
// 	if calledSAW == 0 {
// 		t.Errorf("doStart: calledSAW == 0, didn't call startAllWorkers()?")
// 	}
// 	if time.Now().Sub(started) < delay {
// 		t.Errorf("doStart: not enough time passed, didn't call startAllWorkers()?")
// 	}

// 	// test stopDirector here too
// 	stopDirector(r)
// 	if !stopped {
// 		t.Errorf("stopDirector didn't stop dispatcher")
// 	}

// 	director = saveDisp
// 	startAllWorkers = saveSaw
// }

// func Test_startstop_Receiver_doStop(t *testing.T) {
// 	f1, f2 := stopDirector, stopAllWorkers
// 	called, calledSAW := 0, 0
// 	stopDirector = func(_ *Receiver) { called++ }
// 	stopAllWorkers = func(_ *Receiver) { calledSAW++ }
// 	r := &Receiver{}
// 	c := &fakeCluster{}
// 	doStop(r, c)
// 	if c.nLeave != 1 {
// 		t.Errorf("doStop: never called cluster.Leave, or not first: %d", c.nLeave)
// 	}
// 	if c.nShutdown != 2 {
// 		t.Errorf("doStop: never called cluster.Shutdown, or not second: %d", c.nShutdown)
// 	}
// 	stopDirector, stopAllWorkers = f1, f2
// }

// func Test_startstop_stopWorkers(t *testing.T) {
// 	workerChs := make([]chan *incomingDpWithDs, 0)
// 	workerChs = append(workerChs, make(chan *incomingDpWithDs))
// 	closed := 0
// 	var closeWatchWg sync.WaitGroup
// 	closeWatchWg.Add(1)
// 	go func() {
// 		defer closeWatchWg.Done()
// 		_, ok := <-workerChs[0]
// 		if !ok {
// 			closed++
// 		}
// 	}()
// 	var workerWg sync.WaitGroup
// 	workerWg.Add(1)
// 	delay := time.Millisecond * 100
// 	started := time.Now()
// 	go func() {
// 		defer workerWg.Done()
// 		time.Sleep(delay)
// 	}()
// 	stopWorkers(workerChs, &workerWg)
// 	closeWatchWg.Wait()
// 	if closed != 1 {
// 		t.Errorf("stopWorkers: closed != 1 didn't close channel?")
// 	}
// 	if time.Now().Sub(started) < delay {
// 		t.Errorf("stopWorkers: not enough time passed, didn't wait on the WaitGroup?")
// 	}
// }

// func Test_startstop_stopFlushers(t *testing.T) {
// 	workerChs := make([]chan *dsFlushRequest, 0)
// 	workerChs = append(workerChs, make(chan *dsFlushRequest))
// 	closed := 0
// 	var closeWatchWg sync.WaitGroup
// 	closeWatchWg.Add(1)
// 	go func() {
// 		defer closeWatchWg.Done()
// 		_, ok := <-workerChs[0]
// 		if !ok {
// 			closed++
// 		}
// 	}()
// 	var workerWg sync.WaitGroup
// 	workerWg.Add(1)
// 	delay := time.Millisecond * 100
// 	started := time.Now()
// 	go func() {
// 		defer workerWg.Done()
// 		time.Sleep(delay)
// 	}()
// 	stopFlushers(workerChs, &workerWg)
// 	closeWatchWg.Wait()
// 	if closed != 1 {
// 		t.Errorf("stopFlushers: closed != 1 didn't close channel?")
// 	}
// 	if time.Now().Sub(started) < delay {
// 		t.Errorf("stopFlushers: not enough time passed, didn't wait on the WaitGroup?")
// 	}
// }

// func Test_startstop_stopPacedMetricWorker(t *testing.T) {
// 	workerCh := make(chan *pacedMetric)
// 	closed := 0
// 	var closeWatchWg sync.WaitGroup
// 	closeWatchWg.Add(1)
// 	go func() {
// 		defer closeWatchWg.Done()
// 		_, ok := <-workerCh
// 		if !ok {
// 			closed++
// 		}
// 	}()
// 	var workerWg sync.WaitGroup
// 	workerWg.Add(1)
// 	delay := time.Millisecond * 100
// 	started := time.Now()
// 	go func() {
// 		defer workerWg.Done()
// 		time.Sleep(delay)
// 	}()
// 	stopPacedMetricWorker(workerCh, &workerWg)
// 	closeWatchWg.Wait()
// 	if closed != 1 {
// 		t.Errorf("stopPacedMetricWorker: closed != 1 didn't close channel?")
// 	}
// 	if time.Now().Sub(started) < delay {
// 		t.Errorf("stopPacedMetricWorker: not enough time passed, didn't wait on the WaitGroup?")
// 	}
// }

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

// func Test_startstop_stopAllWorkers(t *testing.T) {
// 	// Save
// 	f1, f2, f3, f4 := stopWorkers, stopFlushers, stopAggWorker, stopPacedMetricWorker
// 	called := 0
// 	stopWorkers = func(workerChs []chan *incomingDpWithDs, workerWg *sync.WaitGroup) { called++ }
// 	stopFlushers = func(flusherChs []chan *dsFlushRequest, flusherWg *sync.WaitGroup) { called++ }
// 	stopAggWorker = func(aggCh chan *aggregator.Command, aggWg *sync.WaitGroup) { called++ }
// 	stopPacedMetricWorker = func(pacedMetricCh chan *pacedMetric, pacedMetricWg *sync.WaitGroup) { called++ }
// 	stopAllWorkers(&Receiver{flusher: &fakeDsFlusher{}})
// 	if called != 4 {
// 		t.Errorf("stopAllWorkers: called != 4")
// 	}
// 	// Restore
// 	stopWorkers, stopFlushers, stopAggWorker, stopPacedMetricWorker = f1, f2, f3, f4
// }

// func Test_startstop_startWorkers(t *testing.T) {
// 	nWorkers := 0
// 	saveWorker := worker
// 	worker = func(wc wController, dsf dsFlusherBlocking, workerCh chan *incomingDpWithDs, minCacheDur, maxCacheDur time.Duration, maxPoints int, flushInt time.Duration, sr statReporter) {
// 		wc.onEnter()
// 		defer wc.onExit()
// 		nWorkers++
// 		wc.onStarted()
// 	}

// 	var startWg sync.WaitGroup
// 	r := &Receiver{NWorkers: 5}
// 	startWorkers(r, &startWg)
// 	startWg.Wait()

// 	if nWorkers != 5 {
// 		t.Errorf("startWorkers: nWorkers started != 5")
// 	}
// 	worker = saveWorker
// }

// func Test_startstop_startFlushers(t *testing.T) {
// 	nFlushers := 0
// 	saveFlusher := flusher
// 	flusher = func(wc wController, dsf dsFlusherBlocking, flusherCh chan *dsFlushRequest) {
// 		wc.onEnter()
// 		defer wc.onExit()
// 		nFlushers++
// 		wc.onStarted()
// 	}

// 	var startWg sync.WaitGroup
// 	db := &fakeSerde{}
// 	sr := &fakeSr{}
// 	r := &Receiver{NWorkers: 5}
// 	r.flusher = &dsFlusher{db: db, sr: sr}
// 	startFlushers(r, &startWg)
// 	startWg.Wait()

// 	if nFlushers != 5 {
// 		t.Errorf("startFlushers: nFlushers started != 5")
// 	}

// 	// no flusher support
// 	nFlushers = 0
// 	r.flusher = &dsFlusher{}
// 	startFlushers(r, &startWg)
// 	startWg.Wait()

// 	if nFlushers != 0 {
// 		t.Errorf("nil flusher: nFlushers != 0")
// 	}

// 	flusher = saveFlusher
// }

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
