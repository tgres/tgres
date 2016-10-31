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
	"github.com/tgres/tgres/rrd"
	"log"
	"math"
	"os"
	"strings"
	"sync"
	"testing"
	"time"
)

func Test_workerPeriodicFlushSignal(t *testing.T) {

	ch := make(chan bool)
	ch2 := make(chan bool)
	called := 0
	go func() {
		for {
			<-ch
			called++
			ch2 <- true
		}
	}()

	go workerPeriodicFlushSignal(ch, 0, 10*time.Millisecond)
	<-ch

	time.Sleep(20 * time.Millisecond)
	if called == 0 {
		t.Errorf("workerPeriodicFlushSignal: should be called at least once")
	}
}

func Test_workerPeriodicFlush(t *testing.T) {

	// fake logger
	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	// dsf
	f := &fakeDsFlusher{}

	// recent
	recent := make(map[int64]bool)
	recent[7] = true

	// dsc
	db := &fakeSerde{}
	df := &dftDSFinder{}
	c := &fakeCluster{}
	dsc := newDsCache(db, df, c, nil, true)

	workerPeriodicFlush("ident", f, recent, dsc, 0, 10*time.Millisecond, 10)

	if !strings.Contains(string(fl.last), "annot lookup") {
		t.Errorf("workerPeriodicFlush: non-existent ds did not log 'annot lookup'")
	}
	if len(recent) != 0 {
		t.Errorf("workerPeriodicFlush: the recent entry should have been deleted even if it cannot be looked up")
	}

	recent[7] = true
	ds := rrd.NewDataSource(7, "foo", 0, 0, time.Time{}, 0)
	rds := &receiverDs{DataSource: ds}
	dsc.insert(rds)

	workerPeriodicFlush("ident", f, recent, dsc, 0, 10*time.Millisecond, 10)

	if f.called > 0 {
		t.Errorf("workerPeriodicFlush: no flush should have happened")
	}

	// make an rds with points
	ds = rrd.NewDataSource(7, "foo", 0, 0, time.Time{}, 0)
	rra, _ := rrd.NewRoundRobinArchive(1, 0, "WMEAN", 10*time.Second, 100, 30, 0.5, time.Unix(1000, 0))
	ds.SetRRAs([]*rrd.RoundRobinArchive{rra})
	ds.ProcessIncomingDataPoint(123, time.Unix(2000, 0))
	ds.ProcessIncomingDataPoint(123, time.Unix(3000, 0))
	rds = &receiverDs{DataSource: ds}
	dsc.insert(rds)
	recent[7] = true
	debug = true

	workerPeriodicFlush("ident", f, recent, dsc, 0, 10*time.Millisecond, 0)
	if f.called == 0 {
		t.Errorf("workerPeriodicFlush: should have been flushed")
	}
}

func Test_theWorker(t *testing.T) {

	// fake logger
	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	ident := "FOO"
	wc := &wrkCtl{wg: &sync.WaitGroup{}, startWg: &sync.WaitGroup{}, id: ident}
	dsf := &fakeDsFlusher{}
	workerCh := make(chan *incomingDpWithDs)

	// dsc
	db := &fakeSerde{}
	df := &dftDSFinder{}
	c := &fakeCluster{}
	dsc := newDsCache(db, df, c, nil, true)

	saveFn1, saveFn2 := workerPeriodicFlushSignal, workerPeriodicFlush

	wpfsCalled := 0
	workerPeriodicFlushSignal = func(periodicFlushCheck chan bool, minCacheDur, maxCacheDur time.Duration) {
		wpfsCalled++
		periodicFlushCheck <- true
	}
	wpfCalled := 0
	workerPeriodicFlush = func(ident string, dsf dsFlusherBlocking, recent map[int64]bool, dss *dsCache, minCacheDur, maxCacheDur time.Duration, maxPoints int) {
		wpfCalled++
	}

	wc.startWg.Add(1)
	go worker(wc, dsf, workerCh, dsc, 0, 0, 10)
	wc.startWg.Wait()

	time.Sleep(5 * time.Millisecond) // because go workerPeriodicFlushSignal()
	if wpfsCalled == 0 {
		t.Errorf("worker: workerPeriodicFlushSignal should be called")
	}

	if !strings.Contains(string(fl.last), ident) {
		t.Errorf("worker: missing worker started log entry for ident: %s", ident)
	}

	if wpfCalled == 0 {
		t.Errorf("worker: at least one periodic flush should have happened")
	}

	debug = true

	// make an rds with points
	ds := rrd.NewDataSource(0, "foo", 0, 0, time.Time{}, 0)
	rra, _ := rrd.NewRoundRobinArchive(1, 0, "WMEAN", 10*time.Second, 100, 30, 0.5, time.Unix(1000, 0))
	ds.SetRRAs([]*rrd.RoundRobinArchive{rra})
	rds := &receiverDs{DataSource: ds}
	dp := &IncomingDP{Name: "foo", TimeStamp: time.Unix(2000, 0), Value: 123}
	workerCh <- &incomingDpWithDs{dp, rds}
	dp = &IncomingDP{Name: "foo", TimeStamp: time.Unix(3000, 0), Value: 123}
	workerCh <- &incomingDpWithDs{dp, rds}
	dp = &IncomingDP{Name: "foo", TimeStamp: time.Unix(4000, 0), Value: 123}
	workerCh <- &incomingDpWithDs{dp, rds}

	if dsf.called < 1 {
		t.Errorf("worker: flushDs should be called at least once: %d", dsf.called)
	}

	if !strings.Contains(string(fl.last), "Requesting flush") {
		t.Errorf("worker: missing 'Requesting flush' log entry")
	}

	// trigger an error
	dp = &IncomingDP{Name: "foo", TimeStamp: time.Unix(5000, 0), Value: math.Inf(-1)}
	workerCh <- &incomingDpWithDs{dp, rds}

	close(workerCh)
	wc.wg.Wait()

	if !strings.Contains(string(fl.last), "not a valid data point") {
		t.Errorf("worker: missing 'not a valid data point' log entry")
	}

	// restore funcs
	workerPeriodicFlushSignal, workerPeriodicFlush = saveFn1, saveFn2
}
