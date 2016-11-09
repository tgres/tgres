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

func Test_workerPeriodicFlush(t *testing.T) {

	// fake logger
	fl := &fakeLogger{}
	log.SetOutput(fl)
	defer func() {
		// restore default output
		log.SetOutput(os.Stderr)
	}()

	// dsf
	f := &fakeDsFlusher{fdsReturn: true}

	// recent
	recent := make(map[int64]*receiverDs)

	// dsc
	db := &fakeSerde{}
	df := &dftDSFinder{}
	c := &fakeCluster{}
	dsc := newDsCache(db, df, c, nil, true)

	ds := rrd.NewDataSource(7, "foo", 0, 0, time.Time{}, 0)
	rds := &receiverDs{DataSource: ds}
	recent[7] = rds
	dsc.insert(rds)

	workerPeriodicFlush("workerperiodic2", f, recent, 0, 10*time.Millisecond, 10, 1)

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
	recent[7] = rds
	debug = true

	leftover := workerPeriodicFlush("workerperiodic3", f, recent, 0, 10*time.Millisecond, 0, 1)
	if f.called == 0 {
		t.Errorf("workerPeriodicFlush: should have called flushDs")
	}
	if len(recent) != 0 {
		t.Errorf("workerPeriodicFlush: should have deleted the point after flushDs")
	}
	if len(leftover) != 0 {
		t.Errorf("workerPeriodicFlush: leftover should be empty")
	}

	f.fdsReturn = false
	f.called = 0
	recent[7] = rds
	ds.ProcessIncomingDataPoint(123, time.Unix(4000, 0))
	ds.ProcessIncomingDataPoint(123, time.Unix(5000, 0))
	leftover = workerPeriodicFlush("workerperiodic4", f, recent, 0, 10*time.Millisecond, 0, 0)
	if f.called == 0 {
		t.Errorf("workerPeriodicFlush: should have called flushDs")
	}
	if len(recent) != 0 {
		t.Errorf("workerPeriodicFlush: should have deleted the point on flushDs (2)")
	}
	if len(leftover) == 0 {
		t.Errorf("workerPeriodicFlush: leftover should NOT be empty, it should have the point from recent")
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
	dsf := &fakeDsFlusher{fdsReturn: true}
	workerCh := make(chan *incomingDpWithDs)

	saveFn1 := workerPeriodicFlush

	wpfCalled := 0
	workerPeriodicFlush = func(ident string, dsf dsFlusherBlocking, recent map[int64]*receiverDs, minCacheDur, maxCacheDur time.Duration, maxPoints, maxFlush int) map[int64]*receiverDs {
		wpfCalled++
		return map[int64]*receiverDs{1: nil, 2: nil}
	}

	sr := &fakeSr{}

	wc.startWg.Add(1)
	go worker(wc, dsf, workerCh, 0, 0, 10, 10*time.Millisecond, sr)
	wc.startWg.Wait()

	if !strings.Contains(string(fl.last), ident) {
		t.Errorf("worker: missing worker started log entry for ident: %s", ident)
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

	pc := ds.PointCount()
	if pc == 0 {
		t.Errorf("After dps being sent to workerCh, ds should have some points: %d", pc)
	}

	time.Sleep(50 * time.Millisecond) // wait for a flush or two
	if wpfCalled == 0 {
		t.Errorf("worker: at least one periodic flush should have happened")
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
	workerPeriodicFlush = saveFn1
}

func Test_reportWorkerChannelFillPercent(t *testing.T) {
	workerCh := make(chan *incomingDpWithDs, 10)
	sr := &fakeSr{}
	go reportWorkerChannelFillPercent(workerCh, sr, "iDenT", time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if sr.called == 0 {
		t.Errorf("reportWorkerChannelFillPercent: statReporter should have been called a bunch of times")
	}
}
