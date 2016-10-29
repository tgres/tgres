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
	"os"
	"strings"
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

func Test_theWorkerPeriodicFlush(t *testing.T) {

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
