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
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

func Test_flusher_flusherChannels_queueBlocking(t *testing.T) {
	var fcs flusherChannels = make([]chan *dsFlushRequest, 2)
	fcs[0] = make(chan *dsFlushRequest)
	fcs[1] = make(chan *dsFlushRequest)

	called := 0
	go func() {
		fr := <-fcs[0]
		fr.resp <- true
		called++
	}()

	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}
	fcs.queueBlocking(rds, true)

	if called != 1 {
		t.Errorf("id 0 should be send to flusher 0")
	}
}

// This is a receiver
type fakeDsFlusher struct {
	called    int
	fdsReturn bool
	sr        statReporter
}

func (f *fakeDsFlusher) flushDs(ds serde.DbDataSourcer, block bool) bool {
	f.called++
	return f.fdsReturn
}

func (*fakeDsFlusher) enabled() bool { return true }

func (f *fakeDsFlusher) statReporter() statReporter {
	return f.sr
}

func (f *fakeDsFlusher) flusher() serde.Flusher { return f }

func (f *fakeDsFlusher) FlushDataSource(ds rrd.DataSourcer) error {
	f.called++
	return fmt.Errorf("Fake error.")
}

func (f *fakeDsFlusher) channels() flusherChannels {
	return make(flusherChannels, 0)
}

func (f *fakeDsFlusher) start(n int, flusherWg, startWg *sync.WaitGroup, mfs int) {}

// fake stats reporter
type fakeSr struct {
	called int
}

func (f *fakeSr) reportStatCount(string, float64) {
	f.called++
}

func (f *fakeSr) reportStatGauge(string, float64) {
	f.called++
}

func Test_flusher(t *testing.T) {

	wc := &wrkCtl{wg: &sync.WaitGroup{}, startWg: &sync.WaitGroup{}, id: "FOO"}
	sr := &fakeSr{}
	dsf := &fakeDsFlusher{sr: sr}
	fc := make(chan *dsFlushRequest)

	wc.startWg.Add(1)
	go flusher(wc, dsf, fc)
	wc.startWg.Wait()

	//ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}
	resp := make(chan bool)
	fc <- &dsFlushRequest{ds: rds, resp: resp}
	<-resp

	if dsf.called != 1 {
		t.Errorf("FlushDataSource() not called.")
	}

	if sr.called != 2 {
		t.Errorf("reportStatCount() should have been called 2 times.")
	}

	close(fc)
	wc.wg.Wait()
}

func Test_flusher_reportFlusherChannelFillPercent(t *testing.T) {
	ch := make(chan *dsFlushRequest, 10)
	sr := &fakeSr{}
	go reportFlusherChannelFillPercent(ch, sr, "IdenT", time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if sr.called == 0 {
		t.Errorf("reportFlusherChannelFillPercent: statReporter should have been called a bunch of times")
	}
}

func Test_flusher_start(t *testing.T) {
	db := &fakeSerde{}
	sr := &fakeSr{}
	f := &dsFlusher{db: db.Flusher(), sr: sr}
	var (
		startWg, flusherWg sync.WaitGroup
	)
	fCalled := 0
	save1 := flusher
	flusher = func(wc wController, dsf dsFlusherBlocking, flusherCh chan *dsFlushRequest) {
		fCalled++
		wc.onStarted()
	}
	startWg.Add(1)
	f.start(1, &flusherWg, &startWg, 10)
	startWg.Wait()
	if fCalled == 0 {
		t.Errorf("fCalled == 0")
	}
	if f.flushLimiter == nil {
		t.Errorf("f.flushLimiter == nil")
	}
	flusher = save1
}

func Test_flusher_flushDs(t *testing.T) {
	db := &fakeSerde{}
	sr := &fakeSr{}
	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))

	f := &dsFlusher{}
	if !f.flushDs(nil, false) {
		t.Errorf("nil database should return true")
	}

	var (
		startWg, flusherWg sync.WaitGroup
	)
	save1 := flusher
	flusher = func(wc wController, dsf dsFlusherBlocking, flusherCh chan *dsFlushRequest) {}
	f = &dsFlusher{db: db, sr: sr}
	f.start(1, &flusherWg, &startWg, 1)

	f.flushDs(ds, false)
	f.flushDs(ds, false)

	if len(f.flusherChs[0]) != 1 {
		t.Errorf("len(f.flusherChs[0])) != 1")
	}
	if sr.called != 1 {
		fmt.Printf("sr.called != 1 (second flushDs should be rate limited)")
	}

	flusher = save1
}

func Test_flusher_methods(t *testing.T) {
	db := &fakeSerde{}
	sr := &fakeSr{}

	f := &dsFlusher{db: db, sr: sr}

	if !f.enabled() {
		t.Errorf("enabled() should be true")
	}
	if db != f.flusher() {
		t.Errorf("db != f.flusher()")
	}
	if sr != f.statReporter() {
		t.Errorf("sr != f.statReporter()")
	}
	if len(f.channels()) != 0 {
		t.Errorf("len(f.channels()) != 0")
	}
}
