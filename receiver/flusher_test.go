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

func Test_flusherChannels_queueBlocking(t *testing.T) {
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

func Test_reportFlusherChannelFillPercent(t *testing.T) {
	ch := make(chan *dsFlushRequest, 10)
	sr := &fakeSr{}
	go reportFlusherChannelFillPercent(ch, sr, "IdenT", time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if sr.called == 0 {
		t.Errorf("reportFlusherChannelFillPercent: statReporter should have been called a bunch of times")
	}
}

// func Test_Receiver_flushDs(t *testing.T) {
// 	// So we need to test that this calls queueblocking...
// 	r := &Receiver{flusherChs: make([]chan *dsFlushRequest, 1), flushLimiter: rate.NewLimiter(10, 10)}
// 	r.flusherChs[0] = make(chan *dsFlushRequest)
// 	called := 0
// 	var wg sync.WaitGroup
// 	wg.Add(1)
// 	go func() {
// 		defer wg.Done()
// 		for {
// 			if _, ok := <-r.flusherChs[0]; !ok {
// 				break
// 			}
// 			called++
// 		}
// 	}()
// 	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
// 	rra, _ := rrd.NewRoundRobinArchive(0, 0, "WMEAN", time.Second, 10, 10, 0, time.Time{})
// 	ds.SetRRAs([]*rrd.RoundRobinArchive{rra})
// 	ds.ProcessDataPoint(10, time.Unix(100, 0))
// 	ds.ProcessDataPoint(10, time.Unix(101, 0))
// 	rds := &receiverDs{DataSource: ds}
// 	r.SetMaxFlushRate(1)
// 	r.flushDs(rds, false)
// 	r.flushDs(rds, false)
// 	close(r.flusherChs[0])
// 	wg.Wait()
// 	if called != 1 {
// 		t.Errorf("flushDs call count not 1: %d", called)
// 	}
// 	if ds.PointCount() != 0 {
// 		t.Errorf("ClearRRAs was not called by flushDs")
// 	}
// }
