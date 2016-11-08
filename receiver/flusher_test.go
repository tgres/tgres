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
	"github.com/tgres/tgres/rrd"
	"sync"
	"testing"
	"time"
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

	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
	rds := &receiverDs{DataSource: ds}
	fcs.queueBlocking(rds, true)

	if called != 1 {
		t.Errorf("id 0 should be send to flusher 0")
	}
}

// fake ds flusher
type fFlusher struct {
	called int
}

func (f *fFlusher) FlushDataSource(ds *rrd.DataSource) error {
	f.called++
	return fmt.Errorf("Fake error.")
}

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
	dsf := &fFlusher{}
	sr := &fakeSr{}
	fc := make(chan *dsFlushRequest)

	wc.startWg.Add(1)
	go flusher(wc, dsf, sr, fc)
	wc.startWg.Wait()

	ds := rrd.NewDataSource(0, "", 0, 0, time.Time{}, 0)
	resp := make(chan bool)
	fc <- &dsFlushRequest{ds: ds, resp: resp}
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
