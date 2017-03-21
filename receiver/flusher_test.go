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

type fakeDsFlusher struct {
	called int
	sr     statReporter
}

func (f *fakeDsFlusher) flushDS(ds serde.DbDataSourcer, block bool)         { f.called++ }
func (f *fakeDsFlusher) flushToVCache(serde.DbDataSourcer)                  {}
func (f *fakeDsFlusher) enabled() bool                                      { return true }
func (f *fakeDsFlusher) flusher() serde.Flusher                             { return f }
func (f *fakeDsFlusher) statReporter() statReporter                         { return f.sr }
func (f *fakeDsFlusher) start(_, _ *sync.WaitGroup, _ time.Duration, n int) {}
func (f *fakeDsFlusher) stop()                                              {}
func (f *fakeDsFlusher) FlushDataSource(ds rrd.DataSourcer) error {
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

func Test_flusher_dsFlusher_basicOp(t *testing.T) {

	// TODO: What are we testing here?

	sr := &fakeSr{}
	flusherWg, startWg := &sync.WaitGroup{}, &sync.WaitGroup{}
	dsf := &dsFlusher{flusherCh: make(flusherChannel), sr: sr} //, vdb: serde.VerticalFlusher(), sr: r}
	dsf.start(flusherWg, startWg, time.Second, 1)
	startWg.Wait()

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
	resp := make(chan bool)
	dsf.flusherCh <- &dsFlushRequest{ds: ds, resp: resp}
	<-resp

	dsf.stop()
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
}
