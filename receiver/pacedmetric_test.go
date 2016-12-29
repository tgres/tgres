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
	"log"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/rrd"
)

type fakeAggregatorCommandQueuer struct {
	qacCalled int
}

func (f *fakeAggregatorCommandQueuer) QueueAggregatorCommand(*aggregator.Command) {
	f.qacCalled++
}

type fakeDataPointQueuer struct {
	qdpCalled int
}

func (f *fakeDataPointQueuer) QueueDataPoint(string, time.Time, float64) {
	f.qdpCalled++
}

func Test_pacedMetricFlush(t *testing.T) {

	sums := map[string]float64{"foo": 123}
	gauges := make(map[string]*rrd.ClockPdp)
	gauges["bar"] = &rrd.ClockPdp{}
	acq := &fakeAggregatorCommandQueuer{}
	dpq := &fakeDataPointQueuer{}

	sums = pacedMetricFlush(sums, gauges, acq, dpq)

	if len(sums) > 0 {
		t.Errorf("pacedMetricFlush did not return empty sums")
	}

	if acq.qacCalled == 0 {
		t.Errorf("QueueAggregatorCommand wasn't called")
	}

	if dpq.qdpCalled == 0 {
		t.Errorf("QueueDataPoint wasn't called")
	}
}

func Test_pacedMetricPeriodicFlushSignal(t *testing.T) {

	fl := &fakeLogger{}
	log.SetOutput(fl)

	defer func() {
		log.SetOutput(os.Stderr) // restore default output
	}()

	var flushCh = make(chan bool, 1)
	go pacedMetricPeriodicFlushSignal(flushCh, 2*time.Millisecond, "signaltest")

	time.Sleep(50 * time.Millisecond)
	if len(flushCh) != 1 {
		t.Errorf("len(flushCh) != 1")
	}
	if !strings.Contains(string(fl.last), "dropping") {
		t.Errorf("Missing log entry with 'dropping'")
	}

	// drain the channel
	go func() {
		for {
			<-flushCh
		}
	}()

	close(flushCh)
	time.Sleep(10 * time.Millisecond) // cause panic/recover
}

func Test_pacedMetricWorker(t *testing.T) {

	fl := &fakeLogger{}
	log.SetOutput(fl)

	defer func() {
		log.SetOutput(os.Stderr) // restore default output
	}()

	ident := "pacedident"
	wc := &wrkCtl{wg: &sync.WaitGroup{}, startWg: &sync.WaitGroup{}, id: ident}
	pmCh := make(chan *pacedMetric)
	acq := &fakeAggregatorCommandQueuer{}
	dpq := &fakeDataPointQueuer{}

	saveFn1, saveFn2 := pacedMetricFlush, pacedMetricPeriodicFlushSignal
	var saveGauges map[string]*rrd.ClockPdp
	var saveSums map[string]float64
	pacedMetricFlush = func(sums map[string]float64, gauges map[string]*rrd.ClockPdp, acq aggregatorCommandQueuer, dpq dataPointQueuer) map[string]float64 {
		saveGauges = gauges
		saveSums = sums
		return sums
	}

	pacedMetricPeriodicFlushSignal = func(flushCh chan bool, frequency time.Duration, ident string) {
		for {
			flushCh <- true
		}
	}

	sr := &fakeSr{}

	wc.startWg.Add(1)
	go pacedMetricWorker(wc, pmCh, acq, dpq, 2*time.Millisecond, sr)
	wc.startWg.Wait()

	pmCh <- &pacedMetric{pacedSum, "bar", 123}
	pmCh <- &pacedMetric{pacedGauge, "bar", 123}

	time.Sleep(10 * time.Millisecond)

	close(pmCh) // should cause flush

	wc.wg.Wait()

	if _, ok := saveSums["bar"]; !ok {
		t.Errorf("sums['bar'] missing")
	}
	if _, ok := saveGauges["bar"]; !ok {
		t.Errorf("gauges['bar'] missing")
	}

	pacedMetricFlush, pacedMetricPeriodicFlushSignal = saveFn1, saveFn2
}

func Test_paced_reportPaceMetricChannelFillPercent(t *testing.T) {
	ch := make(chan *pacedMetric, 4)
	sr := &fakeSr{}
	go reportPacedMetricChannelFillPercent(ch, sr, time.Millisecond)
	time.Sleep(50 * time.Millisecond)
	if sr.called == 0 {
		t.Errorf("reportPacedMetricChannelFillPercent: statReporter should have been called a bunch of times")
	}
}
