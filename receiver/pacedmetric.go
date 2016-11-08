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
	"github.com/tgres/tgres/aggregator"
	"github.com/tgres/tgres/rrd"
	"log"
	"time"
)

type pacedMetricType int

const (
	pacedSum pacedMetricType = iota
	pacedGauge
)

type pacedMetric struct {
	kind  pacedMetricType
	name  string
	value float64
}

var pacedMetricFlush = func(sums map[string]float64, gauges map[string]*rrd.ClockPdp, acq aggregatorCommandQueuer, dpq dataPointQueuer) map[string]float64 {
	for name, sum := range sums {
		acq.QueueAggregatorCommand(aggregator.NewCommand(aggregator.CmdAdd, name, sum))
	}
	for name, gauge := range gauges {
		dpq.QueueDataPoint(name, gauge.End, gauge.Reset())
	}
	// NB: We do not reset the gauges map, it lives on
	return make(map[string]float64)
}

var pacedMetricPeriodicFlushSignal = func(flushCh chan bool, frequency time.Duration, ident string) {
	defer func() { recover() }()
	for {
		time.Sleep(frequency)
		if len(flushCh) == 0 {
			flushCh <- true
		} else {
			log.Printf("%s: dropping flush timer on the floor - busy system?", ident)
		}
	}
}

func reportPacedMetricChannelFillPercent(pacedMetricCh chan *pacedMetric, sr statReporter) {
	fillStatName := "receiver.pacedmetric.channel.fill_percent"
	lenStatName := "receiver.pacedmetric.channel.len"
	cp := float64(cap(pacedMetricCh))
	for {
		time.Sleep(time.Second)
		ln := float64(len(pacedMetricCh))
		if cp > 0 {
			fillPct := (ln / cp) * 100
			sr.reportStatGauge(fillStatName, fillPct)
		}
		sr.reportStatGauge(lenStatName, ln)
	}
}

var pacedMetricWorker = func(wc wController, pacedMetricCh chan *pacedMetric, acq aggregatorCommandQueuer, dpq dataPointQueuer, frequency time.Duration, sr statReporter) {
	wc.onEnter()
	defer wc.onExit()

	sums := make(map[string]float64)
	gauges := make(map[string]*rrd.ClockPdp)

	var flushCh = make(chan bool, 1)
	go pacedMetricPeriodicFlushSignal(flushCh, frequency, wc.ident())

	go reportPacedMetricChannelFillPercent(pacedMetricCh, sr)

	log.Printf("%s: started.", wc.ident())
	wc.onStarted()

	for {
		select {
		case <-flushCh:
			sums = pacedMetricFlush(sums, gauges, acq, dpq)
		case ps, ok := <-pacedMetricCh:
			if !ok {
				sums = pacedMetricFlush(sums, gauges, acq, dpq)
				return
			} else {
				switch ps.kind {
				case pacedSum:
					sums[ps.name] += ps.value
				case pacedGauge:
					if _, ok := gauges[ps.name]; !ok {
						gauges[ps.name] = &rrd.ClockPdp{}
					}
					gauges[ps.name].AddValue(ps.value)
				}
			}
		}
	}
}
