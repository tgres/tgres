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

// Package receiver manages the receiving end of the data. All of the
// queueing, caching, perioding flushing and cluster forwarding logic
// is here.
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

var pacedMetricWorker = func(wc wController, pacedMetricCh chan *pacedMetric, acq aggregatorCommandQueuer, dpq dataPointQueuer, frequency time.Duration) {
	wc.onEnter()
	defer wc.onExit()

	sums := make(map[string]float64)
	gauges := make(map[string]*rrd.ClockPdp)

	flush := func() {
		for name, sum := range sums {
			acq.QueueAggregatorCommand(aggregator.NewCommand(aggregator.CmdAdd, name, sum))
		}
		for name, gauge := range gauges {
			dpq.QueueDataPoint(name, gauge.End, gauge.Reset())
		}
		sums = make(map[string]float64)
		// NB: We do not reset gauges, they need to live on
	}

	var flushCh = make(chan bool, 1)
	go func() {
		for {
			time.Sleep(frequency)
			if len(flushCh) == 0 {
				flushCh <- true
			} else {
				log.Printf("%s: dropping flush timer on the floor - busy system?", wc.ident())
			}
		}
	}()

	log.Printf("%s: started.", wc.ident())
	wc.onStarted()

	for {
		select {
		case <-flushCh:
			flush()
		case ps, ok := <-pacedMetricCh:
			if !ok {
				flush()
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
