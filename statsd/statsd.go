//
// Copyright 2015 Gregory Trubetskoy. All Rights Reserved.
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

package statsd

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"github.com/tgres/tgres/misc"
	"math"
	"strings"
	"time"
)

// This is the transceiver really
type dataPointQueuer interface {
	QueueDataPoint(string, time.Time, float64)
}

type Aggregator struct {
	t         dataPointQueuer
	prefix    string
	counts    map[string]int64
	gauges    map[string]float64
	timers    map[string][]float64
	lastFlush time.Time
}

func NewAggregator(t dataPointQueuer, prefix string) *Aggregator {
	return &Aggregator{
		t:         t,
		prefix:    prefix,
		counts:    make(map[string]int64),
		gauges:    make(map[string]float64),
		timers:    make(map[string][]float64),
		lastFlush: time.Now(),
	}
}

func (a *Aggregator) Flush() {
	for name, count := range a.counts {
		dur := time.Now().Sub(a.lastFlush)
		perSec := float64(count) / dur.Seconds()
		a.t.QueueDataPoint(a.prefix+"."+name, time.Now(), perSec)
	}
	for name, gauge := range a.gauges {
		a.t.QueueDataPoint(a.prefix+".gauges."+name, time.Now(), gauge)
	}
	for name, times := range a.timers {
		// count
		a.t.QueueDataPoint(a.prefix+".timers."+name+".count", time.Now(), float64(len(times)))

		// lower, upper, sum, mean
		if len(times) > 0 {
			var (
				lower, upper = times[0], times[0]
				sum          float64
			)

			for _, v := range times[1:] {
				lower = math.Min(lower, v)
				upper = math.Max(upper, v)
				sum += v
			}
			a.t.QueueDataPoint(a.prefix+".timers."+name+".lower", time.Now(), lower)
			a.t.QueueDataPoint(a.prefix+".timers."+name+".upper", time.Now(), upper)
			a.t.QueueDataPoint(a.prefix+".timers."+name+".sum", time.Now(), sum)
			a.t.QueueDataPoint(a.prefix+".timers."+name+".mean", time.Now(), sum/float64(len(times)))
		}

		// TODO - these will require sorting:
		// count_ps ?
		// mean_90
		// median
		// std
		// sum_90
		// upper_90

	}
	// clear the maps
	a.counts = make(map[string]int64)
	a.gauges = make(map[string]float64)
	a.timers = make(map[string][]float64)
	a.lastFlush = time.Now()
}

func (a *Aggregator) Process(st *Stat) error {
	if st.Metric == "c" {
		if _, ok := a.counts[st.Name]; !ok {
			a.counts[st.Name] = 0
		}
		a.counts[st.Name] += int64(st.Value)
	} else if st.Metric == "g" {
		a.gauges[st.Name] = st.Value
	} else if st.Metric == "ms" {
		if _, ok := a.timers[st.Name]; !ok {
			a.timers[st.Name] = make([]float64, 4)
		}
		a.timers[st.Name] = append(a.timers[st.Name], st.Value)
	} else {
		return fmt.Errorf("invalid metric type: %q, ignoring.", st.Metric)
	}
	return nil
}

type Stat struct {
	Name   string
	Value  float64
	Metric string
	Sample float64
	Hops   int
}

// TODO Why do we need these if all the members are public
// base types?
func (st *Stat) GobEncode() ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	var err error
	check := func(er error) {
		if er != nil && err == nil {
			err = er
		}
	}
	check(enc.Encode(st.Name))
	check(enc.Encode(st.Value))
	check(enc.Encode(st.Metric))
	check(enc.Encode(st.Sample))
	check(enc.Encode(st.Hops))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (st *Stat) GobDecode(b []byte) error {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var err error
	check := func(er error) {
		if er != nil && err == nil {
			err = er
		}
	}
	check(dec.Decode(&st.Name))
	check(dec.Decode(&st.Value))
	check(dec.Decode(&st.Metric))
	check(dec.Decode(&st.Sample))
	check(dec.Decode(&st.Hops))
	return err
}

// ParseStatsdPacket parses a statsd packet e.g: gorets:1|c|@0.1. See
// https://github.com/etsy/statsd/blob/master/docs/metric_types.md
// There is no need to support multi-metric packets here, since it
// uses newline as separator, the text handler in daemon/services.go
// would take care of it.
func ParseStatsdPacket(packet string) (*Stat, error) {

	var (
		result = &Stat{}
		parts  []string
	)

	parts = strings.Split(packet, ":")
	if len(parts) < 1 {
		return nil, fmt.Errorf("invalid packet: %q", packet)
	}

	result.Name = misc.SanitizeName(parts[0])
	if len(parts) == 1 {
		result.Value, result.Metric = 1, "c"
		return result, nil
	}

	parts = strings.Split(parts[1], "|")
	if len(parts) < 2 {
		return nil, fmt.Errorf("invalid packet: %q", packet)
	}

	if n, err := fmt.Sscanf(parts[0], "%f", &result.Value); n != 1 || err != nil {
		return nil, fmt.Errorf("error %v scanning input (cannot parse value|metric): %q", err, packet)
	}
	result.Metric = parts[1]

	if len(parts) > 2 {
		if n, err := fmt.Sscanf(parts[2], "@%f", &result.Sample); n != 1 || err != nil {
			return nil, fmt.Errorf("error %v scanning input (bad @sample?): %q", err, packet)
		}
	}

	return result, nil
}
