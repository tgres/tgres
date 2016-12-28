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

// Package aggregator provides the ability to aggregate data points
// from various sources similar to statsd. On flush, aggregator passes
// the consolidated values to a DataPointQueuer
// (e.g. tgres.receiver). The aggregator only aggregates the data, it
// does not concern itself with the periodic flushing, that is the job
// of its user.
package aggregator

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"sort"
	"time"
)

type DataPointQueuer interface {
	QueueDataPoint(string, time.Time, float64)
}

type aggKind int

const (
	aggKindValue aggKind = iota
	aggKindGauge
	aggKindList
)

type aggregation struct {
	kind  aggKind
	value float64
	list  []float64
}

// The Aggregator keeps the intermediate state for all data that is
// being aggregated.
type Aggregator interface {
	// Process an aggregator command, which is a data point with insturctions on how to process it.
	ProcessCmd(cmd *Command)
	// Flush all aggregations to the undelying DataPointQueuer. If now is zero, time.Now() is used.
	// All internal state is cleared after a flush.
	Flush(now time.Time)
}

type State struct {
	t          DataPointQueuer
	m          map[string]*aggregation
	lastFlush  time.Time
	Thresholds []int // List of percentiles for CmdAppend
}

// Returns a new aggregator. The only argument needs to provide a
// QueueDataPoint() method which is what the aggregator will use to
// queue the aggregated points. The returned aggregator state has
// Thresholds set to {90}.
func NewAggregator(t DataPointQueuer) *State {
	return &State{
		t:          t,
		m:          make(map[string]*aggregation),
		lastFlush:  time.Now(),
		Thresholds: []int{90},
	}
}

// Add to an already existing value at key name, created as
// 0.0/aggKindValue if not existing.
func (a *State) add(name string, value float64) {
	if a.m[name] == nil {
		a.m[name] = &aggregation{kind: aggKindValue}
	}
	a.m[name].value += value
}

// Add to an already existing value at key name, created as
// 0.0/aggKindGauge if not existing.
func (a *State) addGauge(name string, value float64) {
	if a.m[name] == nil {
		a.m[name] = &aggregation{kind: aggKindGauge}
	}
	a.m[name].value += value
}

// Set the value at key name overwriting any previous, created as
// 0.0./aggKindGauge if not existing
func (a *State) setGauge(name string, value float64) {
	if a.m[name] == nil {
		a.m[name] = &aggregation{kind: aggKindGauge, value: value}
	} else {
		a.m[name].value = value
	}
}

// Append to values at key name, created as aggKindList if not
// existing.
func (a *State) append(name string, value float64) {
	if a.m[name] == nil {
		a.m[name] = &aggregation{kind: aggKindList, list: make([]float64, 0, 2)}
	}
	if a.m[name].list != nil {
		a.m[name].list = append(a.m[name].list, value)
	}
}

func (a *State) ProcessCmd(cmd *Command) {
	if !cmd.ts.IsZero() && cmd.ts.Before(a.lastFlush) {
		return // this command is too old for this aggregator, ignore it
	}
	switch cmd.cmd {
	case CmdAdd:
		a.add(cmd.name, cmd.value)
	case CmdAddGauge:
		a.addGauge(cmd.name, cmd.value)
	case CmdSetGauge:
		a.setGauge(cmd.name, cmd.value)
	case CmdAppend:
		a.append(cmd.name, cmd.value)
	}
}

func (a *State) Flush(now time.Time) {
	if now.IsZero() {
		now = time.Now()
	}

	for name, agg := range a.m {

		switch agg.kind {
		case aggKindValue:
			// store rate
			if now.After(a.lastFlush) {
				a.t.QueueDataPoint(name, now, agg.value/now.Sub(a.lastFlush).Seconds())
			}

		case aggKindGauge:
			// store as is
			a.t.QueueDataPoint(name, now, agg.value)

		case aggKindList:
			list := agg.list

			// count
			a.t.QueueDataPoint(name+".count", now, float64(len(list)))

			// lower, upper, sum, mean
			if len(list) > 0 {
				sort.Float64s(list)

				cumul := make([]float64, len(list))
				for n, v := range list {
					cumul[n] += v
				}

				a.t.QueueDataPoint(name+".lower", now, list[0])
				a.t.QueueDataPoint(name+".upper", now, list[len(list)-1])
				a.t.QueueDataPoint(name+".sum", now, cumul[len(list)-1])
				a.t.QueueDataPoint(name+".mean", now, cumul[len(list)-1]/float64(len(list)))

				// make a little round() since Go doesn't have one...
				round := func(f float64) int {
					return int(math.Floor(f + .5))
				}

				// TODO may be add "median" and "std"?
				for _, threshold := range a.Thresholds {
					idx := round(float64(threshold)/100*float64(len(list))) - 1
					a.t.QueueDataPoint(name+fmt.Sprintf(".sum_%02d", threshold), now, cumul[idx])
					a.t.QueueDataPoint(name+fmt.Sprintf(".mean_%02d", threshold), now, cumul[idx]/float64(idx+1))
					a.t.QueueDataPoint(name+fmt.Sprintf(".upper_%02d", threshold), now, list[idx])
				}
			}
		}
	}

	// clear the map
	a.m = make(map[string]*aggregation)
	a.lastFlush = now
}

type AggCmd int

const (
	CmdAdd      AggCmd = iota // Add the value, the flushed value is a per second rate.
	CmdAddGauge               // Add the value, the flushed value is the sum as is (e.g. total traffic for all routers).
	CmdSetGauge               // Overwrite the value, the flushed value is the last value as is.
	CmdAppend                 // Append the value to a slice. The flushed values will be upper/lower/sum/mean and Threshold percentiles.
)

// An aggregator command. Use NewCommand() to create one.
type Command struct {
	cmd   AggCmd
	name  string
	value float64
	ts    time.Time
	Hops  int // For cluster forwarding
}

func (ac *Command) GobEncode() ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	var err error
	check := func(er error) {
		if er != nil && err == nil {
			err = er
		}
	}
	check(enc.Encode(ac.cmd))
	check(enc.Encode(ac.name))
	check(enc.Encode(ac.value))
	check(enc.Encode(ac.ts))
	check(enc.Encode(ac.Hops))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (ac *Command) GobDecode(b []byte) error {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var err error
	check := func(er error) {
		if er != nil && err == nil {
			err = er
		}
	}
	check(dec.Decode(&ac.cmd))
	check(dec.Decode(&ac.name))
	check(dec.Decode(&ac.value))
	check(dec.Decode(&ac.ts))
	check(dec.Decode(&ac.Hops))
	return err
}

// Create an aggregator command. The cmd argument dictates how the
// data will be aggregated, see AggCmd.
func NewCommand(cmd AggCmd, name string, value float64) *Command {
	return &Command{cmd: cmd, name: name, value: value, ts: time.Now()}
}
