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

package rrd

import (
	"math"
	"time"
)

type Consolidation int

const (
	WMEAN Consolidation = iota // Time-weighted average
	MAX                        // Max
	MIN                        // Min
	LAST                       // Last
)

// RoundRobinArchive and all its parameters.
type RoundRobinArchive struct {
	Pdp
	// Consolidation function (CF). How data points from a
	// higher-resolution RRA are aggregated into a lower-resolution
	// one. Must be WMEAN, MAX, MIN, LAST.
	cf Consolidation
	// The RRA step
	step time.Duration
	// Number of data points in the RRA.
	size int64
	// Time at which most recent data point and the RRA end.
	latest time.Time
	// X-Files Factor (XFF). When consolidating, how much of the
	// higher-resolution RRA (as a value between 0 and 1) must be
	// known for the consolidated data not to be considered unknown.
	// Note that this is inverse of the RRDTool definition of
	// XFF. (This is because the Go zero-value works out as a nice
	// default, meaning we don't consider NaNs when consolidating).
	// NB: The name "X-Files Factor" comes from RRDTool where it was
	// named for being "unscientific", as it contracticts the rule
	// that any operation on a NaN is a NaN. We are not in complete
	// agreement with this, as the weighted consolidation logic gives
	// then NaN a 0 weight, and thus simply ignores it, not
	// contradicting any rules.
	xff float32

	// The list of data points (as a map so that it's sparse). Slots in
	// dps are time-aligned starting at zero time. This means that if
	// Latest is defined, we can compute any slot's timestamp without
	// having to store it. Slot numbers are aligned on millisecond,
	// therefore an RRA step cannot be less than a millisecond.
	dps map[int64]float64

	// Index of the first slot for which we have data. (Should be
	// between 0 and Size-1)
	start int64
	// Index of the last slot for which we have data. Note that it's
	// possible for end to be less than start, which means the RRD
	// wraps around.
	end int64
}

type RoundRobinArchiver interface {
	Pdper
	Latest() time.Time
	Step() time.Duration
	Size() int64
	Start() int64
	End() int64
	PointCount() int
	Dps() map[int64]float64
	Copy() RoundRobinArchiver
	Begins(now time.Time) time.Time

	// A side benifit from these being unexported is that you can only
	// satisfy this interface by including this implementation
	clear()
	includes(t time.Time) bool
	update(periodBegin, periodEnd time.Time, value float64, duration time.Duration)
}

func (rra *RoundRobinArchive) Latest() time.Time      { return rra.latest }
func (rra *RoundRobinArchive) Step() time.Duration    { return rra.step }
func (rra *RoundRobinArchive) Size() int64            { return rra.size }
func (rra *RoundRobinArchive) Start() int64           { return rra.start }
func (rra *RoundRobinArchive) End() int64             { return rra.end }
func (rra *RoundRobinArchive) Dps() map[int64]float64 { return rra.dps }

func NewRoundRobinArchive(spec *RRASpec) *RoundRobinArchive {
	return &RoundRobinArchive{
		step:   spec.Step,
		size:   spec.Span.Nanoseconds() / spec.Step.Nanoseconds(),
		xff:    spec.Xff,
		latest: spec.Latest,
		Pdp: Pdp{
			value:    spec.Value,
			duration: spec.Duration,
		},
		dps: make(map[int64]float64),
	}
}

func (rra *RoundRobinArchive) Copy() RoundRobinArchiver {
	new_rra := &RoundRobinArchive{
		Pdp:    Pdp{value: rra.value, duration: rra.duration},
		cf:     rra.cf,
		step:   rra.step,
		size:   rra.size,
		latest: rra.latest,
		xff:    rra.xff,
		start:  rra.start,
		end:    rra.end,
		dps:    make(map[int64]float64, len(rra.dps)),
	}
	for k, v := range rra.dps {
		new_rra.dps[k] = v
	}
	return new_rra
}

// Begins returns the timestamp of the beginning of this RRA assuming
// that that the argument "now" is within it. This will be a time
// approximately but not exactly the RRA length ago, because it is
// aligned on the RRA step boundary.
func (rra *RoundRobinArchive) Begins(now time.Time) time.Time {
	rraStart := now.Add(rra.step * time.Duration(rra.size) * -1).Truncate(rra.step)
	if now.Equal(now.Truncate(rra.step)) {
		rraStart = rraStart.Add(rra.step)
	}
	return rraStart
}

// PointCount returns the number of points in this RRA.
func (rra *RoundRobinArchive) PointCount() int {
	return len(rra.dps)
}

// Includes tells whether the given time is within the RRA
func (rra *RoundRobinArchive) includes(t time.Time) bool {
	begin := rra.Begins(rra.latest)
	return t.After(begin) && !t.After(rra.latest)
}

// update the RRA. If duration is less than the period, then the difference is considered unknown.
func (rra *RoundRobinArchive) update(periodBegin, periodEnd time.Time, value float64, duration time.Duration) {

	// currentBegin is a cursor pointing at the beginning of the
	// current slot, currentEnd points at its end. We start out
	// with currentBegin pointing at the slot one RRA-length ago
	// from periodEnd, then we move it up to periodBegin if it is
	// later. This way we end up with the latest of periodBegin or
	// rra-begin.
	currentBegin := rra.Begins(periodEnd)
	if periodBegin.After(currentBegin) {
		currentBegin = periodBegin
	}

	// for each RRA slot before periodEnd
	for currentBegin.Before(periodEnd) {

		endOfSlot := currentBegin.Truncate(rra.step).Add(rra.step)

		currentEnd := endOfSlot
		if currentEnd.After(periodEnd) { // i.e. currentEnd < endOfSlot
			currentEnd = periodEnd
		}

		switch rra.cf {
		case WMEAN:
			rra.AddValue(value, duration)
		case MAX:
			rra.AddValueMax(value, duration)
		case MIN:
			rra.AddValueMin(value, duration)
		case LAST:
			rra.AddValueLast(value, duration)
		}

		// if end of slot, move PDP into its place in dps.
		if currentEnd.Equal(endOfSlot) {
			rra.movePdpToDps(endOfSlot)
		}

		// move up the cursor
		currentBegin = currentEnd
	}
}

// movePdpToDps moves the PDP into its proper slot in the dps map and
// resets the PDP.
func (rra *RoundRobinArchive) movePdpToDps(endOfSlot time.Time) {
	// Check XFF
	known := float64(rra.duration) / float64(rra.step)
	if known < float64(rra.xff) {
		rra.SetValue(math.NaN(), 0)
	}

	if rra.dps == nil {
		rra.dps = make(map[int64]float64)
	}

	slotN := ((endOfSlot.UnixNano() / 1000000) / (rra.step.Nanoseconds() / 1000000)) % int64(rra.size)
	rra.latest = endOfSlot
	rra.dps[slotN] = rra.value

	if len(rra.dps) == 1 {
		rra.start = slotN
	}
	rra.end = slotN

	rra.Reset()
}

func (rra *RoundRobinArchive) clear() {
	if len(rra.dps) > 0 {
		rra.dps = make(map[int64]float64)
	}
	rra.start, rra.end = 0, 0
}

// RRASpec is the RRA definition part of DSSpec.
type RRASpec struct {
	Function Consolidation
	Step     time.Duration // duration of a single step
	Span     time.Duration // duration of the whole series (should be multiple of step)
	Xff      float32

	// These can be used to fill the initial value
	Latest   time.Time
	Value    float64
	Duration time.Duration
}
