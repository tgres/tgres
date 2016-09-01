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
	"bytes"
	"fmt"
	"strconv"
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
	id   int64 // Id
	dsId int64 // DS id
	// Consolidation function (CF). How data points from a
	// higher-resolution RRA are aggregated into a lower-resolution
	// one. Must be WMEAN, MAX, MIN, LAST.
	cf Consolidation
	// A single "row" (i.e. a single value) span in DS steps.
	stepsPerRow int64
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

	// The list of data points (as a map so that its sparse). Slots in
	// dps are time-aligned starting at zero time. This means that if
	// Latest is defined, we can compute any slot's timestamp without
	// having to store it.
	dps map[int64]float64

	// In the undelying storage, how many data points are stored in a
	// single (database) row.
	width int64
	// Index of the first slot for which we have data. (Should be
	// between 0 and Size-1)
	start int64
	// Index of the last slot for which we have data. Note that it's
	// possible for end to be less than start, which means the RRD
	// wraps around.
	end int64
}

func (rra *RoundRobinArchive) Id() int64         { return rra.id }
func (rra *RoundRobinArchive) Latest() time.Time { return rra.latest }
func (rra *RoundRobinArchive) Size() int64       { return rra.size }
func (rra *RoundRobinArchive) Width() int64      { return rra.width }
func (rra *RoundRobinArchive) Start() int64      { return rra.start }
func (rra *RoundRobinArchive) End() int64        { return rra.end }

// NewRoundRobinArchive returns a pointer to a new RRAs. It is mostly
// useful for serde implementations.
func NewRoundRobinArchive(id, dsId int64, cf string, stepsPerRow, size, width int64, xff float32, latest time.Time) (*RoundRobinArchive, error) {
	rra := &RoundRobinArchive{
		id:          id,
		dsId:        dsId,
		stepsPerRow: stepsPerRow,
		size:        size,
		width:       width,
		xff:         xff,
		latest:      latest,
		dps:         make(map[int64]float64),
	}
	switch cf {
	case "WMEAN":
		rra.cf = WMEAN
	case "MIN":
		rra.cf = MIN
	case "MAX":
		rra.cf = MAX
	case "LAST":
		rra.cf = LAST
	default:
		return nil, fmt.Errorf("Invalid cf: %q (valid funcs: wmean, min, max, last)", cf)
	}
	return rra, nil
}

// Step returns the RRA step as time.Duration given the DS step as
// argument. (RRA itself only knows its step as a number of DS steps).
func (rra *RoundRobinArchive) Step(dsStep time.Duration) time.Duration {
	return dsStep * time.Duration(rra.stepsPerRow)
}

func (rra *RoundRobinArchive) mostlyCopy() *RoundRobinArchive {

	// Only copy elements that change or needed for saving/rendering
	new_rra := new(RoundRobinArchive)
	new_rra.id = rra.id
	new_rra.dsId = rra.dsId
	new_rra.stepsPerRow = rra.stepsPerRow
	new_rra.size = rra.size
	new_rra.value = rra.value
	new_rra.duration = rra.duration
	new_rra.latest = rra.latest
	new_rra.start = rra.start
	new_rra.end = rra.end
	new_rra.size = rra.size
	new_rra.width = rra.width
	new_rra.dps = make(map[int64]float64)

	for k, v := range rra.dps {
		new_rra.dps[k] = v
	}

	return new_rra
}

// SlotRow returns the row number given a slot number. This is mostly
// useful in serde implementations.
func (rra *RoundRobinArchive) SlotRow(slot int64) int64 {
	if slot%rra.width == 0 {
		return slot / rra.width
	} else {
		return (slot / rra.width) + 1
	}
}

// Begins returns the timestamp of the beginning of this RRA assuming
// that that the argument "now" is within it. This will be a time
// approximately but not exactly the RRA length ago, because it is
// aligned on the RRA step boundary.
func (rra *RoundRobinArchive) Begins(now time.Time, rraStep time.Duration) time.Time {
	rraStart := now.Add(rraStep * time.Duration(rra.size) * -1).Truncate(rraStep)
	if now.Equal(now.Truncate(rraStep)) {
		rraStart = rraStart.Add(rraStep)
	}
	return rraStart
}

// DpsAsPGString returns data points as a PostgreSQL-compatible array string
func (rra *RoundRobinArchive) DpsAsPGString(start, end int64) string {
	var b bytes.Buffer
	b.WriteString("{")
	for i := start; i <= end; i++ {
		b.WriteString(strconv.FormatFloat(rra.dps[int64(i)], 'f', -1, 64))
		if i != end {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.String()
}

// PointCount returns the number of points in this RRA.
func (rra *RoundRobinArchive) PointCount() int {
	return len(rra.dps)
}
