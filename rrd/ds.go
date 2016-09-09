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
	"fmt"
	"math"
	"time"
)

// DataSource describes a time series and its parameters, RRA and
// intermediate state (PDP).
type DataSource struct {
	Pdp
	id         int64                // Id
	name       string               // Series name
	step       time.Duration        // Step (PDP) size
	heartbeat  time.Duration        // Heartbeat is inactivity period longer than this causes NaN values. 0 -> no heartbeat.
	lastUpdate time.Time            // Last time we received an update (series time - can be in the past or future)
	lastDs     float64              // Last final value we saw
	rras       []*RoundRobinArchive // Array of Round Robin Archives
}

// NewDataSource returns a pointer to a new DataSource. This function
// is meant primarily for internal use such as serde implementations.
func NewDataSource(id int64, name string, step, hb time.Duration, lu time.Time, lds float64) *DataSource {
	return &DataSource{
		id:         id,
		name:       name,
		step:       step,
		heartbeat:  hb,
		lastUpdate: lu,
		lastDs:     lds,
	}
}

func (ds *DataSource) Name() string                      { return ds.name }
func (ds *DataSource) Id() int64                         { return ds.id }
func (ds *DataSource) Step() time.Duration               { return ds.step }
func (ds *DataSource) Heartbeat() time.Duration          { return ds.heartbeat }
func (ds *DataSource) LastUpdate() time.Time             { return ds.lastUpdate }
func (ds *DataSource) LastDs() float64                   { return ds.lastDs }
func (ds *DataSource) RRAs() []*RoundRobinArchive        { return ds.rras }
func (ds *DataSource) SetRRAs(rras []*RoundRobinArchive) { ds.rras = rras }

// BestRRA examines the RRAs and returns the one that best matches the
// given start, end and resolution (as number of points).
func (ds *DataSource) BestRRA(start, end time.Time, points int64) *RoundRobinArchive {
	var result []*RoundRobinArchive

	// Any RRA include start?
	for _, rra := range ds.rras {
		// We need to include RRAs that were last updated before start too
		// or we end up with nothing, then the lowest resolution RRA
		if rra.Includes(start) || rra.Latest().Before(start) {
			result = append(result, rra)
		}
	}

	if len(result) == 0 { // if we found nothing above, simply select the longest RRA
		var longest *RoundRobinArchive
		for _, rra := range ds.rras {
			if longest == nil || longest.size*int64(longest.step) < rra.size*int64(rra.step) {
				longest = rra
			}
		}
		if longest != nil {
			result = append(result, longest)
		}
	}

	if len(result) == 1 {
		return result[0] // nothing else to do
	}

	if len(result) > 1 {
		if points > 0 {
			// select the one with the closest matching resolution
			expectedStep := end.Sub(start) / time.Duration(points)
			var best *RoundRobinArchive
			for _, rra := range result {
				if best == nil {
					best = rra
				} else {
					rraDiff := math.Abs(float64(expectedStep - rra.step))
					bestDiff := math.Abs(float64(expectedStep - best.step))
					if bestDiff > rraDiff {
						best = rra
					}
				}
			}
			return best
		} else { // no points specified, select maximum resolution (i.e. smallest step)
			var best *RoundRobinArchive
			for _, rra := range result {
				if best == nil {
					best = rra
				} else {
					if best.step > rra.step {
						best = rra
					}
				}
			}
			return best
		}
	}

	// Sorry, nothing
	return nil
}

// PointCount returns the sum of all point counts of every RRA in this
// DS.
func (ds *DataSource) PointCount() int {
	total := 0
	for _, rra := range ds.rras {
		total += rra.PointCount()
	}
	return total
}

// surroundingStep returns begin and end of a PDP which either
// includes or ends on a given time mark.
func surroundingStep(mark time.Time, step time.Duration) (time.Time, time.Time) {
	begin := mark.Truncate(step)
	if mark.Equal(begin) { // We are exactly at the end, need to move one step back.
		begin = begin.Add(step * -1)
	}
	return begin, begin.Add(step)
}

// updateRange takes a range given to it (which can be less than a PDP
// or span multiple PDPs) and performs at most 3 updates to the RRAs:
//
//        [1]                 [2] [3]
//      ‖--|------- ... -------|---‖    the update range
//   |-----|-----|- ... -|-----|-----|  ---> time
//
// 1 - for the remaining piece of the first PDP in the range
// 2 - for all the full PDPs in between
// 3 - for the starting piece of the last PDP
func (ds *DataSource) updateRange(begin, end time.Time, value float64) {

	// Begin and end of the last (possibly partial) PDP in the range.
	endPdpBegin, endPdpEnd := surroundingStep(end, ds.step)

	// If the range begins *before* the last PDP, or ends *exactly* on
	// the end of a PDP, then at least one PDP is now completed, and
	// updates need to trickle down to RRAs.
	if begin.Before(endPdpBegin) || end.Equal(endPdpEnd) {

		// If range begins in the middle of a now completed PDP
		// (which may be the last one IFF end == endPdpEnd)
		if begin.Truncate(ds.step) != begin {

			// periodBegin and periodEnd mark the PDP beginning just
			// before the beginning of the range. periodEnd points at
			// the end of the first PDP or end of the last PDP if (and
			// only if) end == endPdpEnd.
			periodBegin := begin.Truncate(ds.step)
			periodEnd := periodBegin.Add(ds.step)
			offset := periodEnd.Sub(begin)
			ds.AddValue(value, offset)

			// Update the RRAs
			ds.updateRRAs(periodBegin, periodEnd)

			// The DS value now becomes zero, it has been "sent" to RRAs.
			ds.Reset()

			begin = periodEnd
		}

		// Note that "begin" has been modified just above and is now
		// aligned on a PDP boundary. If the (new) range still begins
		// before the last PDP, or is exactly the last PDP, then we
		// have 1+ whole PDPs in the range. (Since begin is now
		// aligned, the only other possibility is begin == endPdpEnd,
		// thus the code could simply be "if begin != endPdpEnd", but
		// we go extra expressive for clarity).
		if begin.Before(endPdpBegin) || (begin.Equal(endPdpBegin) && end.Equal(endPdpEnd)) {

			ds.SetValue(value, ds.step) // Since begin is aligned, we can set the whole value.

			periodBegin := begin
			periodEnd := endPdpBegin
			if end.Equal(end.Truncate(ds.step)) {
				periodEnd = end
			}
			ds.updateRRAs(periodBegin, periodEnd)

			// The DS value now becomes zero, it has been "sent" to RRAs.
			ds.Reset()

			// Advance begin to the aligned end
			begin = periodEnd
		}
	}

	// If there is still a small part of an incomlete PDP between
	// begin and end, update the PDP value.
	if begin.Before(end) {
		ds.AddValue(value, end.Sub(begin))
	}
}

// ProcessIncomingDataPoint checks the values and updates the DS
// PDP. If this the very first call for this DS (lastUpdate is 0),
// then it only sets lastUpdate and returns.
func (ds *DataSource) ProcessIncomingDataPoint(value float64, ts time.Time) error {

	if math.IsInf(value, 0) {
		return fmt.Errorf("±Inf is not a valid data point value: %v", value)
	}

	if ts.Before(ds.lastUpdate) {
		return fmt.Errorf("Data point time stamp %v is not greater than data source last update time %v", ts, ds.lastUpdate)
	}

	// ds value is NaN if HB is exceeded
	if ds.heartbeat > 0 && ts.Sub(ds.lastUpdate) > ds.heartbeat {
		value = math.NaN()
	}

	if !ds.lastUpdate.IsZero() { // Do not update a never-before-updated DS
		ds.updateRange(ds.lastUpdate, ts, value)
	}

	ds.lastUpdate = ts
	ds.lastDs = value

	return nil
}

func (ds *DataSource) updateRRAs(periodBegin, periodEnd time.Time) {
	// for each of this DS's RRAs
	for _, rra := range ds.rras {
		rra.update(periodBegin, periodEnd, ds.value, ds.duration)
	}
}

// ClearRRAs clears the data in all RRAs. It is meant to be called
// immedately after flushing the DS to permanent storage. If clearLU
// flag is true, then lastUpdate will get zeroed-out. (Normally you do
// not want to reset lastUpdate so that PDP is updated correctly). A
// DS with a zero lastUpdate is never saved, this is a prevention
// measure for nodes that are only forwarding events, preventing them
// from at some point saving their stale state and overwriting good
// data..
func (ds *DataSource) ClearRRAs(clearLU bool) {
	for _, rra := range ds.rras {
		if len(rra.dps) > 0 {
			rra.dps = make(map[int64]float64)
		}
		rra.start, rra.end = 0, 0
	}
	if clearLU {
		ds.lastUpdate = time.Time{}
	}
}

// Copy returns a copy of the DataSource.
func (ds *DataSource) Copy() *DataSource {

	new_ds := &DataSource{
		Pdp:        Pdp{value: ds.value, duration: ds.duration},
		id:         ds.id,
		name:       ds.name,
		step:       ds.step,
		heartbeat:  ds.heartbeat,
		lastUpdate: ds.lastUpdate,
		lastDs:     ds.lastDs,
		rras:       make([]*RoundRobinArchive, len(ds.rras)),
	}

	for n, rra := range ds.rras {
		new_ds.rras[n] = rra.copy()
	}

	return new_ds
}
