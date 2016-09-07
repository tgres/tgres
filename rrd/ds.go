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
	heartbeat  time.Duration        // Heartbeat is inactivity period longer than this causes NaN values
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

	for _, rra := range ds.rras {
		// is start within this RRA's range?
		rraBegin := rra.latest.Add(rra.step * time.Duration(rra.size) * -1)
		if start.After(rraBegin) {
			result = append(result, rra)
		}
	}

	if len(result) == 0 {
		// if we found nothing above, simply select the longest RRA
		var longest *RoundRobinArchive
		for _, rra := range ds.rras {
			if longest == nil || longest.size*int64(longest.step) < rra.size*int64(rra.step) {
				longest = rra
			}
		}
		result = append(result, longest)
	}

	if len(result) > 1 && points > 0 {
		// select the one with the closest matching resolution
		expectedStep := end.Sub(start) / time.Duration(points)
		var best *RoundRobinArchive
		for _, rra := range result {
			if best == nil {
				best = rra
			} else {
				rraDiff := math.Abs(float64(expectedStep - rra.step))
				bestDiff := math.Abs(float64(expectedStep - rra.step))
				if bestDiff > rraDiff {
					best = rra
				}
			}
		}
		return best
	} else if len(result) == 1 {
		return result[0]
	} else {
		// select maximum resolution (i.e. smallest step)?
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

func (ds *DataSource) updateRange(begin, end time.Time, value float64) error {

	// This range can be less than a PDP or span multiple PDPs. Only
	// the last PDP is current, the rest are all in the past.

	// Beginning of the last PDP in the range.
	endPdpBegin := end.Truncate(ds.step)
	if end.Equal(endPdpBegin) {
		// We are exactly at the end, need to move one step back.
		endPdpBegin.Add(ds.step * -1)
	}
	// End of the last PDP.
	endPdpEnd := endPdpBegin.Add(ds.step)

	// If the range begins *before* the last PDP, or ends
	// *exactly* on the end of a PDP, at last one PDP is now
	// completed, and updates need to trickle down to RRAs.
	if begin.Before(endPdpBegin) || (end.Equal(endPdpEnd)) {

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
			if err := ds.updateRRAs(periodBegin, periodEnd); err != nil {
				return err
			}

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

			ds.SetValue(value, ds.step) // Since begin is aligned, we can bluntly set the value.

			periodBegin := begin
			periodEnd := endPdpBegin
			if end.Equal(end.Truncate(ds.step)) {
				periodEnd = end
			}
			if err := ds.updateRRAs(periodBegin, periodEnd); err != nil {
				return err
			}

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

	return nil
}

func (ds *DataSource) ProcessIncomingDataPoint(value float64, ts time.Time) error {

	if math.IsInf(value, 0) {
		return fmt.Errorf("±Inf is not a valid data point value: %v", value)
	}

	if ts.Before(ds.lastUpdate) {
		return fmt.Errorf("Data point time stamp %v is not greater than data source last update time %v", ts, ds.lastUpdate)
	}

	// ds value is NaN if HB is exceeded
	if ts.Sub(ds.lastUpdate) > ds.heartbeat {
		value = math.NaN()
	}

	if !ds.lastUpdate.IsZero() { // Do not update a never-before-updated DS
		if err := ds.updateRange(ds.lastUpdate, ts, value); err != nil {
			return err
		}
	}

	ds.lastUpdate = ts
	ds.lastDs = value

	return nil
}

func (ds *DataSource) updateRRAs(periodBegin, periodEnd time.Time) error {

	// for each of this DS's RRAs
	for _, rra := range ds.rras {

		// currentBegin is a cursor pointing at the beginning of the
		// current slot, currentEnd points at its end. We start out
		// with currentBegin pointing at the slot one RRA-length ago
		// from periodEnd, then we move it up to periodBegin if it is
		// later. This way we end up with the latest of periodBegin or
		// rra-begin.
		currentBegin := rra.Begins(periodEnd, rra.step)
		if periodBegin.After(currentBegin) {
			currentBegin = periodBegin
		}

		// for each RRA slot before periodEnd
		for currentBegin.Before(periodEnd) {

			endOfSlot := currentBegin.Truncate(rra.step).Add(rra.step)

			currentEnd := endOfSlot
			if currentEnd.After(periodEnd) {
				currentEnd = periodEnd // i.e. currentEnd < endOfSlot
			}

			switch rra.cf {
			case MAX:
				rra.AddValueMax(ds.value, ds.duration)
			case MIN:
				rra.AddValueMin(ds.value, ds.duration)
			case LAST:
				rra.AddValueLast(ds.value, ds.duration)
			case WMEAN:
				rra.AddValue(ds.value, ds.duration)
			}

			// if end of slot
			if currentEnd.Equal(endOfSlot) {

				// Check XFF
				known := float64(rra.duration) / float64(rra.step)
				if known < float64(rra.xff) {
					rra.SetValue(math.NaN(), 0)
				}

				slotN := ((endOfSlot.UnixNano() / 1000000) / (rra.step.Nanoseconds() / 1000000)) % int64(rra.size)
				rra.latest = endOfSlot
				rra.dps[slotN] = rra.value

				if len(rra.dps) == 1 {
					rra.start = slotN
				}
				rra.end = slotN

				// reset
				rra.Reset()
			}

			// move up the cursor
			currentBegin = currentEnd
		}
	}

	return nil
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

// MostlyCopy returns a copy of the DataSource that contains all the
// fields that should be persisted when flushing. The idea is that
// this copy can then be saved in a separate goroutine without having
// to lock the DataSource.
func (ds *DataSource) MostlyCopy() *DataSource {

	// Only copy elements that change or needed for saving/rendering
	new_ds := new(DataSource)
	new_ds.id = ds.id
	new_ds.name = ds.name
	new_ds.step = ds.step
	new_ds.heartbeat = ds.heartbeat
	new_ds.lastUpdate = ds.lastUpdate
	new_ds.lastDs = ds.lastDs
	new_ds.value = ds.value
	new_ds.duration = ds.duration
	new_ds.rras = make([]*RoundRobinArchive, len(ds.rras))

	for n, rra := range ds.rras {
		new_ds.rras[n] = rra.copy()
	}

	return new_ds
}
