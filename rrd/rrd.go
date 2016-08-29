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

// Package rrd contains the logic for updating in-memory partial
// Round-Robin Archives of data points. In other words, this is the
// logic governing how incoming data modifies RRAs only, there is no
// code here to load an RRA from db and do something with it.
//
// Throughout documentation and code the following terms are used
// (sometimes as abbreviations, listed in parenthesis):
//
// Round-Robin Database (RRD): Collectively all the logic in this
// package and an instance of the data it maintains is referred to as
// an RRD.
//
// Data Point (DP): There actually isn't a data structure representing
// a data point (except for an incoming data point IncomingDP). A
// datapoint is just a float64.
//
// Data Sourse (DS): Data Source is all there is to know about a time
// series, its name, resolution and other parameters, as well as the
// data. A DS has at least one, but usually several RRAs.
//
// DS Step: Step is the smallest unit of time for the DS in
// milliseconds. RRA resolutions and sizes must be multiples of the DS
// step.
//
// DS Heartbeat (HB): Duration of time that can pass without data. A
// gap in data which exceeds HB is filled with NaNs.
//
// Round-Robin Archive (RRA): An array of data points at a specific
// resolutoin and going back a pre-defined duration of time.
//
// Primary Data Point (PDP): A conceptual data point which represents
// a time slot. Many actual data points can come in and fall into the
// current (not-yet-complete) PDP. There is one PDP per DS and one per
// each RRA. When the DS PDP is complete its content is saved into one
// or more RRA PDPs.
//
// How Datapoints build. The DS PDP always uses weighted mean (WMEAN)
// as its consolidation, while RRAs have a choice of WMEAN, MIN, MAX
// and LAST. The default for everything is WMEAN.
//
//  ||    +--------+    ||
//  ||    |	     3 +----||
//  ||----+	       |  2 ||
//  ||  1 |	       |    ||
//  ||==================||
//
// In the above data point, 0.25 of the value is 1, 0.50 is 3 and 0.25
// is 2, for a total of 0.25*1 + 0.50*3 + 0.25*2 = 2.25.
//
// If a part of the data point is NaN, then that part does not
// count. Even if NaN is at the end:
//
//  ||    +--------+    ||
//  ||    |	     3 |    ||
//  ||----+	       | NaN||
//  ||  1 |	       |    ||
//  ||==================||
//
// In the above datapoint, the datapoint size is what is taken up by 1
// and 3, without the NaN. Thus 1/3 of the value is 1 and 2/3 of the
// value is 3, for a total of 1/3*1 + 2/3*3 = 2.33333...
//
// An alternative way of looking at the above data point is that it is
// simply shorter or has a shorter duration:
//
//  ||    +--------||
//  ||    |      3 ||
//  ||----+        ||
//  ||  1 |        ||
//  ||=============||
//
// A datapoint must be all NaN for its value to be NaN.
package rrd

import (
	"fmt"
	"math"
	"sync"
	"time"
)

// DSSpec describes a DataSource. DSSpec is a schema that is used to
// create the DataSource. This is necessary so that DS's can be crated
// on-the-fly.
type DSSpec struct {
	Step      time.Duration
	Heartbeat time.Duration
	RRAs      []*RRASpec
}

type Consolidation int

const (
	WMEAN Consolidation = iota // Time-weighted average
	MAX                        // Max
	MIN                        // Min
	LAST                       // Last
)

// RRASpec is the RRA definition part of DSSpec.
type RRASpec struct {
	Function Consolidation
	Step     time.Duration
	Size     time.Duration
	Xff      float64
}

// IncomingDP represents incoming data, i.e. this is the form in which
// input data is expected. This is not an internal representation of a
// data point, it's the format in which they are expected to arrive.
type IncomingDP struct {
	DS        *DataSource
	Name      string
	TimeStamp time.Time
	Value     float64
	Hops      int
}

// Process will append the data point to the the DS's archive(s). Once
// an incoming data point is processed, it can be discarded, it's not
// very useful for anything.
func (dp *IncomingDP) Process() error {
	if dp.DS == nil {
		return fmt.Errorf("Cannot process data point with nil DS.")
	}
	return dp.DS.processIncomingDP(dp)
}

// DataSource describes a time series and its parameters, RRA and
// intermediate state (PDP).
type DataSource struct {
	pdp
	Step        time.Duration        // Step (PDP) size
	Id          int64                // Id
	Name        string               // Series name
	Heartbeat   time.Duration        // Heartbeat is inactivity period longer than this causes NaN values
	LastUpdate  time.Time            // Last time we received an update (series time - can be in the past or future)
	LastDs      float64              // Last final value we saw
	RRAs        []*RoundRobinArchive // Array of Round Robin Archives
	lastFlushRT time.Time            // Last time this DS was flushed (actual real time).
}

// A collection of data sources kept by an integer id as well as a
// string name.
type DataSources struct {
	l      rwLocker
	byName map[string]*DataSource
	byId   map[int64]*DataSource
}

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

// Returns a new DataSources object. If locking is true, the resulting
// DataSources will maintain a lock, otherwise there is no locking,
// but the caller needs to ensure that it is never used concurrently
// (e.g. always in the same goroutine).
func NewDataSources(locking bool) *DataSources {
	dss := &DataSources{
		byId:   make(map[int64]*DataSource),
		byName: make(map[string]*DataSource),
	}
	if locking {
		dss.l = &sync.RWMutex{}
	}
	return dss
}

// RoundRobinArchive and all its parameters.
type RoundRobinArchive struct {
	pdp
	Id   int64 // Id
	DsId int64 // DS id
	// Consolidation function (CF). How data points from a
	// higher-resolution RRA are aggregated into a lower-resolution
	// one. Must be WMEAN, MAX, MIN, LAST.
	Cf Consolidation
	// A single "row" (i.e. a single value) span in DS steps.
	StepsPerRow int32
	// Number of data points in the RRA.
	Size int32
	// Time at which most recent data point and the RRA end.
	Latest time.Time
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
	Xff float32

	// The slice of data points (as a map so that its sparse). Slots
	// in DPs are time-aligned starting at the "beginning of the
	// epoch" (Jan 1 1971 UTC). This means that if Latest is defined,
	// we can compute any slot's timestamp without having to store it.
	DPs map[int64]float64

	// In the undelying storage, how many data points are stored in a single (database) row.
	Width int64
	// Index of the first slot for which we have data. (Should be
	// between 0 and Size-1)
	Start int64
	// Index of the last slot for which we have data. Note that it's
	// possible for End to be less than Start, which means the RRD
	// wraps around.
	End int64
}

// GetByName rlocks and gets a DS pointer.
func (dss *DataSources) GetByName(name string) *DataSource {
	if dss.l != nil {
		dss.l.RLock()
		defer dss.l.RUnlock()
	}
	return dss.byName[name]
}

// GetById rlocks and gets a DS pointer.
func (dss *DataSources) GetById(id int64) *DataSource {
	if dss.l != nil {
		dss.l.RLock()
		defer dss.l.RUnlock()
	}
	return dss.byId[id]
}

// Insert locks and inserts a DS.
func (dss *DataSources) Insert(ds *DataSource) {
	if dss.l != nil {
		dss.l.Lock()
		defer dss.l.Unlock()
	}
	dss.byName[ds.Name] = ds
	dss.byId[ds.Id] = ds
}

// List rlocks, then returns a slice of *DS
func (dss *DataSources) List() []*DataSource {
	if dss.l != nil {
		dss.l.RLock()
		defer dss.l.RUnlock()
	}

	result := make([]*DataSource, len(dss.byId))
	n := 0
	for _, ds := range dss.byId {
		result[n] = ds
		n++
	}
	return result
}

// This only deletes it from memory, it is still in
// the database.
func (dss *DataSources) Delete(ds *DataSource) {
	if dss.l != nil {
		dss.l.Lock()
		defer dss.l.Unlock()
	}

	delete(dss.byName, ds.Name)
	delete(dss.byId, ds.Id)
}

func (ds *DataSource) BestRRA(start, end time.Time, points int64) *RoundRobinArchive {
	var result []*RoundRobinArchive

	for _, rra := range ds.RRAs {
		// is start within this RRA's range?
		rraBegin := rra.Latest.Add(time.Duration(rra.StepsPerRow) * ds.Step * time.Duration(rra.Size) * -1)

		if start.After(rraBegin) {
			result = append(result, rra)
		}
	}

	if len(result) == 0 {
		// if we found nothing above, simply select the longest RRA
		var longest *RoundRobinArchive
		for _, rra := range ds.RRAs {
			if longest == nil || longest.Size*longest.StepsPerRow < rra.Size*rra.StepsPerRow {
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
				rraDiff := math.Abs(float64(expectedStep - time.Duration(rra.StepsPerRow)*ds.Step))
				bestDiff := math.Abs(float64(expectedStep - time.Duration(best.StepsPerRow)*ds.Step))
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
				if best.StepsPerRow > rra.StepsPerRow {
					best = rra
				}
			}
		}
		return best
	}

	return nil
}

func (ds *DataSource) PointCount() int {
	total := 0
	for _, rra := range ds.RRAs {
		total += len(rra.DPs)
	}
	return total
}

func (ds *DataSource) updateRange(begin, end time.Time, value float64) error {

	// This range can be less than a PDP or span multiple PDPs. Only
	// the last PDP is current, the rest are all in the past.

	// Beginning of the last PDP in the range.
	endPdpBegin := end.Truncate(ds.Step)
	if end.Equal(endPdpBegin) {
		// We are exactly at the end, need to move one step back.
		endPdpBegin.Add(ds.Step * -1)
	}
	// End of the last PDP.
	endPdpEnd := endPdpBegin.Add(ds.Step)

	// If the range begins *before* the last PDP, or ends
	// *exactly* on the end of a PDP, at last one PDP is now
	// completed, and updates need to trickle down to RRAs.
	if begin.Before(endPdpBegin) || (end.Equal(endPdpEnd)) {

		// If range begins in the middle of a now completed PDP
		// (which may be the last one IFF end == endPdpEnd)
		if begin.Truncate(ds.Step) != begin {

			// periodBegin and periodEnd mark the PDP beginning just
			// before the beginning of the range. periodEnd points at
			// the end of the first PDP or end of the last PDP if (and
			// only if) end == endPdpEnd.
			periodBegin := begin.Truncate(ds.Step)
			periodEnd := periodBegin.Add(ds.Step)
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

			ds.SetValue(value, ds.Step) // Since begin is aligned, we can bluntly set the value.

			periodBegin := begin
			periodEnd := endPdpBegin
			if end.Equal(end.Truncate(ds.Step)) {
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

func (ds *DataSource) processIncomingDP(dp *IncomingDP) error {

	if math.IsInf(dp.Value, 0) {
		return fmt.Errorf("±Inf is not a valid data point value: %#v", dp)
	}

	if dp.TimeStamp.Before(ds.LastUpdate) {
		return fmt.Errorf("Data point time stamp %v is not greater than data source last update time %v", dp.TimeStamp, ds.LastUpdate)
	}

	// ds value is NaN if HB is exceeded
	if dp.TimeStamp.Sub(ds.LastUpdate) > ds.Heartbeat {
		dp.Value = math.NaN()
	}

	if !ds.LastUpdate.IsZero() { // Do not update a never-before-updated DS
		if err := ds.updateRange(ds.LastUpdate, dp.TimeStamp, dp.Value); err != nil {
			return err
		}
	}

	ds.LastUpdate = dp.TimeStamp
	ds.LastDs = dp.Value

	return nil
}

func (ds *DataSource) updateRRAs(periodBegin, periodEnd time.Time) error {

	// for each of this DS's RRAs
	for _, rra := range ds.RRAs {

		// The RRA step (TODO should this be a method?)
		rraStep := ds.Step * time.Duration(rra.StepsPerRow)

		// currentBegin is a cursor pointing at the beginning of the
		// current slot, currentEnd points at its end
		currentBegin := rra.Begins(periodBegin, rraStep)

		// move the cursor up to at least the periodBegin
		if periodBegin.After(currentBegin) {
			currentBegin = periodBegin
		}

		// for each RRA slot before periodEnd
		for currentBegin.Before(periodEnd) {

			endOfSlot := currentBegin.Truncate(rraStep).Add(rraStep)

			currentEnd := endOfSlot
			if currentEnd.After(periodEnd) {
				currentEnd = periodEnd // i.e. currentEnd < endOfSlot
			}

			switch rra.Cf {
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
				known := float64(rra.duration) / float64(rraStep)
				if known < float64(rra.Xff) {
					rra.SetValue(math.NaN(), 0)
				}

				slotN := ((endOfSlot.UnixNano() / 1000000) / (rraStep.Nanoseconds() / 1000000)) % int64(rra.Size)
				rra.Latest = endOfSlot
				rra.DPs[slotN] = rra.value

				if len(rra.DPs) == 1 {
					rra.Start = slotN
				}
				rra.End = slotN

				// reset
				rra.Reset()
			}

			// move up the cursor
			currentBegin = currentEnd
		}
	}

	return nil
}

func (ds *DataSource) ClearRRAs() {
	for _, rra := range ds.RRAs {
		rra.DPs = make(map[int64]float64)
		rra.Start, rra.End = 0, 0
	}
	ds.lastFlushRT = time.Now()
}

func (ds *DataSource) ShouldBeFlushed(maxCachedPoints int, minCache, maxCache time.Duration) bool {
	if ds.LastUpdate.IsZero() {
		return false
	}
	pc := ds.PointCount()
	if pc > maxCachedPoints {
		return ds.lastFlushRT.Add(minCache).Before(time.Now())
	} else if pc > 0 {
		return ds.lastFlushRT.Add(maxCache).Before(time.Now())
	}
	return false
}

func (ds *DataSource) MostlyCopy() *DataSource {

	// Only copy elements that change or needed for saving/rendering
	new_ds := new(DataSource)
	new_ds.Id = ds.Id
	new_ds.Step = ds.Step
	new_ds.Heartbeat = ds.Heartbeat
	new_ds.LastUpdate = ds.LastUpdate
	new_ds.LastDs = ds.LastDs
	new_ds.value = ds.value
	new_ds.duration = ds.duration
	new_ds.RRAs = make([]*RoundRobinArchive, len(ds.RRAs))

	for n, rra := range ds.RRAs {
		new_ds.RRAs[n] = rra.mostlyCopy()
	}

	return new_ds
}

func (rra *RoundRobinArchive) mostlyCopy() *RoundRobinArchive {

	// Only copy elements that change or needed for saving/rendering
	new_rra := new(RoundRobinArchive)
	new_rra.Id = rra.Id
	new_rra.DsId = rra.DsId
	new_rra.StepsPerRow = rra.StepsPerRow
	new_rra.Size = rra.Size
	new_rra.value = rra.value
	new_rra.duration = rra.duration
	new_rra.Latest = rra.Latest
	new_rra.Start = rra.Start
	new_rra.End = rra.End
	new_rra.Size = rra.Size
	new_rra.Width = rra.Width
	new_rra.DPs = make(map[int64]float64)

	for k, v := range rra.DPs {
		new_rra.DPs[k] = v
	}

	return new_rra
}

func (rra *RoundRobinArchive) SlotRow(slot int64) int64 {
	if slot%rra.Width == 0 {
		return slot / rra.Width
	} else {
		return (slot / rra.Width) + 1
	}
}

func (rra *RoundRobinArchive) Begins(now time.Time, rraStep time.Duration) time.Time {
	rraStart := now.Add(rraStep * time.Duration(rra.Size) * -1).Truncate(rraStep)
	if now.Equal(now.Truncate(rraStep)) {
		rraStart = rraStart.Add(rraStep)
	}
	return rraStart
}

func (rra *RoundRobinArchive) SlotTimeStamp(ds *DataSource, slot int64) time.Time {
	// TODO this is kind of ugly too...
	slot = slot % int64(rra.Size) // just in case
	dsStepMs := ds.Step.Nanoseconds() / 1000000
	rraStepMs := dsStepMs * int64(rra.StepsPerRow)
	latestMs := rra.Latest.UnixNano() / 1000000
	latestSlotN := (latestMs / rraStepMs) % int64(rra.Size)
	distance := (int64(rra.Size) + latestSlotN - slot) % int64(rra.Size)
	return rra.Latest.Add(time.Duration(rraStepMs*distance) * time.Millisecond * -1)
}
