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

// Package rrd contains the logic for maintaining in-memory
// Round-Robin Archives of data points.
//
// Throughout documentation and code the following terms are used
// (sometimes as abbreviations, listed in parenthesis):
//
// Round-Robin Database (RRD): Collectively all the logic in this
// package and an instance of the data it maintains is referred to as
// an RRD.
//
// Data Sourse (DS): Data Source is all there is to know about a time
// series, its name, resolution and other parameters, as well as the
// data. A DS has at least one, but usually several RRAs. DS is also
// the structure which stores the PDP state.
//
// Data Point (DP): There actually isn't a data structure representing
// a data point (except for an incoming data point IncomingDP). A
// datapoint is just a float64.
//
// Round-Robin Archive (RRA): An array of data points at a specific
// resolutoin and going back a pre-defined duration of time.
//
// Primary Data Point (PDP): A conceptual data point which represents
// the most current and not-yet-complete time slot. There is one PDP
// per DS and per each RRA. When the PDP is complete its content is
// saved into one or more RRAs. The PDP state is part of the DS
// structure.
//
// DS Step: Step is the smallest unit of time for the DS in
// milliseconds. RRA resolutions and sizes must be multiples of the DS
// step.
//
// DS Heartbeat (HB): Duration of time that can pass without data. A
// gap in data which exceeds HB is filled with NaNs.
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

// RRASpec is the RRA definition part of DSSpec.
type RRASpec struct {
	Function string
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
	Id          int64                // Id
	Name        string               // Series name
	StepMs      int64                // Step Size in Ms
	HeartbeatMs int64                // Heartbeat in Ms (i.e. inactivity period longer than this causes NaN values)
	LastUpdate  time.Time            // Last time we received an update (series time - can be in the past or future)
	LastDs      float64              // Last final value we saw
	Value       float64              // Weighted value (e.g. f we are 2/3 way into a step, Value should be 2/3 of the final step value)
	UnknownMs   int64                // Ms of the data that is "unknown" (e.g. because of exceeded HB)
	RRAs        []*RoundRobinArchive // Array of Round Robin Archives
	LastFlushRT time.Time            // Last time this DS was flushed (actual real time).
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
	Id   int64 // Id
	DsId int64 // DS id
	// Consolidation function (CF). How data points from a
	// higher-resolution RRA are aggregated into a lower-resolution
	// one. Must be MAX, MIN, LAST or AVERAGE.
	Cf string
	// A single "row" (i.e. a single value) span in DS steps.
	StepsPerRow int32
	// Number of data points in the RRA.
	Size int32
	// XFiles Factor (XFF). When consolidating, how much of the
	// higher-resolution RRA (as a value between 0 and 1) is allowed
	// to be NaN before the consolidated data becomes NaN as well.
	Xff float32
	// PDP, store for intermediate value during consolidation.
	Value float64
	// How much of the PDP is "unknown".
	UnknownMs int64
	// Time at which most recent data point and the RRA end.
	Latest time.Time

	// The slice of data points (as a map so that its sparse). Slots
	// in DPs are time-aligned starting at the "beginning of the
	// epoch" (Jan 1 1971 UTC). This means that if Latest is defined,
	// we can compute any slot's timestamp without having to store it.
	DPs map[int64]float64

	// In the undelying storage, how many data points are stored in a single (database) row.
	Width int64
	// DPs index of the starting slot (important for sparsity).
	Start int64
	// DPs index of the ending slot (not necessarily Start-1).
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
		rraBegin := rra.Latest.Add(time.Duration(int64(rra.StepsPerRow)*ds.StepMs*int64(rra.Size)) * time.Millisecond * -1)
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
		expectedStepMs := (end.UnixNano()/1000000 - start.UnixNano()/1000000) / points
		var best *RoundRobinArchive
		for _, rra := range result {
			if best == nil {
				best = rra
			} else {
				rraDiff := expectedStepMs - int64(rra.StepsPerRow)*ds.StepMs
				rraDiff = rraDiff * rraDiff // keep it positive
				bestDiff := expectedStepMs - int64(best.StepsPerRow)*ds.StepMs
				bestDiff = bestDiff * bestDiff
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

func (ds *DataSource) pointCount() int {
	total := 0
	for _, rra := range ds.RRAs {
		total += len(rra.DPs)
	}
	return total
}

func (ds *DataSource) setValue(value float64) {
	ds.Value = value
	ds.UnknownMs = 0
}

func (ds *DataSource) addValue(value float64, durationMs int64, allowNaNtoValue bool) error {
	if durationMs > ds.StepMs {
		return fmt.Errorf("ds.addValue(): duration (%v) cannot be greater than ds.StepMs (%v)", durationMs, ds.StepMs)
	}
	// A DS can go from NaN to a value, but only if the previous update was in the same PDP
	if math.IsNaN(ds.Value) && allowNaNtoValue {
		ds.Value = 0
	}
	weight := float64(durationMs) / float64(ds.StepMs)
	ds.Value = ds.Value + weight*value
	if math.IsNaN(value) {
		ds.UnknownMs = ds.UnknownMs + durationMs
	}
	return nil
}

func (ds *DataSource) finalizeValue() {
	ds.Value = ds.Value / (float64(ds.StepMs-ds.UnknownMs) / float64(ds.StepMs))
}

func (ds *DataSource) reset() {
	ds.Value = math.NaN()
	ds.UnknownMs = 0
}

func (ds *DataSource) updateRange(begin, end int64, value float64) error {

	endPdpBegin := end / ds.StepMs * ds.StepMs
	if end%ds.StepMs == 0 {
		endPdpBegin -= ds.StepMs
	}
	endPdpEnd := endPdpBegin + ds.StepMs

	if begin < endPdpBegin || (end == endPdpEnd) { // RRAs will be updated

		if begin%ds.StepMs != 0 { // begin in the middle of now completed PDP

			periodBegin := begin / ds.StepMs * ds.StepMs
			periodEnd := periodBegin + ds.StepMs
			if err := ds.addValue(value, periodEnd-begin, false); err != nil {
				return err
			}
			ds.finalizeValue()

			if err := ds.updateRRAs(periodBegin, periodEnd); err != nil {
				return err
			}
			ds.reset()

			begin = periodEnd
		}

		if begin < endPdpBegin || (begin == endPdpBegin && end == endPdpEnd) { // we have 1+ whole pdps
			ds.setValue(value)

			periodBegin := begin
			periodEnd := endPdpBegin
			if end%ds.StepMs == 0 {
				periodEnd = end
			}

			if err := ds.updateRRAs(periodBegin, periodEnd); err != nil {
				return err
			}
			ds.reset()

			begin = periodEnd
		}
	}

	if begin < end { // Update DS with remaining partial PDP
		ds.addValue(value, end-begin, true)
	}

	return nil
}

func (ds *DataSource) processIncomingDP(dp *IncomingDP) error {

	if math.IsInf(dp.Value, 0) {
		return fmt.Errorf("Inf is not a valid data point value: %#v", dp)
	}

	// Do everything in milliseconds
	dpTimeStamp := dp.TimeStamp.UnixNano() / 1000000
	dsLastUpdate := ds.LastUpdate.UnixNano() / 1000000

	if dpTimeStamp < dsLastUpdate {
		return fmt.Errorf("Data point time stamp %v is not greater than data source last update time %v", dp.TimeStamp, dp.DS.LastUpdate)
	}

	if dsLastUpdate == 0 { // never-before updated (or was zeroed out in ClearRRA)
		for _, rra := range ds.RRAs {
			rraStepMs := ds.StepMs * int64(rra.StepsPerRow)
			roundedDpEndsOn := dpTimeStamp / ds.StepMs * ds.StepMs
			slotBegin := roundedDpEndsOn / rraStepMs * rraStepMs
			rra.UnknownMs = roundedDpEndsOn - slotBegin
		}
	}

	if (dpTimeStamp - dsLastUpdate) > ds.HeartbeatMs {
		dp.Value = math.NaN()
	}

	if dsLastUpdate != 0 {
		if err := ds.updateRange(dsLastUpdate, dpTimeStamp, dp.Value); err != nil {
			return err
		}
	}

	ds.LastUpdate = dp.TimeStamp
	ds.LastDs = dp.Value

	return nil
}

func (ds *DataSource) updateRRAs(periodBegin, periodEnd int64) error {

	for _, rra := range ds.RRAs {

		rraStepMs := ds.StepMs * int64(rra.StepsPerRow)

		currentBegin := rra.GetStartGivenEndMs(ds, periodBegin)
		if periodBegin > currentBegin {
			currentBegin = periodBegin
		}

		for currentBegin < periodEnd {

			endOfSlot := currentBegin/rraStepMs*rraStepMs + rraStepMs
			currentEnd := endOfSlot
			if currentEnd > periodEnd {
				currentEnd = periodEnd
			}

			steps := (currentEnd - currentBegin) / ds.StepMs

			if math.IsNaN(ds.Value) {
				rra.UnknownMs = rra.UnknownMs + ds.StepMs*steps
			}

			xff := float64(rra.UnknownMs+ds.UnknownMs) / float64(rraStepMs)
			if (xff > float64(rra.Xff)) || math.IsNaN(ds.Value) {
				// So the issue there is that for RRAs that span long
				// periods of time have a high probability of hitting a
				// NaN and thus NaN-ing the whole thing... For now the
				// solution is a hack where xff of 1 will ignore NaNs
				if rra.Xff != 1 {
					rra.Value = math.NaN()
				}
			} else {
				// aggregations
				if math.IsNaN(rra.Value) {
					rra.Value = 0
				}

				switch rra.Cf {
				case "MAX":
					if ds.Value > rra.Value {
						rra.Value = ds.Value
					}
				case "MIN":
					if ds.Value < rra.Value {
						rra.Value = ds.Value
					}
				case "LAST":
					rra.Value = ds.Value
				case "AVERAGE":
					rra_weight := 1.0 / float64(rra.StepsPerRow) * float64(steps)
					rra.Value = rra.Value + ds.Value*rra_weight
				default:
					return fmt.Errorf("Invalid consolidation function: %q", rra.Cf)
				}
			}

			if currentEnd >= endOfSlot {

				if rra.Cf == "AVERAGE" && !math.IsNaN(rra.Value) && rra.UnknownMs > 0 {
					// adjust the final value
					rra.Value = rra.Value / (float64(rraStepMs-rra.UnknownMs) / float64(rraStepMs))
				}

				slotN := (currentEnd / rraStepMs) % int64(rra.Size)
				rra.Latest = time.Unix(currentEnd/1000, (currentEnd%1000)*1000000)
				rra.DPs[slotN] = rra.Value

				if len(rra.DPs) == 1 {
					rra.Start = slotN
				}
				rra.End = slotN

				// reset
				rra.Value = 0
				rra.UnknownMs = 0

			}

			currentBegin = currentEnd
		} // currentEnd <= periodEnd
	}

	return nil
}

func (ds *DataSource) ClearRRAs(clearLU bool) {
	for _, rra := range ds.RRAs {
		rra.DPs = make(map[int64]float64)
		rra.Start, rra.End = 0, 0
	}
	if clearLU {
		// This is so that if we are a cluster node that is no longer
		// responsible for an event, but then become responsible
		// again, the new DP doesn't set NaNs all the way to LU. We're
		// making an assumption that this is done whenever a blocking
		// flush is requested (i.e. at the Relinquish).
		ds.LastUpdate = time.Unix(0, 0) // Not to be confused with time.Time{}
	}
}

func (ds *DataSource) ShouldBeFlushed(maxCachedPoints int, minCache, maxCache time.Duration) bool {
	if ds.LastUpdate == time.Unix(0, 0) {
		return false
	}
	pc := ds.pointCount()
	if pc > maxCachedPoints {
		return ds.LastFlushRT.Add(minCache).Before(time.Now())
	} else if pc > 0 {
		return ds.LastFlushRT.Add(maxCache).Before(time.Now())
	}
	return false
}

func (ds *DataSource) MostlyCopy() *DataSource {

	// Only copy elements that change or needed for saving/rendering
	new_ds := new(DataSource)
	new_ds.Id = ds.Id
	new_ds.StepMs = ds.StepMs
	new_ds.HeartbeatMs = ds.HeartbeatMs
	new_ds.LastUpdate = ds.LastUpdate
	new_ds.LastDs = ds.LastDs
	new_ds.Value = ds.Value
	new_ds.UnknownMs = ds.UnknownMs
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
	new_rra.Value = rra.Value
	new_rra.UnknownMs = rra.UnknownMs
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

func (rra *RoundRobinArchive) GetStartGivenEndMs(ds *DataSource, timeMs int64) int64 {
	rraStepMs := ds.StepMs * int64(rra.StepsPerRow)
	rraStart := (timeMs - rraStepMs*int64(rra.Size)) / rraStepMs * rraStepMs
	if timeMs%rraStepMs != 0 {
		rraStart += rraStepMs
	}
	return rraStart
}

func (rra *RoundRobinArchive) SlotTimeStamp(ds *DataSource, slot int64) time.Time {
	// TODO this is kind of ugly too...
	slot = slot % int64(rra.Size) // just in case
	rraStepMs := ds.StepMs * int64(rra.StepsPerRow)
	latestMs := rra.Latest.UnixNano() / 1000000
	latestSlotN := (latestMs / rraStepMs) % int64(rra.Size)
	distance := (int64(rra.Size) + latestSlotN - slot) % int64(rra.Size)
	return rra.Latest.Add(time.Duration(rraStepMs*distance) * time.Millisecond * -1)
}
