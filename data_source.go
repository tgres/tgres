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

package timeriver

import (
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// This represents incoming data, as is.

type trDataPoint struct {
	ds        *trDataSource
	Name      string
	TimeStamp time.Time
	Value     float64
}

func (dp *trDataPoint) process() error {
	return dp.ds.processDataPoint(dp)
}

// When a DP arrives in-between PDP boundary, there is state we need
// to maintain. We cannot save it in an RRA until a subsequent DP
// completes the PDP. This state is kept in trDataSource.

type trDataSource struct {
	Id           int64
	Name         string
	StepMs       int64
	HeartbeatMs  int64
	LastUpdate   time.Time
	LastDs       float64
	Value        float64
	UnknownMs    int64
	RRAs         []*trRoundRobinArchive
	LastUpdateRT time.Time
	LastFlushRT  time.Time
}

type trDataSources struct {
	sync.RWMutex
	byName   map[string]*trDataSource
	byId     map[int64]*trDataSource
	prefixes map[string]bool // for Graphite-like listings
}

type trRoundRobinArchive struct {
	Id          int64
	DsId        int64
	Cf          string
	StepsPerRow int32
	Size        int32
	Xff         float32
	Value       float64
	UnknownMs   int64
	Latest      time.Time
	DPs         map[int64]float64
	Width       int64
	start       int64
	end         int64
}

type Series interface {
	Next() bool

	CurrentValue() float64
	CurrentPosBeginsAfter() time.Time
	CurrentPosEndsOn() time.Time

	Close() error
	StepMs() int64
	SetGroupByMs(int64)
	GroupByMs() int64
	Alias(...string) *string
}

func (dss *trDataSources) getByName(name string) *trDataSource {
	dss.RLock()
	defer dss.RUnlock()
	return dss.byName[name]
}

func (dss *trDataSources) getById(id int64) *trDataSource {
	dss.RLock()
	defer dss.RUnlock()
	return dss.byId[id]
}

func (dss *trDataSources) insert(ds *trDataSource) {
	dss.Lock()
	defer dss.Unlock()
	dss.byName[ds.Name] = ds
	dss.byId[ds.Id] = ds

	// TODO this code is dupe of fetchDataSources()
	prefix := ds.Name
	for ext := filepath.Ext(prefix); ext != ""; {
		prefix = ds.Name[0 : len(prefix)-len(ext)]
		dss.prefixes[prefix] = true
		ext = filepath.Ext(prefix)
	}

}

type fsFindNode struct {
	Name string
	leaf bool
	dsId int64
}

type fsNodes []*fsFindNode

// sort.Interface
func (fns fsNodes) Len() int {
	return len(fns)
}
func (fns fsNodes) Less(i, j int) bool {
	return fns[i].Name < fns[j].Name
}
func (fns fsNodes) Swap(i, j int) {
	fns[i], fns[j] = fns[j], fns[i]
}

func (dss *trDataSources) fsFind(pattern string) []*fsFindNode {

	// TODO This should happen in Postgres because at 1M+ series this
	// wouldn't work that well... But then this API is ill designed
	// for that too...

	dss.RLock()
	defer dss.RUnlock()

	dots := strings.Count(pattern, ".")

	set := make(map[string]*fsFindNode)
	for k, ds := range dss.byName {

		// NB: It's safe to touch DS id, because it is assigned
		// protected by the same dss lock, but not other members!

		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &fsFindNode{Name: k, leaf: true, dsId: ds.Id}
		}
	}

	for k, _ := range dss.prefixes {
		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &fsFindNode{Name: k, leaf: false}
		}
	}

	// convert to array
	result := make(fsNodes, 0)
	for _, v := range set {
		result = append(result, v)
	}

	// so that results are consistently ordered, or Grafanas get confused
	sort.Sort(result)

	return result
}

func (dss *trDataSources) reload() error {

	by_name, by_id, prefixes, err := fetchDataSources()
	if err != nil {
		return err
	}

	dss.Lock()
	dss.byName = by_name
	dss.byId = by_id
	dss.prefixes = prefixes
	dss.Unlock()

	return nil
}

func (ds *trDataSource) bestRRA(start, end time.Time, points int64) *trRoundRobinArchive {

	var result []*trRoundRobinArchive

	for _, rra := range ds.RRAs {
		// is start within this RRA's range?
		rraBegin := rra.Latest.Add(time.Duration(int64(rra.StepsPerRow)*ds.StepMs*int64(rra.Size)) * time.Millisecond * -1)
		if start.After(rraBegin) {
			result = append(result, rra)
		}
	}

	if len(result) == 0 {
		// if we found nothing above, simply select the longest RRA
		var longest *trRoundRobinArchive
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
		var best *trRoundRobinArchive
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
		var best *trRoundRobinArchive
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

func (ds *trDataSource) pointCount() int {
	total := 0
	for _, rra := range ds.RRAs {
		total += len(rra.DPs)
	}
	return total
}

func (ds *trDataSource) setValue(value float64) {
	ds.Value = value
	ds.UnknownMs = 0
}

func (ds *trDataSource) addValue(value float64, durationMs int64, allowNaNtoValue bool) error {
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

func (ds *trDataSource) finalizeValue() {
	ds.Value = ds.Value / (float64(ds.StepMs-ds.UnknownMs) / float64(ds.StepMs))
}

func (ds *trDataSource) reset() {
	ds.Value = math.NaN()
	ds.UnknownMs = 0
}

func (ds *trDataSource) updateRange(begin, end int64, value float64) error {

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

func (ds *trDataSource) processDataPoint(dp *trDataPoint) error {

	// Do everything in milliseconds
	dpTimeStamp := dp.TimeStamp.UnixNano() / 1000000
	dsLastUpdate := ds.LastUpdate.UnixNano() / 1000000

	if dpTimeStamp < dsLastUpdate {
		return fmt.Errorf("Data point time stamp %v is not greater than data source last update time %v", dp.TimeStamp, dp.ds.LastUpdate)
	}

	if dsLastUpdate == 0 { // never-before updated
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
	ds.LastUpdateRT = time.Now()
	ds.LastDs = dp.Value

	return nil
}

func (ds *trDataSource) updateRRAs(periodBegin, periodEnd int64) error {

	for _, rra := range ds.RRAs {

		rraStepMs := ds.StepMs * int64(rra.StepsPerRow)

		currentBegin := rra.getStartGivenEndMs(ds, periodBegin)
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
					rra.start = slotN
				}
				rra.end = slotN

				// reset
				rra.Value = 0
				rra.UnknownMs = 0

			}

			currentBegin = currentEnd
		} // currentEnd <= periodEnd
	}

	return nil
}

func (ds *trDataSource) clearRRAs() {
	for _, rra := range ds.RRAs {
		rra.DPs = make(map[int64]float64)
		rra.start, rra.end = 0, 0
	}
}

func (ds *trDataSource) flushCopy() *trDataSource {

	// Only copy elements that change or needed for saving/rendering
	new_ds := new(trDataSource)
	new_ds.Id = ds.Id
	new_ds.StepMs = ds.StepMs
	new_ds.HeartbeatMs = ds.HeartbeatMs
	new_ds.LastUpdate = ds.LastUpdate
	new_ds.LastDs = ds.LastDs
	new_ds.Value = ds.Value
	new_ds.UnknownMs = ds.UnknownMs
	new_ds.RRAs = make([]*trRoundRobinArchive, len(ds.RRAs))

	for n, rra := range ds.RRAs {
		new_ds.RRAs[n] = rra.flushCopy()
	}

	return new_ds
}

func (rra *trRoundRobinArchive) flushCopy() *trRoundRobinArchive {

	// Only copy elements that change or needed for saving/rendering
	new_rra := new(trRoundRobinArchive)
	new_rra.Id = rra.Id
	new_rra.DsId = rra.DsId
	new_rra.StepsPerRow = rra.StepsPerRow
	new_rra.Size = rra.Size
	new_rra.Value = rra.Value
	new_rra.UnknownMs = rra.UnknownMs
	new_rra.Latest = rra.Latest
	new_rra.start = rra.start
	new_rra.end = rra.end
	new_rra.Size = rra.Size
	new_rra.Width = rra.Width
	new_rra.DPs = make(map[int64]float64)

	for k, v := range rra.DPs {
		new_rra.DPs[k] = v
	}

	return new_rra
}

func (rra *trRoundRobinArchive) slotRow(slot int64) int64 {
	if slot%rra.Width == 0 {
		return slot / rra.Width
	} else {
		return (slot / rra.Width) + 1
	}
}

func (rra *trRoundRobinArchive) getStartGivenEndMs(ds *trDataSource, timeMs int64) int64 {
	rraStepMs := ds.StepMs * int64(rra.StepsPerRow)
	rraStart := (timeMs - rraStepMs*int64(rra.Size)) / rraStepMs * rraStepMs
	if timeMs%rraStepMs != 0 {
		rraStart += rraStepMs
	}
	return rraStart
}

func (rra *trRoundRobinArchive) slotTimeStamp(ds *trDataSource, slot int64) time.Time {
	// TODO this is kind of ugly too...
	slot = slot % int64(rra.Size) // just in case
	rraStepMs := ds.StepMs * int64(rra.StepsPerRow)
	latestMs := rra.Latest.UnixNano() / 1000000
	latestSlotN := (latestMs / rraStepMs) % int64(rra.Size)
	distance := (int64(rra.Size) + latestSlotN - slot) % int64(rra.Size)
	return rra.Latest.Add(time.Duration(rraStepMs*distance) * time.Millisecond * -1)
}

func seriesFromIdent(t *trTransceiver, ident string, from, to *time.Time, maxPoints int64) (map[string]Series, error) {
	result := make(map[string]Series)
	targets := t.dss.fsFind(ident)
	for _, node := range targets {
		if node.leaf { // only leaf nodes are series names
			ds := t.requestDsCopy(node.dsId)
			dps, err := seriesQuery(ds, from, to, maxPoints)
			if err != nil {
				return nil, fmt.Errorf("seriesFromIdent(): Error %v", err)
			}
			result[node.Name] = dps
		}
	}
	return result, nil
}
