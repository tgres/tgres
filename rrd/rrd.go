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

// Package rrd does the thing ZZZ.
package rrd

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"math"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// DSSpec describes a DataSource. DSSpec is a schema that is used to
// create the DataSource.

type DSSpec struct {
	Step      time.Duration
	Heartbeat time.Duration
	RRAs      []*RRASpec
}
type RRASpec struct {
	Function string
	Step     time.Duration
	Size     time.Duration
	Xff      float64
}

// This represents incoming data, as is.

type DataPoint struct {
	DS        *DataSource
	Name      string
	TimeStamp time.Time
	Value     float64
	Hops      int
}

func (dp *DataPoint) Process() error {
	return dp.DS.processDataPoint(dp)
}

func (dp *DataPoint) GobEncode() ([]byte, error) {
	buf := bytes.Buffer{}
	enc := gob.NewEncoder(&buf)
	var err error
	check := func(er error) {
		if er != nil && err == nil {
			err = er
		}
	}
	check(enc.Encode(dp.Name))
	check(enc.Encode(dp.TimeStamp))
	check(enc.Encode(dp.Value))
	check(enc.Encode(dp.Hops))
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func (dp *DataPoint) GobDecode(b []byte) error {
	dec := gob.NewDecoder(bytes.NewBuffer(b))
	var err error
	check := func(er error) {
		if er != nil && err == nil {
			err = er
		}
	}
	check(dec.Decode(&dp.Name))
	check(dec.Decode(&dp.TimeStamp))
	check(dec.Decode(&dp.Value))
	check(dec.Decode(&dp.Hops))
	return err
}

// When a DP arrives in-between PDP boundary, there is state we need
// to maintain. We cannot save it in an RRA until a subsequent DP
// completes the PDP. This state is kept in DataSource.

type DataSource struct {
	Id           int64                // Id
	Name         string               // Series name
	StepMs       int64                // Step Size in Ms
	HeartbeatMs  int64                // Heartbeat in Ms (i.e. inactivity period longer than this causes NaN values)
	LastUpdate   time.Time            // Last time we received an update (series time - can be in the past or future)
	LastDs       float64              // Last value we saw
	Value        float64              // Current Value (the weighted average)
	UnknownMs    int64                // Ms of the data that is "unknown" (e.g. because of exceeded HB)
	RRAs         []*RoundRobinArchive // Array of Round Robin Archives
	LastFlushRT  time.Time            // Last time this DS was flushed (actual real time).
	LastUpdateRT time.Time            // Last time this DS was update (actual real time).
}

type DataSources struct {
	sync.RWMutex
	byName   map[string]*DataSource
	byId     map[int64]*DataSource
	prefixes map[string]bool // for Graphite-like listings
}

type RoundRobinArchive struct {
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
	Start       int64
	End         int64
}

// This thing knows how to load/save series in some storage

type SerDe interface {
	// Create a DS with name, and/or return it
	CreateOrReturnDataSource(name string, dsSpec *DSSpec) (*DataSource, error)
	// Fetch a list of data sources from the storage
	FetchDataSources() ([]*DataSource, error)
	// Flush a DS
	FlushDataSource(ds *DataSource) error
	// Query
	SeriesQuery(ds *DataSource, from, to time.Time, maxPoints int64) (Series, error)
	// Use the database to infer outside IPs of other connected clients
	ListDbClientIps() ([]string, error)
	MyDbAddr() (*string, error)
}

// This is a Series

type Series interface {
	Next() bool
	Close() error

	CurrentValue() float64
	CurrentPosBeginsAfter() time.Time
	CurrentPosEndsOn() time.Time

	StepMs() int64
	GroupByMs(...int64) int64
	TimeRange(...time.Time) (time.Time, time.Time)
	LastUpdate() time.Time
	MaxPoints(...int64) int64

	Alias(...string) string
}

func (dss *DataSources) GetByName(name string) *DataSource {
	dss.RLock()
	defer dss.RUnlock()
	return dss.byName[name]
}

func (dss *DataSources) GetById(id int64) *DataSource {
	dss.RLock()
	defer dss.RUnlock()
	return dss.byId[id]
}

func (dss *DataSources) Insert(ds *DataSource) {
	dss.Lock()
	defer dss.Unlock()
	dss.byName[ds.Name] = ds
	dss.byId[ds.Id] = ds
	dss.addPrefixes(ds.Name)
}

func (dss *DataSources) List() []*DataSource {
	result := make([]*DataSource, len(dss.byId))
	n := 0
	for _, ds := range dss.byId {
		result[n] = ds
		n++
	}
	return result
}

// Add prefixes given a name
func (dss *DataSources) addPrefixes(name string) {
	prefix := name
	for ext := filepath.Ext(prefix); ext != ""; {
		prefix = name[0 : len(prefix)-len(ext)]
		dss.prefixes[prefix] = true
		ext = filepath.Ext(prefix)
	}

}

// This only deletes it from memory, it is still in
// the database.
func (dss *DataSources) Delete(ds *DataSource) {
	dss.Lock()
	defer dss.Unlock()

	delete(dss.byName, ds.Name)
	delete(dss.byId, ds.Id)
}

type FsFindNode struct {
	Name string
	Leaf bool
	dsId int64
}

type fsNodes []*FsFindNode

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

func (dss *DataSources) FsFind(pattern string) []*FsFindNode {

	// TODO This should happen in Postgres because at 1M+ series this
	// wouldn't work that well... But then this API is ill designed
	// for that too...

	dss.RLock()
	defer dss.RUnlock()

	dots := strings.Count(pattern, ".")

	set := make(map[string]*FsFindNode)
	for k, ds := range dss.byName {

		// NB: It's safe to touch DS id, because it is assigned
		// protected by the same dss lock, but not other members!

		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &FsFindNode{Name: k, Leaf: true, dsId: ds.Id}
		}
	}

	for k, _ := range dss.prefixes {
		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &FsFindNode{Name: k, Leaf: false}
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

func (dss *DataSources) DsIdsFromIdent(ident string) map[string]int64 {
	result := make(map[string]int64)
	for _, node := range dss.FsFind(ident) {
		if node.Leaf { // only leaf nodes are series names
			result[node.Name] = node.dsId
		}
	}
	return result
}

func (dss *DataSources) Reload(serde SerDe) error {

	dsList, err := serde.FetchDataSources()
	if err != nil {
		return err
	}

	dss.Lock()
	currentDss := dss.byId
	dss.byName = make(map[string]*DataSource)
	dss.byId = make(map[int64]*DataSource)
	dss.prefixes = make(map[string]bool)
	for _, newDs := range dsList {
		if currentDs, ok := currentDss[newDs.Id]; ok {
			if currentDs.LastUpdate.After(newDs.LastUpdate) {
				// Our cached data is more recent, save it
				newDs.LastUpdate = currentDs.LastUpdate
				newDs.LastDs = currentDs.LastDs
				newDs.Value = currentDs.Value
				newDs.UnknownMs = currentDs.UnknownMs
				newDs.RRAs = currentDs.RRAs
				newDs.LastFlushRT = currentDs.LastFlushRT
			}
		}
		newDs.LastUpdateRT = time.Now()
		dss.byName[newDs.Name] = newDs
		dss.byId[newDs.Id] = newDs
		dss.addPrefixes(newDs.Name)
	}
	dss.Unlock()

	return nil
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

func (ds *DataSource) processDataPoint(dp *DataPoint) error {

	// Do everything in milliseconds
	dpTimeStamp := dp.TimeStamp.UnixNano() / 1000000
	dsLastUpdate := ds.LastUpdate.UnixNano() / 1000000

	if dpTimeStamp < dsLastUpdate {
		return fmt.Errorf("Data point time stamp %v is not greater than data source last update time %v", dp.TimeStamp, dp.DS.LastUpdate)
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
	ds.LastDs = dp.Value
	ds.LastUpdateRT = time.Now()

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
