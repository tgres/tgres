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
package rrd

import (
	"bytes"
	"encoding/gob"
	"fmt"
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

// IncomingDP is incoming data, i.e. this is the form in which input
// data is expected. This is not an internal representation of a data
// point, it's the format in which they are expected to arrive and is
// easy to convert to from most ant data point representation out
// there. This data point representation has no notion of duration and
// therefore must rely on some kind of an externally stored "last
// update" time.
type IncomingDP struct {
	Name      string
	TimeStamp time.Time
	Value     float64
	Hops      int
}

// Implement GobEncoder (or else we get a "has no exported fields")
func (dp *IncomingDP) GobEncode() ([]byte, error) {
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

func (dp *IncomingDP) GobDecode(b []byte) error {
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

// Process will append the data point to the the DS's archive(s). Once
// an incoming data point is processed, it can be discarded, it's not
// very useful for anything.
func (dp *IncomingDP) Process(ds *DataSource) error {
	if ds == nil {
		return fmt.Errorf("Cannot process data point with nil DS.")
	}
	return ds.processIncomingDP(dp)
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
	dss.byName[ds.name] = ds
	dss.byId[ds.id] = ds
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

	delete(dss.byName, ds.name)
	delete(dss.byId, ds.id)
}
