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
	"reflect"
	"testing"
	"time"
)

func Test_DataSource(t *testing.T) {

	var (
		id       int64
		name     string
		step, hb time.Duration
		lu       time.Time
		lds      float64
	)

	// NewDataSource
	id, name, step, hb, lu, lds = 1, "foo.bar", 10*time.Second, 300*time.Second, time.Now(), 1.234
	ds := NewDataSource(id, name, step, hb, lu, lds)

	if id != ds.id || name != ds.name || step != ds.step ||
		hb != ds.heartbeat || lu != ds.lastUpdate || lds != ds.lastDs {
		t.Errorf("NewDataSource: id != ds.id || name != ds.name || step != ds.step || hb != ds.heartbeat || lu != ds.lastUpdate || lds != ds.lastDs")
	}

	// Accessors
	if ds.Name() != ds.name {
		t.Errorf("ds.Name() != ds.name")
	}
	if ds.Id() != ds.id {
		t.Errorf("ds.Id() != ds.id")
	}
	if ds.Step() != ds.step {
		t.Errorf("ds.Step() != ds.step")
	}
	if ds.Heartbeat() != ds.heartbeat {
		t.Errorf("ds.Heartbeat() != ds.heartbeat")
	}
	if ds.LastUpdate() != ds.lastUpdate {
		t.Errorf("ds.LastUpdate() != ds.lastUpdate")
	}
	if ds.LastDs() != ds.lastDs {
		t.Errorf("ds.LastDs() != ds.lastDs")
	}

	rras := []*RoundRobinArchive{&RoundRobinArchive{}}
	ds.SetRRAs(rras)
	if !reflect.DeepEqual(ds.RRAs(), rras) {
		t.Errorf("ds.RRAs() != ds.rras")
	}

	ds.SetRRAs([]*RoundRobinArchive{&RoundRobinArchive{dps: map[int64]float64{1: 1, 2: 2, 3: 3}}})
	// PointCount
	pc := ds.PointCount()
	if pc != 3 {
		t.Errorf("ds.PointCount: should return 3, got %v", pc)
	}
}

func Test_DataSource_BestRRA(t *testing.T) {
	var (
		id, points             int64
		name                   string
		step, hb               time.Duration
		lu, latest, start, end time.Time
		lds                    float64
		rras                   []*RoundRobinArchive
		best                   *RoundRobinArchive
	)

	// NewDataSource
	id, name, step, hb, lu, lds = 1, "foo.bar", 10*time.Second, 300*time.Second, time.Now(), 1.234
	ds := NewDataSource(id, name, step, hb, lu, lds)

	ten, twenty := 10*time.Second, 20*time.Second

	// Includes
	latest = time.Unix(10000, 0)
	start = time.Unix(9500, 0)
	end = time.Unix(9600, 0)
	points = int64(10)
	rras = []*RoundRobinArchive{&RoundRobinArchive{latest: latest, step: ten, size: 100}}
	ds.SetRRAs(rras)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.step != ten {
		t.Errorf("BestRRA: The % step should have been selected as the only available within range, instead we got %#v", ten, best)
	}

	// Does not Include, go for longest
	start = time.Unix(5500, 0)
	end = time.Unix(5600, 0)
	rras = []*RoundRobinArchive{
		&RoundRobinArchive{latest: latest, step: ten, size: 100},
		&RoundRobinArchive{latest: latest, step: twenty, size: 100},
	}
	ds.SetRRAs(rras)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.step != twenty {
		t.Errorf("BestRRA: The % step should have been selected as the longest, instead we got %#v", twenty, best)
	}

	// Both include, it should be nearest points, which is 10
	start = time.Unix(9500, 0)
	end = time.Unix(9600, 0)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.step != ten {
		t.Errorf("BestRRA: The % step should have been selected as the nearest resolution, instead we got %#v", ten, best)
	}

	// And now fewer points bigger step
	points = 3
	best = ds.BestRRA(start, end, points)
	if best == nil || best.step != twenty {
		t.Errorf("BestRRA: The % step should have been selected as the nearest resolution, instead we got %#v", twenty, best)
	}

	// And now no points
	points = 0
	// order RRA so as to catch the best > rra comparison
	rras = []*RoundRobinArchive{
		&RoundRobinArchive{latest: latest, step: twenty, size: 100},
		&RoundRobinArchive{latest: latest, step: ten, size: 100},
	}
	ds.SetRRAs(rras)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.step != ten {
		t.Errorf("BestRRA: The % step should have been selected as the smallest resolution (no points), instead we got %#v", ten, best)
	}

	// Test nil
	ds.SetRRAs([]*RoundRobinArchive{})
	best = ds.BestRRA(start, end, points)
	if best != nil {
		t.Errorf("BestRRA should have returned nil, got: %#v", best)
	}

}

func Test_DataSource_surroundingStep(t *testing.T) {
	//ts, step, begin, end:= time.Unix(1000, 0), 10*time.Second, time.Unix(990,0), time.Unix(

	data := map[int64][]int64{
		1000: {10, 990, 1000},
		1001: {10, 1000, 1010},
	}

	for tt, vals := range data {
		ts := time.Unix(tt, 0)
		step := time.Duration(vals[0]) * time.Second
		begin := time.Unix(vals[1], 0)
		end := time.Unix(vals[2], 0)
		b, e := surroundingStep(ts, step)
		if b != begin || e != end {
			t.Errorf("surroundingStep(%v, %v) should return (%v, %v), we get (%v, %v)", ts, step, begin, end, b, e)
		}
	}
}

func Test_DataSource_updateRange(t *testing.T) {

	// To test this, we need a range that begins mid-pdp and ends
	// mid-pdp and spans multiple RRA slots.

	ds := &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]*RoundRobinArchive{
		&RoundRobinArchive{step: 10 * time.Second, size: 10},
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})

	begin, end := time.Unix(104, 0), time.Unix(156, 0)
	ds.updateRange(begin, end, 100.0)

	exp1 := map[int64]float64{1: 100, 2: 100, 3: 100, 4: 100, 5: 100}
	if !reflect.DeepEqual(ds.rras[0].dps, exp1) {
		t.Errorf("updateRange: expecting rra[0].dps: %v, got %v", exp1, ds.rras[0].dps)
	}
	if !math.IsNaN(ds.rras[0].value) || ds.rras[0].duration != 0 {
		t.Errorf("updateRange: !math.IsNaN(ds.rras[0].value) || ds.rras[1].duration != 0")
	}

	exp2 := map[int64]float64{6: 100, 7: 100}
	if !reflect.DeepEqual(ds.rras[1].dps, exp2) {
		t.Errorf("updateRange: expecting rra1.dps: %v, got %v", exp2, ds.rras[1].dps)
	}
	if ds.rras[1].value != 100 || ds.rras[1].duration != 10*time.Second {
		t.Errorf("updateRange: ds.rras[1].value != 100 || ds.rras[1].duration != 10*time.Second")
	}

	// Now do this again with the end aligned on PDP
	ds = &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]*RoundRobinArchive{
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})

	begin, end = time.Unix(104, 0), time.Unix(160, 0)
	ds.updateRange(begin, end, 100.0)

	exp3 := map[int64]float64{6: 100, 7: 100, 8: 100}
	if !reflect.DeepEqual(ds.rras[0].dps, exp3) {
		t.Errorf("updateRange: expecting rra[0].dps: %v, got %v", exp3, ds.rras[0].dps)
	}
	if !math.IsNaN(ds.rras[0].value) || ds.rras[0].duration != 0 {
		t.Errorf("updateRange: !math.IsNaN(ds.rras[0].value) || ds.rras[1].duration != 0")
	}
}

func Test_DataSource_ProcessIncomingDataPoint(t *testing.T) {

	ds := &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]*RoundRobinArchive{
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})
	ds.Reset()
	ds.rras[0].Reset()

	ds.ProcessIncomingDataPoint(100, time.Unix(104, 0))
	if !math.IsNaN(ds.value) || ds.duration != 0 || !math.IsNaN(ds.rras[0].value) || ds.rras[0].duration != 0 || len(ds.rras[0].dps) > 0 {
		t.Errorf("ProcessIncomingDataPoint: on first call, !math.IsNaN(ds.value) || ds.duration != 0 || !math.IsNaN(ds.rras[0].value) || ds.rras[0].duration != 0 || len(ds.rras[0].dps) > 0")
	}
	if ds.lastDs != 100 || !ds.lastUpdate.Equal(time.Unix(104, 0)) {
		t.Errorf("ProcessIncomingDataPoint: on first call, ds.lastDs != 100 || !ds.lastUpdate.Equal(time.Unix(104, 0))")
	}

	ds.ProcessIncomingDataPoint(100, time.Unix(156, 0))
	if ds.value != 100 || ds.duration != 6*time.Second || ds.rras[0].value != 100 || ds.rras[0].duration != 10*time.Second {
		t.Errorf("ProcessIncomingDataPoint: on second call, ds.value != 100 || ds.duration != 6s || ds.rras[0].value != 100 || ds.rras[0].duration != 10s: %v %v %v %v",
			ds.value, ds.duration, ds.rras[0].value, ds.rras[0].duration)
	}

	// heartbeat
	ds = &DataSource{step: 10 * time.Second, heartbeat: time.Second}
	ds.Reset()
	ds.ProcessIncomingDataPoint(100, time.Unix(104, 0))
	if !math.IsNaN(ds.lastDs) {
		t.Errorf("ProcessIncomingDataPoint: heartbeat exceeded but !math.IsNaN(ds.lastDs)")
	}

	// Inf
	if err := ds.ProcessIncomingDataPoint(math.Inf(1), time.Unix(104, 0)); err == nil {
		t.Errorf("ProcessIncomingDataPoint: no error on Inf value")
	}

	// Before last update
	if err := ds.ProcessIncomingDataPoint(100, time.Unix(1, 0)); err == nil {
		t.Errorf("ProcessIncomingDataPoint: no error on data point prior to last update")
	}
}

func Test_DataSource_ClearRRAs(t *testing.T) {

	ds := &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]*RoundRobinArchive{
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})
	ds.Reset()
	ds.rras[0].Reset()

	ds.ClearRRAs(false)
	if ds.rras[0].dps != nil {
		t.Errorf("ClearRRAs: ds.rras[0].dps got replaced even though it's empty?")
	}

	ds.rras[0].dps = map[int64]float64{1: 1}
	ds.lastUpdate = time.Now()
	ds.ClearRRAs(false)
	if len(ds.rras[0].dps) != 0 || ds.rras[0].start != 0 || ds.rras[0].end != 0 {
		t.Errorf("ClearRRAs: len(ds.rras[0].dps) != 0 || ds.rras[0].start != 0 || ds.rras[0].end != 0")
	}
	if ds.lastUpdate.IsZero() {
		t.Errorf("ClearRRAs: with false: ds.lastUpdate.IsZero()")
	}

	ds.ClearRRAs(true)
	if !ds.lastUpdate.IsZero() {
		t.Errorf("ClearRRAs: with true: !ds.lastUpdate.IsZero()")
	}
}

func Test_DataSource_Copy(t *testing.T) {

	ds := &DataSource{
		Pdp: Pdp{
			value:    123,
			duration: 4 * time.Second,
		},
		id:         34534,
		name:       "dfssslksdmlfkm",
		step:       10 * time.Second,
		heartbeat:  45 * time.Second,
		lastUpdate: time.Now(),
		lastDs:     93458,
	}
	ds.SetRRAs([]*RoundRobinArchive{
		&RoundRobinArchive{step: 20 * time.Second, size: 10, dps: map[int64]float64{6: 100, 7: 100, 8: 100}},
	})

	cpy := ds.Copy()
	if !reflect.DeepEqual(ds, cpy) {
		t.Errorf("Copy: !reflect.DeepEqual(ds, cpy)")
	}
}
