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
		step, hb time.Duration
		lu       time.Time
	)

	// NewDataSource
	step, hb, lu = 10*time.Second, 300*time.Second, time.Now()
	ds := NewDataSource(DSSpec{
		Step:       step,
		Heartbeat:  hb,
		LastUpdate: lu,
		RRAs: []RRASpec{
			RRASpec{Step: 10 * time.Second, Span: 100 * time.Second},
		},
	})

	if step != ds.step || hb != ds.heartbeat || lu != ds.lastUpdate {
		t.Errorf("NewDataSource: step != ds.step || hb != ds.heartbeat || lu != ds.lastUpdate")
	}

	// Accessors
	if ds.Step() != ds.step {
		t.Errorf("ds.Step() != ds.step")
	}
	if ds.Heartbeat() != ds.heartbeat {
		t.Errorf("ds.Heartbeat() != ds.heartbeat")
	}
	if ds.LastUpdate() != ds.lastUpdate {
		t.Errorf("ds.LastUpdate() != ds.lastUpdate")
	}

	if len(ds.rras) == 0 {
		t.Errorf("len(ds.rras) == 0")
	}

	ds.SetRRAs([]RoundRobinArchiver{&RoundRobinArchive{dps: map[int64]float64{1: 1, 2: 2, 3: 3}}})
	// PointCount
	pc := ds.PointCount()
	if pc != 3 {
		t.Errorf("ds.PointCount: should return 3, got %v", pc)
	}
}

func Test_DataSource_BestRRA(t *testing.T) {
	var (
		points                 int64
		step, hb               time.Duration
		lu, latest, start, end time.Time
		rras                   []RoundRobinArchiver
		best                   RoundRobinArchiver
	)

	// NewDataSource
	step, hb, lu = 10*time.Second, 300*time.Second, time.Now()
	ds := NewDataSource(DSSpec{Step: step, Heartbeat: hb, LastUpdate: lu})

	ten, twenty := 10*time.Second, 20*time.Second

	// Includes
	latest = time.Unix(10000, 0)
	start = time.Unix(9500, 0)
	end = time.Unix(9600, 0)
	points = int64(10)
	rras = []RoundRobinArchiver{&RoundRobinArchive{latest: latest, step: ten, size: 100}}
	ds.SetRRAs(rras)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.Step() != ten {
		t.Errorf("BestRRA: The % step should have been selected as the only available within range, instead we got %#v", ten, best)
	}

	// Does not Include, go for longest
	start = time.Unix(5500, 0)
	end = time.Unix(5600, 0)
	rras = []RoundRobinArchiver{
		&RoundRobinArchive{latest: latest, step: ten, size: 100},
		&RoundRobinArchive{latest: latest, step: twenty, size: 100},
	}
	ds.SetRRAs(rras)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.Step() != twenty {
		t.Errorf("BestRRA: The % step should have been selected as the longest, instead we got %#v", twenty, best)
	}

	// Both include, it should be nearest points, which is 10
	start = time.Unix(9500, 0)
	end = time.Unix(9600, 0)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.Step() != ten {
		t.Errorf("BestRRA: The % step should have been selected as the nearest resolution, instead we got %#v", ten, best)
	}

	// If start is past latest, it should include
	start = time.Unix(10100, 0)
	end = time.Unix(10200, 0)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.Step() != ten {
		t.Errorf("BestRRA: The % step should have been selected even if start > latest, instead we got %#v", ten, best)
	}

	// And now fewer points bigger step
	start = time.Unix(9500, 0)
	end = time.Unix(9600, 0)
	points = 3
	best = ds.BestRRA(start, end, points)
	if best == nil || best.Step() != twenty {
		t.Errorf("BestRRA: The % step should have been selected as the nearest resolution, instead we got %#v", twenty, best)
	}

	// And now no points
	points = 0
	// order RRA so as to catch the best > rra comparison
	rras = []RoundRobinArchiver{
		&RoundRobinArchive{latest: latest, step: twenty, size: 100},
		&RoundRobinArchive{latest: latest, step: ten, size: 100},
	}
	ds.SetRRAs(rras)
	best = ds.BestRRA(start, end, points)
	if best == nil || best.Step() != ten {
		t.Errorf("BestRRA: The % step should have been selected as the smallest resolution (no points), instead we got %#v", ten, best)
	}

	// Test nil
	ds.SetRRAs([]RoundRobinArchiver{})
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
	ds.SetRRAs([]RoundRobinArchiver{
		&RoundRobinArchive{step: 10 * time.Second, size: 10},
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
		&RoundRobinArchive{step: 50 * time.Second, size: 10},
		&RoundRobinArchive{step: 100 * time.Second, size: 10},
	})

	begin, end := time.Unix(103, 0), time.Unix(156, 0)
	ds.updateRange(begin, end, 100.0)

	exp1 := map[int64]float64{1: 100, 2: 100, 3: 100, 4: 100, 5: 100}
	if !reflect.DeepEqual(ds.rras[0].DPs(), exp1) {
		t.Errorf("updateRange: expecting rra[0].DPs(): %v, got %v", exp1, ds.rras[0].DPs())
	}
	if !math.IsNaN(ds.rras[0].Value()) || ds.rras[0].Duration() != 0 {
		t.Errorf("updateRange: !math.IsNaN(ds.rras[0].Value()) || ds.rras[1].Duration() != 0")
	}

	exp2 := map[int64]float64{6: 100, 7: 100}
	if !reflect.DeepEqual(ds.rras[1].DPs(), exp2) {
		t.Errorf("updateRange: expecting rra[1].DPs(): %v, got %v", exp2, ds.rras[1].DPs())
	}
	// duration 10 is correct, the remaining 6 are in the DS PDP
	if ds.rras[1].Value() != 100 || ds.rras[1].Duration() != 10*time.Second {
		t.Errorf("updateRange: ds.rras[1].Value() %v != 100 || ds.rras[1].Duration() %v != 10*time.Second", ds.rras[1].Value(), ds.rras[1].Duration())
	}

	exp3 := map[int64]float64{3: 100}
	if !reflect.DeepEqual(ds.rras[2].DPs(), exp3) {
		t.Errorf("updateRange: expecting rra[2].DPs(): %v, got %v", exp3, ds.rras[2].DPs())
	}
	// Nothing in the 50, the 6 secs are in PDP
	if !math.IsNaN(ds.rras[2].Value()) || ds.rras[2].Duration() != 0 {
		t.Errorf("updateRange: !math.IsNaN(ds.rras[2].Value()) %v || ds.rras[2].Duration() %v != 0", ds.rras[2].Value(), ds.rras[2].Duration())
	}

	if len(ds.rras[3].DPs()) > 0 {
		t.Errorf("rra[3].DPs() should be empty")
	}
	// here 6s are in ds.pdp, 3 skipped in the beginning, 47 is left
	if ds.rras[3].Value() != 100 || ds.rras[3].Duration() != 47*time.Second {
		t.Errorf("updateRange: ds.rras[3].Value() %v != 100 || ds.rras[3].Duration() %v != 47s", ds.rras[3].Value(), ds.rras[3].Duration())
	}

	// The remaining 6 seconds should be in the DS
	if ds.Duration() != 6*time.Second || ds.Value() != 100 {
		t.Errorf("updateRange: ds.Duration() != 6*time.Second || ds.Value() != 100")
	}

	// Now do this again with the end aligned on PDP
	ds = &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]RoundRobinArchiver{
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})

	begin, end = time.Unix(104, 0), time.Unix(160, 0)
	ds.updateRange(begin, end, 100.0)

	exp4 := map[int64]float64{6: 100, 7: 100, 8: 100}
	if !reflect.DeepEqual(ds.rras[0].DPs(), exp4) {
		t.Errorf("updateRange: expecting rra[0].DPs(): %v, got %v", exp3, ds.rras[0].DPs())
	}
	if !math.IsNaN(ds.rras[0].Value()) || ds.rras[0].Duration() != 0 {
		t.Errorf("updateRange: !math.IsNaN(ds.rras[0].Value()) || ds.rras[1].Duration() != 0")
	}
}

func Test_DataSource_ProcessDataPoint(t *testing.T) {

	ds := &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]RoundRobinArchiver{
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})

	ds.ProcessDataPoint(100, time.Unix(104, 0))
	if ds.value != 0 || ds.duration != 0 || !math.IsNaN(ds.rras[0].Value()) || ds.rras[0].Duration() != 0 || len(ds.rras[0].DPs()) > 0 {
		t.Errorf("ProcessDataPoint: on first call, ds.value != 0 || ds.duration != 0 || !math.IsNaN(ds.rras[0].Value()) || ds.rras[0].Vuration() != 0 || len(ds.rras[0].DPs()) > 0")
	}
	if !ds.lastUpdate.Equal(time.Unix(104, 0)) {
		t.Errorf("ProcessDataPoint: on first call, !ds.lastUpdate.Equal(time.Unix(104, 0))")
	}

	ds.ProcessDataPoint(100, time.Unix(156, 0))
	if ds.value != 100 || ds.duration != 6*time.Second || ds.rras[0].Value() != 100 || ds.rras[0].Duration() != 10*time.Second {
		t.Errorf("ProcessDataPoint: on second call, ds.value != 100 || ds.duration != 6s || ds.rras[0].Value() != 100 || ds.rras[0].Duration() != 10s: %v %v %v %v",
			ds.value, ds.duration, ds.rras[0].Value(), ds.rras[0].Duration())
	}

	// heartbeat
	ds = &DataSource{step: 10 * time.Second, heartbeat: time.Second}
	ds.ProcessDataPoint(100, time.Unix(104, 0))

	// Inf
	if err := ds.ProcessDataPoint(math.Inf(1), time.Unix(104, 0)); err == nil {
		t.Errorf("ProcessDataPoint: no error on Inf value")
	}

	// Before last update
	if err := ds.ProcessDataPoint(100, time.Unix(1, 0)); err == nil {
		t.Errorf("ProcessDataPoint: no error on data point prior to last update")
	}
}

func Test_DataSource_ClearRRAs(t *testing.T) {

	ds := &DataSource{step: 10 * time.Second}
	ds.SetRRAs([]RoundRobinArchiver{
		&RoundRobinArchive{step: 20 * time.Second, size: 10},
	})

	ds.ClearRRAs()
	if ds.rras[0].DPs() != nil {
		t.Errorf("ClearRRAs: ds.rras[0].dps got replaced even though it's empty?")
	}

	ds.SetRRAs([]RoundRobinArchiver{
		&RoundRobinArchive{step: 20 * time.Second, size: 10, dps: map[int64]float64{1: 1}},
	})
	ds.lastUpdate = time.Now()
	ds.ClearRRAs()
	if len(ds.rras[0].DPs()) != 0 || ds.rras[0].Start() != 0 || ds.rras[0].End() != 0 {
		t.Errorf("ClearRRAs: len(ds.rras[0].DPs()) != 0 || ds.rras[0].Start() != 0 || ds.rras[0].End() != 0")
	}
	if ds.lastUpdate.IsZero() {
		t.Errorf("ClearRRAs: with false: ds.lastUpdate.IsZero()")
	}
}

func Test_DataSource_Copy(t *testing.T) {

	ds := &DataSource{
		Pdp: Pdp{
			value:    123,
			duration: 4 * time.Second,
		},
		step:       10 * time.Second,
		heartbeat:  45 * time.Second,
		lastUpdate: time.Now(),
	}
	ds.SetRRAs([]RoundRobinArchiver{
		&RoundRobinArchive{step: 20 * time.Second, size: 10, dps: map[int64]float64{6: 100, 7: 100, 8: 100}},
	})

	cpy := ds.Copy()
	if !reflect.DeepEqual(ds, cpy) {
		t.Errorf("Copy: !reflect.DeepEqual(ds, cpy)")
	}
}
