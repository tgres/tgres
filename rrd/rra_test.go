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

func Test_RoundRobinArchive(t *testing.T) {
	var (
		size   int64
		step   time.Duration
		cf     Consolidation
		xff    float32
		latest time.Time
	)

	step, size, cf, xff, latest = 10*time.Second, 100, WMEAN, 0.5, time.Now()

	// Again, this time good data
	rra := NewRoundRobinArchive(RRASpec{Step: step, Span: time.Duration(size) * step, Function: cf, Xff: xff, Latest: latest})

	if rra.cf != WMEAN || step != rra.step ||
		size != rra.size || xff != rra.xff ||
		latest != rra.latest {
		t.Errorf(`cf != rra.cf || step != rra.step || size != rra.size || xff != rra.xff || latest != rra.latest`)
	}

	if rra.dps == nil {
		t.Errorf("NewRoundRobinArchive: len(rra.dps) == 0")
	}

	// Accessors
	if rra.Latest() != rra.latest {
		t.Errorf("rra.Latest(): %v  != rra.latest: %v", rra.Latest(), rra.latest)
	}
	if rra.Size() != rra.size {
		t.Errorf("rra.Size(): %v  != rra.size: %v", rra.Size(), rra.size)
	}
	if rra.Start() != rra.start {
		t.Errorf("rra.Start(): %v  != rra.start: %v", rra.Start(), rra.start)
	}
	if rra.End() != rra.end {
		t.Errorf("rra.End(): %v  != rra.end: %v", rra.End(), rra.end)
	}
	if rra.Step() != rra.step {
		t.Errorf("rra.Step(): %v  != rra.step: %v", rra.Step(), rra.step)
	}

	// Copy()
	rra.dps[1] = 123.45
	cpy := rra.Copy()
	if !reflect.DeepEqual(cpy, rra) {
		t.Errorf("rra.copy() is not a copy")
	}

	// Begins()
	// Step 60s Size 10 => 600s
	now := time.Unix(1472700000, 0)
	rra.size = 9
	rra.step = 61 * time.Second
	et := time.Unix(1472699394, 0)
	begins := rra.Begins(now)
	if !et.Equal(begins) {
		t.Errorf("Begins: expecting %v, but got %v", et, begins)
	}
	rra.size = 10
	rra.step = 60 * time.Second
	et = time.Unix(1472699460, 0)
	begins = rra.Begins(now)
	if !et.Equal(begins) {
		t.Errorf("Begins: expecting %v, but got %v", et, begins)
	}

	// PointCount
	if rra.PointCount() != len(rra.dps) {
		t.Errorf("PointCount != rra.dps")
	}

	// includes
	it := rra.latest.Add(-time.Second)
	if !rra.includes(it) {
		t.Errorf("Includes: %v should be included. rra.latest: %v rra.Begins(rra.latest): %v", it, rra.latest, rra.Begins(rra.latest))
	}
	it = rra.latest.Add(time.Second)
	if rra.includes(it) {
		t.Errorf("Includes: %v should NOT be included. rra.latest: %v rra.Begins(rra.latest): %v", it, rra.latest, rra.Begins(rra.latest))
	}
	it = rra.Begins(latest).Add(-time.Second)
	if rra.includes(it) {
		t.Errorf("Includes: %v should NOT be included. rra.latest: %v rra.Begins(rra.latest): %v", it, rra.latest, rra.Begins(rra.latest))
	}
}

func Test_RoundRobinArchive_update(t *testing.T) {

	// All the possibilities we want to test. Value is 50 unless noted.
	//
	//     0     10     20     30     40
	//     |------|------|------|------| begin, end, ds_dur => expected dps, rra_value, rra_dur
	//  0:               +------+        20, 30, 10 => {3:50},           NaN, 0s
	//  1:               +--+            20, 24, 4  =>     {},            50, 4s
	//  2:                  +---+        24, 30, 6  => {3:50},           NaN, 0s
	//  3:                      +---UU-+ 30, 40, 6  => {0:50},           NaN, 0s
	//                              ^^ 50 is correct! NaN is NOT a 0! (30 would be wrong).
	//  4:  WMEAN        +--+     val 5  20, 24, 4  =>     {},            50, 4s
	//  5:                  +---+ keep   24, 30, 6  => {3:50},           NaN, 0s
	//  6:  MIN          +--+     val 5  20, 24, 4  =>     {},             5, 4s
	//  7:                  +---+ keep   24, 30, 6  => {3:5},            NaN, 0s
	//  8:  MAX          +--+     val 5  20, 24, 4  =>     {},             5, 4s
	//  9:                  +---+ keep   24, 30, 6  => {3:50},           NaN, 0s
	// 10:  LAST         +--+     val 5  20, 24, 4  =>     {},             5, 4s
	// 11:                  +---+ keep   24, 30, 6  => {3:50},           NaN, 0s
	// 12:           +----------+        14, 30, 10 => {2:50,3:50},      NaN, 0
	// 13:        +-------------+        10, 30, 10 => {2:50,3:50},      NaN, 0
	// 14:      +---------------+         4, 30, 10 => {1:50,2:50,3:50}, NaN, 0
	// 15:      +------------+            4, 24, 4  => {1:50,2:50},       50, 1s
	// 16:                      +---UU-+ 30, 40, 6  => {0:NaN},          NaN, 0s // xff 0.7
	// 17:               +-----+  v: NaN 20, 30, 9  => {0:0},            NaN, 0s // partial NaN == NOOP
	// 18:               +------+ v: NaN 20, 30, 10 => {0:NaN},          NaN, 0s // full NaN == NaN
	//     |------|------|------|------|

	step := 10 * time.Second
	size := int64(4)

	type valz struct {
		begin, end time.Time
		dsVal      float64
		dsDur      time.Duration
		rraDps     map[int64]float64
		rraVal     float64
		rraDur     time.Duration
		keep       bool
		cf         Consolidation
		xff        float32
	}

	testVals := []valz{
		0: {
			begin:  time.Unix(20, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  10 * time.Second,
			rraDps: map[int64]float64{3: 50},
			rraVal: 0,
			rraDur: 0},
		1: {
			begin:  time.Unix(20, 0),
			end:    time.Unix(24, 0),
			dsVal:  50,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{},
			rraVal: 50,
			rraDur: 4 * time.Second},
		2: {
			begin:  time.Unix(24, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  6 * time.Second,
			rraDps: map[int64]float64{3: 50},
			rraVal: 0,
			rraDur: 0},
		3: {
			begin:  time.Unix(30, 0),
			end:    time.Unix(40, 0),
			dsVal:  50,
			dsDur:  6 * time.Second,
			rraDps: map[int64]float64{0: 50},
			rraVal: 0,
			rraDur: 0},
		4: {
			begin:  time.Unix(20, 0),
			end:    time.Unix(24, 0),
			dsVal:  5,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{},
			rraVal: 5,
			rraDur: 4 * time.Second},
		5: {
			keep:   true,
			begin:  time.Unix(24, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  6 * time.Second,
			rraDps: map[int64]float64{3: 32.0},
			rraVal: 0,
			rraDur: 0},
		6: { // MIN
			cf:     MIN,
			begin:  time.Unix(20, 0),
			end:    time.Unix(24, 0),
			dsVal:  5,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{},
			rraVal: 5,
			rraDur: 4 * time.Second},
		7: {
			keep:   true,
			begin:  time.Unix(24, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  6 * time.Second,
			rraDps: map[int64]float64{3: 5},
			rraVal: 0,
			rraDur: 0},
		8: { // MAX
			cf:     MAX,
			begin:  time.Unix(20, 0),
			end:    time.Unix(24, 0),
			dsVal:  5,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{},
			rraVal: 5,
			rraDur: 4 * time.Second},
		9: {
			keep:   true,
			begin:  time.Unix(24, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{3: 50},
			rraVal: 0,
			rraDur: 0},
		10: { // LAST
			cf:     LAST,
			begin:  time.Unix(20, 0),
			end:    time.Unix(24, 0),
			dsVal:  5,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{},
			rraVal: 5,
			rraDur: 4 * time.Second},
		11: {
			keep:   true,
			begin:  time.Unix(24, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  6 * time.Second,
			rraDps: map[int64]float64{3: 50},
			rraVal: 0,
			rraDur: 0},
		12: {
			begin:  time.Unix(14, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  10 * time.Second,
			rraDps: map[int64]float64{2: 50, 3: 50},
			rraVal: 0,
			rraDur: 0},
		13: {
			begin:  time.Unix(10, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  10 * time.Second,
			rraDps: map[int64]float64{2: 50, 3: 50},
			rraVal: 0,
			rraDur: 0},
		14: {
			begin:  time.Unix(4, 0),
			end:    time.Unix(30, 0),
			dsVal:  50,
			dsDur:  10 * time.Second,
			rraDps: map[int64]float64{1: 50, 2: 50, 3: 50},
			rraVal: 0,
			rraDur: 0},
		15: {
			begin:  time.Unix(4, 0),
			end:    time.Unix(24, 0),
			dsVal:  50,
			dsDur:  4 * time.Second,
			rraDps: map[int64]float64{1: 50, 2: 50},
			rraVal: 50,
			rraDur: 4 * time.Second},
		16: {
			begin:  time.Unix(30, 0),
			end:    time.Unix(40, 0),
			dsVal:  50,
			dsDur:  6 * time.Second,
			xff:    0.7,
			rraDps: map[int64]float64{}, // {0: math.NaN()},
			rraVal: 0,
			rraDur: 0},
		17: { // partial step NaN
			begin:  time.Unix(20, 0),
			end:    time.Unix(30, 0),
			dsVal:  math.NaN(),
			dsDur:  9 * time.Second,
			rraDps: map[int64]float64{3: 0},
			rraVal: 0,
			rraDur: 0},
		18: { // whole step NaN
			begin:  time.Unix(20, 0),
			end:    time.Unix(30, 0),
			dsVal:  math.NaN(),
			dsDur:  10 * time.Second,
			rraDps: map[int64]float64{}, //{3: math.NaN()},
			rraVal: 0,
			rraDur: 0},
	}

	var rra *RoundRobinArchive
	for n, vals := range testVals {

		if !vals.keep {
			rra = &RoundRobinArchive{step: step, size: size, cf: vals.cf, xff: vals.xff}
			rra.Reset()
		}

		rra.update(vals.begin, vals.end, vals.dsVal, vals.dsDur)

		// Stupid trick - replace NaNs with 999 (Since we do not store
		// NaNs anymore, it is not really needed anymore)
		for k, v := range rra.dps {
			if math.IsNaN(v) {
				rra.dps[k] = 999
			}
		}
		for k, v := range vals.rraDps {
			if math.IsNaN(v) {
				vals.rraDps[k] = 999
			}
		}

		if !((len(rra.dps) == 0 && len(vals.rraDps) == 0) || reflect.DeepEqual(rra.dps, vals.rraDps)) {
			t.Errorf("update: for %d (%v, %v, %v, %v) we expect %v, but got %v", n, vals.begin, vals.end, vals.dsVal, vals.dsDur, vals.rraDps, rra.dps)
		}
		if rra.value != vals.rraVal || rra.duration != vals.rraDur {
			if !(math.IsNaN(rra.value) && math.IsNaN(vals.rraVal)) {
				t.Errorf("update: for %d (%v, %v, %v, %v) we expect rra (val, dur) of (%v, %v), but got (%v, %v)",
					n, vals.begin, vals.end, vals.dsVal, vals.dsDur, vals.rraVal, vals.rraDur, rra.value, rra.duration)
			}
		}
	}

}
