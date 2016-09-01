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
	"testing"
	"time"
)

func TestPdp_NewPdp(t *testing.T) {
	dp := NewPdp()
	if !math.IsNaN(dp.value) {
		t.Errorf("NewPdp() return dp not NaN")
	}
}

func TestPdp_Accessors(t *testing.T) {
	dp := NewPdp()
	dp.value = 123
	if dp.Value() != dp.value {
		t.Errorf("dp.Value(): %v  != dp.value: %v", dp.Value(), dp.value)
	}
	if dp.Duration() != dp.Duration() {
		t.Errorf("dp.Duration(): %v  != dp.duration: %v", dp.Duration(), dp.duration)
	}
}

func TestPdp_SetValue(t *testing.T) {
	for v, d := range map[float64]time.Duration{456: 876, math.NaN(): 987, math.Inf(-1): 789} {
		dp := NewPdp()
		dp.SetValue(v, d)
		if dp.Duration() != d {
			t.Errorf("dp.SetValue() did not set duration")
		}
		if !math.IsNaN(v) && dp.Value() != v || math.IsNaN(v) && !math.IsNaN(dp.value) {
			t.Errorf("dp.SetValue() did not set value")
		}
	}
}

func TestPdp_AddValue(t *testing.T) {
	for v1, d1 := range map[float64]time.Duration{ // Starting values
		456:          123 * time.Second, // Just two numbers
		math.NaN():   0,                 // Empty dp
		math.NaN():   567,               // NaN with some duration
		math.Inf(-1): 789,               // -Inf with some duration
	} {
		for v2, d2 := range map[float64]time.Duration{ // Values we add
			567:         8910 * time.Second, // Just two numbers
			math.NaN():  0,                  // NaN, 0
			math.NaN():  234,                // NaN, not 0
			234:         0,                  // not 0, 0
			math.Inf(1): 876,                // +Inf, not 0
		} {
			dp := NewPdp()
			dp.SetValue(v1, d1)

			var (
				ev float64       // expected value
				ed time.Duration // expected duration
			)

			if math.IsNaN(v2) || d2 == 0 {
				ev = v1
				ed = d1
			} else {
				adj_v1 := v1
				if math.IsNaN(adj_v1) {
					adj_v1 = 0
				}
				ev = adj_v1*float64(d1)/float64(d1+d2) + v2*float64(d2)/float64(d1+d2)
				ed = d1 + d2
			}

			dp.AddValue(v2, d2)
			if (!math.IsNaN(ev) && !math.IsNaN(dp.value)) && ev != dp.value || (math.IsNaN(ev) && !math.IsNaN(dp.value) || (!math.IsNaN(ev) && math.IsNaN(dp.value))) {
				t.Errorf("AddValue: (%v, %v) + (%v, %v) != (%v, %v) but is (%v, %v)", v1, d1, v2, d2, ev, ed, dp.value, dp.duration)
			}
		}
	}

}
