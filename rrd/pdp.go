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
	"time"
)

type dynamicPdp struct {
	Value    float64
	Duration time.Duration
}

func (p *dynamicPdp) setValue(value float64, duration time.Duration) {
	p.Value = value
	p.Duration = duration
}

func (p *dynamicPdp) addValue(value float64, duration time.Duration) {
	if !math.IsNaN(value) && duration > 0 {
		if math.IsNaN(p.Value) {
			p.Value = 0
		}
		p.Value = p.Value*float64(p.Duration)/float64(p.Duration+duration) +
			value*float64(duration)/float64(p.Duration+duration)
		p.Duration = p.Duration + duration
	}
}

func (p *dynamicPdp) reset() {
	p.Value = math.NaN()
	p.Duration = 0
}

// type pdp struct {
// 	Value     float64 // Weighted value (e.g. f we are 2/3 way into a step, Value should be 2/3 of the final step value)
// 	UnknownMs int64   // Ms of the data that is "unknown" (e.g. because of exceeded HB)
// 	StepMs    int64   // Step Size in Ms
// }

// func (ds *pdp) setValue(value float64) {
// 	ds.Value = value
// 	ds.UnknownMs = 0
// }

// func (ds *pdp) addValue(value float64, durationMs int64, allowNaNtoValue bool) {
// 	// A DS can go from NaN to a value, but only if the previous update was in the same PDP
// 	if math.IsNaN(ds.Value) && allowNaNtoValue {
// 		ds.Value = 0
// 	}
// 	// We subtract ds.UnknownMs from the step size so that the value is not
// 	// "diluted" by the unknown part, since we cannot assume it is 0.
// 	ds.Value += value * float64(durationMs) / float64(ds.StepMs-ds.UnknownMs)

// 	if math.IsNaN(value) { // NB: if ds.Value is NaN, but value is a number, UnknownMs is not incremented
// 		ds.UnknownMs = ds.UnknownMs + durationMs
// 	}
// }

// func (ds *pdp) addValue2(value float64, durationMs int64) {
// 	if math.IsNaN(value) {
// 		ds.UnknownMs += durationMs
// 		if ds.UnknownMs >= ds.StepMs {
// 			ds.Value = math.NaN()
// 		}
// 		return
// 	}

// 	// We subtract ds.UnknownMs from the step size so that the value is not
// 	// "diluted" by the unknown part, since we cannot assume it is 0.
// 	ds.Value += value * float64(durationMs) / float64(ds.StepMs-ds.UnknownMs)
// }

// func (ds *pdp) reset() {
// 	ds.Value = 0
// 	ds.UnknownMs = 0
// }
