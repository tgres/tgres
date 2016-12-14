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

// Pdp is a Primary Data Point. It provides intermediate state and
// logic to interpolate and store incoming DP data in a consolidated way,
// using a choice of consolidations such as Weighted Mean, Min, Max or
// Last.
//
// This is an illustration of how incoming data points are
// consolidated into a PDP using weighted mean. The PDP below is 4
// units long (the actual unit is not relevant). It shows a time
// period during which 3 values (measurements) arrived: 1.0 at 1, 3.0
// at 3 and 2.0 at 4. The final value of this PDP is 2.25.
//
//  ||    +---------+    ||
//  ||    |	    3.0 +----||
//  ||----+	        | 2.0||
//  || 1.0|	        |    ||
//  ||====+====+====+====||
//   0    1    2    3     4  ---> time
//
// In this PDP 0.25 of the value is 1.0, 0.50 is 3.0 and 0.25 is 2.0,
// for a total of 0.25*1 + 0.50*3 + 0.25*2 = 2.25.
//
// If a part of the data point is NaN, then that part does not
// count. Even if NaN is at the end:
//
//  ||    +---------+    ||
//  ||    |	     3.0|    ||
//  ||----+	        | NaN||
//  || 1.0|	        |    ||
//  ||====+====+====+====||
//   0    1    2    3     4  ---> time
//
// In the above PDP, the size is what is taken up by 1.0 and 3.0,
// without the NaN. Thus 1/3 of the value is 1.0 and 2/3 of the value
// is 3.0, for a total of 1/3*1 + 2/3*3 = 2.33333.
//
// An alternative way of looking at the above data point is that it is
// simply shorter or has a shorter duration:
//
//  ||    +---------||
//  ||    |      3.0||
//  ||----+         ||
//  || 1.0|         ||
//  ||====+====+====||
//   0    1    2    3     4  ---> time
//
// A datapoint must be all NaN for its value to be NaN. If duration is
// 0, then the value is irrelevant.
//
// To create an "empty" Pdp, simply use its zero value.
type Pdp struct {
	value    float64
	duration time.Duration
}

func (p *Pdp) Value() float64 {
	if p.duration == 0 {
		return math.NaN()
	}
	return p.value
}
func (p *Pdp) Duration() time.Duration { return p.duration }

// SetValue sets both value and duration of the PDP
func (p *Pdp) SetValue(val float64, dur time.Duration) {
	p.value = val
	p.duration = dur
}

// AddValue adds a value to a PDP using weighted mean.
func (p *Pdp) AddValue(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) {
			p.value = 0
		}
		p.value = p.value*float64(p.duration)/float64(p.duration+dur) +
			val*float64(dur)/float64(p.duration+dur)
		p.duration = p.duration + dur
	}
}

// AddValueMax adds a value using max. A non-NaN value is considered
// greater than zero value (duration 0) or NaN.
func (p *Pdp) AddValueMax(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) || p.duration == 0 {
			p.value = val // val "wins" over NaN
		} else if p.value < val {
			p.value = val
		}
		p.duration = p.duration + dur
	}
}

// AddValueMin adds a value using min. A non-NaN value is considered
// lesser than zero value (duration 0) or NaN.
func (p *Pdp) AddValueMin(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) || p.duration == 0 {
			p.value = val // val "wins" over NaN
		} else if p.value > val {
			p.value = val
		}
		p.duration = p.duration + dur
	}
}

// AddValueLast replaces the current value. This is different from
// SetValue in that it's a noop if val is NaN or dur is 0.
func (p *Pdp) AddValueLast(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		p.value = val
		p.duration = p.duration + dur
	}
}

// Reset sets the value to zero value and returns the value of
// the PDP before Reset.
func (p *Pdp) Reset() float64 {
	result := p.Value()
	p.value = 0 // superfluous, but just in case
	p.duration = 0
	return result
}

// ClockPdp is a PDP that uses current time to determine the duration.
type ClockPdp struct {
	Pdp
	End time.Time
}

// AddValue adds a value using a weighted mean. The first time it is
// called it only updates the End field and returns a zero value, the
// second and on will use End to compute the duration.
func (p *ClockPdp) AddValue(val float64) {
	if p.End.IsZero() {
		p.End = time.Now()
		return
	}
	now := time.Now()
	dur := now.Sub(p.End)
	p.Pdp.AddValue(val, dur)
	p.End = now
}
