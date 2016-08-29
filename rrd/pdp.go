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

type pdp struct {
	value    float64
	duration time.Duration
}

func (p *pdp) Value() float64 {
	return p.value
}

func (p *pdp) Duration() time.Duration {
	return p.duration
}

func (p *pdp) SetValue(val float64, dur time.Duration) {
	p.value = val
	p.duration = dur
}

func (p *pdp) AddValue(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) {
			p.value = 0
		}
		p.value = p.value*float64(p.duration)/float64(p.duration+dur) +
			val*float64(dur)/float64(p.duration+dur)
		p.duration = p.duration + dur
	}
}

func (p *pdp) AddValueMax(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) {
			p.value = 0
		}
		if p.value < val {
			p.value = val
		}
		p.duration = p.duration + dur
	}
}

func (p *pdp) AddValueMin(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) {
			p.value = 0
		}
		if p.value > val {
			p.value = val
		}
		p.duration = p.duration + dur
	}
}

func (p *pdp) AddValueLast(val float64, dur time.Duration) {
	if !math.IsNaN(val) && dur > 0 {
		if math.IsNaN(p.value) {
			p.value = 0
		}
		p.value = val
		p.duration = p.duration + dur
	}
}

func (p *pdp) Reset() float64 {
	result := p.value
	p.value = math.NaN()
	p.duration = 0
	return result
}

type ClockPdp struct {
	pdp
	End time.Time
}

func (p *ClockPdp) AddValue(val float64) {
	if p.End.IsZero() {
		p.End = time.Now()
		return
	}
	now := time.Now()
	dur := now.Sub(p.End)
	p.pdp.AddValue(val, dur)
	p.End = now
}
