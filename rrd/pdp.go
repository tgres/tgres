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
	Value    float64
	Duration time.Duration
}

func (p *pdp) SetValue(value float64, duration time.Duration) {
	p.Value = value
	p.Duration = duration
}

func (p *pdp) AddValue(value float64, duration time.Duration) {
	if !math.IsNaN(value) && duration > 0 {
		if math.IsNaN(p.Value) {
			p.Value = 0
		}
		p.Value = p.Value*float64(p.Duration)/float64(p.Duration+duration) +
			value*float64(duration)/float64(p.Duration+duration)
		p.Duration = p.Duration + duration
	}
}

func (p *pdp) AddValueMax(value float64, duration time.Duration) {
	if !math.IsNaN(value) && duration > 0 {
		if math.IsNaN(p.Value) {
			p.Value = 0
		}
		if p.Value < value {
			p.Value = value
		}
		p.Duration = p.Duration + duration
	}
}

func (p *pdp) AddValueMin(value float64, duration time.Duration) {
	if !math.IsNaN(value) && duration > 0 {
		if math.IsNaN(p.Value) {
			p.Value = 0
		}
		if p.Value > value {
			p.Value = value
		}
		p.Duration = p.Duration + duration
	}
}

func (p *pdp) AddValueLast(value float64, duration time.Duration) {
	if !math.IsNaN(value) && duration > 0 {
		if math.IsNaN(p.Value) {
			p.Value = 0
		}
		p.Value = value
		p.Duration = p.Duration + duration
	}
}

func (p *pdp) Reset() float64 {
	result := p.Value
	p.Value = math.NaN()
	p.Duration = 0
	return result
}

type ClockPdp struct {
	pdp
	End time.Time
}

func (p *ClockPdp) AddValue(value float64) {
	if p.End.IsZero() {
		p.End = time.Now()
		return
	}
	now := time.Now()
	duration := now.Sub(p.End)
	p.pdp.AddValue(value, duration)
	p.End = now
}
