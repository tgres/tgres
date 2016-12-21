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

package dsl

import (
	"math"
	"time"
)

type SliceSeries struct {
	data  []float64
	start time.Time
	pos   int
	step  time.Duration
	alias string
}

func NewSliceSeries(data []float64, start time.Time, step time.Duration) *SliceSeries {
	return &SliceSeries{
		data:  data,
		start: start,
		pos:   -1,
		step:  step,
	}
}

func (s *SliceSeries) Next() bool {
	if s.pos < len(s.data) {
		s.pos++ // Allow pos to roll 1 past len so that CurrentValue knows to return a NaN
		return s.pos < len(s.data)
	}
	return false
}

func (s *SliceSeries) CurrentValue() float64 {
	if s.pos >= 0 && s.pos < len(s.data) {
		return s.data[s.pos]
	}
	return math.NaN()
}

func (s *SliceSeries) CurrentTime() time.Time {
	if s.pos >= 0 && s.pos < len(s.data) {
		return s.start.Add(s.step * time.Duration(s.pos))
	}
	return time.Time{}
}

func (s *SliceSeries) Close() error {
	s.pos = -1
	return nil
}

func (s *SliceSeries) Step() time.Duration {
	return s.step
}

func (s *SliceSeries) GroupBy(...time.Duration) time.Duration {
	return s.step
}

func (s *SliceSeries) TimeRange(...time.Time) (time.Time, time.Time) {
	return time.Time{}, time.Time{} // not applicable
}

func (s *SliceSeries) Latest() time.Time {
	return s.start.Add(s.step * time.Duration(len(s.data)))
}

func (s *SliceSeries) MaxPoints(...int64) int64 {
	return 0 // not applicable
}

func (s *SliceSeries) Alias(a ...string) string {
	if len(a) > 0 {
		s.alias = a[0]
	}
	return s.alias
}
