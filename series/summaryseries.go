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

package series

import "math"

// SummarySeries provides some whole-series summary functions,
// e.g. Max(), Avg(), StdDev(), etc. (Not to be confused with the
// cross-series aggregation SeriesSlice provides). Note that all of
// these function require iterating over the entire series.
type SummarySeries struct {
	Series
}

// Returns the max of all the values in the series.
func (f *SummarySeries) Max() (max float64) {
	max = math.NaN()
	for f.Series.Next() {
		value := f.Series.CurrentValue()
		if math.IsNaN(max) || value > max {
			max = value
		}
	}
	f.Series.Close()
	return
}

// Returns the min of all the values in the series.
func (f *SummarySeries) Min() (min float64) {
	min = math.NaN()
	for f.Series.Next() {
		value := f.Series.CurrentValue()
		if math.IsNaN(min) || value < min {
			min = value
		}
	}
	f.Series.Close()
	return
}

// Returns the simple average of all the values in the series.
func (f *SummarySeries) Avg() float64 {
	count := 0
	sum := float64(0)
	for f.Series.Next() {
		sum += f.Series.CurrentValue()
		count++
	}
	f.Series.Close()
	return sum / float64(count)
}

// Returns the standard deviation of all the values in the series.
func (f *SummarySeries) StdDev(avg float64) float64 {
	count := 0
	sum := float64(0)
	for f.Series.Next() {
		sum += math.Pow(f.Series.CurrentValue()-avg, 2)
		count++
	}
	f.Series.Close()
	return math.Sqrt(sum / float64(count-1))
}

// Returns the last value in the series.
func (f *SummarySeries) Last() (last float64) {
	for f.Series.Next() {
		last = f.Series.CurrentValue()
	}
	f.Series.Close()
	return
}
