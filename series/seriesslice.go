//
// Copyright 2015 Gregory Trubetskoy. All Rights Reserved.
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

import (
	"math"
	"sort"
	"time"
)

// A slice of series which implements the Series interface (almost).
// SeriesSlice is an "abstract" Series because it purposely does not
// implement CurrentValue(). It is used for bunching Series together
// for cross-series aggregation. A call to Next(), Close() etc will
// call respective methods on all series in the slice.
type SeriesSlice []Series

func (sl SeriesSlice) Next() bool {
	if len(sl) == 0 {
		return false
	}
	for _, series := range sl {
		if !series.Next() {
			return false
		}
	}
	return true
}

// Returns CurrentTime of the first series in the slice or zero time
// if slice is empty.
func (sl SeriesSlice) CurrentTime() time.Time {
	if len(sl) > 0 {
		return sl[0].CurrentTime()
	}
	return time.Time{}
}

// Closes all series in the slice. First error encountered is returned
// and the rest of the series is not closed.
func (sl SeriesSlice) Close() error {
	for _, series := range sl {
		if err := series.Close(); err != nil {
			return err
		}
	}
	return nil
}

// Returns the step of the first series in the slice or 0 if slice is
// empty.
func (sl SeriesSlice) Step() time.Duration {
	// This returns the StepMs of the first series since we should
	// assume they are aligned (thus equeal). Then if we happen to be
	// encapsulated in a another SeriesSlice, it might overrule the
	// GroupByMs.
	if len(sl) > 0 {
		return sl[0].Step()
	}
	return 0
}

func (sl SeriesSlice) GroupBy(ms ...time.Duration) time.Duration {
	// getter only, no setter
	if len(sl) > 0 {
		return sl[0].GroupBy()
	}
	return 0
}

func (sl SeriesSlice) TimeRange(t ...time.Time) (time.Time, time.Time) {
	if len(t) == 1 { // setter 1 arg
		defer func() {
			for _, series := range sl {
				series.TimeRange(t[0])
			}
		}()
	} else if len(t) == 2 { // setter 2 args
		defer func() {
			for _, series := range sl {
				series.TimeRange(t[0], t[1])
			}
		}()
	}
	// getter
	if len(sl) > 0 {
		return sl[0].TimeRange()
	}
	return time.Time{}, time.Time{}
}

// Returns Latest() for the first series in the slice or zero time if
// slice is empty.
func (sl SeriesSlice) Latest() time.Time {
	if len(sl) > 0 {
		return sl[0].Latest()
	}
	return time.Time{}
}

// With argument sets MaxPoints on all series in the slice, without
// argument returns MaxPoints of the first series in the slice or 0 if
// slice is empty.
func (sl SeriesSlice) MaxPoints(n ...int64) int64 {
	if len(n) > 0 { // setter
		defer func() {
			for _, series := range sl {
				series.MaxPoints(n[0])
			}
		}()
	}
	// getter
	if len(sl) > 0 {
		return sl[0].MaxPoints()
	}
	return 0
}

// Least Common Multiple
func lcm(x, y int64) int64 {
	if x == 0 || y == 0 {
		return 0
	}
	p := x * y
	for y != 0 {
		mod := x % y
		x, y = y, mod
	}
	return p / x
}

// Computes the least common multiple of the steps of all the series
// in the slice and calls GroupBy with this value thereby causing all
// series to be of matching resolution (i.e. aligned on data point
// timestamps). Generally you should always Align() the series slice
// before iterting over it.
func (sl SeriesSlice) Align() {
	if len(sl) < 2 {
		return
	}

	var result int64 = -1
	for _, series := range sl {
		if result == -1 {
			result = series.Step().Nanoseconds() / 1e6
			continue
		}
		result = lcm(result, series.Step().Nanoseconds()/1e6)
	}

	for _, series := range sl {
		series.GroupBy(time.Duration(result) * time.Millisecond)
	}
}

// Returns the arithmetic sum of all the current values in the series
// in the slice.
func (sl SeriesSlice) Sum() (result float64) {
	for _, series := range sl {
		result += series.CurrentValue()
	}
	return
}

// Returns the simple average of all the current values in the series
// in the slice.
func (sl SeriesSlice) Avg() float64 {
	return sl.Sum() / float64(len(sl))
}

// Returns the max of all the current values in the series in the
// slice.
func (sl SeriesSlice) Max() float64 {
	result := math.NaN()
	for _, series := range sl {
		value := series.CurrentValue()
		if math.IsNaN(result) || result < value {
			result = value
		}
	}
	return result
}

// Returns the min of all the current values in the series in the
// slice.
func (sl SeriesSlice) Min() float64 {
	result := math.NaN()
	for _, series := range sl {
		value := series.CurrentValue()
		if math.IsNaN(result) || result > value {
			result = value
		}
	}
	return result
}

// Returns the current value of the first series in the slice. (The
// ordering of series is up to the implementation).
func (sl SeriesSlice) First() float64 {
	for _, series := range sl {
		return series.CurrentValue()
	}
	return math.NaN()
}

// Given a series as a []float64, returns the value below which p
// fraction of all the values falls. (0 < p < 1).
func Quantile(list []float64, p float64) float64 {
	// https://github.com/rcrowley/go-metrics/blob/a248d281279ea605eccec4f54546fd998c060e38/sample.go#L278
	size := len(list)
	if size == 0 {
		return math.NaN()
	}
	cpy := make([]float64, len(list))
	copy(cpy, list)
	sort.Float64s(cpy)
	pos := p * float64(size+1)
	if pos < 1.0 {
		return cpy[0]
	} else if pos >= float64(size) {
		return cpy[size-1]
	} else {
		lower := cpy[int(pos)-1]
		upper := cpy[int(pos)]
		return lower + (pos-math.Floor(pos))*(upper-lower)
	}
}

// Returns the p-th quantile (0 < p < 1) of the current values of the
// series in the slice.
func (sl SeriesSlice) Quantile(p float64) float64 {
	// This is a percentile of one data point, not the whole series
	dps := make([]float64, 0)
	for _, series := range sl {
		dps = append(dps, series.CurrentValue())
	}
	return Quantile(dps, p)
}

// Returns the difference between max and min of all the current
// values of the series in the slice.
func (sl SeriesSlice) Range() float64 {
	return sl.Max() - sl.Min()
}

// Starting with the current value of the series, subtract the
// remaining values and return the result.
func (sl SeriesSlice) Diff() float64 {
	if len(sl) == 0 {
		return math.NaN()
	}
	// TODO SeriesSlice still needs to be ordered
	result := sl[0].CurrentValue()
	for _, series := range sl[1:] {
		result -= series.CurrentValue()
	}
	return result
}
