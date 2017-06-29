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

// Package series provides fundamental series operations. At its core
// is the Series interface, which describes a Series. A Series is an
// object which can be iterated over with Next(). The idea is that the
// underlying data could be large and fetched as needed from a
// database or some other storage.
package series

import "time"

type Series interface {
	// Advance to the next data point in the series. Returns false if
	// no further advancement is possible.
	Next() bool

	// Resets the internal cursor and closes all underlying
	// cursors. After Close(), Next() should start from the beginning.
	Close() error

	// The current data point value. If called before Next(), or after
	// Next() returned false, should return a NaN.
	CurrentValue() float64

	// The time on which the current data point ends. The next slot begins
	// immediately after this time.
	CurrentTime() time.Time

	// The step of the series.
	Step() time.Duration

	// Signals the underlying storage to group rows by this interval,
	// resulting in fewer (and longer) data points. The values are
	// aggregated using average. By default it is equal to Step.
	// Without arguments returns the value, with an argument sets and
	// returns the previous value.
	GroupBy(...time.Duration) time.Duration

	// Restrict the series to (a subset of) its span. Without
	// arguments returns the value, with arguments sets and returns
	// the previous value. In the ideal case a series should be
	// iterable over the set range regardless of whether underlying
	// storage has data, i.e. when there is no data, we get a value of
	// NaN, but Next returns true over the entire range.
	TimeRange(...time.Time) (time.Time, time.Time)

	// Timestamp of the last data point in the series.
	// TODO is series.Latest() necessary even?
	Latest() time.Time

	// Alternative to GroupBy(), with a similar effect but based on
	// the maximum number of points we expect to receive. MaxPoints is
	// ignored if GroupBy was set.
	// Without arguments returns the value, with an argument sets and
	// returns the previous value.
	MaxPoints(...int64) int64
}
