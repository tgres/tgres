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

import "sort"

// SeriesMap is a map and should not be confused with
// series.SeriesSlice. The key distinction is that you cannot "see"
// beyond a SeriesSlice, it encapsulates and hides all Series within
// it. E.g. when we compute avg(a, b), we end up with a new series and
// no longer have access to a or b. This is done by way of SeriesSlice.
//
// The test is whether a "new" series is created. E.g. scale() is
// given a bunch of series, and returns a bunch of series, so it
// should use SeriesMap. avg() takes a bunch of series and returns
// only one series, so it's a SeriesSlice.
//
// SeriesSlice
//  - based on []Series
//  - satisfies Series interface
//  - aligns Series' within it to match on group by
//  - series in it are not associated with a name (key)
//  - it's a "mix of series" that can be treated as a series
//  - supports duplicates
//
// SeriesMap
//  - alias for map[string]Series
//  - does NOT implement Series, it's a container of series, but not a series
//  - is how we give Series names - the key is the name
//  - does not support duplicates - same series would need different names
type SeriesMap map[string]AliasSeries

func (sm SeriesMap) SortedKeys() []string {
	keys := make([]string, 0, len(sm))
	for k, _ := range sm {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

func (sm SeriesMap) toAliasSeriesSlice() *aliasSeriesSlice {
	result := &aliasSeriesSlice{}
	for _, key := range sm.SortedKeys() {
		result.SeriesSlice = append(result.SeriesSlice, sm[key])
	}
	return result
}
