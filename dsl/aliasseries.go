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

import "github.com/tgres/tgres/series"

type AliasSeries interface {
	series.Series
	Alias(s ...string) string
}

type aliasSeries struct {
	series.Series
	alias string
}

func (as *aliasSeries) Alias(s ...string) string {
	if len(s) > 0 {
		as.alias = s[0]
	}
	return as.alias
}

type aliasSeriesSlice struct {
	series.SeriesSlice
	alias string
}

func (as *aliasSeriesSlice) Alias(s ...string) string {
	if len(s) > 0 {
		as.alias = s[0]
	}
	return as.alias
}

type aliasSummarySeries struct {
	*series.SummarySeries
	alias string
}

func (as *aliasSummarySeries) Alias(s ...string) string {
	if len(s) > 0 {
		as.alias = s[0]
	}
	return as.alias
}

func newAliasSummarySeries(s AliasSeries) *aliasSummarySeries {
	return &aliasSummarySeries{SummarySeries: &series.SummarySeries{s}, alias: s.Alias()}
}
