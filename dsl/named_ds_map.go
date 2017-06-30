//
// Copyright 2017 Gregory Trubetskoy. All Rights Reserved.
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
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/series"
)

// Creates a NamedDsFetcher from a map of DataSourcers. This is useful
// to bootstrap a DSL Context without any database.
func NewNamedDSFetcherMap(dss map[string]rrd.DataSourcer) *namedDsFetcher {
	result := make(mapCache)
	for name, ds := range dss {
		ident := serde.Ident{"name": name}
		result[ident.String()] = &mapCacheEntry{
			ident: ident,
			ds:    ds,
		}
	}
	return NewNamedDSFetcher(result, nil, 0)
}

type mapCacheEntry struct {
	ident serde.Ident
	ds    rrd.DataSourcer
}

// A dsFinder backed by a simple map of DSs
type mapCache map[string]*mapCacheEntry

type memSearchResult struct {
	result []serde.Ident
	pos    int
}

func (sr *memSearchResult) Next() bool {
	sr.pos++
	return sr.pos < len(sr.result)
}

func (sr *memSearchResult) Ident() serde.Ident { return sr.result[sr.pos] }
func (sr *memSearchResult) Close() error       { return nil }

func (m mapCache) Search(_ serde.SearchQuery) (serde.SearchResult, error) {
	sr := &memSearchResult{pos: -1}
	for _, me := range m {
		sr.result = append(sr.result, me.ident)
	}
	return sr, nil
}

func (mapCache) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {
	return series.NewRRASeries(ds.RRAs()[0]), nil
}

func (mapCache) FetchOrCreateDataSource(ident serde.Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	return nil, nil
}
