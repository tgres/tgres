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
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/series"
)

// Creates a NamedDsFetcher from a map of DataSourcers. This is useful
// to bootstrap a DSL Context without any database.
func NewNamedDSFetcherMap(dss map[string]rrd.DataSourcer) *namedDsFetcher {
	return NewNamedDSFetcher(newMapCache(dss))
}

// A dsFinder backed by a simple map of DSs
func newMapCache(dss map[string]rrd.DataSourcer) *mapCache {
	mc := &mapCache{make(map[string]int64), make(map[int64]rrd.DataSourcer)}
	var n int64
	for name, ds := range dss {
		mc.byName[name] = n
		mc.byId[n] = ds
		n++
	}
	return mc
}

type srRow struct {
	ident serde.IdentTags
	id    int64
}

type memSearchResult struct {
	result []*srRow
	pos    int
}

func (sr *memSearchResult) Next() bool {
	sr.pos++
	return sr.pos < len(sr.result)
}
func (sr *memSearchResult) Id() int64              { return sr.result[sr.pos].id }
func (sr *memSearchResult) Ident() serde.IdentTags { return sr.result[sr.pos].ident }
func (sr *memSearchResult) Close() error           { return nil }

type mapCache struct {
	byName map[string]int64
	byId   map[int64]rrd.DataSourcer
}

func (m *mapCache) Search(query serde.SearchQuery) (serde.SearchResult, error) {
	sr := &memSearchResult{pos: -1}
	for k, v := range m.byName {
		sr.result = append(sr.result, &srRow{map[string]string{"name": k}, v})
	}
	return sr, nil
}

func (m *mapCache) FetchDataSourceById(id int64) (rrd.DataSourcer, error) {
	return m.byId[id], nil
}

func (*mapCache) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {
	return series.NewRRASeries(ds.RRAs()[0]), nil
}

func (m *mapCache) FetchDataSources() ([]rrd.DataSourcer, error) {
	result := []rrd.DataSourcer{}
	for _, ds := range m.byId {
		result = append(result, ds)
	}
	return result, nil
}

func (*mapCache) FetchOrCreateDataSource(name string, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	return nil, nil
}
