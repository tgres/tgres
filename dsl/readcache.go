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
	"github.com/tgres/tgres/rrd"
	"time"
)

type ReadCache struct {
	db   seriesQuerierDataSourceFetcher
	dsns *DataSourceNames
}

type dataSourceFetcher interface {
	FetchDataSourceNames() (map[string]int64, error)
	FetchDataSource(id int64) (*rrd.DataSource, error)
}

type seriesQuerier interface {
	SeriesQuery(ds *rrd.DataSource, from, to time.Time, maxPoints int64) (Series, error)
}

type seriesQuerierDataSourceFetcher interface {
	dataSourceFetcher
	seriesQuerier
}

func NewReadCache(db seriesQuerierDataSourceFetcher) *ReadCache {
	return &ReadCache{db: db, dsns: &DataSourceNames{}}
}

func (r *ReadCache) Reload() error {
	return r.dsns.reload(r.db)
}

func (r *ReadCache) getDSById(id int64) *rrd.DataSource {
	ds, _ := r.db.FetchDataSource(id)
	return ds
}

func (r *ReadCache) dsIdsFromIdent(ident string) map[string]int64 {
	return r.dsns.DsIdsFromIdent(ident)
}

func (r *ReadCache) seriesQuery(ds *rrd.DataSource, from, to time.Time, maxPoints int64) (Series, error) {
	return r.db.SeriesQuery(ds, from, to, maxPoints)
}

func (r *ReadCache) FsFind(pattern string) []*FsFindNode {
	r.dsns.reload(r.db)
	return r.dsns.FsFind(pattern)
}
