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

// A NamedDSFetcher is a serde.Fetcher which also implements a method
// for finding DS's by its dotted name using filepath.Match-like
// syntax.
type NamedDSFetcher interface {
	dsFetcher
	fsFinder
}

type fsFinder interface {
	identsFromPattern(ident string) map[string]serde.Ident
	FsFind(pattern string) []*FsFindNode
}

// This is a subset of serde.Fetcher
type dsFetcher interface {
	serde.DataSourceSearcher
	FetchOrCreateDataSource(ident serde.Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error)
	FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error)
}

// Methods necessary for a DSL context
type ctxDSFetcher interface {
	FetchOrCreateDataSource(ident serde.Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error)
	FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error)
	identsFromPattern(pattern string) map[string]serde.Ident
}

type namedDsFetcher struct {
	dsFetcher
	dsns *fsFindCache
}

// Returns a new instance of a NamedDSFetcher. The current
// implementation will re-fetch all series names any time a series
// cannot be found. TODO: Make this better.
func NewNamedDSFetcher(db dsFetcher) *namedDsFetcher {
	return &namedDsFetcher{dsFetcher: db, dsns: &fsFindCache{key: "name"}}
}

func (r *namedDsFetcher) identsFromPattern(ident string) map[string]serde.Ident {
	result := r.dsns.identsFromPattern(ident)
	if len(result) == 0 {
		r.dsns.reload(r)
		result = r.dsns.identsFromPattern(ident)
	}
	return result
}

// FsFind provides a way of searching dot-separated names using same
// rules as filepath.Match, as well as comma-separated values in curly
// braces such as "foo.{bar,baz}".
func (r *namedDsFetcher) FsFind(pattern string) []*FsFindNode {
	r.dsns.reload(r)
	return r.dsns.fsFind(pattern)
}
