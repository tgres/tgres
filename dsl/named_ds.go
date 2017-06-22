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
	"fmt"
	"sync"
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
	*sync.Mutex
	dsFetcher
	dsns       *fsFindCache
	lastReload time.Time
	minAge     time.Duration
	cache      watcher
}

type watcher interface {
	Watch(ident serde.Ident) rrd.DataSourcer
}

// Returns a new instance of a NamedDSFetcher. The current
// implementation will re-fetch all series names any time a series
// cannot be found. TODO: Make this better.
func NewNamedDSFetcher(db dsFetcher, dsc watcher) *namedDsFetcher {
	return &namedDsFetcher{
		dsFetcher: db,
		dsns:      &fsFindCache{key: "name"},
		Mutex:     &sync.Mutex{},
		minAge:    30 * time.Second,
		cache:     dsc,
	}
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
	r.Lock()
	if r.lastReload.Before(time.Now().Add(-r.minAge)) {
		// TODO: This is better done with NOTIFY trigger on ds table changes
		r.dsns.reload(r)
		r.lastReload = time.Now()
	}
	r.Unlock()
	return r.dsns.fsFind(pattern)
}

type watchedDs struct {
	rrd.DataSourcer
}

// This version of FetchOrCreateDataSource checks the cache if present and
// marks the series as watched. Otherwise it uses the "normal" behavior.
func (r *namedDsFetcher) FetchOrCreateDataSource(ident serde.Ident, _ *rrd.DSSpec) (rrd.DataSourcer, error) {
	if r.cache != nil {
		if ds := r.cache.Watch(ident); ds != nil {
			return &watchedDs{ds}, nil
		}
	}
	// TODO: record LRU hits/misses?

	// non-cache behavior
	return r.dsFetcher.FetchOrCreateDataSource(ident, nil)
}

// This FetchSeries checks for the DS being a watchedDS, meaning it
// came from the cache, otherwise it resorts to the old databse
// behavior.
func (r *namedDsFetcher) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {

	if r.cache == nil {
		return r.dsFetcher.FetchSeries(ds, from, to, maxPoints)
	}

	if _, ok := ds.(*watchedDs); !ok {
		return r.dsFetcher.FetchSeries(ds, from, to, maxPoints)
	}

	rra := ds.BestRRA(from, to, maxPoints)
	if rra == nil {
		return nil, fmt.Errorf("FetchSeries (named_ds.go): No adequate RRA found for DS from: %v to: maxPoints: %v", from, to, maxPoints)
	}

	rraLatest := rra.Latest()
	rraEarliest := rra.Begins(rraLatest)

	if from.IsZero() || rraEarliest.After(from) {
		from = rraEarliest
	}

	s := series.NewRRASeriesCopyRange(rra, from, to)
	s.MaxPoints(maxPoints)
	return s, nil
}
