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

type dsFetcher interface {
	FetchOrCreateDataSource(ident serde.Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error)
	FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error)
}

type rraDataLoader interface {
	LoadRRAData(rra rrd.RoundRobinArchiver) (rrd.RoundRobinArchiver, error)
}

type dsFetcherSearcher interface {
	dsFetcher
	serde.DataSourceSearcher
}

type dsFetcherLoader interface {
	dsFetcher
	rraDataLoader
}

// Methods necessary for a DSL context and what namedDsFetch only needs to
// expose.
type ctxDSFetcher interface {
	dsFetcher
	identsFromPattern(pattern string) map[string]serde.Ident
}

type namedDsFetcher struct {
	*sync.Mutex
	*dsLRU     // provides dsFetcher methods
	dsns       *fsFindCache
	lastReload time.Time
	minAge     time.Duration
}

type watcher interface {
	Watch(ident serde.Ident, ch chan DataPoint) rrd.DataSourcer
	Unwatch(ident serde.Ident)
}

// Returns a new instance of a NamedDSFetcher. The current
// implementation will re-fetch all series names any time a series
// cannot be found. TODO: Make this better.
func NewNamedDSFetcher(db dsFetcherSearcher, dsc watcher, lruSize int) *namedDsFetcher {
	return &namedDsFetcher{
		dsns:   newFsFindCache(db.(serde.DataSourceSearcher), "name"),
		Mutex:  &sync.Mutex{},
		minAge: time.Minute,
		dsLRU:  newDsLRU(db.(dsFetcher), dsc, lruSize),
	}
}

func (r *namedDsFetcher) identsFromPattern(ident string) map[string]serde.Ident {
	if r.dsns.empty() {
		r.dsns.reload()
	}
	return r.dsns.identsFromPattern(ident)
}

func (r *namedDsFetcher) Preload() {
	r.Lock()
	r.dsns.reload()
	r.lastReload = time.Now()
	r.Unlock()
}

// FsFind provides a way of searching dot-separated names using same
// rules as filepath.Match, as well as comma-separated values in curly
// braces such as "foo.{bar,baz}".
func (r *namedDsFetcher) FsFind(pattern string) []*FsFindNode {
	result := r.dsns.fsFind(pattern)
	go func() {
		r.Lock()
		if r.lastReload.Before(time.Now().Add(-r.minAge)) {
			// TODO: This is better done with NOTIFY trigger on ds table changes
			r.dsns.reload()
			r.lastReload = time.Now()
		}
		r.Unlock()
	}()
	return result
}

type NamedDsFetcherStats struct {
	LruEvictions int
	LruSize      int
	LruHits      int
	LruMisses    int
}

func (r *namedDsFetcher) Stats() NamedDsFetcherStats {
	if r.dsLRU.Cache == nil {
		return NamedDsFetcherStats{}
	}
	r.dsLRU.Lock()
	defer r.dsLRU.Unlock()
	result := NamedDsFetcherStats{
		LruEvictions: r.dsLRU.evictions,
		LruSize:      r.dsLRU.Len(),
		LruHits:      r.dsLRU.hits,
		LruMisses:    r.dsLRU.misses,
	}
	r.dsLRU.evictions = 0
	r.dsLRU.hits = 0
	r.dsLRU.misses = 0
	return result
}
