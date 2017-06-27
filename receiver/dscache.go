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

package receiver

import (
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

// A collection of data sources kept by serde.Ident.
type dsCache struct {
	sync.RWMutex
	byIdent  map[string]*cachedDs
	db       serde.Fetcher
	dsf      dsFlusherBlocking
	finder   MatchingDSSpecFinder
	clstr    clusterer
	rraCount int
	evicted  int
	maxInact time.Duration
}

// Returns a new dsCache object.
func newDsCache(db serde.Fetcher, finder MatchingDSSpecFinder, dsf dsFlusherBlocking) *dsCache {
	return &dsCache{
		byIdent:  make(map[string]*cachedDs),
		db:       db,
		finder:   finder,
		dsf:      dsf,
		maxInact: time.Hour, // TODO: Make configurable?
	}
}

// getByName rlocks and gets a DS pointer.
func (d *dsCache) getByIdent(ident *cachedIdent) *cachedDs {
	d.RLock()
	defer d.RUnlock()
	return d.byIdent[ident.String()]
}

// Insert locks and inserts a DS.
func (d *dsCache) insert(cds *cachedDs) {
	d.Lock()
	defer d.Unlock()
	if cds.spec != nil {
		d.rraCount += len(cds.spec.RRAs)
	} else if ds, ok := cds.DbDataSourcer.(rrd.DataSourcer); ok && ds != nil {
		d.rraCount += len(ds.RRAs())
	}
	d.byIdent[cds.Ident().String()] = cds
}

// Delete a DS
func (d *dsCache) delete(ident serde.Ident) {
	d.Lock()
	defer d.Unlock()
	s := ident.String()
	if cds := d.byIdent[s]; cds != nil {
		d.rraCount -= len(cds.RRAs())
		delete(d.byIdent, s)
	}
}

func (d *dsCache) preLoad() error {
	dss, err := d.db.FetchDataSources(time.Hour * 3 * 24) // TODO: Make me configurable
	if err != nil {
		return err
	}

	for _, ds := range dss {
		dbds, ok := ds.(serde.DbDataSourcer)
		if !ok {
			return fmt.Errorf("preLoad: ds must be a serde.DbDataSourcer")
		}
		d.insert(&cachedDs{DbDataSourcer: dbds, mu: &sync.Mutex{}, lastProcess: time.Now()})
		d.register(dbds)
	}

	return nil
}

// get or create and empty cached ds
func (d *dsCache) getByIdentOrCreateEmpty(ident *cachedIdent) *cachedDs {
	result := d.getByIdent(ident)
	if result == nil {
		if spec := d.finder.FindMatchingDSSpec(ident.Ident); spec != nil {
			// return a cachedDs with nil DataSourcer
			dbds := serde.NewDbDataSource(0, ident.Ident, 0, 0, nil)
			result = &cachedDs{DbDataSourcer: dbds, spec: spec, mu: &sync.Mutex{}, lastProcess: time.Now()}
			d.insert(result)
		}
	}
	return result
}

// load (or create) via the SerDe given an empty cachedDs with ident and spec
func (d *dsCache) fetchOrCreateByIdent(cds *cachedDs) error {
	ds, err := d.db.FetchOrCreateDataSource(cds.Ident(), cds.spec)
	if err != nil {
		return err
	}
	dbds, ok := ds.(serde.DbDataSourcer)
	if !ok {
		return fmt.Errorf("fetchOrCreateByIdent: ds must be a serde.DbDataSourcer")
	}
	cds.DbDataSourcer = dbds
	cds.spec = nil
	d.register(dbds)
	return nil
}

// register the rds as a DistDatum with the cluster
func (d *dsCache) register(ds serde.DbDataSourcer) {
	if d.clstr != nil {
		dds := &distDs{DbDataSourcer: ds, dsc: d}
		d.clstr.LoadDistData(func() ([]cluster.DistDatum, error) {
			return []cluster.DistDatum{dds}, nil
		})
	}
}

type dscStats struct {
	dsCount, rraCount, evicted int
}

func (d *dsCache) stats() dscStats {
	d.RLock()
	defer d.RUnlock()
	st := dscStats{
		dsCount:  len(d.byIdent),
		rraCount: d.rraCount,
		evicted:  d.evicted,
	}

	d.evicted = 0
	return st
}

func (d *dsCache) evictInact() {
	now := time.Now()
	d.Lock()
	defer d.Unlock()
	for _, cds := range d.byIdent {
		cds.mu.Lock()
		if !cds.sentToLoader && cds.DbDataSourcer != nil && !cds.lastProcess.IsZero() && now.Add(-d.maxInact).After(cds.lastProcess) {
			d.rraCount -= len(cds.RRAs())
			delete(d.byIdent, cds.Ident().String())
			d.evicted++
		}
		cds.mu.Unlock()
	}
	return
}

// Watch checks the cache for presence of ident, if found, it marks it
// as watched by setting the watchCh and starts sending a copy of all
// data points matching this ident to the provided channel, until
// Unwatch() is called.
func (d *dsCache) Watch(ident serde.Ident, ch chan dsl.DataPoint) rrd.DataSourcer {

	cds := d.getByIdent(newCachedIdent(ident))

	// nil means it has no recent data points, and loading it here is
	// not appropriate, it cannot be watched.
	if cds == nil {
		return nil
	}

	cds.mu.Lock()
	defer cds.mu.Unlock()

	if cds.sentToLoader { // no partially loaded allowed
		return nil
	}

	if cds.watchCh == nil {
		cds.watchCh = ch
	}

	return cds
}

func (d *dsCache) Unwatch(ident serde.Ident) {
	if cds := d.getByIdent(newCachedIdent(ident)); cds != nil {
		cds.mu.Lock()
		defer cds.mu.Unlock()
		cds.watchCh = nil
	}
}

func dsCachePeriodicCleanup(dsc *dsCache) {
	for {
		time.Sleep(time.Minute)
		dsc.evictInact()
	}
}

// Sortable array of incomingDP
type sortableIncomingDPs []*incomingDP

func (a sortableIncomingDPs) Len() int           { return len(a) }
func (a sortableIncomingDPs) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a sortableIncomingDPs) Less(i, j int) bool { return a[i].timeStamp.Before(a[j].timeStamp) }

// cachedDs is a DS that keeps a queue of incoming data points, which
// can all processed at once.
type cachedDs struct {
	serde.DbDataSourcer
	incoming     sortableIncomingDPs
	spec         *rrd.DSSpec // for when DS needs to be created
	sentToLoader bool
	lastProcess  time.Time
	lastFlush    time.Time
	watchCh      chan dsl.DataPoint
	mu           *sync.Mutex
}

func (cds *cachedDs) appendIncoming(dp *incomingDP) {
	cds.mu.Lock()
	defer cds.mu.Unlock()
	cds.incoming = append(cds.incoming, dp)
}

func (cds *cachedDs) processIncoming() (int, int, error) {

	const BIG = 32 // this number was chosen rather arbitrarily

	cds.mu.Lock()
	defer cds.mu.Unlock()
	var err error

	count := len(cds.incoming)
	if count == 0 {
		return 0, 0, nil
	}

	// delay processing by 1/10 of a step, in a clustered situation it
	// is possible for forwarded data points to arrive slightly late,
	// this (along with the Sort() just below) addresses it.  Unless
	// there are already a bunch of points queued up
	if !(cds.lastProcess.Before(time.Now().Add(-cds.Step()/10)) || count > BIG) {
		return 0, 0, nil
	}

	sort.Sort(cds.incoming)

	blocked := 0 // watched ch blocked
	for _, dp := range cds.incoming {
		// continue on errors
		err = cds.ProcessDataPoint(dp.value, dp.timeStamp)

		if cds.watchCh != nil {
			select {
			case cds.watchCh <- dsl.DataPoint{Ident: cds.Ident(), T: dp.timeStamp, V: dp.value}:
			default:
				blocked++
			}
		}
	}

	cds.lastProcess = time.Now()

	if count < BIG {
		// leave the backing array in place to avoid extra memory allocations
		cds.incoming = cds.incoming[:0]
	} else {
		cds.incoming = nil
	}

	return count, blocked, err
}

// This is exported so as to be Gob-Encodable
type cachedIdent struct {
	serde.Ident
	s string
}

func (ci *cachedIdent) String() string {
	if ci.s == "" {
		ci.s = ci.Ident.String()
	}
	return ci.s
}

func newCachedIdent(ident serde.Ident) *cachedIdent {
	return &cachedIdent{Ident: ident, s: ident.String()}
}

// distDs keeps a pointer to the dsCache so that it can delete itself
// from it, as well as access the Flusher to persist during Relinquish
type distDs struct {
	serde.DbDataSourcer
	dsc *dsCache
}

// cluster.DistDatum interface

func (ds *distDs) Relinquish() error {

	// NB: Data points that are in vcache are complete, immutable and
	// there is no situation where they are ever updated. This means
	// that the transition should only concern itself with incomplete
	// state in DS and its RRAs, as long as it is passed correctly
	// between nodes, the data is correct. Requirement #2 is that the
	// vcache is flushed, but this does not need synchronization.
	//
	// vcache is fully flushed in flusher.stop()
	if ds.PointCount() == 0 {
		// Look it up in the cache and double check that it needs not
		// be flushed by chance.
		if cds := ds.dsc.getByIdent(newCachedIdent(ds.Ident())); cds != nil {
			cds.mu.Lock()
			if !cds.lastFlush.IsZero() && cds.lastFlush.Before(cds.lastProcess) {
				ds.dsc.dsf.flushToVCache(ds.DbDataSourcer)
			}
			cds.mu.Unlock()
		}
	} else {
		ds.dsc.dsf.flushToVCache(ds.DbDataSourcer)
	}
	ds.dsc.delete(ds.Ident())

	return nil
}

func (ds *distDs) Acquire() error {
	ds.dsc.delete(ds.Ident())
	return nil
}

func (ds *distDs) Id() int64       { return ds.DbDataSourcer.Id() }
func (ds *distDs) Type() string    { return "DataSource" }
func (ds *distDs) GetName() string { return ds.DbDataSourcer.Ident().String() }

// end cluster.DistDatum interface

type statster interface {
	Stats() dsl.NamedDsFetcherStats
}

// TODO: This is a hack, there should be a better way to report
// stats from outside the receiver package.
func ReportRcacheStats(ndsf dsl.NamedDSFetcher, sr statReporter) {
	str := ndsf.(statster)
	for {
		time.Sleep(5 * time.Second)
		st := str.Stats()
		sr.reportStatCount("dsl.lru_evicted", float64(st.LruEvictions))
		sr.reportStatCount("dsl.lru_hits", float64(st.LruHits))
		sr.reportStatCount("dsl.lru_misses", float64(st.LruMisses))
		sr.reportStatGauge("dsl.lru_size", float64(st.LruSize))
	}
}
