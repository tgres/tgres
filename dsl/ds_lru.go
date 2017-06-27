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

	lru "github.com/hashicorp/golang-lru"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/series"
)

type dsLRU struct {
	*lru.Cache
	*sync.Mutex
	db        dsFetcher
	dl        rraDataLoader
	ch        chan DataPoint
	dsc       watcher
	evictions int
	hits      int
	misses    int
}

type DataPoint struct {
	serde.Ident
	T time.Time
	V float64
}

// Returns a new dsCache object.
func newDsLRU(db dsFetcher, dsc watcher) *dsLRU {
	const LRU_SIZE = 9276 // TODO make me configurable?

	// If db does not provide rraDataLoader none of this is possible.
	dl, _ := db.(rraDataLoader)
	d := &dsLRU{
		db:    db,
		dl:    dl,
		ch:    make(chan DataPoint, 256),
		dsc:   dsc,
		Mutex: &sync.Mutex{},
	}
	d.Cache, _ = lru.NewWithEvict(LRU_SIZE, d.unwatch)
	go d.worker()
	return d
}

func (d *dsLRU) unwatch(_, val interface{}) {
	d.dsc.Unwatch(val.(serde.Ident))
	d.Lock()
	d.evictions++
	d.Unlock()
}

func (d *dsLRU) worker() {
	for dp := range d.ch {
		var wds *watchedDs
		if ds, ok := d.Get(dp.Ident.String()); ok && ds != nil {
			wds, _ = ds.(*watchedDs)
		}
		if wds != nil {
			wds.Lock()
			wds.ProcessDataPoint(dp.V, dp.T)
			wds.Unlock()
		}
	}
}

func (d *dsLRU) FetchOrCreateDataSource(ident serde.Ident, _ *rrd.DSSpec) (rrd.DataSourcer, error) {
	if d.dl == nil { // RRA loading not available
		return d.db.FetchOrCreateDataSource(ident, nil)
	}
	var wds *watchedDs
	if ds, ok := d.Get(ident.String()); ok && ds != nil {
		wds, _ = ds.(*watchedDs)
	}
	if wds != nil {
		wds.RLock()
		if wds.loading {
			wds.RUnlock()
			// non-cache behavior
			d.Lock()
			d.misses++
			d.Unlock()
			return d.db.FetchOrCreateDataSource(ident, nil)
		}

		// Warning, returning it RLocked!
		d.Lock()
		d.hits++
		d.Unlock()
		return wds, nil
	}

	if ds := d.dsc.Watch(ident, d.ch); ds != nil {

		wds = &watchedDs{RWMutex: &sync.RWMutex{}, loading: true, DataSourcer: ds}
		d.Add(ident.String(), wds) // Can cause an eviction

		// Submit load job
		go d.loadDs(wds)
	}

	// revert to non-cache behavior because it is being loaded (or not)
	d.Lock()
	d.misses++
	d.Unlock()
	return d.db.FetchOrCreateDataSource(ident, nil)
}

func (d *dsLRU) loadDs(wds *watchedDs) {
	rras := wds.RRAs()
	newRRAs := make([]rrd.RoundRobinArchiver, len(rras))
	for i, rra := range rras {
		dbrra, ok := rra.(*serde.DbRoundRobinArchive)
		if !ok {
			return // must be db rra
		}
		if r, err := d.dl.LoadRRAData(dbrra); err == nil {
			newRRAs[i] = r
		} else {
			return // serde logs something here
		}
	}

	wds.Lock()
	wds.DataSourcer = wds.Copy()
	wds.SetRRAs(newRRAs)
	wds.loading = false
	wds.Unlock()
}

func (d *dsLRU) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {
	var wds *watchedDs
	if wds, _ = ds.(*watchedDs); wds == nil {
		return d.db.FetchSeries(ds, from, to, maxPoints)
	}

	// NOTE: It is already locked!
	rra := wds.BestRRA(from, to, maxPoints)
	if rra == nil {
		wds.Unlock()
		return nil, fmt.Errorf("FetchSeries (ds_lru.go): No adequate RRA found for DS from: %v to: maxPoints: %v", from, to, maxPoints)
	}

	s := series.NewRRASeries(&watchedRRA{RoundRobinArchiver: rra, RWMutex: wds.RWMutex})
	s.TimeRange(from, to)
	s.MaxPoints(maxPoints)

	// NOTE: returning a locked series, rra_series will unlock it
	return s, nil
}

type watchedDs struct {
	rrd.DataSourcer
	*sync.RWMutex
	loading bool
}

type watchedRRA struct {
	rrd.RoundRobinArchiver
	*sync.RWMutex
}
