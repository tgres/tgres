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
func newDsLRU(db dsFetcher, dsc watcher, size int) *dsLRU {
	// If db does not provide rraDataLoader none of this is possible.
	dl, _ := db.(rraDataLoader)
	d := &dsLRU{
		db:    db,
		dl:    dl,
		ch:    make(chan DataPoint, 256),
		dsc:   dsc,
		Mutex: &sync.Mutex{},
	}
	// 0 size == diable LRU
	if size == 0 {
		d.dl = nil
	} else {
		d.Cache, _ = lru.NewWithEvict(size, d.unwatch)
		go d.worker()
	}

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
		defer wds.RUnlock()
		if wds.loading {
			// non-cache behavior
			d.Lock()
			d.misses++
			d.Unlock()
			return d.db.FetchOrCreateDataSource(ident, nil)
		}

		d.Lock()
		d.hits++
		d.Unlock()
		return wds, nil
	}

	// if we got this far wds == nil

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
		// Not a watchedDs, fallback to non-cache behavior
		return d.db.FetchSeries(ds, from, to, maxPoints)
	}

	wds.RLock()
	defer wds.RUnlock()

	rra := wds.BestRRA(from, to, maxPoints)
	if rra == nil {
		return nil, fmt.Errorf("FetchSeries (ds_lru.go): No adequate RRA found for DS from: %v to: maxPoints: %v", from, to, maxPoints)
	}

	// We pass the wds lock here, so that when whatever downstream locks the rra,
	// it will actually end up locking the DS. Note that a locked DS cannot have
	// data points added to it, so it is imperative that the lock isn't held for a long
	// time in http handler and what not.
	s := series.NewRRASeries(&watchedRRA{RoundRobinArchiver: rra, RWMutex: wds.RWMutex})
	s.TimeRange(from, to)
	s.MaxPoints(maxPoints)

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
