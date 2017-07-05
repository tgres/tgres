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
	cap       int
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
func newDsLRU(db dsFetcher, dsc watcher, cap int) *dsLRU {
	// If db does not provide rraDataLoader none of this is possible.
	dl, _ := db.(rraDataLoader)
	d := &dsLRU{
		db:    db,
		dl:    dl,
		ch:    make(chan DataPoint, 256),
		dsc:   dsc,
		Mutex: &sync.Mutex{},
		cap:   cap,
	}
	// 0 cap == diable LRU
	if cap == 0 {
		d.dl = nil
	} else {
		d.Cache, _ = lru.NewWithEvict(cap, d.unwatch)
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
			if wds.loading {
				// do not process, queue them up
				wds.pending = append(wds.pending, dp)
			} else {
				if len(wds.pending) > 0 {
					// process pending first, if any
					for _, dp := range wds.pending {
						wds.ProcessDataPoint(dp.V, dp.T)
					}
					wds.pending = nil
				}
				// finally process the dp that just came in
				wds.ProcessDataPoint(dp.V, dp.T)
			}
			wds.Unlock()
		}
	}
}

type lruStateSaver interface {
	SaveDSLCacheKeys(idents []serde.Ident) error
	LoadDSLCacheKeys() ([]serde.Ident, error)
}

func (d *dsLRU) saveState() error {
	if db, ok := d.db.(lruStateSaver); ok {

		keys := make([]serde.Ident, 0, d.Len())
		for _, key := range d.Keys() {
			if val, ok := d.Peek(key); ok {
				if wds, ok := val.(*watchedDs); ok {
					keys = append(keys, wds.ident)
				}
			}
		}
		if len(keys) > 0 {
			if err := db.SaveDSLCacheKeys(keys); err != nil {
				return err
			}
		}
	}
	return nil
}

func (d *dsLRU) loadState() error {
	if d.cap == 0 {
		return nil
	}
	if db, ok := d.db.(lruStateSaver); ok {
		idents, err := db.LoadDSLCacheKeys()
		if err != nil {
			return err
		}
		var wg sync.WaitGroup
		jobs := 0
		for n, ident := range idents {
			if n >= d.cap {
				break // enough
			}
			istr := ident.String()
			if !d.Contains(istr) {
				if ds := d.dsc.Watch(ident, d.ch); ds != nil {
					wds := &watchedDs{
						RWMutex:     &sync.RWMutex{},
						loading:     true,
						DataSourcer: ds,
						ident:       ident,
					}
					if d.Add(istr, wds) {
						// LRU is full
						d.Remove(istr)
						break
					}
					wg.Add(1)
					go d.loadDs(wds, &wg)
					jobs++
					if jobs >= 32 {
						wg.Wait()
					}
				}
			}
		}
		wg.Wait()
	}
	return nil
}

func (d *dsLRU) stateSaver(nap time.Duration) {
	if d.cap == 0 {
		return
	}
	for {
		time.Sleep(nap)
		d.saveState()
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

		wds = &watchedDs{
			RWMutex:     &sync.RWMutex{},
			loading:     true,
			DataSourcer: ds,
			ident:       ident,
		}
		d.Add(ident.String(), wds) // Can cause an eviction

		// Submit load job
		var wg sync.WaitGroup
		wg.Add(1)
		go d.loadDs(wds, &wg)
	} else {
		// This DS is not watchable, which means it is somehow bogus,
		// and we shouldn't bother with it. DSL should warn about it
		// (it's a TODO).
		return nil, nil
	}

	// revert to non-cache behavior because it is being loaded (or not)
	d.Lock()
	d.misses++
	d.Unlock()
	return d.db.FetchOrCreateDataSource(ident, nil)
}

func (d *dsLRU) loadDs(wds *watchedDs, wg *sync.WaitGroup) {
	defer wg.Done()
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
		return nil, fmt.Errorf("FetchSeries (ds_lru.go): No adequate RRA found for DS from: %v to: %v maxPoints: %v", from, to, maxPoints)
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
	ident   serde.Ident
	pending []DataPoint
}

type watchedRRA struct {
	rrd.RoundRobinArchiver
	*sync.RWMutex
}
