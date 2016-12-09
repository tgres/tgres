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
	"sync"
	"time"

	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

// A collection of data sources kept by an integer id as well as a
// string name.
type dsCache struct {
	rwLocker
	byName map[string]*receiverDs
	db     serde.DataSourceSerDe
	dsf    dsFlusherBlocking
	finder MatchingDSSpecFinder
	clstr  clusterer
}

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

// Returns a new dsCache object. If locking is true, the resulting
// dsCache will maintain a lock, otherwise there is no locking,
// but the caller needs to ensure that it is never used concurrently
// (e.g. always in the same goroutine).
func newDsCache(db serde.DataSourceSerDe, finder MatchingDSSpecFinder, clstr clusterer, dsf dsFlusherBlocking, locking bool) *dsCache {
	d := &dsCache{
		byName: make(map[string]*receiverDs),
		db:     db,
		finder: finder,
		clstr:  clstr,
		dsf:    dsf,
	}
	if locking {
		d.rwLocker = &sync.RWMutex{}
	}
	return d
}

// getByName rlocks and gets a DS pointer.
func (d *dsCache) getByName(name string) *receiverDs {
	if d.rwLocker != nil {
		d.RLock()
		defer d.RUnlock()
	}
	return d.byName[name]
}

// Insert locks and inserts a DS.
func (d *dsCache) insert(rds *receiverDs) {
	if d.rwLocker != nil {
		d.Lock()
		defer d.Unlock()
	}
	d.byName[rds.Name()] = rds
}

func (d *dsCache) preLoad() error {
	dss, err := d.db.FetchDataSources()
	if err != nil {
		return err
	}

	for _, ds := range dss {
		rds := &receiverDs{DataSource: ds, dsf: d.dsf}
		d.insert(rds)
		d.register(rds)
	}

	return nil
}

func (d *dsCache) loadOrCreateDS(name string) (*rrd.DataSource, error) {
	if dsSpec := d.finder.FindMatchingDSSpec(name); dsSpec != nil {
		return d.db.CreateOrReturnDataSource(name, dsSpec)
	}
	return nil, nil // We couldn't find anything, which is not an error
}

// getByNameOrLoadOrCreate gets, or loads from db (and possibly creates) a ds
func (d *dsCache) getByNameOrLoadOrCreate(name string) (*receiverDs, error) {
	rds := d.getByName(name)
	if rds == nil || rds.stale {
		rds = nil
		ds, err := d.loadOrCreateDS(name)
		if err != nil {
			return nil, err
		}
		if ds != nil {
			rds = &receiverDs{DataSource: ds, dsf: d.dsf}
			d.insert(rds)
			d.register(rds)
		}
	}
	return rds, nil
}

// register the rds as a DistDatum with the cluster
func (d *dsCache) register(rds *receiverDs) {
	if d.clstr != nil {
		d.clstr.LoadDistData(func() ([]cluster.DistDatum, error) {
			return []cluster.DistDatum{rds}, nil
		})
	}
}

type receiverDs struct {
	*rrd.DataSource
	lastFlushRT time.Time // Last time this DS was flushed (actual real time).
	dsf         dsFlusherBlocking
	stale       bool // Reload me from db at next opportunity
}

// cluster.DistDatum interface

func (rds *receiverDs) Relinquish() error {
	if !rds.LastUpdate().IsZero() {
		rds.dsf.flushDs(rds, true)
		rds.stale = true
	}
	return nil
}

func (rds *receiverDs) Acquire() error {
	rds.stale = true // it will get loaded afresh when needed
	return nil
}

func (rds *receiverDs) Id() int64       { return rds.DataSource.Id() }
func (rds *receiverDs) Type() string    { return "DataSource" }
func (rds *receiverDs) GetName() string { return rds.DataSource.Name() }

// end cluster.DistDatum interface

func (rds *receiverDs) shouldBeFlushed(maxCachedPoints int, minCache, maxCache time.Duration) bool {
	if rds.LastUpdate().IsZero() {
		return false
	}
	pc := rds.PointCount()
	if pc > maxCachedPoints {
		return rds.lastFlushRT.Add(minCache).Before(time.Now())
	} else if pc > 0 {
		return rds.lastFlushRT.Add(maxCache).Before(time.Now())
	}
	return false
}
