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
	sync.RWMutex
	byName map[string]*cachedDs
	db     serde.DataSourceSerDe
	dsf    dsFlusherBlocking
	finder MatchingDSSpecFinder
	clstr  clusterer
}

// Returns a new dsCache object. If locking is true, the resulting
// dsCache will maintain a lock, otherwise there is no locking,
// but the caller needs to ensure that it is never used concurrently
// (e.g. always in the same goroutine).
func newDsCache(db serde.DataSourceSerDe, finder MatchingDSSpecFinder, clstr clusterer, dsf dsFlusherBlocking) *dsCache {
	d := &dsCache{
		byName: make(map[string]*cachedDs),
		db:     db,
		finder: finder,
		clstr:  clstr,
		dsf:    dsf,
	}
	return d
}

// getByName rlocks and gets a DS pointer.
func (d *dsCache) getByName(name string) *cachedDs {
	d.RLock()
	defer d.RUnlock()
	return d.byName[name]
}

// Insert locks and inserts a DS.
func (d *dsCache) insert(cds *cachedDs) {
	d.Lock()
	defer d.Unlock()
	d.byName[cds.Name()] = cds
}

// Delete a DS
func (d *dsCache) delete(name string) {
	d.Lock()
	defer d.Unlock()
	delete(d.byName, name)
}

func (d *dsCache) preLoad() error {
	dss, err := d.db.FetchDataSources()
	if err != nil {
		return err
	}

	for _, ds := range dss {
		d.insert(&cachedDs{MetaDataSource: ds})
		d.register(ds)
	}

	return nil
}

// get a cached ds
func (d *dsCache) fetchDataSourceByName(name string) (*cachedDs, error) {
	cds := d.getByName(name)
	if cds == nil {
		if dsSpec := d.finder.FindMatchingDSSpec(name); dsSpec != nil {
			ds, err := d.db.FetchOrCreateDataSource(name, dsSpec)
			if err != nil {
				return nil, err
			}
			if ds != nil {
				cds = &cachedDs{MetaDataSource: ds}
				d.insert(cds)
				d.register(ds)
			}
		}
	}
	return cds, nil
}

// register the rds as a DistDatum with the cluster
func (d *dsCache) register(ds *rrd.MetaDataSource) {
	if d.clstr != nil {
		dds := &distDs{MetaDataSource: ds, dsc: d}
		d.clstr.LoadDistData(func() ([]cluster.DistDatum, error) {
			return []cluster.DistDatum{dds}, nil
		})
	}
}

type cachedDs struct {
	*rrd.MetaDataSource
	lastFlushRT time.Time // Last time this DS was flushed (actual real time).
}

func (cds *cachedDs) shouldBeFlushed(maxCachedPoints int, minCache, maxCache time.Duration) bool {
	if cds.LastUpdate().IsZero() {
		return false
	}
	pc := cds.PointCount()
	if pc > maxCachedPoints {
		return cds.lastFlushRT.Add(minCache).Before(time.Now())
	} else if pc > 0 {
		return cds.lastFlushRT.Add(maxCache).Before(time.Now())
	}
	return false
}

type distDs struct {
	*rrd.MetaDataSource
	dsc *dsCache
}

// cluster.DistDatum interface

func (ds *distDs) Relinquish() error {
	if !ds.LastUpdate().IsZero() {
		ds.dsc.dsf.flushDs(ds.MetaDataSource, true)
		ds.dsc.delete(ds.Name())
	}
	return nil
}

func (ds *distDs) Acquire() error {
	ds.dsc.delete(ds.Name())
	return nil
}

func (ds *distDs) Id() int64       { return ds.MetaDataSource.Id() }
func (ds *distDs) Type() string    { return "DataSource" }
func (ds *distDs) GetName() string { return ds.MetaDataSource.Name() }

// end cluster.DistDatum interface
