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
	"sync"

	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

// A collection of data sources kept by name (string).
type dsCache struct {
	sync.RWMutex
	byIdent map[string]*cachedDs
	db      serde.Fetcher
	dsf     dsFlusherBlocking
	finder  MatchingDSSpecFinder
	clstr   clusterer
}

// Returns a new dsCache object.
func newDsCache(db serde.Fetcher, finder MatchingDSSpecFinder, dsf dsFlusherBlocking) *dsCache {
	d := &dsCache{
		byIdent: make(map[string]*cachedDs),
		db:      db,
		finder:  finder,
		dsf:     dsf,
	}
	return d
}

// getByName rlocks and gets a DS pointer.
func (d *dsCache) getByIdent(ident serde.Ident) *cachedDs {
	d.RLock()
	defer d.RUnlock()
	return d.byIdent[ident.String()]
}

// Insert locks and inserts a DS.
func (d *dsCache) insert(cds *cachedDs) {
	d.Lock()
	defer d.Unlock()
	d.byIdent[cds.Ident().String()] = cds
}

// Delete a DS
func (d *dsCache) delete(ident serde.Ident) {
	d.Lock()
	defer d.Unlock()
	delete(d.byIdent, ident.String())
}

func (d *dsCache) preLoad() error {
	dss, err := d.db.FetchDataSources()
	if err != nil {
		return err
	}

	for _, ds := range dss {
		dbds, ok := ds.(serde.DbDataSourcer)
		if !ok {
			return fmt.Errorf("preLoad: ds must be a serde.DbDataSourcer")
		}
		d.insert(&cachedDs{DbDataSourcer: dbds})
		d.register(dbds)
	}

	return nil
}

// get or create and empty cached ds
func (d *dsCache) getByIdentOrCreateEmpty(ident serde.Ident) *cachedDs {
	result := d.getByIdent(ident)
	if result == nil {
		if spec := d.finder.FindMatchingDSSpec(ident); spec != nil {
			// return a cachedDs with nil DataSourcer
			dbds := serde.NewDbDataSource(0, ident, nil)
			result = &cachedDs{DbDataSourcer: dbds, spec: spec}
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

// cachedDs is a DS that keeps a queue of incoming data points, which
// can all processed at once.
type cachedDs struct {
	serde.DbDataSourcer
	incoming []*incomingDP
	spec     *rrd.DSSpec // for when DS needs to be created
}

func (cds *cachedDs) appendIncoming(dp *incomingDP) {
	cds.incoming = append(cds.incoming, dp)
}

func (cds *cachedDs) processIncoming() (int, error) {
	var err error
	for _, dp := range cds.incoming {
		// continue on errors
		err = cds.ProcessDataPoint(dp.Value, dp.TimeStamp)
	}
	count := len(cds.incoming)
	cds.incoming = nil
	return count, err
}

// distDs keeps a pointer to the dsCache so that it can delete itself
// from it, as well as access the Flusher to persist during Relinquish
type distDs struct {
	serde.DbDataSourcer
	dsc *dsCache
}

// cluster.DistDatum interface

func (ds *distDs) Relinquish() error {
	if !ds.LastUpdate().IsZero() {
		ds.dsc.dsf.flushDs(ds.DbDataSourcer, true)
		ds.dsc.delete(ds.Ident())
	}
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
