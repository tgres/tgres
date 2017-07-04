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
	"testing"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"github.com/tgres/tgres/series"
)

func Test_dscache_newDsCache(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	if d == nil {
		t.Error("newDsCache returned nil")
	}
}

func Test_dscache_getByIdent(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	foo := newCachedIdent(serde.Ident{"name": "foo"})
	bar := newCachedIdent(serde.Ident{"name": "bar"})
	d.byIdent[foo.String()] = &cachedDs{}
	if rds := d.getByIdent(foo); rds == nil {
		t.Errorf("getByIdent did not return correct value")
	}
	if rds := d.getByIdent(bar); rds != nil {
		t.Errorf("getByIdent did not return nil")
	}
}

func Test_dscache_insert(t *testing.T) {
	d := newDsCache(nil, nil, nil)

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}

	d.insert(rds)

	if rds2 := d.getByIdent(newCachedIdent(foo)); rds2 != rds {
		t.Errorf("insert: getByName did not return correct value")
	}
}

func Test_dscache_delete(t *testing.T) {
	d := newDsCache(nil, nil, nil)

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}
	d.insert(rds)

	d.delete(foo)

	if rds = d.getByIdent(newCachedIdent(foo)); rds != nil {
		t.Errorf("delete: did not delete")
	}
}

func Test_dscache_preLoad(t *testing.T) {
	db := &fakeSerde{}
	d := newDsCache(db, nil, nil)

	d.preLoad()
	if db.fetchCalled == 0 {
		t.Errorf("db.fetchCalled == 0")
	}

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec))
	db.returnDss = []rrd.DataSourcer{ds}
	d.preLoad()
	if len(d.byIdent) == 0 {
		t.Errorf("len(d.byIdent) == 0")
	}
	db.fakeErr = true
	if err := d.preLoad(); err == nil {
		t.Errorf("preLoad: err == nil")
	}

	// non-DbDataSource should error
	nds := rrd.NewDataSource(*DftDSSPec)
	db.fakeErr = false
	db.returnDss = []rrd.DataSourcer{nds}
	if err := d.preLoad(); err == nil {
		t.Errorf("preload: a non-DbDataSource should cause error")
	}
}

type fakeSerde struct {
	flushCalled, createCalled, fetchCalled int
	fakeErr                                bool
	returnDss                              []rrd.DataSourcer
	nondb                                  bool
}

func (m *fakeSerde) Fetcher() serde.Fetcher                                { return m }
func (m *fakeSerde) Flusher() serde.Flusher                                { return nil } // Flushing not supported
func (m *fakeSerde) EventListener() serde.EventListener                    { return nil } // not supported
func (f *fakeSerde) FetchDataSourceById(id int64) (rrd.DataSourcer, error) { return nil, nil }
func (m *fakeSerde) Search(serde.SearchQuery) (serde.SearchResult, error)  { return nil, nil }
func (f *fakeSerde) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {
	return nil, nil
}

func (f *fakeSerde) FetchDataSources() ([]rrd.DataSourcer, error) {
	f.fetchCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	if len(f.returnDss) == 0 {
		return make([]rrd.DataSourcer, 0), nil
	}
	return f.returnDss, nil
}

func (f *fakeSerde) FlushDataSource(ds rrd.DataSourcer) error {
	f.flushCalled++
	return nil
}

func (f *fakeSerde) FetchOrCreateDataSource(ident serde.Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	f.createCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	if f.nondb {
		return rrd.NewDataSource(*DftDSSPec), nil
	} else {
		foo := serde.Ident{"name": "foo"}
		return serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec)), nil
	}
}

func Test_dscache_fetchOrCreateByIdent(t *testing.T) {
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	sr := &fakeSr{}
	dsf := &dsFlusher{db: db.Flusher(), sr: sr}
	d := newDsCache(db, df, dsf)

	cds := d.getByIdentOrCreateEmpty(newCachedIdent(serde.Ident{"name": "foo"}))
	d.fetchOrCreateByIdent(cds)
	if db.createCalled != 1 {
		t.Errorf("fetchOrCreateByIdent: CreateOrReturnDataSource should be called once, we got: %d", db.createCalled)
	}

	cds = d.getByIdentOrCreateEmpty(newCachedIdent(serde.Ident{"name": ""}))
	if cds != nil {
		t.Errorf("getByIdentOrCreateEmpty: for a blank name we should get nil")
	}

	d = newDsCache(db, df, dsf)
	db.fakeErr = true
	cds = d.getByIdentOrCreateEmpty(newCachedIdent(serde.Ident{"name": "foo"}))
	if err := d.fetchOrCreateByIdent(cds); err == nil {
		t.Errorf("fetchOrCreateByIdent: db error should error")
	}

	// non-DbDataSource should error
	nds := rrd.NewDataSource(*DftDSSPec)
	db.fakeErr = false
	db.nondb = true
	db.returnDss = []rrd.DataSourcer{nds}
	d = newDsCache(db, df, dsf)
	cds = d.getByIdentOrCreateEmpty(newCachedIdent(serde.Ident{"name": "foo"}))
	if err := d.fetchOrCreateByIdent(cds); err == nil {
		t.Errorf("fetchOrCreateByIdent: non-DbDataSource should error")
	}
}

func Test_dscache_register(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	d.clstr = &fakeCluster{}

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec))
	d.register(ds) // not sure what we are testing here...
}

func Test_dscache_cachedDs_Relinquish(t *testing.T) {
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	dsf := &fakeDsFlusher{}
	dsc := newDsCache(db, df, dsf)

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, 0, 0, rrd.NewDataSource(*DftDSSPec))
	rds := &distDs{DbDataSourcer: ds, dsc: dsc}

	err := rds.Relinquish()
	if err != nil {
		t.Errorf("rds.Relinquish: err != nil: %v", err)
	}
	if dsf.called != 0 {
		t.Errorf("if lastupdate is zero, ds should not be flushed")
	}

	ds.ProcessDataPoint(123, time.Unix(1000, 0))
	err = rds.Relinquish()
	if err != nil {
		t.Errorf("rds.Relinquish (2): err != nil: %v", err)
	}
	if dsf.called != 0 {
		t.Errorf("if lastupdate is not zero but there is no rds in cache, ds should not be flushed")
	}

	// test Acquire while we're at it
	dsf.called = 0
	err = rds.Acquire()
	if err != nil {
		t.Errorf("Acquire: err != nil")
	}
	if dsf.called != 0 {
		t.Errorf("Acquire: should not call flush")
	}

	// receiverDs methods
	if rds.Type() != "DataSource" {
		t.Errorf(`rds.Type() != "DataSource"`)
	}

	if rds.GetName() != foo.String() {
		t.Errorf(`rds.GetName() != foo.String(): %v`, rds.GetName())
	}

	if rds.Id() != 0 {
		t.Errorf("id should be 0")
	}
}
