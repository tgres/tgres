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

func Test_dscache_getByName(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	foo := serde.Ident{"name": "foo"}
	bar := serde.Ident{"name": "bar"}
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
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}

	d.insert(rds)

	if rds2 := d.getByIdent(foo); rds2 != rds {
		t.Errorf("insert: getByName did not return correct value")
	}
}

func Test_dscache_delete(t *testing.T) {
	d := newDsCache(nil, nil, nil)

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}
	d.insert(rds)

	d.delete(foo)

	if rds = d.getByIdent(foo); rds != nil {
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
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
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
		return serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec)), nil
	}
}

func Test_dscache_fetchOrCreateByName(t *testing.T) {
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	sr := &fakeSr{}
	dsf := &dsFlusher{db: db, sr: sr}
	d := newDsCache(db, df, dsf)
	d.fetchOrCreateByName("foo")
	if db.createCalled != 1 {
		t.Errorf("fetchOrCreateByName: CreateOrReturnDataSource should be called once, we got: %d", db.createCalled)
	}

	ds, err := d.fetchOrCreateByName("")
	if ds != nil {
		t.Errorf("fetchOrCreateByName: for a blank name we should get nil")
	}
	if err != nil {
		t.Errorf("fetchOrCreateByName: err != nil")
	}

	d = newDsCache(db, df, dsf)
	db.fakeErr = true
	if ds, err = d.fetchOrCreateByName("foo"); err == nil {
		t.Errorf("fetchOrCreateByName: db error should error")
	}
	if ds != nil {
		t.Errorf("fetchOrCreateByName: on db error we should get nil")
	}

	// non-DbDataSource should error
	nds := rrd.NewDataSource(*DftDSSPec)
	db.fakeErr = false
	db.nondb = true
	db.returnDss = []rrd.DataSourcer{nds}
	d = newDsCache(db, df, dsf)
	if ds, err = d.fetchOrCreateByName("foo"); err == nil {
		t.Errorf("fetchOrCreateByName: non-DbDataSource should error")
	}

}

func Test_dscache_register(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	d.clstr = &fakeCluster{}

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
	d.register(ds) // not sure what we are testing here...
}

func Test_dscache_cachedDs_shouldBeFlushed(t *testing.T) {
	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}

	// When rds.LastUpdate().IsZero() it should be false
	if rds.shouldBeFlushed(0, 0, 0) {
		t.Errorf("with rds.LastUpdate().IsZero(), rds.shouldBeFlushed == true")
	}

	// this will cause a LastUpdate != 0
	rds.ProcessDataPoint(123, time.Now().Add(-2*time.Hour))
	if rds.LastUpdate().IsZero() {
		t.Errorf("LastUpdate is zero?")
	}

	// so far we still have 0 points, so nothing to flush
	if rds.shouldBeFlushed(0, 0, 0) {
		t.Errorf("with PointCount 0, rds.shouldBeFlushed == true")
	}

	rds.ProcessDataPoint(123, time.Now().Add(-time.Hour))

	if !rds.shouldBeFlushed(0, 0, 24*time.Hour) {
		t.Errorf("with maxCachedPoints == 0, rds.shouldBeFlushed != true")
	}

	if !rds.shouldBeFlushed(1000, 0, 24*time.Hour) {
		t.Errorf("with neverflushed maxCachedPoints == 1000, rds.shouldBeFlushed != true")
	}

	if !rds.shouldBeFlushed(1000, 24*time.Hour, 24*time.Hour) {
		t.Errorf("with neverFlushed maxCachedPoints == 1000, minCache 24hr, rds.shouldBeFlushed != true")
	}

	if !rds.shouldBeFlushed(1000, 0, 0) {
		t.Errorf("with neverflusdhed maxCachedPoints == 1000, minCache 0, maxCache 0, rds.shouldBeFlushed != true")
	}

	rds.lastFlushRT = time.Now().Add(-time.Hour)

	if rds.shouldBeFlushed(1000, 0, 24*time.Hour) {
		t.Errorf("with flushed maxCachedPoints == 1000, rds.shouldBeFlushed == true")
	}

	if rds.shouldBeFlushed(1000, 24*time.Hour, 24*time.Hour) {
		t.Errorf("with flushed maxCachedPoints == 1000, minCache 24hr, rds.shouldBeFlushed == true")
	}

	if !rds.shouldBeFlushed(1000, 0, 0) {
		t.Errorf("with flushed maxCachedPoints == 1000, minCache 0, maxCache 0, rds.shouldBeFlushed != true")
	}

}

func Test_dscache_cachedDs_Relinquish(t *testing.T) {
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	dsf := &fakeDsFlusher{}
	dsc := newDsCache(db, df, dsf)

	foo := serde.Ident{"name": "foo"}
	ds := serde.NewDbDataSource(0, foo, rrd.NewDataSource(*DftDSSPec))
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
	if dsf.called != 1 {
		t.Errorf("if lastupdate is not zero, ds should be flushed")
	}

	// test Acquire while we're at it
	err = rds.Acquire()
	if err != nil {
		t.Errorf("Acquire: err != nil")
	}
	if dsf.called != 1 {
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
