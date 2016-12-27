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

func Test_dsCache_newDsCache(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	if d == nil {
		t.Error("newDsCache returned nil")
	}
}

func Test_dsCache_getByName(t *testing.T) {
	d := newDsCache(nil, nil, nil)
	d.byName["foo"] = &cachedDs{}
	if rds := d.getByName("foo"); rds == nil {
		t.Errorf("getByName did not return correct value")
	}
	if rds := d.getByName("bar"); rds != nil {
		t.Errorf("getByName did not return nil")
	}
}

func Test_dsCache_insert(t *testing.T) {
	d := newDsCache(nil, nil, nil)

	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))
	rds := &cachedDs{DbDataSourcer: ds}

	d.insert(rds)

	if rds2 := d.getByName("foo"); rds2 != rds {
		t.Errorf("insert: getByName did not return correct value")
	}
}

func Test_dsCache_preLoad(t *testing.T) {
	db := &fakeSerde{}
	d := newDsCache(db, nil, nil)

	d.preLoad()
	if db.fetchCalled == 0 {
		t.Errorf("db.fetchCalled == 0")
	}

	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))
	db.returnDss = []rrd.DataSourcer{ds}
	d.preLoad()
	if len(d.byName) == 0 {
		t.Errorf("len(d.byName) == 0")
	}

	db.fakeErr = true
	if err := d.preLoad(); err == nil {
		t.Errorf("preLoad: err == nil")
	}
}

type fakeSerde struct {
	flushCalled, createCalled, fetchCalled int
	fakeErr                                bool
	returnDss                              []rrd.DataSourcer
}

func (m *fakeSerde) Fetcher() serde.Fetcher                                { return m }
func (m *fakeSerde) Flusher() serde.Flusher                                { return nil } // Flushing not supported
func (f *fakeSerde) FetchDataSourceById(id int64) (rrd.DataSourcer, error) { return nil, nil }
func (m *fakeSerde) FetchDataSourceNames() (map[string]int64, error)       { return nil, nil }
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

func (f *fakeSerde) FetchOrCreateDataSource(name string, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	f.createCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	return serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec)), nil
}

func Test_dsCache_fetchDataSourceByName(t *testing.T) {
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	sr := &fakeSr{}
	dsf := &dsFlusher{db: db, sr: sr}
	d := newDsCache(db, df, dsf)
	d.fetchDataSourceByName("foo")
	if db.createCalled != 1 {
		t.Errorf("fetchDataSourceByName: CreateOrReturnDataSource should be called once, we got: %d", db.createCalled)
	}
	ds, err := d.fetchDataSourceByName("")
	if ds != nil {
		t.Errorf("fetchDataSourceByName: for a blank name we should get nil")
	}
	if err != nil {
		t.Errorf("fetchDataSourceByName: err != nil")
	}
}

// func Test_getByNameOrLoadOrCreate(t *testing.T) {
// 	db := &fakeSerde{}
// 	df := &dftDSFinder{}
// 	c := &fakeCluster{}
// 	d := newDsCache(db, df, c, nil, true)

// 	rds, err := d.getByNameOrLoadOrCreate("foo")
// 	if db.createCalled != 1 {
// 		t.Errorf("getByNameOrLoadOrCreate: CreateOrReturnDataSource should be called once, we got: %d", db.createCalled)
// 	}
// 	if err != nil || rds == nil {
// 		t.Errorf("getByNameOrLoadOrCreate: err != nil || rds == nil")
// 	}

// 	d = newDsCache(db, df, nil, nil, true)
// 	db.fakeErr = true
// 	rds, err = d.getByNameOrLoadOrCreate("foo")
// 	if err == nil || rds != nil {
// 		t.Errorf("getByNameOrLoadOrCreate: err == nil || rds != nil")
// 	}
// 	if db.createCalled != 2 || db.flushCalled != 0 {
// 		t.Errorf("getByNameOrLoadOrCreate (2): db.createCalled != 2 || db.flushCalled != 0, got: %d %d", db.createCalled, db.flushCalled)
// 	}
// }

// This is a receiver
type fakeDsFlusher struct {
	called    int
	fdsReturn bool
}

func (f *fakeDsFlusher) flushDs(ds serde.DbDataSourcer, block bool) bool {
	f.called++
	return f.fdsReturn
}

func (*fakeDsFlusher) enabled() bool { return true }

func Test_receiverDs_Relinquish(t *testing.T) {
	db := &fakeSerde{}
	df := &SimpleDSFinder{DftDSSPec}
	dsf := &fakeDsFlusher{}
	dsc := newDsCache(db, df, dsf)

	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))
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

	if rds.GetName() != "foo" {
		t.Errorf(`rds.GetName() != "foo"`)
	}
}

// func Test_receiverDs_shouldBeFlushed(t *testing.T) {
// 	var (
// 		id, dsId, size, width int64
// 		step                  time.Duration
// 		cf                    string
// 		xff                   float32
// 		latest                time.Time
// 	)

// 	db := &fakeSerde{}
// 	df := &SimpleDSFinder{DftDSSPec}
// 	dsf := &fakeDsFlusher{}
// 	dsc := newDsCache(db, df, dsf)

// 	ds := serde.NewDbDataSource(0, "foo", rrd.NewDataSource(*DftDSSPec))
// 	rds := &cachedDs{DbDataSourcer: ds}

// 	// When rds.LastUpdate().IsZero() it should be false
// 	if rds.shouldBeFlushed(0, 0, 0) {
// 		t.Errorf("with rds.LastUpdate().IsZero(), rds.shouldBeFlushed == true")
// 	}

// 	// this will cause a LastUpdate != 0
// 	ds.ProcessDataPoint(123, time.Now().Add(-2*time.Hour))

// 	// so far we still have 0 points, so nothing to flush
// 	if rds.shouldBeFlushed(0, 0, 0) {
// 		t.Errorf("with PointCount 0, rds.shouldBeFlushed == true")
// 	}

// 	ds.ProcessDataPoint(123, time.Now().Add(-time.Hour))

// 	if !rds.shouldBeFlushed(0, 0, 24*time.Hour) {
// 		t.Errorf("with maxCachedPoints == 0, rds.shouldBeFlushed != true")
// 	}

// 	if rds.shouldBeFlushed(1000, 0, 24*time.Hour) {
// 		t.Errorf("with maxCachedPoints == 1000, rds.shouldBeFlushed == true")
// 	}

// 	if rds.shouldBeFlushed(1000, 24*time.Hour, 24*time.Hour) {
// 		t.Errorf("with maxCachedPoints == 1000, minCache 24hr, rds.shouldBeFlushed == true")
// 	}

// 	if !rds.shouldBeFlushed(1000, 0, 0) {
// 		t.Errorf("with maxCachedPoints == 1000, minCache 0, maxCache 0, rds.shouldBeFlushed != true")
// 	}

// }
