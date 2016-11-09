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
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
	"testing"
	"time"
)

func Test_dsCache_newDsCache(t *testing.T) {
	d := newDsCache(nil, nil, nil, nil, true)
	if d.rwLocker == nil {
		t.Error("locking true did not create a d.rwLocker")
	}
}

func Test_dsCache_getByName(t *testing.T) {
	d := newDsCache(nil, nil, nil, nil, true)
	d.byName["foo"] = &receiverDs{stale: true}
	if rds := d.getByName("foo"); rds == nil || !rds.stale {
		t.Errorf("getByName did not return correct value")
	}
	if rds := d.getByName("bar"); rds != nil {
		t.Errorf("getByName did not return nil")
	}
}

func Test_dsCache_getById(t *testing.T) {
	d := newDsCache(nil, nil, nil, nil, true)
	d.byId[1] = &receiverDs{stale: true}
	if rds := d.getById(1); rds == nil || !rds.stale {
		t.Errorf("getById did not return correct value")
	}
	if rds := d.getById(7); rds != nil {
		t.Errorf("getById did not return nil")
	}
}

func Test_dsCache_insert(t *testing.T) {
	d := newDsCache(nil, nil, nil, nil, true)

	ds := rrd.NewDataSource(7, "foo", 0, 0, time.Time{}, 0)
	rds := &receiverDs{DataSource: ds}

	d.insert(rds)
	if rds2 := d.getById(7); rds2 != rds {
		t.Errorf("insert: getById did not return correct value")
	}
	if rds2 := d.getByName("foo"); rds2 != rds {
		t.Errorf("insert: getByName did not return correct value")
	}
}

func Test_dsCache_preLoad(t *testing.T) {
	db := &fakeSerde{}
	d := newDsCache(db, nil, nil, nil, true)

	d.preLoad()
	if db.fetchCalled == 0 {
		t.Errorf("db.fetchCalled == 0")
	}

	ds := rrd.NewDataSource(0, "foo", 0, 0, time.Time{}, 0)
	db.returnDss = []*rrd.DataSource{ds}
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
	returnDss                              []*rrd.DataSource
}

func (f *fakeSerde) FetchDataSources() ([]*rrd.DataSource, error) {
	f.fetchCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	if len(f.returnDss) == 0 {
		return make([]*rrd.DataSource, 0), nil
	}
	return f.returnDss, nil
}

func (f *fakeSerde) FlushDataSource(ds *rrd.DataSource) error {
	f.flushCalled++
	return nil
}

func (f *fakeSerde) CreateOrReturnDataSource(name string, dsSpec *serde.DSSpec) (*rrd.DataSource, error) {
	f.createCalled++
	if f.fakeErr {
		return nil, fmt.Errorf("some error")
	}
	return rrd.NewDataSource(7, name, 0, 0, time.Time{}, 0), nil
}

func Test_dsCache_loadOrCreateDS(t *testing.T) {
	db := &fakeSerde{}
	df := &dftDSFinder{}
	d := newDsCache(db, df, nil, nil, true)
	d.loadOrCreateDS("foo")
	if db.createCalled != 1 {
		t.Errorf("loadOrCreateDS: CreateOrReturnDataSource should be called once, we got: %d", db.createCalled)
	}
	ds, err := d.loadOrCreateDS("")
	if ds != nil {
		t.Errorf("loadOrCreateDS: for a blank name we should get nil")
	}
	if err != nil {
		t.Errorf("loadOrCreateDS: err != nil")
	}
}

func Test_getByNameOrLoadOrCreate(t *testing.T) {
	db := &fakeSerde{}
	df := &dftDSFinder{}
	c := &fakeCluster{}
	d := newDsCache(db, df, c, nil, true)

	rds, err := d.getByNameOrLoadOrCreate("foo")
	if db.createCalled != 1 {
		t.Errorf("getByNameOrLoadOrCreate: CreateOrReturnDataSource should be called once, we got: %d", db.createCalled)
	}
	if err != nil || rds == nil {
		t.Errorf("getByNameOrLoadOrCreate: err != nil || rds == nil")
	}

	d = newDsCache(db, df, nil, nil, true)
	db.fakeErr = true
	rds, err = d.getByNameOrLoadOrCreate("foo")
	if err == nil || rds != nil {
		t.Errorf("getByNameOrLoadOrCreate: err == nil || rds != nil")
	}
	if db.createCalled != 2 || db.flushCalled != 0 {
		t.Errorf("getByNameOrLoadOrCreate (2): db.createCalled != 2 || db.flushCalled != 0, got: %d %d", db.createCalled, db.flushCalled)
	}
}

// This is a receiver
type fakeDsFlusher struct {
	called    int
	fdsReturn bool
}

func (f *fakeDsFlusher) flushDs(rds *receiverDs, block bool) bool {
	f.called++
	return f.fdsReturn
}

func Test_receiverDs_Relinquish(t *testing.T) {
	ds := rrd.NewDataSource(7, "foo", 0, 0, time.Time{}, 0)
	f := &fakeDsFlusher{}
	rds := &receiverDs{DataSource: ds, dsf: f}

	err := rds.Relinquish()
	if err != nil {
		t.Errorf("rds.Relinquish: err != nil: %v", err)
	}
	if f.called != 0 {
		t.Errorf("if lastupdate is zero, ds should not be flushed")
	}

	ds.ProcessIncomingDataPoint(123, time.Unix(1000, 0))
	err = rds.Relinquish()
	if err != nil {
		t.Errorf("rds.Relinquish (2): err != nil: %v", err)
	}
	if f.called != 1 {
		t.Errorf("if lastupdate is not zero, ds should be flushed")
	}

	// test Acquire while we're at it
	err = rds.Acquire()
	if err != nil {
		t.Errorf("Acquire: err != nil")
	}
	if !rds.stale {
		t.Errorf("Acquire: !rds.stale")
	}
	if f.called != 1 {
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

func Test_receiverDs_shouldBeFlushed(t *testing.T) {
	var (
		id, dsId, size, width int64
		step                  time.Duration
		cf                    string
		xff                   float32
		latest                time.Time
	)

	id, dsId, step, size, width, cf, xff, latest = 1, 3, 10*time.Second, 100, 30, "WMEAN", 0.5, time.Unix(1000, 0)
	ds := rrd.NewDataSource(dsId, "foo", 0, 0, time.Time{}, 0)
	rra, _ := rrd.NewRoundRobinArchive(id, dsId, cf, step, size, width, xff, latest)
	ds.SetRRAs([]*rrd.RoundRobinArchive{rra})
	f := &fakeDsFlusher{}
	rds := &receiverDs{DataSource: ds, dsf: f, lastFlushRT: time.Now()}

	// When rds.LastUpdate().IsZero() it should be false
	if rds.shouldBeFlushed(0, 0, 0) {
		t.Errorf("with rds.LastUpdate().IsZero(), rds.shouldBeFlushed == true")
	}

	// this will cause a LastUpdate != 0
	ds.ProcessIncomingDataPoint(123, time.Now().Add(-2*time.Hour))

	// so far we still have 0 points, so nothing to flush
	if rds.shouldBeFlushed(0, 0, 0) {
		t.Errorf("with PointCount 0, rds.shouldBeFlushed == true")
	}

	ds.ProcessIncomingDataPoint(123, time.Now().Add(-time.Hour))

	if !rds.shouldBeFlushed(0, 0, 24*time.Hour) {
		t.Errorf("with maxCachedPoints == 0, rds.shouldBeFlushed != true")
	}

	if rds.shouldBeFlushed(1000, 0, 24*time.Hour) {
		t.Errorf("with maxCachedPoints == 1000, rds.shouldBeFlushed == true")
	}

	if rds.shouldBeFlushed(1000, 24*time.Hour, 24*time.Hour) {
		t.Errorf("with maxCachedPoints == 1000, minCache 24hr, rds.shouldBeFlushed == true")
	}

	if !rds.shouldBeFlushed(1000, 0, 0) {
		t.Errorf("with maxCachedPoints == 1000, minCache 0, maxCache 0, rds.shouldBeFlushed != true")
	}

}
