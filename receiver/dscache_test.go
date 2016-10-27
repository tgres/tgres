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

type fakeSerde struct {
	flushCalled, createCalled int
	fakeErr                   bool
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
	called int
}

func (f *fakeDsFlusher) flushDs(rds *receiverDs, block bool) {
	f.called++
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

	ds.ProcessIncomingDataPoint(123, time.Now())
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
}
