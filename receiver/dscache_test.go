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
	called int
}

func (f *fakeSerde) FlushDataSource(ds *rrd.DataSource) error {
	f.called++
	return nil
}

func (f *fakeSerde) CreateOrReturnDataSource(name string, dsSpec *serde.DSSpec) (*rrd.DataSource, error) {
	f.called++
	return nil, nil
}

func Test_dsCache_loadOrCreateDS(t *testing.T) {
	db := &fakeSerde{}
	df := &dftDSFinder{}
	d := newDsCache(db, df, nil, nil, true)
	d.loadOrCreateDS("foo")
	if db.called != 1 {
		t.Errorf("loadOrCreateDS: CreateOrReturnDataSource should be called once, we got: %d", db.called)
	}
	if ds, _ := d.loadOrCreateDS(""); ds != nil {
		t.Errorf("loadOrCreateDS: for a blank name we should get nil")
	}
}
