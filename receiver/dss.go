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
	"sync"
	"time"
)

// A collection of data sources kept by an integer id as well as a
// string name.
type dataSources struct {
	l      rwLocker
	byName map[string]*receiverDs
	byId   map[int64]*receiverDs
}

type rwLocker interface {
	sync.Locker
	RLock()
	RUnlock()
}

type receiverDs struct {
	ds          *rrd.DataSource
	lastFlushRT time.Time // Last time this DS was flushed (actual real time).
}

// Returns a new dataSources object. If locking is true, the resulting
// dataSources will maintain a lock, otherwise there is no locking,
// but the caller needs to ensure that it is never used concurrently
// (e.g. always in the same goroutine).
func newDataSources(locking bool) *dataSources {
	dss := &dataSources{
		byId:   make(map[int64]*receiverDs),
		byName: make(map[string]*receiverDs),
	}
	if locking {
		dss.l = &sync.RWMutex{}
	}
	return dss
}

// GetByName rlocks and gets a DS pointer.
func (dss *dataSources) GetByName(name string) *receiverDs {
	if dss.l != nil {
		dss.l.RLock()
		defer dss.l.RUnlock()
	}
	return dss.byName[name]
}

// GetById rlocks and gets a DS pointer.
func (dss *dataSources) GetById(id int64) *receiverDs {
	if dss.l != nil {
		dss.l.RLock()
		defer dss.l.RUnlock()
	}
	return dss.byId[id]
}

// Insert locks and inserts a DS.
func (dss *dataSources) Insert(ds *rrd.DataSource) *receiverDs {
	if dss.l != nil {
		dss.l.Lock()
		defer dss.l.Unlock()
	}
	fds := &receiverDs{ds: ds}
	dss.byName[ds.Name()] = fds
	dss.byId[ds.Id()] = fds
	return fds
}

// List rlocks, then returns a slice of *DS
func (dss *dataSources) List() []*rrd.DataSource {
	if dss.l != nil {
		dss.l.RLock()
		defer dss.l.RUnlock()
	}

	result := make([]*rrd.DataSource, len(dss.byId))
	n := 0
	for _, fds := range dss.byId {
		result[n] = fds.ds
		n++
	}
	return result
}

// This only deletes it from memory, it is still in
// the database.
func (dss *dataSources) Delete(rds *receiverDs) {
	if dss.l != nil {
		dss.l.Lock()
		defer dss.l.Unlock()
	}

	delete(dss.byName, rds.ds.Name())
	delete(dss.byId, rds.ds.Id())
}

func (f *receiverDs) shouldBeFlushed(maxCachedPoints int, minCache, maxCache time.Duration) bool {
	if f.ds.LastUpdate().IsZero() {
		return false
	}
	pc := f.ds.PointCount()
	if pc > maxCachedPoints {
		return f.lastFlushRT.Add(minCache).Before(time.Now())
	} else if pc > 0 {
		return f.lastFlushRT.Add(maxCache).Before(time.Now())
	}
	return false
}
