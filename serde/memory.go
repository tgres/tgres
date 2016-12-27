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

package serde

import (
	"sync"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/series"
)

type memSerDe struct {
	*sync.RWMutex
	byName map[string]int64
	byId   map[int64]rrd.DataSourcer
	lastId int64
}

// Returns a SerDe which keeps everything in memory.
func NewMemSerDe() *memSerDe {
	return &memSerDe{RWMutex: &sync.RWMutex{}, byName: make(map[string]int64), byId: make(map[int64]rrd.DataSourcer)}
}

func (m *memSerDe) Fetcher() Fetcher                         { return m }
func (m *memSerDe) Flusher() Flusher                         { return nil } // Flushing not supported
func (m *memSerDe) FlushDataSource(ds rrd.DataSourcer) error { return nil }

func (m *memSerDe) FetchDataSourceNames() (map[string]int64, error) {
	m.RLock()
	defer m.RUnlock()
	result := make(map[string]int64, len(m.byName))
	for k, v := range m.byName {
		result[k] = v
	}
	return result, nil
}

func (m *memSerDe) FetchDataSourceById(id int64) (rrd.DataSourcer, error) {
	m.RLock()
	defer m.RUnlock()
	return m.byId[id], nil
}

func (*memSerDe) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {
	return series.NewRRASeries(ds.RRAs()[0]), nil
}

func (m *memSerDe) FetchDataSources() ([]rrd.DataSourcer, error) {
	m.RLock()
	defer m.RUnlock()
	result := []rrd.DataSourcer{}
	for _, ds := range m.byId {
		result = append(result, ds)
	}
	return result, nil
}

func (m *memSerDe) FetchOrCreateDataSource(name string, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	m.Lock()
	defer m.Unlock()
	if id, ok := m.byName[name]; ok {
		return m.byId[id], nil
	}
	m.lastId++
	ds := NewDbDataSource(m.lastId, name, rrd.NewDataSource(*dsSpec))
	m.byName[name] = m.lastId
	m.byId[m.lastId] = ds
	return ds, nil
}
