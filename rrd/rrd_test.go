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

package rrd

import (
	"fmt"
	//	"math"
	"testing"
	"time"
)

func makeRRA(id, dsId, perRow, size int64) *RoundRobinArchive {
	return &RoundRobinArchive{
		id:          id,
		dsId:        dsId,
		cf:          WMEAN,
		stepsPerRow: perRow,
		size:        size,
		width:       768,
		xff:         1,
	}
}

func makeRRAs(startId, dsId int64) []*RoundRobinArchive {
	return []*RoundRobinArchive{
		makeRRA(startId+1, dsId, 1, 2160),
		makeRRA(startId+2, dsId, 6, 14400),
		makeRRA(startId+3, dsId, 60, 13392),
	}
}

func makeDS(id int64) *DataSource {
	return &DataSource{
		id:        id,
		name:      fmt.Sprintf("foo.bar%d", id),
		step:      time.Duration(10) * time.Second,
		heartbeat: time.Duration(7200) * time.Second,
		rras:      makeRRAs(id*100, id),
	}
}

// func makeDSs() *DataSources {
// 	dsSlice := []*DataSource{
// 		makeDS(1),
// 		makeDS(2),
// 		makeDS(3),
// 	}

// 	dss := &DataSources{
// 		byName: make(map[string]*DataSource),
// 		byId:   make(map[int64]*DataSource),
// 	}

// 	for _, ds := range dsSlice {
// 		dss.byName[ds.Name] = ds
// 		dss.byId[ds.Id] = ds
// 	}

// 	return dss
// }

// func TestDataSources(t *testing.T) {
// 	dss := makeDSs()
// 	for _, ds1 := range dss.byId {
// 		if ds2 := dss.GetByName(ds1.Name); ds1 != ds2 {
// 			t.Errorf("dss.GetByName: ds1 != ds2 for name: %s", ds1.Name)
// 		}
// 		if ds2 := dss.GetById(ds1.Id); ds1 != ds2 {
// 			t.Errorf("dss.GetById: ds1 != ds2 for id: %s", ds1.Id)
// 		}
// 	}

// 	if len(dss.byName) != len(dss.List()) {
// 		t.Errorf("len(dss.byName) != len(dss.List())")
// 	}

// 	for _, ds := range dss.List() {
// 		dss.Delete(ds)
// 	}
// 	if (len(dss.byName) + len(dss.byId)) != 0 {
// 		t.Errorf("(len(dss.byName) + len(dss.byId)) != 0")
// 	}
// }

// func TestDataSource_setValue(t *testing.T) {
// 	ds := &DataSource{value: 123, duration: 456}
// 	ds.SetValue(math.Inf(-1))
// 	if ds.UnknownMs != 0 {
// 		t.Errorf("ds.UnknownMs != 0")
// 	}
// }

func TestDataSource_addValue(t *testing.T) {
	//ds := &DataSource{Value: 123}
	// ZZZ NEXT
}

// TODO
// Incoming.Process() as an actual test
