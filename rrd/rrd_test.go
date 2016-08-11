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
	"bytes"
	"encoding/gob"
	"fmt"
	"reflect"
	"testing"
	"time"
)

// IncomingDP must be gob encodable
func TestIncomingDP_gobEncodable(t *testing.T) {
	now := time.Now()
	dp1 := &IncomingDP{
		Name:      "foo.bar",
		TimeStamp: now,
		Value:     1.2345,
		Hops:      7,
	}

	var bb bytes.Buffer
	enc := gob.NewEncoder(&bb)
	dec := gob.NewDecoder(&bb)

	err := enc.Encode(dp1)
	if err != nil {
		t.Errorf("gob encode error:", err)
	}

	var dp2 *IncomingDP
	dec.Decode(&dp2)

	if !reflect.DeepEqual(dp1, dp2) {
		t.Errorf("dp1 != dp2 after gob encode/decode")
	}
}

// IncomingDP.Process()
func TestIncomingDP_Process(t *testing.T) {
	dp := &IncomingDP{}
	if dp.Process() == nil {
		t.Errorf("dp.Process() == nil (should be error because DS is nil)")
	}
}

func makeRRA(id, dsId int64, perRow, size int32) *RoundRobinArchive {
	return &RoundRobinArchive{
		Id:          id,
		DsId:        dsId,
		Cf:          "AVERAGE",
		StepsPerRow: perRow,
		Size:        size,
		Width:       768,
		Xff:         1,
		Latest:      time.Unix(0, 0),
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
		Id:          id,
		Name:        fmt.Sprintf("foo.bar%d", id),
		StepMs:      10000,
		HeartbeatMs: 7200000,
		LastUpdate:  time.Unix(0, 0),
		RRAs:        makeRRAs(id*100, id),
	}
}

func makeDSs() *DataSources {
	dsSlice := []*DataSource{
		makeDS(1),
		makeDS(2),
		makeDS(3),
	}

	dss := &DataSources{
		byName: make(map[string]*DataSource),
		byId:   make(map[int64]*DataSource),
	}

	for _, ds := range dsSlice {
		dss.byName[ds.Name] = ds
		dss.byId[ds.Id] = ds
	}

	return dss
}

func TestDataSources(t *testing.T) {
	dss := makeDSs()
	for _, ds1 := range dss.byId {
		if ds2 := dss.GetByName(ds1.Name); ds1 != ds2 {
			t.Errorf("dss.GetByName: ds1 != ds2 for name: %s", ds1.Name)
		}
		if ds2 := dss.GetById(ds1.Id); ds1 != ds2 {
			t.Errorf("dss.GetById: ds1 != ds2 for id: %s", ds1.Id)
		}
	}

	if len(dss.byName) != len(dss.List()) {
		t.Errorf("len(dss.byName) != len(dss.List())")
	}

	for _, ds := range dss.List() {
		dss.Delete(ds)
	}
	if (len(dss.byName) + len(dss.byId)) != 0 {
		t.Errorf("(len(dss.byName) + len(dss.byId)) != 0")
	}
}

// TODO
// Incoming.Process() as an actual test
