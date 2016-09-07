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
	"reflect"
	"testing"
	"time"
)

func Test_DataSource(t *testing.T) {

	var (
		id       int64
		name     string
		step, hb time.Duration
		lu       time.Time
		lds      float64
	)

	// NewDataSource
	id, name, step, hb, lu, lds = 1, "foo.bar", 10*time.Second, 300*time.Second, time.Now(), 1.234
	ds := NewDataSource(id, name, step, hb, lu, lds)

	if id != ds.id || name != ds.name || step != ds.step ||
		hb != ds.heartbeat || lu != ds.lastUpdate || lds != ds.lastDs {
		t.Errorf("NewDataSource: id != ds.id || name != ds.name || step != ds.step || hb != ds.heartbeat || lu != ds.lastUpdate || lds != ds.lastDs")
	}

	// Accessors
	if ds.Name() != ds.name {
		t.Errorf("ds.Name() != ds.name")
	}
	if ds.Id() != ds.id {
		t.Errorf("ds.Id() != ds.id")
	}
	if ds.Step() != ds.step {
		t.Errorf("ds.Step() != ds.step")
	}
	if ds.Heartbeat() != ds.heartbeat {
		t.Errorf("ds.Heartbeat() != ds.heartbeat")
	}
	if ds.LastUpdate() != ds.lastUpdate {
		t.Errorf("ds.LastUpdate() != ds.lastUpdate")
	}
	if ds.LastDs() != ds.lastDs {
		t.Errorf("ds.LastDs() != ds.lastDs")
	}

	rras := []*RoundRobinArchive{&RoundRobinArchive{id: 1}}
	ds.SetRRAs(rras)
	if !reflect.DeepEqual(ds.RRAs(), rras) {
		t.Errorf("ds.RRAs() != ds.rras")
	}

}
