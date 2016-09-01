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
	"testing"
	"time"
)

func Test_NewRoundRobinArchive(t *testing.T) {
	var (
		id, dsId, stepsPerRow, size, width int64
		cf                                 string
		xff                                float32
		latest                             time.Time
	)

	id, dsId, stepsPerRow, size, width, cf, xff, latest = 1, 3, 10, 100, 30, "FOOBAR", 0.5, time.Now()

	rra, err := NewRoundRobinArchive(id, dsId, cf, stepsPerRow, size, width, xff, latest)
	if err == nil {
		t.Errorf("Invalid cf %q did not cause an error")
	}

	// Again, his time good data
	for _, cf = range []string{"MIN", "MAX", "LAST", "WMEAN"} {
		rra, err = NewRoundRobinArchive(id, dsId, cf, stepsPerRow, size, width, xff, latest)
		if err != nil {
			t.Errorf("NewRoundRobinArchive(cf = %q): error: %v", cf, err)
			return
		}
	}

	if id != rra.id || dsId != rra.dsId ||
		rra.cf != WMEAN || stepsPerRow != rra.stepsPerRow ||
		size != rra.size || width != rra.width || xff != rra.xff ||
		latest != rra.latest {
		t.Errorf(`id != rra.id || dsId != rra.dsId || cf != rra.cf || stepsPerRow != rra.stepsPerRow || size != rra.size || width != rra.sidth || xff != rra.xff || latest != rra.latest`)
	}

	if rra.dps == nil {
		t.Errorf("NewRoundRobinArchive: len(rra.dps) == 0")
	}

	if rra.Id() != rra.id {
		t.Errorf("rra.Id(): %v  != rra.id: %v", rra.Id(), rra.id)
	}
	if rra.Latest() != rra.latest {
		t.Errorf("rra.Latest(): %v  != rra.latest: %v", rra.Latest(), rra.latest)
	}
	if rra.Size() != rra.size {
		t.Errorf("rra.Size(): %v  != rra.size: %v", rra.Size(), rra.size)
	}
	if rra.Width() != rra.width {
		t.Errorf("rra.Width(): %v  != rra.width: %v", rra.Width(), rra.width)
	}
	if rra.Start() != rra.start {
		t.Errorf("rra.Start(): %v  != rra.start: %v", rra.Start(), rra.start)
	}
	if rra.End() != rra.end {
		t.Errorf("rra.End(): %v  != rra.end: %v", rra.End(), rra.end)
	}

	dsStep := 25 * time.Second
	if rra.Step(dsStep) != time.Duration(rra.stepsPerRow)*dsStep {
		t.Errorf("rra.Step(%v) != time.Duration(rra.stepsPerRow)*%v", dsStep, dsStep)
	}
}
