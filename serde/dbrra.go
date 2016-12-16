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
	"bytes"
	"fmt"
	"strconv"
	"time"

	"github.com/tgres/tgres/rrd"
)

type DbRoundRobinArchiver interface {
	rrd.RoundRobinArchiver
	Id() int64
	Width() int64
	SlotRow(slot int64) int64
	DpsAsPGString(start, end int64) string
}

type DbRoundRobinArchive struct {
	rrd.RoundRobinArchiver
	id    int64 // Id
	width int64 // In the undelying storage, how many points are stored in a single (database) row.
}

func (rra *DbRoundRobinArchive) Id() int64    { return rra.id }
func (rra *DbRoundRobinArchive) Width() int64 { return rra.width }

func NewDbRoundRobinArchive(id, width int64, spec *rrd.RRASpec) (*DbRoundRobinArchive, error) {
	if spec.Span == 0 {
		return nil, fmt.Errorf("Invalid size: Span cannot be 0.")
	}
	// Steps are aligned on milliseconds, so a step of less than a
	// millisecond would result in a division by zero (see rra.update())
	if spec.Step < time.Millisecond {
		return nil, fmt.Errorf("Invalid step: Step: %v cannot be less than 1 millisecond.", spec.Step)
	}
	if (spec.Span % spec.Step) != 0 {
		return nil, fmt.Errorf("Invalid Step and/or Size: Size (%v) must be a multiple of Step (%v).", spec.Span, spec.Step)
	}
	rra := &DbRoundRobinArchive{
		RoundRobinArchiver: rrd.NewRoundRobinArchive(spec),
		id:                 id,
		width:              width,
	}
	return rra, nil
}

// SlotRow returns the row number given a slot number. This is mostly
// useful in serde implementations.
func (rra *DbRoundRobinArchive) SlotRow(slot int64) int64 {
	if slot%rra.width == 0 {
		return slot / rra.width
	} else {
		return (slot / rra.width) + 1
	}
}

// DpsAsPGString returns data points as a PostgreSQL-compatible array string
func (rra *DbRoundRobinArchive) DpsAsPGString(start, end int64) string {
	var b bytes.Buffer
	b.WriteString("{")
	dps := rra.Dps()
	for i := start; i <= end; i++ {
		b.WriteString(strconv.FormatFloat(dps[int64(i)], 'f', -1, 64))
		if i != end {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.String()
}

func (rra *DbRoundRobinArchive) Copy() rrd.RoundRobinArchiver {
	return &DbRoundRobinArchive{
		RoundRobinArchiver: rra.RoundRobinArchiver.Copy(),
		id:                 rra.id,
		width:              rra.width,
	}
}
