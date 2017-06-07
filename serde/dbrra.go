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
	DPsAsPGString(start, end int64) string
	//Pos() int64
	Seg() int64
	Idx() int64
	BundleId() int64
}

type DbRoundRobinArchive struct {
	rrd.RoundRobinArchiver
	id       int64 // Id
	width    int64 // In the undelying storage, how many points are stored in a single (database) row.
	bundleId int64 // rra bundle id
	pos      int64 // absolute bundle position (seg and idx can be inferred from pos)
	seg      int64 // segment
	idx      int64 // array index
}

func (rra *DbRoundRobinArchive) Id() int64       { return rra.id }
func (rra *DbRoundRobinArchive) Width() int64    { return rra.width }
func (rra *DbRoundRobinArchive) BundleId() int64 { return rra.bundleId }
func (rra *DbRoundRobinArchive) Seg() int64      { return rra.seg }
func (rra *DbRoundRobinArchive) Idx() int64      { return rra.idx }

//func (rra *DbRoundRobinArchive) Pos() int64      { return rra.pos }

func segIdxFromPosWidth(pos, width int64) (seg, idx int64) {
	// Careful: pos is 1-based. but to get a proper 1-based offset
	// into a segment (idx), we need to start out with a 0-based
	// value, then add 1!
	return (pos - 1) / width, (pos-1)%width + 1
}

func newDbRoundRobinArchive(id, width, bundleId, pos int64, spec rrd.RRASpec) (*DbRoundRobinArchive, error) {
	if spec.Span == 0 {
		return nil, fmt.Errorf("Invalid span: Span cannot be 0.")
	}
	// Steps are aligned on milliseconds, so a step of less than a
	// millisecond would result in a division by zero (see rra.update())
	if spec.Step < time.Millisecond {
		return nil, fmt.Errorf("Invalid step: Step: %v cannot be less than 1 millisecond.", spec.Step)
	}
	if (spec.Span % spec.Step) != 0 {
		return nil, fmt.Errorf("Invalid Step and/or Size: Size (%v) must be a multiple of Step (%v).", spec.Span, spec.Step)
	}
	seg, idx := segIdxFromPosWidth(pos, width)
	rra := &DbRoundRobinArchive{
		RoundRobinArchiver: rrd.NewRoundRobinArchive(spec),
		id:                 id,
		width:              width,
		bundleId:           bundleId,
		pos:                pos,
		seg:                seg,
		idx:                idx,
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

// DPsAsPGString returns data points as a PostgreSQL-compatible array string
func (rra *DbRoundRobinArchive) DPsAsPGString(start, end int64) string {
	var b bytes.Buffer
	b.WriteString("{")
	dps := rra.DPs()
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
		bundleId:           rra.bundleId,
		pos:                rra.pos,
	}
}

type rraBundleRecord struct {
	id     int64
	stepMs int64
	size   int64
	width  int64
}

// Representation of a row in the rra table - there is not enough
// information here to create a proper DbRoundRobinArchive in it.
type rraRecord struct {
	id       int64
	dsId     int64
	bundleId int64
	pos      int64
	seg      int64
	idx      int64
	cf       string
	xff      float32
}

type rraStateRecord struct {
	bundleId   int64
	seg        int64
	latest     *time.Time
	durationMs *int64
	value      *float64
}
