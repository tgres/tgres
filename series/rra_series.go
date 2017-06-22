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

package series

import (
	"math"
	"time"

	"github.com/tgres/tgres/rrd"
)

// RRASeries transforms a rrd.RoundRobinArchiver into a Series.
type RRASeries struct {
	data      map[int64]float64
	start     int64
	end       int64
	size      int64
	pos       int64
	step      time.Duration
	latest    time.Time
	alias     string
	from, to  time.Time
	groupBy   time.Duration
	maxPoints int64
	val       float64 // if there is a group by
}

func NewRRASeriesCopyRange(rra rrd.RoundRobinArchiver, from, to time.Time) *RRASeries {
	dps := rra.DPs()
	s := &RRASeries{
		data:   make(map[int64]float64, len(dps)),
		start:  rra.Start(),
		size:   rra.Size(),
		end:    rra.End(),
		pos:    -1,
		step:   rra.Step(),
		latest: rra.Latest(),
		from:   from,
		to:     to,
	}

	for n, v := range rra.DPs() {
		t := rrd.SlotTime(n, s.latest, s.step, s.size)
		if !t.Before(from) && !t.After(to) {
			s.data[n] = v
		}
	}

	s.start, s.end = rrd.ComputeStartEnd(s.data, s.latest, s.step, s.size)
	return s
}

func NewRRASeries(rra rrd.RoundRobinArchiver) *RRASeries {
	return &RRASeries{
		data:   rra.DPs(),
		start:  rra.Start(),
		size:   rra.Size(),
		end:    rra.End(),
		pos:    -1,
		step:   rra.Step(),
		latest: rra.Latest(),
	}
}

func (s *RRASeries) posValid() bool {
	if s.pos < 0 || s.pos >= s.size || len(s.data) == 0 || s.start == s.end {
		return false
	}
	if s.start < s.end {
		return s.pos >= s.start && s.pos <= s.end
	}
	return s.pos >= s.start || s.pos <= s.end
}

func (s *RRASeries) Next() bool {

	// groupBy trumps maxPoints, otherwise maxPoints sets groupBy
	groupBy := s.groupBy

	if groupBy == 0 && s.maxPoints > 0 {
		if s.from.Before(s.to) { // time range set
			groupBy = s.to.Sub(s.from) / time.Duration(s.maxPoints)
		} else { // whole series
			groupBy = time.Duration(s.size) * s.step / time.Duration(s.maxPoints)
		}
	}

	// We deal with groupBy by approximating the number of advances a
	// group by contains. It's not perfect, but good enough.
	moves := 1
	if groupBy > 0 && groupBy > s.step {
		moves = int(groupBy.Seconds()/s.step.Seconds() + 0.5)
	}

	sum, cnt := float64(0), 0
	for i := 0; i < moves; i++ {
		if !s.advance() {
			s.val = math.NaN()
			return false
		}
		val := s.curVal()
		if !math.IsNaN(val) && !math.IsInf(val, 0) {
			sum += val
			cnt++
		}
	}
	s.val = sum / float64(cnt)
	return true
}

func (s *RRASeries) advance() bool {
	if len(s.data) == 0 {
		return false
	}
	if s.pos == -1 {
		s.pos = s.start
		return true
	}
	if s.start < s.end {
		if s.pos < s.end {
			s.pos++
			return true
		}
	} else {
		if s.pos == s.size-1 {
			s.pos = 0
			return true
		}
		if s.pos > s.end {
			s.pos++
			return true
		} else {
			s.pos++
			return s.pos <= s.end
		}
	}
	return false
}

func (s *RRASeries) CurrentValue() float64 {
	if s.groupBy > 0 && s.groupBy > s.step {
		return s.val
	}
	return s.curVal()
}

func (s *RRASeries) curVal() float64 {
	if s.posValid() {
		if result, ok := s.data[s.pos]; ok {
			return result
		}
	}
	return math.NaN()
}

func (s *RRASeries) CurrentTime() time.Time {
	if s.posValid() {
		return rrd.SlotTime(s.pos, s.latest, s.step, s.size)
	}
	return time.Time{}
}

func (s *RRASeries) Close() error {
	s.pos = -1
	return nil
}

func (s *RRASeries) Step() time.Duration {
	return s.step
}

func (s *RRASeries) GroupBy(td ...time.Duration) time.Duration {
	if len(td) > 0 {
		defer func() { s.groupBy = td[0] }()
	}
	if s.groupBy == 0 {
		return s.step
	}
	return s.groupBy
}

func (s *RRASeries) TimeRange(t ...time.Time) (time.Time, time.Time) {
	// Not fully implemented TODO do we need it?
	if len(t) == 1 {
		defer func() { s.from = t[0] }()
	} else if len(t) == 2 {
		defer func() { s.from, s.to = t[0], t[1] }()
	}
	return s.from, s.to
}

func (s *RRASeries) Latest() time.Time {
	return s.latest
}

func (s *RRASeries) MaxPoints(n ...int64) int64 {
	if len(n) > 0 { // setter
		defer func() { s.maxPoints = n[0] }()
	}
	return s.maxPoints // getter
}

func (s *RRASeries) Alias(a ...string) string {
	if len(a) > 0 {
		s.alias = a[0]
	}
	return s.alias
}
