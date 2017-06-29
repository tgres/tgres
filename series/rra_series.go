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

type RLocker interface {
	RLock()
	RUnlock()
}

// RRASeries transforms a rrd.RoundRobinArchiver into a Series.
type RRASeries struct {
	rra       rrd.RoundRobinArchiver
	lck       RLocker
	latest    time.Time     // for Latest()
	step      time.Duration // for Step()
	pos       int64
	tim       time.Time // if timeRange was set
	alias     string
	from, to  time.Time
	groupBy   time.Duration
	maxPoints int64
	grpVal    float64 // if there is a group by
	rraLocked bool
}

func NewRRASeries(rra rrd.RoundRobinArchiver) *RRASeries {
	// If the rra is an RLocker, it is the responsibility of the
	// caller of this func to have it locked and unlocked (so that
	// rra.Latest() and Step() are safe).
	result := &RRASeries{
		rra:    rra,
		pos:    -1,
		latest: rra.Latest(),
		step:   rra.Step(),
	}
	if srra, ok := rra.(RLocker); ok {
		result.lck = srra
	}
	return result
}

func (s *RRASeries) Next() bool {

	if s.pos == -1 {
		if s.lck != nil && !s.rraLocked {
			s.rraLocked = true
			s.lck.RLock()
		}

		// Set initial values to from/to if they were not set by TimeRange()
		if s.from.IsZero() && s.to.IsZero() && !s.latest.IsZero() {
			s.from = s.rra.Latest().Add(-s.rra.Step() * time.Duration(s.rra.Size()))
			s.to = s.rra.Latest()
		}
	}

	// groupBy trumps maxPoints, otherwise maxPoints sets groupBy
	groupBy := s.groupBy

	if groupBy == 0 && s.maxPoints > 0 {
		groupBy = s.to.Sub(s.from) / time.Duration(s.maxPoints)
	}

	// Approximate the number of advances a group by contains.
	moves := 1
	if groupBy > 0 && groupBy > s.rra.Step() {
		moves = int(groupBy.Seconds()/s.rra.Step().Seconds() + 0.5)
	}

	// Compute agerage if we are grouping
	sum, cnt := float64(0), 0
	for i := 0; i < moves; i++ {
		if !s.advance() {
			s.grpVal = math.NaN()
			if s.lck != nil && s.rraLocked {
				s.lck.RUnlock()
				s.rraLocked = false
			}
			return false
		}
		val := s.curVal()
		if !math.IsNaN(val) && !math.IsInf(val, 0) {
			sum += val
			cnt++
		}
	}
	s.grpVal = sum / float64(cnt)
	return true
}

func (s *RRASeries) advance() bool {
	if s.to.Before(s.from) {
		s.tim = time.Time{}
		s.pos = -1
		return false
	}

	if s.tim.IsZero() {
		s.tim = s.from
	} else if s.tim.Before(s.to) {
		s.tim = s.tim.Add(s.rra.Step())
	} else {
		s.tim = time.Time{}
		s.pos = -1
		return false
	}

	latest, step, size := s.rra.Latest(), s.rra.Step(), s.rra.Size()
	if latest.IsZero() || s.tim.After(latest) || s.tim.Before(latest.Add(-step*time.Duration(size))) {
		// pos is invalid, but we're still returning true, because we can advance
		s.pos = -1
	} else {
		s.pos = rrd.SlotIndex(s.tim, step, size)
	}
	return true
}

func (s *RRASeries) CurrentValue() float64 {
	if s.groupBy > 0 || s.maxPoints > 0 {
		return s.grpVal
	}
	return s.curVal()
}

func (s *RRASeries) curVal() float64 {
	if result, ok := s.rra.DPs()[s.pos]; ok {
		return result
	}
	return math.NaN()
}

func (s *RRASeries) CurrentTime() time.Time {
	return s.tim
}

func (s *RRASeries) Close() error {
	if s.pos != -1 {
		if s.lck != nil && s.rraLocked {
			s.lck.RUnlock()
			s.rraLocked = false
		}
	}
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
	return s.groupBy
}

func (s *RRASeries) setTimeRange(from, to time.Time) {
	if to.IsZero() {
		to = s.latest // which can be zero too
	}
	s.from, s.to = from.Truncate(s.step), to.Truncate(s.step)
}

func (s *RRASeries) TimeRange(t ...time.Time) (time.Time, time.Time) {
	if len(t) == 1 {
		defer func() { s.setTimeRange(t[0], time.Time{}) }()
	} else if len(t) == 2 {
		defer func() { s.setTimeRange(t[0], t[1]) }()
	}
	return s.from, s.to
}

func (s *RRASeries) Latest() time.Time {
	if !s.to.IsZero() {
		return s.to
	}
	return s.latest
}

func (s *RRASeries) MaxPoints(n ...int64) int64 {
	if len(n) > 0 {
		defer func() { s.maxPoints = n[0] }()
	}
	return s.maxPoints
}

func (s *RRASeries) Alias(a ...string) string {
	if len(a) > 0 {
		s.alias = a[0]
	}
	return s.alias
}
