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

package dsl

import (
	"math"
	"time"

	"github.com/tgres/tgres/rrd"
)

// RRASeries transforms a rrd.RoundRobinArchiver into a Series.
type RRASeries struct {
	data   map[int64]float64
	start  int64
	end    int64
	size   int64
	pos    int64
	step   time.Duration
	latest time.Time
	alias  string
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

func (s *RRASeries) GroupBy(...time.Duration) time.Duration {
	return s.step
}

func (s *RRASeries) TimeRange(...time.Time) (time.Time, time.Time) {
	return time.Time{}, time.Time{} // NIY
}

func (s *RRASeries) Latest() time.Time {
	return s.latest
}

func (s *RRASeries) MaxPoints(...int64) int64 {
	return 0 // not applicable
}

func (s *RRASeries) Alias(a ...string) string {
	if len(a) > 0 {
		s.alias = a[0]
	}
	return s.alias
}
