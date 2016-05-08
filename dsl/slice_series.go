package dsl

import (
	"math"
	"time"
)

type SliceSeries struct {
	data   []float64
	start  time.Time
	pos    int
	stepMs int64
	alias  string
}

func (s *SliceSeries) Next() bool {
	if s.pos < len(s.data) {
		s.pos++
		return true
	}
	return false
}

func (s *SliceSeries) CurrentValue() float64 {
	if s.pos < len(s.data) {
		return s.data[s.pos]
	}
	return math.NaN()
}

func (s *SliceSeries) CurrentPosBeginsAfter() time.Time {
	// this is correct, first one is negative
	return s.start.Add(time.Duration(s.stepMs*int64(s.pos-1)) * time.Millisecond)
}

func (s *SliceSeries) CurrentPosEndsOn() time.Time {
	return s.start.Add(time.Duration(s.stepMs*int64(s.pos)) * time.Millisecond)
}

func (s *SliceSeries) Close() error {
	s.pos = -1
	return nil
}

func (s *SliceSeries) StepMs() int64 {
	return s.stepMs
}

func (s *SliceSeries) GroupByMs(ms ...int64) int64 {
	return s.stepMs
}

func (s *SliceSeries) TimeRange(...time.Time) (time.Time, time.Time) {
	return time.Time{}, time.Time{} // not applicable
}

func (s *SliceSeries) LastUpdate() time.Time {
	return time.Time{}
}

func (s *SliceSeries) MaxPoints(...int64) int64 {
	return 0 // not applicable
}

func (s *SliceSeries) Alias(a ...string) string {
	if len(a) > 0 {
		s.alias = a[0]
	}
	return s.alias
}
