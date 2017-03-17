package dsl

import (
	"testing"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

func Test_funcs_dsl(t *testing.T) {
	var (
		sm  SeriesMap
		err error
		n   = 0
	)

	DBTime := "2006-01-02 15:04:05"
	when, _ := time.Parse(DBTime, "2017-03-16 09:41:00")

	// averageSeries
	sm, err = ParseDsl(nil, "averageSeries(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	n = 0
	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 20 {
				t.Errorf("s.CurrentValue != 20: %v", v)
			}
			n++
		}
		if n != 2 {
			t.Errorf("n != 2")
		}
	}

	// avg (alias for averageSeries)
	sm, err = ParseDsl(nil, "avg(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	n = 0
	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 20 {
				t.Errorf("s.CurrentValue != 20: %v", v)
			}
			n++
		}
		if n != 2 {
			t.Errorf("n != 2")
		}
	}

	// averageSeriesWithWildcards(host.cpu-[0-7].cpu-{user,system}.value, 1)
	ms := serde.NewMemSerDe()
	rcache := NewNamedDSFetcher(ms.Fetcher())

	rspec := rrd.RRASpec{
		Function: rrd.WMEAN,
		Step:     time.Minute,
		Span:     time.Hour,
		Latest:   when,
	}
	size := rspec.Span.Nanoseconds() / rspec.Step.Nanoseconds()

	spec := &rrd.DSSpec{
		Step: time.Second,
		RRAs: []rrd.RRASpec{rspec},
	}

	rspec.DPs = make(map[int64]float64)
	for i := int64(0); i < size; i++ {
		rspec.DPs[i] = 10
	}
	_, err = ms.FetchOrCreateDataSource(serde.Ident{"name": "foo.bar1.baz"}, spec)
	if err != nil {
		t.Error(err)
	}

	rspec.DPs = make(map[int64]float64)
	for i := int64(0); i < size; i++ {
		rspec.DPs[i] = 30
	}
	_, err = ms.FetchOrCreateDataSource(serde.Ident{"name": "foo.bar2.baz"}, spec)

	_, err = ParseDsl(rcache, `averageSeriesWithWildcards("foo.bleh.baz", 1)`, when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 20 {
				t.Errorf("s.CurrentValue != 20: %v", v)
			}
		}
	}

	// group
	sm, err = ParseDsl(nil, "avg(group(constantLine(10), constantLine(20), constantLine(30)))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	n = 0
	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 20 {
				t.Errorf("s.CurrentValue != 20: %v", v)
			}
			n++
		}
		if n != 2 {
			t.Errorf("n != 2")
		}
	}

	// isNonNull
	sm, err = ParseDsl(nil, "isNonNull(group(constantLine(10), constantLine(20), constantLine(30)))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 3 {
				t.Errorf("s.CurrentValue != 3: %v", v)
			}
		}
	}

	// maxSeries
	sm, err = ParseDsl(nil, "maxSeries(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 30 {
				t.Errorf("s.CurrentValue != 30: %v", v)
			}
		}
	}
}
