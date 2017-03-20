package dsl

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

func checkEveryValueIs(sm SeriesMap, expected float64) (bool, float64) {
	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != expected {
				return false, v
			}
		}
	}
	return true, 0
}

func Test_funcs_dsl(t *testing.T) {

	// TODO: These are happy path tests, need more edge-case testing

	var (
		sm  SeriesMap
		err error
	)

	DBTime := "2006-01-02 15:04:05"
	when, _ := time.Parse(DBTime, "2017-03-16 09:41:00")

	// averageSeries
	sm, err = ParseDsl(nil, "averageSeries(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// avg (alias for averageSeries)
	sm, err = ParseDsl(nil, "avg(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// averageSeriesWithWildcards
	// sumSeriesWithWildcards
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

	spec.RRAs[0].DPs = make(map[int64]float64)
	for i := int64(0); i < size; i++ {
		spec.RRAs[0].DPs[i] = 10
	}

	_, err = ms.FetchOrCreateDataSource(serde.Ident{"name": "foo.bar1.baz"}, spec)
	if err != nil {
		t.Error(err)
	}

	spec.RRAs[0].DPs = make(map[int64]float64)
	for i := int64(0); i < size; i++ {
		spec.RRAs[0].DPs[i] = 30
	}
	_, err = ms.FetchOrCreateDataSource(serde.Ident{"name": "foo.bar2.baz"}, spec)

	_, err = ParseDsl(rcache, `averageSeriesWithWildcards("foo.bleh.baz", 1)`, when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	_, err = ParseDsl(rcache, `sumSeriesWithWildcards("foo.bleh.baz", 1)`, when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 40); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// group
	sm, err = ParseDsl(nil, "avg(group(constantLine(10), constantLine(20), constantLine(30)))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// isNonNull
	sm, err = ParseDsl(nil, "isNonNull(group(constantLine(10), constantLine(20), constantLine(30)))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 3); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// maxSeries
	sm, err = ParseDsl(nil, "maxSeries(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 30); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// minSeries
	sm, err = ParseDsl(nil, "minSeries(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 10); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// percentileOfSeries
	sm, err = ParseDsl(nil, "percentileOfSeries(group(constantLine(10), constantLine(20), constantLine(30)), 50)", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// rangeOfSeries
	sm, err = ParseDsl(nil, "rangeOfSeries(group(constantLine(10), constantLine(20), constantLine(30)))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// sumSeries
	sm, err = ParseDsl(nil, "sumSeries(group(constantLine(10), constantLine(20), constantLine(30)))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 60); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// absolute
	sm, err = ParseDsl(nil, "absolute(constantLine(-10))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 10); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	// derivative
	sm, err = ParseDsl(rcache, `derivative(generate())`, when.Unix(), when.Add(-time.Hour).Unix(), 10)
	if err != nil {
		t.Error(err)
	}

	for _, s := range sm {
		i, last := 0, float64(0)
		for s.Next() {
			gen := math.Sin(2 * math.Pi / float64(10) * float64(i))
			if i > 0 {
				v := s.CurrentValue()
				if v != gen-last {
					t.Errorf("Incorrect derivative: %v (expected: %v)", v, gen-last)
				}
			}
			last = gen
			i++
		}
	}

	// integral
	sm, err = ParseDsl(nil, "integral(generate())", when.Unix(), when.Add(-time.Hour).Unix(), 10)
	if err != nil {
		t.Error(err)
	}

	for _, s := range sm {
		i, sum := 0, float64(0)
		for s.Next() {
			gen := math.Sin(2 * math.Pi / float64(10) * float64(i))
			if i > 0 {
				v := s.CurrentValue()
				if v != sum {
					t.Errorf("Incorrect sum: %v (expected: %v)", v, sum)
				}
			}
			sum += gen
			i++
		}
	}

	// logarithm
	for _, fn := range []string{"log", "logarithm"} {
		expr := fmt.Sprintf("%s(constantLine(10))", fn)
		sm, err = ParseDsl(nil, expr, when.Unix(), when.Add(-time.Hour).Unix(), 10)
		if err != nil {
			t.Error(err)
		}

		if ok, unexpected := checkEveryValueIs(sm, 1); !ok {
			t.Errorf("Unexpected value: %v", unexpected)
		}
	}

	// nonNegativeDerivative
	sm, err = ParseDsl(rcache, `nonNegativeDerivative(generate())`, when.Unix(), when.Add(-time.Hour).Unix(), 10)
	if err != nil {
		t.Error(err)
	}

	for _, s := range sm {
		i, last := 0, float64(0)
		for s.Next() {
			gen := math.Sin(2 * math.Pi / float64(10) * float64(i))
			if i > 0 {
				v := s.CurrentValue()
				expect := gen - last
				if expect < 0 {
					if !math.IsNaN(v) {
						t.Errorf("Incorrect derivative: %v (expected: NaN)", v)
					}
				} else if v != expect {
					t.Errorf("Incorrect derivative: %v (expected: %v)", v, gen-last)
				}
			}
			last = gen
			i++
		}
	}
}
