package dsl

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

// TODO: These are happy path tests, need more edge-case testing

func checkEveryValueIs(sm SeriesMap, expected float64) (bool, float64) {
	n := 0
	for _, s := range sm {
		for s.Next() {
			n++
			v := s.CurrentValue()
			if v != expected {
				return false, v
			}
		}
	}
	if n == 0 {
		return false, math.NaN()
	}
	return true, 0
}

type testData struct {
	when, from, to time.Time
	rcache         ctxDSFetcher
	db             dsFetcher
}

var _td *testData

func setupTestData() *testData {
	if _td != nil {
		return _td
	}

	DBTime := "2006-01-02 15:04:05"
	when, _ := time.Parse(DBTime, "2017-03-16 09:41:00")
	from, to := when.Add(-time.Hour), when

	db := serde.NewMemSerDe()
	rcache := NewNamedDSFetcher(db.Fetcher())

	_td = &testData{
		when:   when,
		from:   from,
		to:     to,
		db:     db,
		rcache: rcache,
	}
	return _td
}

// averageSeries, avg
func Test_dsl_averageSeries(t *testing.T) {
	td := setupTestData()
	for _, fn := range []string{"averageSeries", "avg"} {
		expr := fmt.Sprintf("%s(constantLine(10), constantLine(20), constantLine(30))", fn)
		sm, err := ParseDsl(nil, expr, td.from, td.to, 100)
		if err != nil {
			t.Error(err)
		}
		if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
			t.Errorf("Unexpected value: %v", unexpected)
		}
	}
}

// averageSeriesWithWildcards
// sumSeriesWithWildcards
func Test_dsl_averageSeriesWithWildcards(t *testing.T) {
	td := setupTestData()

	rspec := rrd.RRASpec{
		Function: rrd.WMEAN,
		Step:     time.Minute,
		Span:     time.Hour,
		Latest:   td.when,
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

	var err error
	_, err = td.db.FetchOrCreateDataSource(serde.Ident{"name": "foo.bar1.baz"}, spec)
	if err != nil {
		t.Error(err)
	}

	spec.RRAs[0].DPs = make(map[int64]float64)
	for i := int64(0); i < size; i++ {
		spec.RRAs[0].DPs[i] = 20
	}
	_, err = td.db.FetchOrCreateDataSource(serde.Ident{"name": "foo.bar2.baz"}, spec)

	sm, err := ParseDsl(td.rcache, `averageSeriesWithWildcards("foo.bleh.baz", 1)`, td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 15); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}

	sm, err = ParseDsl(td.rcache, `sumSeriesWithWildcards("foo.bleh.baz", 1)`, td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 30); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// group
func Test_dsl_group(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "avg(group(constantLine(10), constantLine(20), constantLine(30)))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// isNonNull
func Test_dsl_isNonNull(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "isNonNull(group(constantLine(10), constantLine(20), constantLine(30)))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 3); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// maxSeries
func Test_dsl_maxSeries(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "maxSeries(constantLine(10), constantLine(20), constantLine(30))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 30); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// minSeries
func Test_dsl_minSeries(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "minSeries(constantLine(10), constantLine(20), constantLine(30))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 10); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// percentileOfSeries
func Test_dsl_percentileOfSeries(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "percentileOfSeries(group(constantLine(10), constantLine(20), constantLine(30)), 50)", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}

	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// rangeOfSeries
func Test_dsl_rangeOfSeries(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "rangeOfSeries(group(constantLine(10), constantLine(20), constantLine(30)))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// sumSeries
func Test_dsl_sumSeries(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "sumSeries(group(constantLine(10), constantLine(20), constantLine(30)))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 60); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// absolute
func Test_dsl_absolute(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "absolute(constantLine(-10))", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 10); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// derivative
func Test_dsl_derivative(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, `derivative(sinusoid())`, td.from, td.to, 10)
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
}

// integral
func Test_dsl_integral(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "integral(sinusoid())", td.from, td.to, 10)
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
}

// logarithm
func Test_dsl_logarithm(t *testing.T) {
	td := setupTestData()
	for _, fn := range []string{"log", "logarithm"} {
		expr := fmt.Sprintf("%s(constantLine(10))", fn)
		sm, err := ParseDsl(nil, expr, td.from, td.to, 10)
		if err != nil {
			t.Error(err)
		}
		if ok, unexpected := checkEveryValueIs(sm, 1); !ok {
			t.Errorf("Unexpected value: %v", unexpected)
		}
	}
}

// nonNegativeDerivative
func Test_dsl_nonNegativeDerivative(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, `nonNegativeDerivative(sinusoid())`, td.from, td.to, 10)
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

// offset
func Test_dsl_offset(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "offset(constantLine(-10), 5)", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, -5); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// offsetToZero
func Test_dsl_offsetToZero(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, `offsetToZero(sinusoid())`, td.from, td.to, 10)
	if err != nil {
		t.Error(err)
	}
	for _, s := range sm {
		i, offset := 0, float64(0.9510565162951536) // don't ask why
		for s.Next() {
			gen := math.Sin(2 * math.Pi / float64(10) * float64(i))
			if i > 0 {
				v := s.CurrentValue()
				expect := gen + offset
				if v != expect {
					t.Errorf("Incorrect offset: %v (expected: %v)", v, expect)
				}
			}
			i++
		}
	}
}

// scale
func Test_dsl_scale(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, "scale(constantLine(10), 2)", td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	if ok, unexpected := checkEveryValueIs(sm, 20); !ok {
		t.Errorf("Unexpected value: %v", unexpected)
	}
}

// timeshift
func Test_dsl_timeshift(t *testing.T) {
	td := setupTestData()
	sm, err := ParseDsl(nil, `timeShift(constantLine(10), "1h")`, td.from, td.to, 100)
	if err != nil {
		t.Error(err)
	}
	for _, s := range sm {
		i := 1
		for s.Next() {
			expect := td.when.Add(time.Duration(i) * time.Hour)
			v := s.CurrentTime()
			if !v.Equal(expect) {
				t.Errorf("Incorrect offset: %v (expected: %v)", v, expect)
			}
			i++
		}
	}
}
