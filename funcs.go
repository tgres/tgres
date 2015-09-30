//
// Copyright 2015 Gregory Trubetskoy. All Rights Reserved.
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

package timeriver

import (
	"fmt"
	"math"
	"regexp"
	"sort"
	"strings"
	"time"
)

// We have two confusingly similar types here - SeriesMap and
// SeriesList. The key distinction is that you cannot see beyond a
// SeriesList, it encapsulates and hides all Series within
// it. E.g. when we compute avg(a, b), we end up with a new series and
// no longer have access to a or b. This is done by way of SeriesList.
//
// The test is whether a "new" series is created. E.g. scale() is
// given a bunch of of series, and returns a bunch of series, so it
// should use SeriesMap. avg() takes a bunch of series and returns
// only one series, so it's a SeriesList.
//
// SeriesList
//  - based on []Series
//  - implements Series
//  - aligns Series' within it to match on group by
//  - series in it are not associated with a name (key)
//  - it's a "mix of series" that can be treated as a series
//  - supports duplicates
//
// SeriesMap
//  - alias for map[string]Series
//  - does NOT implement Series, it's a container of series, but not a series
//  - is how we give Series names - the key is the name
//  - does not support duplicates - same series would need different names

type SeriesMap map[string]Series

func (sm SeriesMap) SortedKeys() []string {
	keys := make([]string, 0, len(sm))
	for k, _ := range sm {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

type dslFuncType func(*DslCtx, []interface{}) (SeriesMap, error)

type FuncMap map[string]dslFuncType

var funcs = FuncMap{
	"scale":                  dslScale,
	"absolute":               dslAbsolute,
	"averageSeries":          dslAverageSeries,
	"avg":                    dslAverageSeries,
	"group":                  dslGroup,
	"isNonNull":              dslIsNonNull,
	"maxSeries":              dslMaxSeries,
	"max":                    dslMaxSeries,
	"minSeries":              dslMinSeries,
	"min":                    dslMinSeries,
	"sumSeries":              dslSumSeries,
	"sum":                    dslSumSeries,
	"percentileOfSeries":     dslPercentileOfSeries,
	"rangeOfSeries":          dslRangeOfSeries,
	"asPercent":              dslAsPercent,
	"alias":                  dslAlias,
	"sumSeriesWithWildcards": dslSumSeriesWithWildcards,
	"derivative":             dslDerivative,
	"nonNegativeDerivative":  dslNonNegativeDerivative,
	"integral":               dslIntegral,
	"logarithm":              dslLogarithm,
	"log":                    dslLogarithm,
	"offset":                 dslOffset,
	"timeShift":              dslTimeShift,
	"transformNull":          dslTransformNull,
	"diffSeries":             dslDiffSeries,
	"nPercentile":            dslNPercentile,
	"highestCurrent":         dslHighestCurrent,
	"highestMax":             dslHighestMax,
	"limit":                  dslLimit,
	"lowestAverage":          dslLowestAverage,
	"lowestCurrent":          dslLowestCurrent,
	"maximumAbove":           dslMaximumAbove,
	"maximumBelow":           dslMaximumBelow,
	"minimumAbove":           dslMinimumAbove,
	"minimumBelow":           dslMinimumBelow,
	"mostDeviant":            dslMostDeviant,
	"movingAverage":          dslMovingAverage,
	"movingMedian":           dslMovingMedian,
	"removeAbovePercentile":  dslRemoveAbovePercentile,
	"removeAboveValue":       dslRemoveAboveValue,
	"removeBelowPercentile":  dslRemoveBelowPercentile,
	"removeBelowValue":       dslRemoveBelowValue,
	"stdev":                  dslMovingStdDev,
	"weightedAverage":        dslWeightedAverage,
	"aliasByMetric":          dslAliasByMetric,
	"aliasByNode":            dslAliasByNode,
	"aliasSub":               dslAliasSub,
	"changed":                dslChanged,
	"countSeries":            dslCountSeries,

	// COMBINE
	// ** averageSeries
	// ** avg
	// ** averageSeriesWithWildcards
	// ** group
	// ** isNonNull
	// -- mapSeries // returns a list of lists (non-standard)
	// ** maxSeries
	// ** minSeries
	// ** percentileOfSeries
	// ** rangeOfSeries
	// -- reduceSeries // relies on mapSeries
	// ** sumSeries
	// ** sumSeriesWithWildcards

	// TRANSFORM
	// ** absolute()
	// ** derivative()
	// -- hitcount() // don't understand this one
	// ** integral()
	// ** log()
	// ** nonNegativeDerivative
	// ** offset
	// -- offsetToZero // would require whole series min()
	// -- perSecond // everything here is perSedond() already
	// ** scale()
	// -- scaleToSeconds()
	// -- smartSummarize
	// -- summarize // seems complicated
	// ** timeShift
	// ?? timeStack // TODO?
	// ** transformNull

	// CALCULATE
	// ** asPercent
	// ** diffSeries
	// ?? holtWintersAbberation
	// ?? holtWintersConfidenceBands
	// ?? holtWintersForecast
	// ** nPercentile

	// FILTER
	// ** highestCurrent
	// ** highestMax
	// ** limit
	// ** lowestAverage
	// ** lowestCurrent
	// ** maximumAbove
	// ** maximumBelow
	// ** minimumAbove
	// ** minimumBelow
	// ** mostDeviant
	// ** movingAverage
	// ** movingMedian
	// ** removeAbovePercentile
	// ** removeAboveValue
	// ** removeBelowPercentile
	// ** removeBelowValue
	// ** stdev
	// ?? useSeriesAbove // ?
	// ** weightedAverage

	// SPECIAL
	// ** alias
	// ** aliasByMetric
	// ** aliasByNode
	// ** aliasSub
	// ?? cactiStyle // TODO should be easy to do?
	// ** changed
	// ?? consolidateBy // doesn't apply to us, it's always avg?
	// ?? constantLine  // it must take a series as arg, or it makes no sense?
	// ** countSeries
	// -- cumulative // == consolidateBy
	// ?? groupByNode // similar to alias by metric
	// ?? keepLastValue // don't really understand this one
	// ?? randomWalk // later?
	// ?? sortByMaxima
	// ?? sortByMinima
	// ?? sortByName
	// ?? sortByTotal
	// ?? stacked
	// ?? substr
}

func seriesFromFunction(dc *DslCtx, name string, args []interface{}) (SeriesMap, error) {

	dslFunc := funcs[name]
	if dslFunc == nil {
		return nil, fmt.Errorf("seriesFromFunction(): No such function: %v", name)
	}

	if series, err := dslFunc(dc, args); err == nil {
		return series, nil
	} else {
		return nil, fmt.Errorf("seriesFromFunction(): %v() reports an error: %v", name, err)
	}
}

// SeriesList is an "abstract" Series (it fails to implement
// CurrentValue()). It is useful for bunching Series together to call
// Next() and Close() on all of them (e.g. in avg() or sum()).

type SeriesSlice []Series
type SeriesList struct {
	SeriesSlice
	alias *string
}

func (sl *SeriesList) Next() bool {
	if len(sl.SeriesSlice) == 0 {
		return false
	}
	for _, series := range sl.SeriesSlice {
		if !series.Next() {
			return false
		}
	}

	return true
}

func (sl *SeriesList) CurrentPosBeginsAfter() time.Time {
	if len(sl.SeriesSlice) > 0 {
		return sl.SeriesSlice[0].CurrentPosBeginsAfter()
	}
	return time.Time{}
}

func (sl *SeriesList) CurrentPosEndsOn() time.Time {
	if len(sl.SeriesSlice) > 0 {
		return sl.SeriesSlice[0].CurrentPosEndsOn()
	}
	return time.Time{}
}

func (sl *SeriesList) Close() error {
	for _, series := range sl.SeriesSlice {
		if err := series.Close(); err != nil {
			return err
		}
	}
	return nil
}

func (sl *SeriesList) StepMs() int64 {
	// This returns the StepMs of the first series since we should
	// assume they are aligned (thus equeal). Then if we happen to be
	// encapsulated in a another SeriesList, it might overrule the
	// GroupByMs.
	return sl.SeriesSlice[0].StepMs()
}

func (sl *SeriesList) SetGroupByMs(ms int64) {} // TODO ?

func (sl *SeriesList) GroupByMs() int64 {
	if len(sl.SeriesSlice) > 0 {
		return sl.SeriesSlice[0].GroupByMs()
	}
	return 0
}

// Least Common Multiple
func lcm(x, y int64) int64 {
	if x == 0 || y == 0 {
		return 0
	}
	p := x * y
	for y != 0 {
		mod := x % y
		x, y = y, mod
	}
	return p / x
}

func (sl *SeriesList) Align() {
	if len(sl.SeriesSlice) < 2 {
		return
	}

	var result int64 = -1
	for _, series := range sl.SeriesSlice {
		if result == -1 {
			result = series.StepMs()
			continue
		}
		result = lcm(result, series.StepMs())
	}

	for _, series := range sl.SeriesSlice {
		series.SetGroupByMs(result)
	}
}

func (sl *SeriesList) Alias(s ...string) *string {
	if len(s) > 0 {
		sl.alias = &s[0]
	}
	return sl.alias
}

func (sl *SeriesList) Sum() (result float64) {
	for _, series := range sl.SeriesSlice {
		result += series.CurrentValue()
	}
	return
}

func (sl *SeriesList) Avg() float64 {
	return sl.Sum() / float64(len(sl.SeriesSlice))
}

func (sl *SeriesList) Max() float64 {
	result := math.NaN()
	for _, series := range sl.SeriesSlice {
		value := series.CurrentValue()
		if math.IsNaN(result) || result < value {
			result = value
		}
	}
	return result
}

func (sl *SeriesList) Min() float64 {
	result := math.NaN()
	for _, series := range sl.SeriesSlice {
		value := series.CurrentValue()
		if math.IsNaN(result) || result > value {
			result = value
		}
	}
	return result
}

func (sl *SeriesList) First() float64 {
	for _, series := range sl.SeriesSlice {
		return series.CurrentValue()
	}
	return math.NaN()
}

func percentile(list []float64, p float64) float64 {
	// https://github.com/rcrowley/go-metrics/blob/a248d281279ea605eccec4f54546fd998c060e38/sample.go#L278
	size := len(list)
	if size == 0 {
		return math.NaN()
	}
	cpy := make([]float64, len(list))
	copy(cpy, list)
	sort.Float64s(cpy)
	pos := p * float64(size+1)
	if pos < 1.0 {
		return cpy[0]
	} else if pos >= float64(size) {
		return cpy[size-1]
	} else {
		lower := cpy[int(pos)-1]
		upper := cpy[int(pos)]
		return lower + (pos-math.Floor(pos))*(upper-lower)
	}
}

func (sl *SeriesList) Percentile(p float64) float64 {
	// This is a percentile of one data point, not the whole series
	dps := make([]float64, 0)
	for _, series := range sl.SeriesSlice {
		dps = append(dps, series.CurrentValue())
	}
	return percentile(dps, p)
}

func (sl *SeriesList) Range() float64 {
	return sl.Max() - sl.Min()
}

func (sl *SeriesList) Diff() float64 {
	if len(sl.SeriesSlice) == 0 {
		return math.NaN()
	}
	// TODO SeriesList still needs to be ordered
	result := sl.SeriesSlice[0].CurrentValue()
	for _, series := range sl.SeriesSlice[1:] {
		result -= series.CurrentValue()
	}
	return result
}

func NewSeriesListFromArgs(dc *DslCtx, args []interface{}) (*SeriesList, error) {

	if len(args) == 0 {
		return nil, fmt.Errorf("NewSeriesListFromArgs(): at least 1 arg required, 0 given")
	}

	result := &SeriesList{SeriesSlice: make(SeriesSlice, 0)}
	for _, arg := range args {
		series, err := dc.seriesFromSeriesOrIdent(arg)
		if err != nil {
			return nil, err
		}
		for _, s := range series {
			result.SeriesSlice = append(result.SeriesSlice, s)
		}
	}
	result.Align()
	return result, nil
}

func AlignSeriesMap(sm SeriesMap) {

	result := &SeriesList{SeriesSlice: make(SeriesSlice, 0)}
	for _, series := range sm {
		result.SeriesSlice = append(result.SeriesSlice, series)
	}
	result.Align()
}

func argsAsString(args []interface{}) string {
	sargs := make([]string, 0, len(args))
	for _, arg := range args {
		sargs = append(sargs, fmt.Sprintf("%v", arg))
	}
	return strings.Join(sargs, ",")
}

// seriesWithSummaries provides some across-series summary functions,
// e.g. Max(), Avr(), StdDev(), etc.

type seriesWithSummaries struct {
	Series
}

func (f *seriesWithSummaries) Max() (max float64) {
	max = math.NaN()
	for f.Series.Next() {
		value := f.Series.CurrentValue()
		if math.IsNaN(max) || value > max {
			max = value
		}
	}
	f.Series.Close()
	return
}

func (f *seriesWithSummaries) Min() (min float64) {
	min = math.NaN()
	for f.Series.Next() {
		value := f.Series.CurrentValue()
		if math.IsNaN(min) || value < min {
			min = value
		}
	}
	f.Series.Close()
	return
}

func (f *seriesWithSummaries) Avg() float64 {
	count := 0
	sum := float64(0)
	for f.Series.Next() {
		sum += f.Series.CurrentValue()
		count++
	}
	f.Series.Close()
	return sum / float64(count)
}

func (f *seriesWithSummaries) StdDev(avg float64) float64 {
	count := 0
	sum := float64(0)
	for f.Series.Next() {
		sum += math.Pow(f.Series.CurrentValue()-avg, 2)
		count++
	}
	f.Series.Close()
	return math.Sqrt(sum / float64(count-1))
}

func (f *seriesWithSummaries) Last() (last float64) {
	for f.Series.Next() {
		last = f.Series.CurrentValue()
	}
	f.Series.Close()
	return
}

// ------------ functions ----------

// maxSeries()

type seriesMaxSeries struct {
	SeriesList
}

func (sl *seriesMaxSeries) CurrentValue() float64 {
	return sl.Max()
}

func dslMaxSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("maxSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesMaxSeries{*seriesList}}, nil
}

// minSeries()

type seriesMinSeries struct {
	SeriesList
}

func (sl *seriesMinSeries) CurrentValue() float64 {
	return sl.Min()
}

func dslMinSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("minSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesMinSeries{*seriesList}}, nil
}

// sumSeries()

type seriesSumSeries struct {
	SeriesList
}

func (sl *seriesSumSeries) CurrentValue() float64 {
	return sl.Sum()
}

func dslSumSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("sumSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesSumSeries{*seriesList}}, nil
}

// diffSeries()

type seriesDiffSeries struct {
	SeriesList
}

func (sl *seriesDiffSeries) CurrentValue() float64 {
	return sl.Diff()
}

func dslDiffSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("diffSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesDiffSeries{*seriesList}}, nil
}

// sumSeriesWithWildcards()
func dslSumSeriesWithWildcards(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) < 2 {
		return nil, fmt.Errorf("Expecting at least 2 arguments, got %d", len(args))
	}

	name, ok := args[0].(string)
	if !ok {
		return nil, fmt.Errorf("%v is not a string", args[0])
	}

	var newName string = name
	for _, arg := range args[1:] {
		switch p := arg.(type) {
		case float64: // our numbers are all float64
			pos := int(p)
			parts := strings.Split(newName, ".")
			if len(parts) > pos {
				parts[pos] = "*"
				newName = strings.Join(parts, ".")
			}
		}
	}

	result := &SeriesList{SeriesSlice: make(SeriesSlice, 0)}
	series, err := dc.seriesFromSeriesOrIdent(newName)
	if err != nil {
		return nil, err
	}
	for _, s := range series {
		result.SeriesSlice = append(result.SeriesSlice, s)
	}
	result.Align()

	name = fmt.Sprintf("sumSeriesWithWildcards(%s)", argsAsString(args))
	return SeriesMap{name: &seriesSumSeries{*result}}, nil
}

// percentileOfSeries()
// TODO the interpolate argument is ignored for now

type seriesPercentileOfSeries struct {
	SeriesList
	ptile float64
}

func (sl *seriesPercentileOfSeries) CurrentValue() float64 {
	return sl.Percentile(sl.ptile)
}

func dslPercentileOfSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) < 2 {
		return nil, fmt.Errorf("Expecting at least 2 arguments, got %d", len(args))
	}

	// First arg is a Series map or an string ident
	seriesList, err := NewSeriesListFromArgs(dc, args[:1])
	if err != nil {
		return nil, err
	}

	ptile := args[1].(float64) / 100
	name := fmt.Sprintf("percentileOfSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesPercentileOfSeries{*seriesList, ptile}}, nil
}

// rangeOfSeries()

type seriesRangeOfSeries struct {
	SeriesList
}

func (sl *seriesRangeOfSeries) CurrentValue() float64 {
	return sl.Range()
}

func dslRangeOfSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("rangeOfSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesRangeOfSeries{*seriesList}}, nil
}

// averageSeries()

type seriesAverageSeries struct {
	SeriesList
}

func (sl *seriesAverageSeries) CurrentValue() float64 {
	return sl.Avg()
}

func dslAverageSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("averageSeries(%s)", argsAsString(args))
	return SeriesMap{name: &seriesAverageSeries{*seriesList}}, nil
}

// group()

func dslGroup(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) == 0 {
		return nil, fmt.Errorf("Expecting at least 1 argument, got %d", len(args))
	}

	result := make(SeriesMap)
	for _, arg := range args {
		series, err := dc.seriesFromSeriesOrIdent(arg)
		if err != nil {
			return nil, err
		}
		for n, s := range series {
			result[n] = s
		}
	}

	AlignSeriesMap(result)

	return result, nil
}

// alias()

func dslAlias(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting at least 2 arguments, got %d", len(args))
	}

	var alias string = "ERROR"
	switch s := args[1].(type) {
	case string:
		alias = s
	}
	result, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	for _, series := range result {
		series.Alias(alias)
	}

	return result, nil
}

// aliasByMetric()

func dslAliasByMetric(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	result, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	for name, series := range result {
		parts := strings.Split(name, ".")
		series.Alias(parts[len(parts)-1])
	}

	return result, nil
}

// aliasByNode()

func dslAliasByNode(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	result, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		for name, series := range result {
			parts := strings.Split(name, ".")
			if n >= len(parts) {
				return nil, fmt.Errorf("node index %v out of range for number of nodes: %v", n, len(parts))
			}
			series.Alias(parts[n])
		}
		return result, nil
	} else {
		return nil, fmt.Errorf("Invalid argument: %v", args[1])
	}
}

// aliasSub()
// TODO regex groups don't work yet

func dslAliasSub(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 3 {
		return nil, fmt.Errorf("Expecting 3 arguments, got %d", len(args))
	}

	result, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	sreg, ok := args[1].(string)
	if !ok {
		return nil, fmt.Errorf("%v is not a string", args[1])
	}
	reg, err := regexp.Compile(sreg)
	if err != nil {
		return nil, err
	}

	for name, series := range result {
		if arg2, ok := args[2].(string); ok {
			rname := reg.ReplaceAllString(name, arg2)
			series.Alias(rname)
		} else {
			return nil, fmt.Errorf("%v is not a string", args[2])
		}
	}

	return result, nil
}

// asPercent()

type seriesAsPercent struct {
	SeriesList
	my_idx int
	total  float64
}

func (sl *seriesAsPercent) CurrentValue() float64 {
	if math.IsNaN(sl.total) {
		return sl.SeriesSlice[sl.my_idx].CurrentValue() / sl.Sum()
	} else {
		return sl.SeriesSlice[sl.my_idx].CurrentValue() / sl.total
	}
}

func dslAsPercent(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) == 0 {
		return nil, fmt.Errorf("Expecting at least 1 argument, got %d", len(args))
	}

	result, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	var total float64 = math.NaN()
	if len(args) == 2 {
		total = args[1].(float64)
	}

	// Wrap in seriesAsPercent AND build a SeriesList so we can do Sum
	// The series needs to know its index in the SeriesList
	sl := &SeriesList{SeriesSlice: make(SeriesSlice, 0)}
	for _, series := range result {
		sl.SeriesSlice = append(sl.SeriesSlice, series)
	}
	sl.Align()
	n := 0
	for name, _ := range result {
		sl.Alias(fmt.Sprintf("asPersent(%s,%v)", name, args[0]))
		result[name] = &seriesAsPercent{*sl, n, total}
		n++
	}

	return result, nil
}

// isNonNull

type seriesIsNonNull struct {
	SeriesList
}

func (sl *seriesIsNonNull) CurrentValue() float64 {
	count := 0
	for _, series := range sl.SeriesSlice {
		if !math.IsNaN(series.CurrentValue()) {
			count++
		}
	}
	return float64(count)
}

func dslIsNonNull(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("isNonNull(%s)", argsAsString(args))
	return SeriesMap{name: &seriesIsNonNull{*seriesList}}, nil
}

// absolute()

type seriesAbsolute struct {
	Series
}

func (f *seriesAbsolute) CurrentValue() float64 {
	return math.Abs(f.Series.CurrentValue())
}

func dslAbsolute(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	// First arg is a Series map or an string ident
	var series SeriesMap
	var err error

	series, err = dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	for name, s := range series {
		s.Alias(fmt.Sprintf("absolute(%s)", name))
		series[name] = &seriesAbsolute{s}
	}

	return series, nil
}

// scale()

type seriesScale struct {
	Series
	factor float64
}

func (f *seriesScale) CurrentValue() float64 {
	return f.Series.CurrentValue() * f.factor
}

func dslScale(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if factor, ok := args[1].(float64); ok {
		// Wrap everything in scale
		for name, s := range series {
			s.Alias(fmt.Sprintf("scale(%v,%v)", name, factor))
			series[name] = &seriesScale{s, factor}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// derivative()

type seriesDerivative struct {
	Series
	last float64
}

func (f *seriesDerivative) CurrentValue() float64 {
	return f.Series.CurrentValue() - f.last
}

func (f *seriesDerivative) Next() bool {
	f.last = f.Series.CurrentValue()
	return f.Series.Next()
}

func dslDerivative(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	for name, s := range series {
		s.Alias(fmt.Sprintf("derivative(%s)", name))
		series[name] = &seriesDerivative{s, math.NaN()}
	}

	return series, nil
}

// integral()

type seriesIntegral struct {
	Series
	total float64
}

func (f *seriesIntegral) CurrentValue() float64 {
	return f.total
}

func (f *seriesIntegral) Next() bool {
	value := f.Series.CurrentValue()
	if !math.IsNaN(value) {
		f.total += value
	}
	return f.Series.Next()
}

func dslIntegral(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	for name, s := range series {
		s.Alias(fmt.Sprintf("integral(%s)", name))
		series[name] = &seriesIntegral{s, 0}
	}

	return series, nil
}

// logarithm()

type seriesLogarithm struct {
	Series
	base float64
}

func (f *seriesLogarithm) CurrentValue() float64 {
	return math.Log(f.CurrentValue()) / math.Log(f.base)
}

func dslLogarithm(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if base, ok := args[1].(float64); ok {
		for name, s := range series {
			s.Alias(fmt.Sprintf("logarithm(%v,%v)", name, base))
			series[name] = &seriesLogarithm{s, base}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// nonNegativeDerivative()

type seriesNonNegativeDerivative struct {
	Series
	last float64
}

func (f *seriesNonNegativeDerivative) CurrentValue() float64 {
	result := f.Series.CurrentValue() - f.last
	if result < 0 {
		return math.NaN()
	}
	return result
}

func (f *seriesNonNegativeDerivative) Next() bool {
	if !f.Series.Next() {
		return false
	}
	value := f.Series.CurrentValue()
	for math.IsNaN(f.last) || value < f.last {
		f.last = value
		if !f.Series.Next() {
			return false
		}
		value = f.Series.CurrentValue()
	}
	f.last = value
	return f.Series.Next()
}

func dslNonNegativeDerivative(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	for name, s := range series {
		s.Alias(fmt.Sprintf("nonNegativeDerivative(%s)", name))
		series[name] = &seriesNonNegativeDerivative{s, math.NaN()}
	}

	return series, nil
}

// offset()

type seriesOffset struct {
	Series
	offset float64
}

func (f *seriesOffset) CurrentValue() float64 {
	return f.Series.CurrentValue() + f.offset
}

func dslOffset(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if offset, ok := args[1].(float64); ok {
		for name, s := range series {
			s.Alias(fmt.Sprintf("offset(%v,%v)", name, offset))
			series[name] = &seriesOffset{s, offset}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// timeShift()
// TODO need to implement resetEnd

type seriesTimeShift struct {
	Series
	timeShift time.Duration
}

func parseTimeShift(s string) (time.Duration, error) {
	if len(s) == 0 {
		return 0, nil
	}
	var sansSign = s
	if s[0] == '-' || s[0] == '+' {
		sansSign = s[1:len(s)]
	}
	if dur, err := betterParseDuration(sansSign); err == nil {
		if s[0] == '+' {
			return dur * -1, nil
		} else {
			return dur, nil
		}
	} else {
		return 0, fmt.Errorf("parseTimeShift(): Error parsing duration %q: %v", s, err)
	}
}

func (f *seriesTimeShift) CurrentPosBeginsAfter() time.Time {
	return f.Series.CurrentPosBeginsAfter().Add(f.timeShift)
}

func (f *seriesTimeShift) CurrentPosEndsOn() time.Time {
	return f.Series.CurrentPosEndsOn().Add(f.timeShift)
}

func dslTimeShift(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if ts, ok := args[1].(string); ok {
		shift, err := parseTimeShift(ts)
		if err != nil {
			return nil, err
		}

		for name, s := range series {
			s.Alias(fmt.Sprintf("timeShift(%v,%v)", name, ts))
			series[name] = &seriesTimeShift{s, shift}
		}

	} else {
		return nil, fmt.Errorf("%v not a string", args[1])
	}

	return series, nil
}

// transformNull()

type seriesTransformNull struct {
	Series
	transformNull float64
}

func (f *seriesTransformNull) CurrentValue() float64 {
	value := f.Series.CurrentValue()
	if math.IsNaN(value) {
		return f.transformNull
	}
	return value
}

func dslTransformNull(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if transformNull, ok := args[1].(float64); ok {
		for name, s := range series {
			s.Alias(fmt.Sprintf("transformNull(%v,%v)", name, transformNull))
			series[name] = &seriesTransformNull{s, transformNull}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}

}

// nPercentile()

type seriesNPercentile struct {
	Series
	n          float64
	percentile float64
}

func (f *seriesNPercentile) CurrentValue() float64 {
	return f.percentile
}

func (f *seriesNPercentile) Next() bool {
	if math.IsNaN(f.percentile) {
		// We traverse the whole series, and then it will be traversed
		// again as the datapoints are sent to the client
		series := make([]float64, 0)
		for f.Series.Next() {
			series = append(series, f.Series.CurrentValue())
		}
		f.Series.Close()
		f.percentile = percentile(series, f.n)
	}
	return f.Series.Next() // restart to the first Next()
}

func dslNPercentile(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if n, ok := args[1].(float64); ok {
		n = n / 100
		for name, s := range series {
			s.Alias(fmt.Sprintf("nPercentile(%v,%v)", name, n*100))
			series[name] = &seriesNPercentile{s, n, math.NaN()}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// sortedMap inspired by
// https://groups.google.com/d/msg/golang-nuts/FT7cjmcL7gw/S4pQnxBFWWwJ

type sortedMap struct {
	m map[string]float64
	s []string
}

func (sm *sortedMap) Len() int {
	return len(sm.m)
}

func (sm *sortedMap) Less(i, j int) bool {
	return sm.m[sm.s[i]] < sm.m[sm.s[j]]
}

func (sm *sortedMap) Swap(i, j int) {
	sm.s[i], sm.s[j] = sm.s[j], sm.s[i]
}

func sortedKeys(m map[string]float64) []string {
	sm := new(sortedMap)
	sm.m = m
	sm.s = make([]string, len(m))
	i := 0
	for key, _ := range m {
		sm.s[i] = key
		i++
	}
	sort.Sort(sm)
	return sm.s
}

// highestCurrent()

type seriesHighestCurrent struct {
	seriesWithSummaries
}

func dslHighestCurrent(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		lasts := make(map[string]float64)
		for name, s := range series {
			s.Alias(fmt.Sprintf("highestCurrent(%v,%v)", name, n))
			shc := &seriesHighestCurrent{seriesWithSummaries{s}}
			lasts[name] = shc.Last()
			series[name] = shc
		}
		sortedLasts := sortedKeys(lasts)
		for i := 0; i < len(sortedLasts)-n; i++ {
			delete(series, sortedLasts[i])
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// highestMax()

type seriesHighestMax struct {
	seriesWithSummaries
}

func dslHighestMax(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		lasts := make(map[string]float64)
		for name, s := range series {
			s.Alias(fmt.Sprintf("highestMax(%v,%v)", name, n))
			shm := &seriesHighestMax{seriesWithSummaries{s}}
			lasts[name] = shm.Max()
			series[name] = shm
		}
		sortedLasts := sortedKeys(lasts)
		for i := 0; i < len(sortedLasts)-n; i++ {
			delete(series, sortedLasts[i])
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// limit()

func dslLimit(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	// note that this isn't the "first", but rather just *any* n metrics
	// graphite docs aren't specific about ordering
	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		for k, _ := range series {
			delete(series, k)
			if len(series) <= n {
				break
			}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// lowestAverage()

type seriesLowestAverage struct {
	seriesWithSummaries
}

func dslLowestAverage(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		avgs := make(map[string]float64)
		for name, s := range series {
			ss := &seriesLowestAverage{seriesWithSummaries{s}}
			avgs[name] = ss.Avg()
			series[name] = ss
		}
		sortedAvgs := sortedKeys(avgs)
		for i := len(sortedAvgs) - 1; i >= n; i-- {
			delete(series, sortedAvgs[i])
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// lowestCurrent()

type seriesLowestCurrent struct {
	seriesWithSummaries
}

func dslLowestCurrent(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		lasts := make(map[string]float64)
		for name, s := range series {
			shc := &seriesLowestCurrent{seriesWithSummaries{s}}
			lasts[name] = shc.Last()
			series[name] = shc
		}
		sortedLasts := sortedKeys(lasts)
		for i := len(sortedLasts) - 1; i >= n; i-- {
			delete(series, sortedLasts[i])
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// maximumAbove()

type seriesMaximumAbove struct {
	seriesWithSummaries
}

func dslMaximumAbove(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if mark, ok := args[1].(float64); ok {
		for name, s := range series {
			shm := &seriesMaximumAbove{seriesWithSummaries{s}}
			if shm.Max() <= mark {
				delete(series, name)
			}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// maximumBelow()

type seriesMaximumBelow struct {
	seriesWithSummaries
}

func dslMaximumBelow(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if mark, ok := args[1].(float64); ok {
		for name, s := range series {
			shm := &seriesMaximumBelow{seriesWithSummaries{s}}
			if shm.Max() >= mark {
				delete(series, name)
			}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// minimumAbove()

type seriesMinimumAbove struct {
	seriesWithSummaries
}

func dslMinimumAbove(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if mark, ok := args[1].(float64); ok {
		for name, s := range series {
			shm := &seriesMinimumAbove{seriesWithSummaries{s}}
			if shm.Min() <= mark {
				delete(series, name)
			}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// minimumBelow()

type seriesMinimumBelow struct {
	seriesWithSummaries
}

func dslMinimumBelow(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if mark, ok := args[1].(float64); ok {
		for name, s := range series {
			shm := &seriesMinimumBelow{seriesWithSummaries{s}}
			if shm.Min() >= mark {
				delete(series, name)
			}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float")
	}
}

// mostDeviant()

type seriesMostDeviant struct {
	seriesWithSummaries
}

func dslMostDeviant(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if nf, ok := args[1].(float64); ok {
		n := int(nf)
		stddevs := make(map[string]float64)
		for name, s := range series {
			shm := &seriesMostDeviant{seriesWithSummaries{s}}
			stddev := shm.StdDev(shm.Avg())
			stddevs[name] = stddev
			series[name] = shm
		}
		sortedStdDevs := sortedKeys(stddevs)
		for i := 0; i < len(sortedStdDevs)-n; i++ {
			delete(series, sortedStdDevs[i])
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// movingAverage()

type seriesMovingAverage struct {
	Series
	window    []float64
	points, n int
	dur       time.Duration
}

func (f *seriesMovingAverage) Next() bool {
	// if we're given a duration, then the number of points is simply
	// the duration / group by period. this works because we outer
	// join with the time generate_series, and thus never skip a time
	// period
	if f.dur != 0 && f.points == 0 {
		f.points = int((f.dur.Nanoseconds()/1000000)/f.GroupByMs()) + 1 // +1 to avoid div by 0
	}
	// initial build up
	for len(f.window) < f.points {
		if f.Series.Next() {
			f.window = append(f.window, f.Series.CurrentValue())
			f.n++
			// if n starts at -1
			// [a] n:0; [a,b] n:1, [a,b,c] n:2 | exit loop
		} else {
			return false
		}
	}
	// now we have enough points
	if f.Series.Next() {
		f.window[f.n%f.points] = f.Series.CurrentValue()
		f.n++
	} else {
		return false
	}
	return true
}

func (f *seriesMovingAverage) CurrentValue() float64 {
	var sum float64
	for _, w := range f.window {
		sum += w
	}
	return sum / float64(len(f.window))
}

func dslMovingAverage(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	switch arg := args[1].(type) {
	case string:
		if dur, err := parseTimeShift(arg); err != nil {
			return nil, err
		} else {
			for name, s := range series {
				s.Alias(fmt.Sprintf("movingAverage(%v,%v)", name, args[1]))
				series[name] = &seriesMovingAverage{Series: s, window: make([]float64, 0), dur: dur, n: -1}
			}
		}
	case float64:
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingAverage(%v,%v)", name, args[1]))
			series[name] = &seriesMovingAverage{Series: s, window: make([]float64, 0), points: int(arg), n: -1}
		}
	}
	return series, nil
}

// movingMedian()
// TODO similar as movingAverage?

type seriesMovingMedian struct {
	Series
	window    []float64
	points, n int
	dur       time.Duration
}

func (f *seriesMovingMedian) Next() bool {
	// if we're given a duration, then the number of points is simply
	// the duration / group by period. this works because we outer
	// join with the time generate_series, and thus never skip a time
	// period
	if f.dur != 0 && f.points == 0 {
		f.points = int((f.dur.Nanoseconds()/1000000)/f.GroupByMs()) + 1 // +1 to avoid div by 0
	}
	// initial build up
	for len(f.window) < f.points {
		if f.Series.Next() {
			f.window = append(f.window, f.Series.CurrentValue())
			f.n++
			// if n starts at -1
			// [a] n:0; [a,b] n:1, [a,b,c] n:2 | exit loop
		} else {
			return false
		}
	}
	// now we have enough points
	if f.Series.Next() {
		f.window[f.n%f.points] = f.Series.CurrentValue()
		f.n++
	} else {
		return false
	}
	return true
}

func (f *seriesMovingMedian) CurrentValue() float64 {
	if len(f.window) == 0 {
		return math.NaN()
	} else {
		cpy := make([]float64, len(f.window))
		copy(cpy, f.window)
		sort.Float64s(cpy)
		middle := len(cpy) / 2
		median := cpy[middle]
		if len(cpy)%2 == 0 {
			median = (median + cpy[middle-1]) / 2
		}
		return median
	}
}

func dslMovingMedian(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	switch arg := args[1].(type) {
	case string:
		if dur, err := parseTimeShift(arg); err != nil {
			return nil, err
		} else {
			for name, s := range series {
				s.Alias(fmt.Sprintf("movingAverage(%v,%v)", name, args[1]))
				series[name] = &seriesMovingMedian{Series: s, window: make([]float64, 0), dur: dur, n: -1}
			}
		}
	case float64:
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingAverage(%v,%v)", name, args[1]))
			series[name] = &seriesMovingMedian{Series: s, window: make([]float64, 0), points: int(arg), n: -1}
		}
	}
	return series, nil
}

// removeAbovePercentile()

type seriesRemoveAbovePercentile struct {
	Series
	n          float64
	percentile float64
}

func (f *seriesRemoveAbovePercentile) CurrentValue() float64 {
	value := f.Series.CurrentValue()
	if value > f.percentile {
		return math.NaN()
	}
	return value
}

func (f *seriesRemoveAbovePercentile) Next() bool {
	if math.IsNaN(f.percentile) {
		// Here we traverse the series, and then it will be traversed
		// again as the datapoints are sent to the client
		series := make([]float64, 0)
		for f.Series.Next() {
			series = append(series, f.Series.CurrentValue())
		}
		f.Series.Close()
		f.percentile = percentile(series, f.n)
	}
	return f.Series.Next() // restart to the first Next()
}

func dslRemoveAbovePercentile(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if n, ok := args[1].(float64); ok {
		n = n / 100
		for name, s := range series {
			s.Alias(fmt.Sprintf("removeAbovePercentile(%v,%v)", name, n*100))
			series[name] = &seriesRemoveAbovePercentile{s, n, math.NaN()}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// removeBelowPercentile()
// TODO similar removeBelowPercentile()

type seriesRemoveBelowPercentile struct {
	Series
	n          float64
	percentile float64
}

func (f *seriesRemoveBelowPercentile) CurrentValue() float64 {
	value := f.Series.CurrentValue()
	if value < f.percentile {
		value = math.NaN()
	}
	return value
}

func (f *seriesRemoveBelowPercentile) Next() bool {
	if math.IsNaN(f.percentile) {
		// Here we traverse the series, and then it will be traversed
		// again as the datapoints are sent to the client
		series := make([]float64, 0)
		for f.Series.Next() {
			series = append(series, f.Series.CurrentValue())
		}
		f.Series.Close()
		f.percentile = percentile(series, f.n)
	}
	return f.Series.Next() // restart to the first Next()
}

func dslRemoveBelowPercentile(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if n, ok := args[1].(float64); ok {
		n = n / 100
		for name, s := range series {
			s.Alias(fmt.Sprintf("removeBelowPercentile(%v,%v)", name, n*100))
			series[name] = &seriesRemoveBelowPercentile{s, n, math.NaN()}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// removeAboveValue()

type seriesRemoveAboveValue struct {
	Series
	threshold float64
}

func (f *seriesRemoveAboveValue) CurrentValue() float64 {
	value := f.Series.CurrentValue()
	if value > f.threshold {
		return math.NaN()
	}
	return value
}

func dslRemoveAboveValue(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if threshold, ok := args[1].(float64); ok {
		// Wrap everything in removeAboveValue
		for name, s := range series {
			s.Alias(fmt.Sprintf("removeAboveValue(%v,%v)", name, threshold))
			series[name] = &seriesRemoveAboveValue{s, threshold}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// removeBelowValue()
// TODO similar to removeAboveValue()

type seriesRemoveBelowValue struct {
	Series
	threshold float64
}

func (f *seriesRemoveBelowValue) CurrentValue() float64 {
	value := f.Series.CurrentValue()
	if value < f.threshold {
		return math.NaN()
	}
	return value
}

func dslRemoveBelowValue(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 2 {
		return nil, fmt.Errorf("Expecting 2 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	if threshold, ok := args[1].(float64); ok {
		// Wrap everything in removeBelowValue
		for name, s := range series {
			s.Alias(fmt.Sprintf("removeBelowValue(%v,%v)", name, threshold))
			series[name] = &seriesRemoveBelowValue{s, threshold}
		}
		return series, nil
	} else {
		return nil, fmt.Errorf("%v not a float", args[1])
	}
}

// stdev

// average of []float64
// TODO Could we make it a method of []float64 type alias?
func avgFloat64(data []float64) float64 {
	var sum float64
	for _, v := range data {
		sum += v
	}
	return sum / float64(len(data))
}

// SD of float64
// TODO Could we make it a method of []float64 type alias?
func stdDevFloat64(data []float64) float64 {
	avg := avgFloat64(data)
	var sum float64
	for _, v := range data {
		sum += math.Pow(v-avg, 2)
	}
	result := math.Sqrt(sum / float64(len(data)-1))
	return result
}

// movingStdDev()
// TODO threshold not yet implemented
type seriesMovingStdDev struct {
	Series
	// avg over n points or time duration for n points, the slice size
	// is the marker
	window    []float64
	points, n int
}

func (f *seriesMovingStdDev) Next() bool {
	// initial build up
	for len(f.window) < f.points {
		if f.Series.Next() {
			f.window = append(f.window, f.Series.CurrentValue())
			f.n++
			// if n starts at -1
			// [a] n:0; [a,b] n:1, [a,b,c] n:2 | exit loop
		} else {
			return false
		}
	}
	// now we have enough points
	if f.Series.Next() {
		f.window[f.n%f.points] = f.Series.CurrentValue()
		f.n++
	} else {
		return false
	}
	return true
}

func (f *seriesMovingStdDev) CurrentValue() float64 {
	if len(f.window) == 0 {
		return math.NaN()
	} else {
		return stdDevFloat64(f.window)
	}
}

func dslMovingStdDev(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) > 3 {
		return nil, fmt.Errorf("Expecting at most 3 arguments, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}
	AlignSeriesMap(series)

	switch arg := args[1].(type) {
	case float64:
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingStdDev(%v,%v)", name, arg))
			series[name] = &seriesMovingStdDev{Series: s, window: make([]float64, 0), points: int(arg), n: -1}
		}
	}
	return series, nil
}

// weightedAverage

type seriesWeightedAverage struct {
	SeriesList
}

func (sl *seriesWeightedAverage) CurrentValue() float64 {
	var (
		productSum float64
		weightSum  float64
	)
	for n, _ := range sl.SeriesSlice {
		if n%2 == 0 {
			avg := sl.SeriesSlice[n].CurrentValue()
			weight := sl.SeriesSlice[n+1].CurrentValue()
			productSum += avg * weight
			weightSum += weight
		}
	}
	if weightSum == 0 || math.IsNaN(weightSum) {
		return math.NaN()
	}
	return productSum / weightSum
}

func dslWeightedAverage(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 3 {
		return nil, fmt.Errorf("Expecting at most 3 arguments, got %d", len(args))
	}

	avgSeries, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	weightSeries, err := dc.seriesFromSeriesOrIdent(args[1])
	if err != nil {
		return nil, err
	}

	if nf, ok := args[2].(float64); !ok {
		return nil, fmt.Errorf("%v not a float", args[2])
	} else {
		n := int(nf)

		avgByPart := make(map[string]Series, 0)
		weightByPart := make(map[string]Series, 0)

		for k, v := range avgSeries {
			parts := strings.Split(k, ".")
			if n >= len(parts) {
				return nil, fmt.Errorf("Element %v our of range for series name %v", n, k)
			}
			avgByPart[parts[n]] = v
		}

		for k, v := range weightSeries {
			parts := strings.Split(k, ".")
			if n >= len(parts) {
				return nil, fmt.Errorf("Element %v our of range for series name %v", n, k)
			}
			weightByPart[parts[n]] = v
		}

		// sort keys
		avgKeys := make([]string, 0, len(avgByPart))
		for k := range avgByPart {
			avgKeys = append(avgKeys, k)
		}
		sort.Strings(avgKeys)

		// make a special SeriesList
		result := &SeriesList{SeriesSlice: make(SeriesSlice, 0)}
		for _, k := range avgKeys {
			w := weightByPart[k]
			if w != nil {
				result.SeriesSlice = append(result.SeriesSlice, avgByPart[k])
				result.SeriesSlice = append(result.SeriesSlice, w)
			}
		}
		result.Align()

		name := fmt.Sprintf("weightedAverage(%s)", argsAsString(args))
		return SeriesMap{name: &seriesWeightedAverage{*result}}, nil
	}
}

// changed()

type seriesChanged struct {
	Series
	last float64
}

func (f *seriesChanged) CurrentValue() float64 {
	if f.Series.CurrentValue() != f.last {
		return 1
	}
	return 0
}

func (f *seriesChanged) Next() bool {
	f.last = f.Series.CurrentValue()
	return f.Series.Next()
}

func dslChanged(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	series, err := dc.seriesFromSeriesOrIdent(args[0])
	if err != nil {
		return nil, err
	}

	// Wrap everything in changed
	for name, s := range series {
		s.Alias(fmt.Sprintf("changed(%s)", name))
		series[name] = &seriesChanged{s, math.NaN()}
	}

	return series, nil
}

// countSeries()

type seriesCountSeries struct {
	SeriesList
	count float64
}

func (f *seriesCountSeries) CurrentValue() float64 {
	return f.count
}

func dslCountSeries(dc *DslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 1 {
		return nil, fmt.Errorf("Expecting 1 argument, got %d", len(args))
	}

	seriesList, err := NewSeriesListFromArgs(dc, args)
	if err != nil {
		return nil, err
	}

	name := fmt.Sprintf("countSeries(%v)", args[0])
	return SeriesMap{name: &seriesCountSeries{*seriesList, float64(len(seriesList.SeriesSlice))}}, nil
}
