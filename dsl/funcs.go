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
	"fmt"
	"log"
	"math"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/tgres/tgres/misc"
	"github.com/tgres/tgres/series"
)

type argType int

const (
	argSeries argType = iota
	argNumber
	argString
	argBool
	argNumberOrSeries // see asPercent() total
)

type argDef struct {
	name string
	tp   argType
	dft  interface{}
}
type dslFuncType struct {
	call   func(map[string]interface{}) (SeriesMap, error)
	varArg bool
	args   []argDef
}
type funcMap map[string]dslFuncType

type dslCtxFuncType func(*dslCtx, []interface{}) (SeriesMap, error)
type dslCtxFuncMap map[string]dslCtxFuncType

var dslCtxFuncs = dslCtxFuncMap{ // functions that require the dslCtx to do their stuff
	"sumSeriesWithWildcards":     dslSumSeriesWithWildcards,
	"averageSeriesWithWildcards": dslAverageSeriesWithWildcards,
	"groupByNode":                dslGroupByNode,
}

var preprocessArgFuncs = funcMap{
	"scale": dslFuncType{dslScale, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"factor", argNumber, nil}}},
	"sinusoid": dslFuncType{dslSinusoid, false, []argDef{}},
	"absolute": dslFuncType{dslAbsolute, false, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"averageSeries": dslFuncType{dslAverageSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"avg": dslFuncType{dslAverageSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"group": dslFuncType{dslGroup, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"isNonNull": dslFuncType{dslIsNonNull, false, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"maxSeries": dslFuncType{dslMaxSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"max": dslFuncType{dslMaxSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"minSeries": dslFuncType{dslMinSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"min": dslFuncType{dslMinSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"sumSeries": dslFuncType{dslSumSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"sum": dslFuncType{dslSumSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"multiplySeries": dslFuncType{dslMultiplySeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"percentileOfSeries": dslFuncType{dslPercentileOfSeries, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil},
		argDef{"interpolate", argBool, "false"}}},
	"rangeOfSeries": dslFuncType{dslRangeOfSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"asPercent": dslFuncType{dslAsPercent, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"total", argNumberOrSeries, math.NaN()}}},
	"alias": dslFuncType{dslAlias, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"newName", argString, nil}}},
	"derivative": dslFuncType{dslDerivative, false, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"nonNegativeDerivative": dslFuncType{dslNonNegativeDerivative, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"maxValue", argNumber, math.NaN()}}},
	"integral": dslFuncType{dslIntegral, false, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"logarithm": dslFuncType{dslLogarithm, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"base", argNumber, 10.0}}},
	"log": dslFuncType{dslLogarithm, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"base", argNumber, 10.0}}},
	"offset": dslFuncType{dslOffset, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"factor", argNumber, nil}}},
	"offsetToZero": dslFuncType{dslOffsetToZero, false, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"timeShift": dslFuncType{dslTimeShift, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"timeShift", argString, nil},
		argDef{"resetEnd", argBool, "true"}}},
	"transformNull": dslFuncType{dslTransformNull, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"default", argNumber, 0.0}}},
	"diffSeries": dslFuncType{dslDiffSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"divideSeries": dslFuncType{dslDivideSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"nPercentile": dslFuncType{dslNPercentile, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"highestCurrent": dslFuncType{dslHighestCurrent, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, 1}}},
	"highestMax": dslFuncType{dslHighestMax, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, 1}}},
	"limit": dslFuncType{dslLimit, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"lowestAverage": dslFuncType{dslLowestAverage, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, 1}}},
	"lowestCurrent": dslFuncType{dslLowestCurrent, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, 1}}},
	"maximumAbove": dslFuncType{dslMaximumAbove, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"maximumBelow": dslFuncType{dslMaximumBelow, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"minimumAbove": dslFuncType{dslMinimumAbove, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"minimumBelow": dslFuncType{dslMinimumBelow, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"mostDeviant": dslFuncType{dslMostDeviant, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"movingAverage": dslFuncType{dslMovingAverage, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"windowSize", argString, nil}}},
	"movingMedian": dslFuncType{dslMovingMedian, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"windowSize", argString, nil}}},
	"removeAbovePercentile": dslFuncType{dslRemoveAbovePercentile, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"removeBelowPercentile": dslFuncType{dslRemoveBelowPercentile, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"removeAboveValue": dslFuncType{dslRemoveAboveValue, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"removeBelowValue": dslFuncType{dslRemoveBelowValue, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"n", argNumber, nil}}},
	"stdev": dslFuncType{dslMovingStdDev, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"points", argNumber, nil},
		argDef{"windowTolerance", argNumber, 0.1}}},
	"weightedAverage": dslFuncType{dslWeightedAverage, false, []argDef{
		argDef{"seriesListAvg", argSeries, nil},
		argDef{"seriesListWeight", argSeries, nil},
		argDef{"node", argNumber, nil}}},
	"aliasByMetric": dslFuncType{dslAliasByMetric, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"aliasByNode": dslFuncType{dslAliasByNode, true, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"nodes", argNumber, nil}}},
	"aliasSub": dslFuncType{dslAliasSub, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"search", argString, nil},
		argDef{"replace", argString, nil}}},
	"changed": dslFuncType{dslChanged, false, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"constantLine": dslFuncType{dslConstantLine, false, []argDef{
		argDef{"value", argNumber, nil}}},
	"countSeries": dslFuncType{dslCountSeries, true, []argDef{
		argDef{"seriesList", argSeries, nil}}},
	"hitcount": dslFuncType{dslHitcount, true, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"intervalString", argString, nil},
		argDef{"alignToInterval", argBool, "false"}}},
	"keepLastValue": dslFuncType{dslKeepLastValue, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"limit", argNumber, 0.0}}},
	"color": dslFuncType{dslColor, true, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"color", argString, "green"}}},
	"exclude": dslFuncType{dslExclude, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"pattern", argString, nil}}},
	"scaleToSeconds": dslFuncType{dslScaleToSeconds, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"seconds", argNumber, nil}}},
	"holtWintersForecast": dslFuncType{dslHoltWintersForecast, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"seasonLen", argString, "1d"},
		argDef{"seasonLimit", argNumber, 7.0}, // most seasons to consider
		argDef{"alpha", argNumber, 0.0},
		argDef{"beta", argNumber, 0.0},
		argDef{"gamma", argNumber, 0.0},
		argDef{"devScale", argNumber, 10.0},
		argDef{"show", argString, "smooth"}}}, // show smooth,conf,aberr
	"holtWintersConfidenceBands": dslFuncType{dslHoltWintersConfidenceBands, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"delta", argNumber, 3.0}}},
	"holtWintersAberration": dslFuncType{dslHoltWintersAberration, false, []argDef{
		argDef{"seriesList", argSeries, nil},
		argDef{"delta", argNumber, 3.0}}},

	// COMBINE
	// ++ averageSeries
	// ++ avg
	// ++ averageSeriesWithWildcards
	// ++ group
	// ++ isNonNull
	// -- mapSeries // returns a list of lists (non-standard)
	// ++ maxSeries
	// ++ minSeries
	// ++ percentileOfSeries
	// ++ rangeOfSeries
	// -- reduceSeries // relies on mapSeries
	// ++ sumSeries
	// ++ sumSeriesWithWildcards
	// ++ averageSeriesWithWildcards
	// ++ multiplySeries

	// TRANSFORM
	// ++ absolute()
	// ++ derivative()
	// ++ hitcount()
	// ++ integral()
	// ++ log()
	// ++ nonNegativeDerivative
	// ++ offset
	// ++ offsetToZero // would require whole series min()
	// -- perSecond // everything here is perSedond() already
	// ++ scale()
	// ++ scaleToSeconds()
	// -- smartSummarize
	// -- summarize // seems complicated
	// ++ timeShift
	// ?? timeStack // TODO?
	// ++ transformNull

	// CALCULATE
	// ++ asPercent
	// ++ diffSeries
	// ++ divideSeries
	// ** holtWintersAberration
	// ** holtWintersConfidenceBands
	// ** holtWintersForecast
	// ++ nPercentile
	// ?? stddevSeries

	// FILTER
	// ?? averageAbove
	// ?? averageBelow
	// ?? currentAbove
	// ?? currentBelow
	// ++ exclude
	// ?? grep
	// ++ highestCurrent
	// ++ highestMax
	// ++ limit
	// ++ lowestAverage
	// ++ lowestCurrent
	// ++ maximumAbove
	// ++ maximumBelow
	// ++ minimumAbove
	// ++ minimumBelow
	// ++ mostDeviant
	// ++ movingAverage
	// ++ movingMedian
	// ++ removeAbovePercentile
	// ++ removeAboveValue
	// ++ removeBelowPercentile
	// ++ removeBelowValue
	// ++ stdev
	// ?? useSeriesAbove // ?
	// ++ weightedAverage

	// SPECIAL
	// ++ alias
	// ++ aliasByMetric
	// ++ aliasByNode
	// ++ aliasSub
	// ?? cactiStyle // TODO should be easy to do?
	// ++ changed
	// ?? consolidateBy // doesn't apply to us, it's always avg?
	// ++ constantLine
	// ++ countSeries
	// -- cumulative // == consolidateBy
	// ?? groupByNode // similar to alias by metric
	// ++ keepLastValue
	// ?? randomWalk // later?
	// ?? sortByMaxima
	// ?? sortByMinima
	// ?? sortByName
	// ?? sortByTotal
	// ?? stacked
	// ?? substr
}

func processArgs(dc *dslCtx, fn *dslFuncType, args []interface{}) (map[string]interface{}, []interface{}, error) {

	result := make(map[string]interface{})
	asSlice := make([]interface{}, 0)

	// Find all the keyword args
	kwargs := make(map[string]string)
	kwargsStart := -1
	for n, arg := range args {
		if s, ok := arg.(string); ok {
			if !strings.Contains(s, "=") {
				if kwargsStart > -1 {
					return nil, nil, fmt.Errorf("Positional values cannot follow keyword parameters: %v", arg)
				}
			} else {
				if kwargsStart == -1 {
					kwargsStart = n
				}
				parts := strings.SplitN(s, "=", 2)
				kwargs[parts[0]] = parts[1]
			}
		}
	}

	// Now we need to be traversing
	for n, fnarg := range fn.args {

		if n >= len(args) {
			if fnarg.dft != nil {
				args = append(args, fnarg.dft)
			} else {
				return nil, nil, fmt.Errorf("Expecting %dth argument, but there are only %d", n+1, len(args))
			}
		}

		// for *arg - keep repeating until the end of args
		var limit int
		if fn.varArg && n == len(fn.args)-1 {
			limit = len(args)
		} else {
			limit = n + 1
		}

		value := make([]interface{}, 0, 1)

		for i := n; i < limit; i++ {

			var (
				arg interface{}
				ok  bool
			)
			if kwargsStart > -1 && i >= kwargsStart {
				if arg, ok = kwargs[fnarg.name]; !ok {
					if fnarg.dft != nil {
						arg = fnarg.dft
					} else {
						return nil, nil, fmt.Errorf("Missing argument: %s", fnarg.name)
					}
				}
			} else {
				arg = args[i]
			}

			switch fnarg.tp {
			case argSeries:
				if series, err := dc.seriesFromSeriesOrIdent(arg); err != nil {
					return nil, nil, err
				} else {
					value = append(value, series)
				}
			case argNumber:
				if number, ok := arg.(float64); ok {
					value = append(value, number)
				} else if v, ok := arg.(string); ok {
					if number, err := strconv.ParseFloat(v, 64); err == nil {
						value = append(value, number)
					} else {
						return nil, nil, fmt.Errorf("argument %d (%s=%s) parsing error: %v", i+1, fnarg.name, v, err)
					}
				} else {
					return nil, nil, fmt.Errorf("argument %d (%q) expecting a number, got: %v", i+1, fnarg.name, arg)
				}
			case argString:
				if str, ok := arg.(string); ok {
					value = append(value, str)
				} else {
					value = append(value, fmt.Sprintf("%v", arg)) // anything can be a string
				}
			case argBool:
				if str, ok := arg.(string); ok {
					str := strings.ToLower(str)
					if str == "true" {
						value = append(value, true)
					} else if str == "false" {
						value = append(value, false)
					} else {
						return nil, nil, fmt.Errorf("argument %d (%q) invalid boolean, expecting true or false, got: %v", i+1, fnarg.name, arg)
					}
				} else {
					return nil, nil, fmt.Errorf("argument %d (%q) invalid boolean, expecting true or false, got: %v", i+1, fnarg.name, arg)
				}
			case argNumberOrSeries:
				if number, ok := arg.(float64); ok {
					value = append(value, number)
				} else if str, ok := arg.(string); ok {
					if number, err := strconv.ParseFloat(str, 64); err == nil { // is it a kw arg float?
						value = append(value, number)
					} else if str == "None" || str == "NaN" {
						value = append(value, math.NaN())
					} else if series, err := dc.seriesFromSeriesOrIdent(str); err == nil {
						if len(series) == 0 {
							return nil, nil, fmt.Errorf("argument %d (%q) no such series: %v", i+1, fnarg.name, arg)
						} else {
							value = append(value, series)
						}
					} else {
						return nil, nil, fmt.Errorf("argument %d (%q) expecting number or series, but got: %v", i+1, fnarg.name, arg)
					}
				}
			default:
				return nil, nil, fmt.Errorf("Invalid argType: %v", fnarg.tp)
			}
		}

		// we either have a single value or a slice (if this is an *arg)
		if fn.varArg && n == len(fn.args)-1 {
			// *seriesList is a special case - combine them into single SeriesMap
			if fnarg.tp == argSeries {
				combined := make(SeriesMap)
				for _, val := range value {
					series, err := dc.seriesFromSeriesOrIdent(val)
					if err != nil {
						return nil, nil, err
					}
					for n, s := range series {
						combined[n] = s
					}
				}
				combined.toAliasSeriesSlice().Align()
				result[fnarg.name] = combined
			} else {
				result[fnarg.name] = value
			}
			for _, v := range value {
				asSlice = append(asSlice, v)
			}
		} else {
			result[fnarg.name] = value[0]
			asSlice = append(asSlice, value[0])
		}
	}

	return result, asSlice, nil
}

func callPreprocessArgFunc(dc *dslCtx, name string, argFunc *dslFuncType,
	args []interface{}, argMap map[string]interface{}, argSlice []interface{}) (SeriesMap, error) {

	argMap["_legend_"] = fmt.Sprintf("%s(%s)", name, argsAsString(args)) // only a suggestion
	argMap["_args_"] = argSlice
	argMap["_from_"] = dc.from
	argMap["_to_"] = dc.to
	argMap["_maxPoints_"] = dc.maxPoints
	if series, err := argFunc.call(argMap); err == nil {
		return series, nil
	} else {
		return nil, fmt.Errorf("%v() reports an error: %v", name, err)
	}
}

func seriesFromFunction(dc *dslCtx, name string, args []interface{}) (SeriesMap, error) {

	argFunc, ok := preprocessArgFuncs[name]
	if ok {
		argMap, argSlice, err := processArgs(dc, &argFunc, args)
		if err != nil {
			return nil, fmt.Errorf("%v() reports an error: %v", name, err)
		}
		return callPreprocessArgFunc(dc, name, &argFunc, args, argMap, argSlice)
	} else {
		// Try a dslCtxFunc
		if dslCtxFunc, ok := dslCtxFuncs[name]; !ok {
			return nil, fmt.Errorf("No such function: %v", name)
		} else {
			if series, err := dslCtxFunc(dc, args); err == nil {
				return series, nil
			} else {
				return nil, fmt.Errorf("%v() reports an error: %v", name, err)
			}
		}
	}
}

func argsAsString(args []interface{}) string {
	sargs := make([]string, 0, len(args))
	for _, arg := range args {
		sargs = append(sargs, fmt.Sprintf("%v", arg))
	}
	return strings.Join(sargs, ",")
}

// ------------ functions ----------

// maxSeries()

type seriesMaxSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesMaxSeries) CurrentValue() float64 {
	return sl.Max()
}

func dslMaxSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesMaxSeries{series}}, nil
}

// minSeries()

type seriesMinSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesMinSeries) CurrentValue() float64 {
	return sl.Min()
}

func dslMinSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesMinSeries{series}}, nil
}

// sumSeries()

type seriesSumSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesSumSeries) CurrentValue() float64 {
	return sl.Sum()
}

func dslSumSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesSumSeries{series}}, nil
}

// multiplySeries()

type seriesMultiplySeries struct {
	*aliasSeriesSlice
}

func (sl *seriesMultiplySeries) CurrentValue() float64 {
	return sl.Prod()
}

func dslMultiplySeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesMultiplySeries{series}}, nil
}

// diffSeries()

type seriesDiffSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesDiffSeries) CurrentValue() float64 {
	return sl.Diff()
}

func dslDiffSeries(args map[string]interface{}) (SeriesMap, error) {
	// We must use _args_ to preserve the order
	argsAsSlice := args["_args_"].([]interface{})
	sl := &aliasSeriesSlice{}
	n := 0
	for _, arg := range argsAsSlice {
		if ss, ok := arg.(SeriesMap); ok {
			for _, s := range ss.toAliasSeriesSlice().SeriesSlice {
				sl.SeriesSlice = append(sl.SeriesSlice, s)
				n++
			}
		} else {
			return nil, fmt.Errorf("invlalid series: %v", arg)
		}
		if n > 1 {
			break
		}
	}
	if n < 2 {
		return nil, fmt.Errorf("diffSeries requires two series, got only %d", n)
	}
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesDiffSeries{sl}}, nil
}

// divideSeries()

type seriesDivideSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesDivideSeries) CurrentValue() float64 {
	return sl.SeriesSlice[0].CurrentValue() / sl.SeriesSlice[1].CurrentValue()
}

func dslDivideSeries(args map[string]interface{}) (SeriesMap, error) {
	// We must use _args_ to preserve the order
	argsAsSlice := args["_args_"].([]interface{})
	sl := &aliasSeriesSlice{}
	n := 0
	for _, arg := range argsAsSlice {
		if ss, ok := arg.(SeriesMap); ok {
			for _, s := range ss.toAliasSeriesSlice().SeriesSlice {
				sl.SeriesSlice = append(sl.SeriesSlice, s)
				n++
			}
		} else {
			return nil, fmt.Errorf("invlalid series: %v", arg)
		}
		if n > 2 {
			return nil, fmt.Errorf("divideSeries requires exactly two series, got %d", n)
		}
	}
	if n < 2 {
		return nil, fmt.Errorf("divideSeries requires two series, got only %d", n)
	}
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesDivideSeries{sl}}, nil
}

// sumSeriesWithWildcards()
//
// Seems inqdeuqtely documented (or I'm thick). What it does is
// replace the part at pos (there can be multiple pos) with a '*' and
// run Sum on that, and then it assigns it an alias where the element
// at pos is removed entirely, so you actually get bogus series names
// as aliases.
func dslSumSeriesWithWildcards(dc *dslCtx, args []interface{}) (SeriesMap, error) {

	specs, err := processSeriesWithWildcards(dc, args)
	if err != nil {
		return nil, err
	}

	result := make(SeriesMap, len(specs))
	for spec, alias := range specs {
		series, err := dc.seriesFromSeriesOrIdent(spec)
		if err != nil {
			return nil, err
		}
		ss := series.toAliasSeriesSlice()
		ss.Align()
		result[alias] = &seriesSumSeries{ss}
	}

	return result, nil
}

// averageSeriesWithWildcards
// same as sum pretty much
func dslAverageSeriesWithWildcards(dc *dslCtx, args []interface{}) (SeriesMap, error) {

	specs, err := processSeriesWithWildcards(dc, args)
	if err != nil {
		return nil, err
	}

	result := make(SeriesMap, len(specs))
	for spec, alias := range specs {
		series, err := dc.seriesFromSeriesOrIdent(spec)
		if err != nil {
			return nil, err
		}
		ss := series.toAliasSeriesSlice()
		ss.Align()
		result[alias] = &seriesAverageSeries{ss}
	}

	return result, nil
}

// Return a map of new aliases by spec
func processSeriesWithWildcards(dc *dslCtx, args []interface{}) (map[string]string, error) {
	if len(args) < 2 {
		return nil, fmt.Errorf("Expecting at least 2 arguments, got %d", len(args))
	}

	var (
		smap SeriesMap
		err  error
	)

	sspec, ok := args[0].(string)
	if !ok {
		// but it could be a SeriesMap
		smap, ok = args[0].(SeriesMap)
		if !ok {
			return nil, fmt.Errorf("%v is not a string or a SeriesMap", args[0])
		}
	}

	var poss []int
	for _, arg := range args[1:] {
		i, ok := arg.(float64)
		if !ok {
			return nil, fmt.Errorf("invalid position: %v", arg)
		}
		poss = append(poss, int(i))
	}

	// First we need a complete list of series (unless it was already given to use)
	if len(smap) == 0 {
		smap, err = dc.seriesFromSeriesOrIdent(sspec)
		if err != nil {
			return nil, err
		}
	}

	// Then we group them by the new spec
	specs := make(map[string]string)
	for name, _ := range smap {
		// given sswwc(a.b.c.d, 1, 2):
		// spec  => a.*.*.d
		// alias => a.d
		parts := strings.Split(name, ".")
		for _, pos := range poss {
			if len(parts) > pos {
				parts[pos] = "*"
			}
		}
		spec := strings.Join(parts, ".")
		i := 0 // count removals
		for _, pos := range poss {
			if len(parts) > pos {
				parts = append(parts[:pos-i], parts[pos+1-i:]...) // remove element
				i++
			}
		}
		alias := strings.Join(parts, ".") // NB: bogus series name

		specs[spec] = alias
	}

	return specs, nil
}

// groupByNode
func dslGroupByNode(dc *dslCtx, args []interface{}) (SeriesMap, error) {

	if len(args) != 3 {
		return nil, fmt.Errorf("Expecting 3 arguments, got %d", len(args))
	}

	var (
		smap SeriesMap
		err  error
	)
	sspec, ok := args[0].(string)
	if !ok {
		// but it could be a SeriesMap
		smap, ok = args[0].(SeriesMap)
		if !ok {
			return nil, fmt.Errorf("first arg %v is not a string or a SeriesMap", args[0])
		}
	}

	fnode, ok := args[1].(float64)
	if !ok {
		return nil, fmt.Errorf("second arg %v is not a number", args[1])
	}
	pos := int(fnode)

	funcName, ok := args[2].(string)
	if !ok {
		return nil, fmt.Errorf("third arg %v is not a string", args[2])
	}

	// Check that the function is valid
	fdef, ok := preprocessArgFuncs[funcName]
	if !ok {
		return nil, fmt.Errorf("%v is not a function we know", args[2])
	}

	// Check that the func is suitable
	if len(fdef.args) != 1 || len(fdef.args) == 0 || fdef.args[0].tp != argSeries {
		return nil, fmt.Errorf("%v is not suitable for callback", args[2])
	}

	// First we need a complete list of series
	if len(smap) == 0 {
		smap, err = dc.seriesFromSeriesOrIdent(sspec)
		if err != nil {
			return nil, err
		}
	}

	groups := make(map[string]SeriesMap)
	for name, s := range smap {
		// a.b.c.d, 2 => c: a.b.c.d
		parts := strings.Split(name, ".")
		if pos >= len(parts) {
			continue // ignore
		}
		group := parts[pos]

		if groups[group] == nil {
			groups[group] = make(SeriesMap)
		}
		groups[group][name] = s
	}

	result := make(SeriesMap)
	for alias, group := range groups {
		argsMap := map[string]interface{}{fdef.args[0].name: group}
		smap, err := callPreprocessArgFunc(dc, funcName, &fdef, nil, argsMap, nil)
		if err != nil {
			return nil, fmt.Errorf("error in callPreprocessArgFunc: %v", err)
		}
		for _, s := range smap {
			// we're expecting the func to return a single thing, or else this
			// probably wold not work...
			result[alias] = s
		}
	}

	return result, nil
}

// percentileOfSeries()
// TODO the interpolate argument is ignored for now

type seriesPercentileOfSeries struct {
	*aliasSeriesSlice
	qtile float64
}

func (sl *seriesPercentileOfSeries) CurrentValue() float64 {
	return sl.Quantile(sl.qtile)
}

func dslPercentileOfSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	qtile := args["n"].(float64) / 100
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesPercentileOfSeries{series, qtile}}, nil
}

// rangeOfSeries()

type seriesRangeOfSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesRangeOfSeries) CurrentValue() float64 {
	return sl.Range()
}

func dslRangeOfSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesRangeOfSeries{series}}, nil
}

// averageSeries()

type seriesAverageSeries struct {
	*aliasSeriesSlice
}

func (sl *seriesAverageSeries) CurrentValue() float64 {
	return sl.Avg()
}

func dslAverageSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesAverageSeries{series}}, nil
}

// group()

func dslGroup(args map[string]interface{}) (SeriesMap, error) {
	return args["seriesList"].(SeriesMap), nil
}

// color()

func dslColor(args map[string]interface{}) (SeriesMap, error) {
	return args["seriesList"].(SeriesMap), nil
}

// alias()

func dslAlias(args map[string]interface{}) (SeriesMap, error) {
	result := args["seriesList"].(SeriesMap)
	alias := args["newName"].(string)
	for _, series := range result {
		series.Alias(alias)
	}
	return result, nil
}

// aliasByMetric()

func dslAliasByMetric(args map[string]interface{}) (SeriesMap, error) {
	result := args["seriesList"].(SeriesMap)
	for name, series := range result {
		parts := strings.Split(name, ".")
		series.Alias(parts[len(parts)-1])
	}
	return result, nil
}

// aliasByNode()
func dslAliasByNode(args map[string]interface{}) (SeriesMap, error) {
	result := args["seriesList"].(SeriesMap)
	nodes := args["nodes"].([]interface{})
	for name, series := range result {
		// very crude fix for a case of sum(a.b.c.*.d)
		if strings.Contains(name, "(") {
			parts := strings.Split(name, "(")
			name = strings.Replace(strings.Join(parts[1:], ""), ")", "", 1)
		}
		parts := strings.Split(name, ".")
		var alias_parts []string
		for _, num := range nodes {
			n := int(num.(float64))
			if n < 0 {
				n = len(parts) + n
			}
			if n >= len(parts) || n < 0 {
				return nil, fmt.Errorf("node index %v out of range for number of nodes: %v", int(num.(float64)), len(parts))
			}
			alias_parts = append(alias_parts, parts[n])
		}
		series.Alias(strings.Join(alias_parts, "."))
	}
	return result, nil
}

// aliasSub()
// TODO regex groups don't work yet (they do with "$1" syntax, but not
// graphite's "\1" syntax)

func dslAliasSub(args map[string]interface{}) (SeriesMap, error) {
	result := args["seriesList"].(SeriesMap)
	search := args["search"].(string)
	replace := args["replace"].(string)

	// convert graphite groups to go: \3 => $3
	groups := regexp.MustCompile(`\\\\([0-9]+)`)
	replace = groups.ReplaceAllString(replace, "$$$1")

	reg, err := regexp.Compile(search)
	if err != nil {
		return nil, err
	}
	for name, series := range result {
		series.Alias(reg.ReplaceAllString(name, replace))
	}
	return result, nil
}

// asPercent()

type seriesAsPercent struct {
	*aliasSeriesSlice
	my_idx      int
	total       float64
	totalSeries *aliasSeriesSlice
}

func (sl *seriesAsPercent) Next() bool {
	if sl.totalSeries != nil {
		sl.totalSeries.Next()
	}
	return sl.SeriesSlice.Next()
}

func (sl *seriesAsPercent) CurrentValue() float64 {
	if sl.totalSeries != nil {
		return sl.SeriesSlice[sl.my_idx].CurrentValue() / sl.totalSeries.Sum() * 100
	} else if math.IsNaN(sl.total) {
		return sl.SeriesSlice[sl.my_idx].CurrentValue() / sl.Sum() * 100
	} else {
		return sl.SeriesSlice[sl.my_idx].CurrentValue() / sl.total * 100
	}
}

func dslAsPercent(args map[string]interface{}) (SeriesMap, error) {

	var (
		total   float64 = math.NaN()
		totSl   *aliasSeriesSlice
		totName string
	)

	switch t := args["total"].(type) {
	case float64:
		total = t
	case SeriesMap:
		totSl = t.toAliasSeriesSlice()
		// This is a hack (what if there is more than one series), but
		// we need some kind of a name
		totName = t.SortedKeys()[0]
	}

	// Wrap in seriesAsPercent AND build a SeriesSlice so we can do Sum
	// The series needs to know its index in the SeriesSlice
	result := args["seriesList"].(SeriesMap)
	sl := &aliasSeriesSlice{}
	for _, key := range result.SortedKeys() {
		sl.SeriesSlice = append(sl.SeriesSlice, result[key])
	}
	n := 0
	for _, name := range result.SortedKeys() {
		if math.IsNaN(total) && totSl == nil {
			sl.Alias(fmt.Sprintf("asPersent(%s)", name))
		} else if totSl != nil {
			sl.Alias(fmt.Sprintf("asPersent(%s,%v)", name, totName))
		} else {
			sl.Alias(fmt.Sprintf("asPersent(%s,%v)", name, total))
		}
		result[name] = &seriesAsPercent{sl, n, total, totSl}
		n++
	}

	return result, nil
}

// isNonNull

type seriesIsNonNull struct {
	*aliasSeriesSlice
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

func dslIsNonNull(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesIsNonNull{series}}, nil
}

// absolute()

type seriesAbsolute struct {
	AliasSeries
}

func (f *seriesAbsolute) CurrentValue() float64 {
	return math.Abs(f.AliasSeries.CurrentValue())
}

func dslAbsolute(args map[string]interface{}) (SeriesMap, error) {

	series := args["seriesList"].(SeriesMap)
	for name, s := range series {
		s.Alias(fmt.Sprintf("absolute(%s)", name))
		series[name] = &seriesAbsolute{s}
	}

	return series, nil
}

// scale()

type seriesScale struct {
	AliasSeries
	factor float64
}

func (f *seriesScale) CurrentValue() float64 {
	return f.AliasSeries.CurrentValue() * f.factor
}

func dslScale(args map[string]interface{}) (SeriesMap, error) {

	series := args["seriesList"].(SeriesMap)
	factor := args["factor"].(float64)

	// Wrap everything in scale
	for name, s := range series {
		s.Alias(fmt.Sprintf("scale(%v,%v)", name, factor))
		series[name] = &seriesScale{s, factor}
	}
	return series, nil
}

// sinusoid()

type seriesSinusoid struct {
	AliasSeries
}

func dslSinusoid(args map[string]interface{}) (SeriesMap, error) {

	from := args["_from_"].(time.Time)
	to := args["_to_"].(time.Time)
	maxPoints := args["_maxPoints_"].(int64)

	span := to.Sub(from)
	step := span / time.Duration(maxPoints)

	from = from.Add(step) // because _from_ is end of slot, not beginning

	var dps []float64
	for i := 0; i < int(maxPoints); i++ {
		dps = append(dps, math.Sin(2*math.Pi/float64(maxPoints)*float64(i)))
	}

	ss := series.NewSliceSeries(dps, from, step)
	ss.Alias("sinusoid()")

	return SeriesMap{"sinusoid()": ss}, nil
}

// derivative()

type seriesDerivative struct {
	AliasSeries
	last float64
}

func (f *seriesDerivative) CurrentValue() float64 {
	return f.AliasSeries.CurrentValue() - f.last
}

func (f *seriesDerivative) Next() bool {
	f.last = f.AliasSeries.CurrentValue()
	return f.AliasSeries.Next()
}

func dslDerivative(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	for name, s := range series {
		s.Alias(fmt.Sprintf("derivative(%s)", name))
		series[name] = &seriesDerivative{s, math.NaN()}
	}
	return series, nil
}

// integral()

type seriesIntegral struct {
	AliasSeries
	total float64
}

func (f *seriesIntegral) CurrentValue() float64 {
	return f.total
}

func (f *seriesIntegral) Next() bool {
	value := f.AliasSeries.CurrentValue()
	if !math.IsNaN(value) {
		f.total += value
	}
	return f.AliasSeries.Next()
}

func dslIntegral(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	for name, s := range series {
		s.Alias(fmt.Sprintf("integral(%s)", name))
		series[name] = &seriesIntegral{s, 0}
	}
	return series, nil
}

// logarithm()

type seriesLogarithm struct {
	AliasSeries
	base float64
}

func (f *seriesLogarithm) CurrentValue() float64 {
	return math.Log(f.AliasSeries.CurrentValue()) / math.Log(f.base)
}

func dslLogarithm(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	base := args["base"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("logarithm(%v,%v)", name, base))
		series[name] = &seriesLogarithm{s, base}
	}
	return series, nil
}

// nonNegativeDerivative()
type seriesNonNegativeDerivative struct {
	AliasSeries
	last     float64
	maxValue float64
}

func (f *seriesNonNegativeDerivative) CurrentValue() float64 {
	current := f.AliasSeries.CurrentValue()
	diff := current - f.last
	if diff > 0 {
		return diff
	} else if !math.IsNaN(f.maxValue) && f.maxValue > current {
		return (f.maxValue - f.last) + current + 1
	}
	return math.NaN()
}

func (f *seriesNonNegativeDerivative) Next() bool {
	f.last = f.AliasSeries.CurrentValue()
	return f.AliasSeries.Next()
}

func dslNonNegativeDerivative(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	maxValue := args["maxValue"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("nonNegativeDerivative(%s)", name))
		series[name] = &seriesNonNegativeDerivative{s, math.NaN(), maxValue}
	}
	return series, nil
}

// offset()

type seriesOffset struct {
	AliasSeries
	offset float64
}

func (f *seriesOffset) CurrentValue() float64 {
	return f.AliasSeries.CurrentValue() + f.offset
}

func dslOffset(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	offset := args["factor"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("offset(%v,%v)", name, offset))
		series[name] = &seriesOffset{s, offset}
	}
	return series, nil
}

// offsetToZero()

type seriesOffsetToZero struct {
	AliasSeries
	offset float64
}

func (f *seriesOffsetToZero) Next() bool {
	if math.IsNaN(f.offset) {
		summary := &aliasSummarySeries{SummarySeries: &series.SummarySeries{f.AliasSeries}}
		f.offset = summary.Min()
	}
	return f.AliasSeries.Next()
}

func (f *seriesOffsetToZero) CurrentValue() float64 {
	return f.AliasSeries.CurrentValue() - f.offset
}

func dslOffsetToZero(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	for name, s := range series {
		s.Alias(fmt.Sprintf("offsetToZero(%v)", name))
		series[name] = &seriesOffsetToZero{s, math.NaN()}
	}
	return series, nil
}

// timeShift()
// We're not implementing resetEnd - it doesn't make much sense if
// we're not actually generating graphs.

type seriesTimeShift struct {
	AliasSeries
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
	if dur, err := misc.BetterParseDuration(sansSign); err == nil {
		if s[0] == '-' {
			return dur * -1, nil
		} else {
			return dur, nil
		}
	} else {
		return 0, fmt.Errorf("parseTimeShift(): Error parsing duration %q: %v", s, err)
	}
}

func (f *seriesTimeShift) CurrentTime() time.Time {
	return f.AliasSeries.CurrentTime().Add(f.timeShift)
}

func dslTimeShift(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	ts := args["timeShift"].(string)

	shift, err := parseTimeShift(ts)
	if err != nil {
		return nil, err
	}

	for name, s := range series {
		s.Alias(fmt.Sprintf("timeShift(%v,%v)", name, ts))
		series[name] = &seriesTimeShift{s, shift}
	}
	return series, nil
}

// transformNull()

type seriesTransformNull struct {
	AliasSeries
	dft float64
}

func (f *seriesTransformNull) CurrentValue() float64 {
	value := f.AliasSeries.CurrentValue()
	if math.IsNaN(value) {
		return f.dft
	}
	return value
}

func dslTransformNull(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	dft := args["default"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("transformNull(%v,%v)", name, dft))
		series[name] = &seriesTransformNull{s, dft}
	}
	return series, nil
}

// nPercentile()

type seriesNPercentile struct {
	AliasSeries
	n     float64
	qtile float64
}

func (f *seriesNPercentile) CurrentValue() float64 {
	return f.qtile
}

func (f *seriesNPercentile) Next() bool {
	if math.IsNaN(f.qtile) {
		// We traverse the whole series, and then it will be traversed
		// again as the datapoints are sent to the client
		s := make([]float64, 0)
		for f.AliasSeries.Next() {
			s = append(s, f.AliasSeries.CurrentValue())
		}
		f.AliasSeries.Close()
		f.qtile = series.Quantile(s, f.n)
	}
	return f.AliasSeries.Next() // restart to the first Next()
}

func dslNPercentile(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	n = n / 100
	for name, s := range series {
		s.Alias(fmt.Sprintf("nPercentile(%v,%v)", name, n*100))
		series[name] = &seriesNPercentile{s, n, math.NaN()}
	}
	return series, nil
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

func dslHighestCurrent(args map[string]interface{}) (SeriesMap, error) {
	ss := args["seriesList"].(SeriesMap)
	n := int(args["n"].(float64))
	lasts := make(map[string]float64)
	for name, s := range ss {
		s.Alias(fmt.Sprintf("highestCurrent(%v,%v)", name, n))
		shc := newAliasSummarySeries(s)
		lasts[name] = shc.Last()
		ss[name] = shc
	}
	sortedLasts := sortedKeys(lasts)
	for i := 0; i < len(sortedLasts)-n; i++ {
		delete(ss, sortedLasts[i])
	}
	return ss, nil
}

// highestMax()

func dslHighestMax(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := int(args["n"].(float64))
	lasts := make(map[string]float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("highestMax(%v,%v)", name, n))
		shm := newAliasSummarySeries(s)
		lasts[name] = shm.Max()
		series[name] = shm
	}
	sortedLasts := sortedKeys(lasts)
	for i := 0; i < len(sortedLasts)-n; i++ {
		delete(series, sortedLasts[i])
	}
	return series, nil
}

// limit()

func dslLimit(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := int(args["n"].(float64))
	result := make(SeriesMap)
	for i, name := range series.SortedKeys() {
		if i >= n {
			break
		}
		result[name] = series[name]
	}
	return result, nil
}

// lowestAverage()

func dslLowestAverage(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := int(args["n"].(float64))
	avgs := make(map[string]float64)
	for name, s := range series {
		ss := newAliasSummarySeries(s)
		avgs[name] = ss.Avg()
		series[name] = ss
	}
	sortedAvgs := sortedKeys(avgs)
	for i := len(sortedAvgs) - 1; i >= n; i-- {
		delete(series, sortedAvgs[i])
	}
	return series, nil
}

// lowestCurrent()

func dslLowestCurrent(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := int(args["n"].(float64))
	lasts := make(map[string]float64)
	for name, s := range series {
		shc := newAliasSummarySeries(s)
		lasts[name] = shc.Last()
		series[name] = shc
	}
	sortedLasts := sortedKeys(lasts)
	for i := len(sortedLasts) - 1; i >= n; i-- {
		delete(series, sortedLasts[i])
	}
	return series, nil
}

// maximumAbove()

func dslMaximumAbove(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	for name, s := range series {
		shm := newAliasSummarySeries(s)
		if shm.Max() <= n {
			delete(series, name)
		}
	}
	return series, nil
}

// maximumBelow()

func dslMaximumBelow(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	for name, s := range series {
		shm := newAliasSummarySeries(s)
		if shm.Max() >= n {
			delete(series, name)
		}
	}
	return series, nil
}

// minimumAbove()

func dslMinimumAbove(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	for name, s := range series {
		shm := newAliasSummarySeries(s)
		if shm.Min() <= n {
			delete(series, name)
		}
	}
	return series, nil
}

// minimumBelow()

func dslMinimumBelow(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	for name, s := range series {
		shm := newAliasSummarySeries(s)
		if shm.Min() >= n {
			delete(series, name)
		}
	}
	return series, nil
}

// mostDeviant()

func dslMostDeviant(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := int(args["n"].(float64))
	stddevs := make(map[string]float64)
	for name, s := range series {
		shm := newAliasSummarySeries(s)
		stddev := shm.StdDev(shm.Avg())
		stddevs[name] = stddev
		series[name] = shm
	}
	sortedStdDevs := sortedKeys(stddevs)
	for i := 0; i < len(sortedStdDevs)-n; i++ {
		delete(series, sortedStdDevs[i])
	}
	return series, nil
}

// movingAverage()

type seriesMovingAverage struct {
	AliasSeries
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
		f.points = int(f.dur / (f.GroupBy() + 1)) // +1 to avoid div by 0
	}
	// initial build up
	for len(f.window) < f.points {
		if f.AliasSeries.Next() {
			f.window = append(f.window, f.AliasSeries.CurrentValue())
			f.n++
			// if n starts at -1
			// [a] n:0; [a,b] n:1, [a,b,c] n:2 | exit loop
		} else {
			return false
		}
	}
	// now we have enough points
	if f.AliasSeries.Next() {
		f.window[f.n%f.points] = f.AliasSeries.CurrentValue()
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

func dslMovingAverage(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	window := args["windowSize"].(string)
	if dur, err := parseTimeShift(window); err == nil {
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingAverage(%v,%v)", name, window))
			series[name] = &seriesMovingAverage{AliasSeries: s, window: make([]float64, 0), dur: dur, n: -1}
		}
	} else if points, err := strconv.ParseInt(window, 10, 64); err == nil {
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingAverage(%v,%v)", name, points))
			series[name] = &seriesMovingAverage{AliasSeries: s, window: make([]float64, 0), points: int(points), n: -1}
		}
	} else {
		return nil, fmt.Errorf("invalid window size: %v", window)
	}
	return series, nil
}

// movingMedian()
// TODO similar as movingAverage?

type seriesMovingMedian struct {
	AliasSeries
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
		f.points = int(f.dur/f.GroupBy()) + 1 // +1 to avoid div by 0
	}
	// initial build up
	for len(f.window) < f.points {
		if f.AliasSeries.Next() {
			f.window = append(f.window, f.AliasSeries.CurrentValue())
			f.n++
			// if n starts at -1
			// [a] n:0; [a,b] n:1, [a,b,c] n:2 | exit loop
		} else {
			return false
		}
	}
	// now we have enough points
	if f.AliasSeries.Next() {
		f.window[f.n%f.points] = f.AliasSeries.CurrentValue()
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

func dslMovingMedian(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	window := args["windowSize"].(string)
	if dur, err := parseTimeShift(window); err == nil {
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingMedian(%v,%v)", name, window))
			series[name] = &seriesMovingMedian{AliasSeries: s, window: make([]float64, 0), dur: dur, n: -1}
		}
	} else if points, err := strconv.ParseInt(window, 10, 64); err == nil {
		for name, s := range series {
			s.Alias(fmt.Sprintf("movingMedian(%v,%v)", name, points))
			series[name] = &seriesMovingMedian{AliasSeries: s, window: make([]float64, 0), points: int(points), n: -1}
		}
	} else {
		return nil, fmt.Errorf("invalid window size: %v", window)
	}
	return series, nil
}

// removeAbovePercentile()

type seriesRemoveAbovePercentile struct {
	AliasSeries
	n        float64
	qtile    float64
	computed bool
}

func (f *seriesRemoveAbovePercentile) CurrentValue() float64 {
	value := f.AliasSeries.CurrentValue()
	if value > f.qtile {
		return math.NaN()
	}
	return value
}

func (f *seriesRemoveAbovePercentile) Next() bool {
	if !f.computed {
		// Here we traverse the series, and then it will be traversed
		// again as the datapoints are sent to the client
		s := make([]float64, 0)
		for f.AliasSeries.Next() {
			s = append(s, f.AliasSeries.CurrentValue())
		}
		f.AliasSeries.Close()
		f.qtile = series.Quantile(s, f.n)
		f.computed = true
	}
	return f.AliasSeries.Next() // restart to the first Next()
}

func dslRemoveAbovePercentile(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64) / 100
	for name, s := range series {
		s.Alias(fmt.Sprintf("removeAbovePercentile(%v,%v)", name, n*100))
		series[name] = &seriesRemoveAbovePercentile{s, n, math.NaN(), false}
	}
	return series, nil
}

// removeBelowPercentile()
// TODO similar to removeBelowPercentile()

type seriesRemoveBelowPercentile struct {
	AliasSeries
	n        float64
	qtile    float64
	computed bool
}

func (f *seriesRemoveBelowPercentile) CurrentValue() float64 {
	value := f.AliasSeries.CurrentValue()
	if value < f.qtile {
		value = math.NaN()
	}
	return value
}

func (f *seriesRemoveBelowPercentile) Next() bool {
	if !f.computed {
		// Here we traverse the series, and then it will be traversed
		// again as the datapoints are sent to the client
		s := make([]float64, 0)
		for f.AliasSeries.Next() {
			s = append(s, f.AliasSeries.CurrentValue())
		}
		f.AliasSeries.Close()
		f.qtile = series.Quantile(s, f.n)
		f.computed = true
	}
	return f.AliasSeries.Next() // restart to the first Next()
}

func dslRemoveBelowPercentile(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64) / 100
	for name, s := range series {
		s.Alias(fmt.Sprintf("removeBelowPercentile(%v,%v)", name, n*100))
		series[name] = &seriesRemoveBelowPercentile{s, n, math.NaN(), false}
	}
	return series, nil
}

// removeAboveValue()

type seriesRemoveAboveValue struct {
	AliasSeries
	n float64
}

func (f *seriesRemoveAboveValue) CurrentValue() float64 {
	value := f.AliasSeries.CurrentValue()
	if value > f.n {
		return math.NaN()
	}
	return value
}

func dslRemoveAboveValue(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("removeAboveValue(%v,%v)", name, n))
		series[name] = &seriesRemoveAboveValue{s, n}
	}
	return series, nil
}

// removeBelowValue()
// TODO similar to removeAboveValue()

type seriesRemoveBelowValue struct {
	AliasSeries
	n float64
}

func (f *seriesRemoveBelowValue) CurrentValue() float64 {
	value := f.AliasSeries.CurrentValue()
	if value < f.n {
		return math.NaN()
	}
	return value
}

func dslRemoveBelowValue(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	n := args["n"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("removeBelowValue(%v,%v)", name, n))
		series[name] = &seriesRemoveBelowValue{s, n}
	}
	return series, nil
}

// stdev
// TODO implement windowTolerance ?
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
	AliasSeries
	// avg over n points or time duration for n points, the slice size
	// is the marker
	window    []float64
	points, n int
}

func (f *seriesMovingStdDev) Next() bool {
	// initial build up
	for len(f.window) < f.points {
		if f.AliasSeries.Next() {
			f.window = append(f.window, f.AliasSeries.CurrentValue())
			f.n++
			// if n starts at -1
			// [a] n:0; [a,b] n:1, [a,b,c] n:2 | exit loop
		} else {
			return false
		}
	}
	// now we have enough points
	if f.AliasSeries.Next() {
		f.window[f.n%f.points] = f.AliasSeries.CurrentValue()
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

func dslMovingStdDev(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	points := int(args["points"].(float64))
	for name, s := range series {
		s.Alias(fmt.Sprintf("movingStdDev(%v,%v)", name, points))
		series[name] = &seriesMovingStdDev{AliasSeries: s, window: make([]float64, 0), points: points, n: -1}
	}
	return series, nil
}

// weightedAverage

type seriesWeightedAverage struct {
	aliasSeriesSlice
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

func dslWeightedAverage(args map[string]interface{}) (SeriesMap, error) {
	avgSeries := args["seriesListAvg"].(SeriesMap)
	weightSeries := args["seriesListWeight"].(SeriesMap)
	n := int(args["node"].(float64))

	avgByPart := make(map[string]AliasSeries, 0)
	weightByPart := make(map[string]AliasSeries, 0)

	for k, v := range avgSeries {
		parts := strings.Split(k, ".")
		if n >= len(parts) {
			return nil, fmt.Errorf("Element %v out of range for series name %v", n, k)
		}
		avgByPart[parts[n]] = v
	}

	for k, v := range weightSeries {
		parts := strings.Split(k, ".")
		if n >= len(parts) {
			return nil, fmt.Errorf("Element %v out of range for series name %v", n, k)
		}
		weightByPart[parts[n]] = v
	}

	// sort keys
	avgKeys := make([]string, 0, len(avgByPart))
	for k := range avgByPart {
		avgKeys = append(avgKeys, k)
	}
	sort.Strings(avgKeys)

	// make a special SeriesSlice
	result := &aliasSeriesSlice{}
	for _, k := range avgKeys {
		w := weightByPart[k]
		if w != nil {
			result.SeriesSlice = append(result.SeriesSlice, avgByPart[k])
			result.SeriesSlice = append(result.SeriesSlice, w)
		}
	}
	result.Align()

	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesWeightedAverage{*result}}, nil
}

// changed()

type seriesChanged struct {
	AliasSeries
	last float64
}

func (f *seriesChanged) CurrentValue() float64 {
	if f.AliasSeries.CurrentValue() != f.last {
		return 1
	}
	return 0
}

func (f *seriesChanged) Next() bool {
	f.last = f.AliasSeries.CurrentValue()
	return f.AliasSeries.Next()
}

func dslChanged(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	// Wrap everything in changed
	for name, s := range series {
		s.Alias(fmt.Sprintf("changed(%s)", name))
		series[name] = &seriesChanged{s, math.NaN()}
	}
	return series, nil
}

// constantLine()

type constantLine struct {
	AliasSeries
}

func dslConstantLine(args map[string]interface{}) (SeriesMap, error) {

	value := args["value"].(float64)
	from := args["_from_"].(time.Time)
	to := args["_to_"].(time.Time)
	step := to.Sub(from)

	// internally we mark ends of slots, not beginnings
	from = from.Add(step)

	ss := series.NewSliceSeries([]float64{value, value}, from, step)

	legend := fmt.Sprintf("constantLine(%v)", value)
	ss.Alias(legend)
	return SeriesMap{legend: ss}, nil
}

// countSeries()

type seriesCountSeries struct {
	*aliasSeriesSlice
	count float64
}

func (f *seriesCountSeries) CurrentValue() float64 {
	return f.count
}

func dslCountSeries(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap).toAliasSeriesSlice()
	name := args["_legend_"].(string)
	return SeriesMap{name: &seriesCountSeries{series, float64(len(series.SeriesSlice))}}, nil
}

// hitCount()
// This really boils down to Sum(Scale())
// alignToInterval is ignored because I think we sort of do that anyway

type seriesHitcount struct {
	AliasSeries
	factor float64
}

func (sl *seriesHitcount) CurrentValue() float64 {
	return sl.AliasSeries.CurrentValue() * sl.factor
}

func dslHitcount(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	is := args["intervalString"].(string)

	dur, err := misc.BetterParseDuration(is)
	if err != nil {
		return nil, err
	}
	factor := dur.Seconds()

	for name, s := range series {
		s.Alias(fmt.Sprintf("hitcount(%v,%v)", name, factor))
		series[name] = &seriesHitcount{s, factor}
	}
	return series, nil
}

// keepLastValue()

type seriesKeepLastValue struct {
	AliasSeries
	maxCnt int
	last   float64
	cnt    int
}

func (s *seriesKeepLastValue) CurrentValue() float64 {
	value := s.AliasSeries.CurrentValue()
	if math.IsNaN(value) {
		if s.maxCnt > 0 && s.cnt < s.maxCnt {
			// Incrementing counter in CV() is wrong, but it will do
			s.cnt++
			return s.last
		}
	} else {
		s.last = value
		s.cnt = 0
	}
	return value
}

func dslKeepLastValue(args map[string]interface{}) (SeriesMap, error) {
	series := args["seriesList"].(SeriesMap)
	maxCnt := args["limit"].(float64)
	for name, s := range series {
		s.Alias(fmt.Sprintf("keepLastValue(%v,%v)", name, maxCnt))
		series[name] = &seriesKeepLastValue{AliasSeries: s, maxCnt: int(maxCnt)}
	}
	return series, nil
}

// exclude()
func dslExclude(args map[string]interface{}) (SeriesMap, error) {
	result := args["seriesList"].(SeriesMap)
	exclude := args["pattern"].(string)
	reg, err := regexp.Compile(exclude)
	if err != nil {
		return nil, err
	}
	for name, _ := range result {
		if reg.MatchString(name) {
			delete(result, name)
		}
	}
	return result, nil
}

// scaleToSeconds()
// Since everything here is per second, this is same as scale()

type seriesScaleToSeconds struct {
	AliasSeries
	factor float64
}

func (f *seriesScaleToSeconds) CurrentValue() float64 {
	return f.AliasSeries.CurrentValue() * f.factor
}

func dslScaleToSeconds(args map[string]interface{}) (SeriesMap, error) {

	series := args["seriesList"].(SeriesMap)
	factor := args["seconds"].(float64)

	for name, s := range series {
		s.Alias(fmt.Sprintf("scaleToSeconds(%v,%v)", name, factor))
		series[name] = &seriesScaleToSeconds{s, factor}
	}
	return series, nil
}

// holtWintersForecast

type seriesHoltWintersForecast struct {
	AliasSeries
	data      []float64
	result    []float64
	upper     []float64
	lower     []float64
	seasonLen time.Duration
}

// nanlessData returns the series as a slice, skipping all leading
// NaNs and replacing the ones in the middle of the series with the
// last non-NaN value
func (f *seriesHoltWintersForecast) nanlessData() ([]float64, time.Time, error) {
	result := make([]float64, 0)
	var last float64
	var start time.Time
	for f.AliasSeries.Next() {
		val := f.AliasSeries.CurrentValue()
		for len(result) == 0 && math.IsNaN(val) {
			if f.AliasSeries.Next() {
				val = f.AliasSeries.CurrentValue()
			} else {
				return nil, time.Time{}, fmt.Errorf("Reached end of series while skipping leading NaNs")
			}
		}
		if math.IsNaN(val) {
			// Recycle the last value if NaN
			val = last // TODO can we do better?
		}
		if start.IsZero() {
			start = f.AliasSeries.CurrentTime()
		}
		result = append(result, val)
		last = val
	}
	f.AliasSeries.Close()
	return result, start, nil
}

// season length in points
func (f *seriesHoltWintersForecast) seasonPoints() int {
	return int(f.seasonLen / f.GroupBy())
}

func dslHoltWintersForecast(args map[string]interface{}) (SeriesMap, error) {
	ss := args["seriesList"].(SeriesMap)
	seasonLen := args["seasonLen"].(string)
	seasonLimit := int(args["seasonLimit"].(float64))
	α := args["alpha"].(float64)
	β := args["beta"].(float64)
	γ := args["gamma"].(float64)
	devScale := args["devScale"].(float64)
	show := args["show"].(string)

	if α > 1 || β > 1 || γ > 1 || α < 0 || β < 0 || γ < 0 {
		return nil, fmt.Errorf("Invalid alpha, beta or gamma - must be > 0 and < 1, or 0 to auto-compute (slower)")
	}
	if α == 0 || β == 0 || γ == 0 {
		if α+β+γ != 0 {
			return nil, fmt.Errorf("Alpha, beta, gamma - if one is zero, all must be zeros")
		}
	}

	result := make(SeriesMap, 0)
	for name, s := range ss {
		s.Alias(fmt.Sprintf("holtWintersForecast(%v)", name))

		var err error
		var slen time.Duration

		slen, err = misc.BetterParseDuration(seasonLen)
		if err != nil {
			return nil, err
		}

		// The specified timerange. NB: trDbSeres.TimeRange() is clipped
		// to what is available in the db, which is why we need these
		from := args["_from_"].(time.Time)
		to := args["_to_"].(time.Time)
		maxPoints := args["_maxPoints_"].(int64)

		// Push back beginning of our data seasonLimit from no later than LastUpdate
		var adjustedFrom time.Time
		if to.Before(s.Latest()) {
			adjustedFrom = to.Add(-slen * time.Duration(seasonLimit))
		} else {
			adjustedFrom = s.Latest().Add(-slen * time.Duration(seasonLimit))
		}

		// If we went beyond "viewport", adjust the underlying Series and MaxPoints
		if adjustedFrom.Before(from) {
			s.TimeRange(adjustedFrom)
			s.MaxPoints(to.Sub(adjustedFrom).Nanoseconds() / (to.Sub(from).Nanoseconds() / maxPoints))
		} else {
			// Set it back to be same as from, disregard seasonLimit when viewport has enough seasons
			adjustedFrom = from
		}

		// This struct knows how to permorm triple exponential smoothing
		shw := &seriesHoltWintersForecast{
			AliasSeries: s,
			seasonLen:   slen}

		// NB: This causes GroupByMs be calculated by dbSeries
		var nanlessBegin time.Time
		if shw.data, nanlessBegin, err = shw.nanlessData(); err != nil {
			return nil, err
		}

		// Calculate the forecast point count
		var nPreds int = 0
		if to.After(s.Latest()) {
			nPreds = int(to.Sub(s.Latest()) / s.GroupBy())
		}

		// Run the exponential smoothing algo
		var smooth, dev []float64
		if trend, err := series.HWInitialTrendFactor(shw.data, shw.seasonPoints()); err != nil {
			return nil, err
		} else {
			if seasonal, err := series.HWInitialSeasonalFactors(shw.data, shw.seasonPoints()); err != nil {
				return nil, err
			} else {
				if α == 0 {
					var e int
					smooth, dev, α, β, γ, _, e = series.HWMinimizeSSE(shw.data, shw.seasonPoints(), trend, seasonal, nPreds)
					log.Printf("Nelder-Mead finished in %d evaluations, resulting in α: %f β: %f γ: %f", e, α, β, γ)
				} else {
					smooth, dev, _ = series.HWTripleExponentialSmoothing(shw.data, shw.seasonPoints(), trend, seasonal, nPreds, α, β, γ)
				}
			}
		}

		// If the "viewport" is smaller than our data, figure out how many points we should
		// send across. Ensure from is aligned on GroupByMs first
		from = from.Truncate(s.GroupBy())
		if nanlessBegin.Before(from) {
			big := to.Sub(nanlessBegin).Seconds()
			small := from.Sub(nanlessBegin).Seconds()
			viewPoints := len(smooth) - int(small/big*float64(len(smooth)))
			nanlessBegin = from
			shw.result = smooth[len(smooth)-viewPoints:]
		} else {
			shw.result = smooth
		}

		// This is the actual output
		ss := series.NewSliceSeries(shw.result, nanlessBegin, shw.GroupBy())
		ss.Alias(shw.Alias())

		if strings.Contains(show, "smooth") {
			result[name] = ss
		}

		if strings.Contains(show, "conf") || strings.Contains(show, "aberr") {

			// upper band
			ucdata := make([]float64, len(shw.result))
			for i := 0; i < len(shw.result); i++ {
				ucdata[i] = shw.result[i] + shw.result[i]*dev[i]*devScale
			}

			uc := series.NewSliceSeries(ucdata, nanlessBegin, shw.GroupBy())
			uc.Alias(fmt.Sprintf("holtWintersConfidenceUpper(%v)", name))

			if strings.Contains(show, "conf") {
				result[name+".upper"] = uc
			}

			// lower band
			lcdata := make([]float64, len(shw.result))
			for i := 0; i < len(shw.result); i++ {
				lcdata[i] = shw.result[i] - shw.result[i]*dev[i]*devScale
			}

			lc := series.NewSliceSeries(lcdata, nanlessBegin, shw.GroupBy())
			lc.Alias(fmt.Sprintf("holtWintersConfidenceLower(%v)", name))

			if strings.Contains(show, "conf") {
				result[name+".lower"] = lc
			}

			if strings.Contains(show, "aberr") {

				// aberrations
				abdata := make([]float64, len(shw.result))
				for i := 0; i < len(shw.result); i++ {
					if shw.result[i] < lcdata[i] {
						abdata[i] = shw.result[i] - lcdata[i]
					} else if shw.result[i] > ucdata[i] {
						abdata[i] = shw.result[i] - ucdata[i]
					}
				}

				ab := series.NewSliceSeries(abdata, nanlessBegin, shw.GroupBy())
				ab.Alias(fmt.Sprintf("holtWintersAberration(%v)", name))

				result[name+".aberrant"] = ab
			}
		}

	}
	return result, nil
}

func dslHoltWintersConfidenceBands(args map[string]interface{}) (SeriesMap, error) {
	args["seasonLen"] = "1d"
	args["seasonLimit"] = float64(7.0)
	args["alpha"] = float64(0)
	args["beta"] = float64(0)
	args["gamma"] = float64(0)
	args["devScale"] = args["delta"]
	args["show"] = "conf"
	return dslHoltWintersForecast(args)
}

func dslHoltWintersAberration(args map[string]interface{}) (SeriesMap, error) {
	args["seasonLen"] = "1d"
	args["seasonLimit"] = float64(7.0)
	args["alpha"] = float64(0)
	args["beta"] = float64(0)
	args["gamma"] = float64(0)
	args["devScale"] = args["delta"]
	args["show"] = "aberr"
	return dslHoltWintersForecast(args)
}
