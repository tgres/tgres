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
	"math"
)

// hwInitialTrendFactor returns a list of best initial trend factor so
// that we can start forecasting.
//
// See http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
// "Initial values for the trend factor"
func hwInitialTrendFactor(data []float64, slen int) (float64, error) {
	if len(data) < slen*2 {
		return math.NaN(), fmt.Errorf("Not enough data to compute initial trend factor, need at least two seasons")
	}
	var tot float64 = 0
	for i := 0; i < slen; i++ {
		tot += (data[i+slen] - data[i]) / float64(slen)
	}
	return tot / float64(slen), nil
}

// hwInitialSeasonalFactors returns a list of best initial seasonal
// factor so that we can start forecasting.
//
// See http://www.itl.nist.gov/div898/handbook/pmc/section4/pmc435.htm
// "Initial values for the Seasonal Indices"
func hwInitialSeasonalFactors(data []float64, slen int) ([]float64, error) {

	nSeasons := len(data) / slen
	if nSeasons == 0 {
		return nil, fmt.Errorf("Not enough data to compute initial seasonal factors, at least one full season required")
	}

	// take the same offset from each of the seasons and average it
	seasonAverages := make([]float64, nSeasons)
	for j := 0; j < nSeasons; j++ {
		var sum float64 = 0
		for i := 0; i < slen; i++ {
			sum += data[j*slen+i]
		}
		seasonAverages[j] = sum / float64(slen)
	}

	result := make([]float64, slen)
	for i := 0; i < slen; i++ {
		var sumOverAvg float64 = 0
		for j := 0; j < nSeasons; j++ {
			sumOverAvg += data[j*slen+i] / seasonAverages[j]
		}
		result[i] = sumOverAvg / float64(nSeasons)
	}

	return result, nil
}

// hwTripleExponentialSmoothing performs triple exponential smoothing
// (multiplicative) on data given season length slen (in number of
// data points per season), initial trend, slice of initial seasonal
// factors, the number of forecasted points and smoothing factors α,
// β, γ. It returns a new slice of smoothed and forcasted data points,
// a slice of deviations such that y±y*d could be used as "confidence
// bands" as well as SSE (Sum of Squared Errors).You can obtain trend
// and seasonal from hwInitialTrendFactor and
// hwInitialSeasonalFactors. Best α, β and γ can be calculated by
// repeatedly calling hwTripleExponentialSmoothing to find the
// combination with the smallest SSE, you can use hwMinimiseSSE for
// this.
func hwTripleExponentialSmoothing(data []float64, slen int, trend float64, seasonal []float64, nPredictions int, α, β, γ float64) ([]float64, []float64, float64) {

	var (
		level, lastLevel float64
		result           = make([]float64, len(data)+nPredictions)
		seasonalDev      = make([]float64, slen)
		dev              = make([]float64, len(data)+nPredictions)
		sse              float64
	)

	if (α <= 0 || α >= 1) || (β <= 0 || β >= 1) || (γ <= 0 || γ >= 1) {
		// return a VERY large SSE to tell Nelder-Mead to stay between 0 and 1
		return []float64{}, []float64{}, float64(^uint(0) >> 1)
	}

	for i := 0; i < len(data)+nPredictions; i++ {

		if i == 0 {
			level = data[i]
			result[i] = data[i]
			continue
		}

		if i >= len(data) { // we are forecasting
			m := i - len(data) + 1
			result[i] = (level + float64(m)*trend) * seasonal[i%slen]
		} else {
			val := data[i]

			lastLevel, level = level, α*val/seasonal[i%slen]+(1-α)*(level+trend)
			trend = β*(level-lastLevel) + (1-β)*trend
			seasonal[i%slen] = γ*val/level + (1-γ)*seasonal[i%slen]
			result[i] = (level + trend) * seasonal[i%slen]

			if i > 2 {
				// smoothed deviation for confidence band
				factor := result[i] / val
				if factor > 1 {
					factor = 1 / factor
				}
				seasonalDev[i%slen] = γ*(1-factor) + (1-γ)*seasonalDev[i%slen]
				// SSE
				diff := result[i] - val
				sse += diff * diff
			}
		}
		dev[i] = seasonalDev[i%slen]
	}
	return result, dev, sse
}

// hwMinimizeSSE repeatedly executes hwTripleExponentialSmoothing over
// data, season, slen, trend and nPred with varying α, β and γ using
// the Nelder-Mead algorithm to minimize SSE.  It returns the final
// (best) smooth and dev returned by hwTripleExponentialSmoothing, as
// well as the resulting α, β, γ (which can be reused later), k
// (number of passes) and e (evocation count for
// hwTripleExponentialSmoothing).
func hwMinimizeSSE(data []float64, slen int, trend float64, seasonal []float64, nPred int) (smooth, dev []float64, α, β, γ float64, k, e int) {
	doit := func(x []float64) float64 {
		α, β, γ, sse := x[0], x[1], x[2], float64(0)
		sc := make([]float64, len(seasonal))
		copy(sc, seasonal)
		smooth, dev, sse = hwTripleExponentialSmoothing(data, slen, trend, sc, nPred, α, β, γ)
		return sse
	}

	// These are 4 initial α, β, γ triplets - TODO these are completely arbitrary?
	//s := [][]float64{{0.1, 0.1, 0.1}, {0.9, 0.9, 0.9}, {0.5, 0.5, 0.5}, {0.1, 0.9, 0.1}}
	s := [][]float64{{0.1, 0.01, 0.9}, {0.9, 0.1, 0.1}, {0.5, 0.2, 0.5}, {0.1, 0.9, 0.1}}
	//s := [][]float64{{0.1, 0.1, 0.1}, {0.9, 0.9, 0.9}, {0.1, 0.1, 0.9}, {0.9, 0.9, 0.1}}
	//s := [][]float64{{0.1, 0.2, 0.3}, {0.9, 0.8, 0.7}, {0.5, 0.4, 0.3}, {0.1, 0.9, 0.01}}

	var r []float64
	r, k, e = nelderMeadOptimize(doit, s, nil)
	α, β, γ = r[0], r[1], r[2]
	return smooth, dev, α, β, γ, k, e
}
