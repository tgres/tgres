//
// This code (with minor modifications: removal of global variables,
// functions and the fmt package, rename of Optimize() to
// nelderMeadOptimize()) is originally from
// https://github.com/jlouis/nmoptim
//
// Copyright 2014-2015 Jesper Louis Andersen
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

// This is an implementation of a variant of the Nelder-Mead (1965) downhill
// simplex optimization heuristic.

package dsl

import (
	"math"
)

// nelderMeadOptimize function f with Nelder-Mead. start points to a slice of starting points
// It is the responsibility of the caller to make sure the dimensionality is correct.
func nelderMeadOptimize(f func([]float64) float64, start [][]float64, cf func([]float64)) ([]float64, int, int) {
	const (
		kMax = 1000     // arbitrarily chosen value for now
		ε    = 0.000001 // Stopping criterion point
		α    = 1.0
		β    = 0.5
		γ    = 2.0
	)

	// point is the type of points in ℝ^n
	type point []float64

	// simplex is the type used to represent a simplex
	type simplex []point

	evaluations := 0
	eval := func(f func([]float64) float64, p point) float64 {
		evaluations++
		return f(p)
	}

	// sub perform point subtraction
	sub := func(x point, y point) point {
		r := make(point, len(x))
		for i := range y {
			r[i] = x[i] - y[i]
		}
		return r
	}

	// add perform point addition
	add := func(x point, y point) point {
		r := make(point, len(x))
		for i := range y {
			r[i] = x[i] + y[i]
		}
		return r
	}

	// scale multiplies a point by a scalar
	scale := func(p point, scalar float64) point {
		r := make(point, len(p))
		for i := range r {
			r[i] = scalar * p[i]
		}
		return r
	}

	// centroid calculates the centroid of a simplex of one dimensionality lower by omitting a point
	centroid := func(s simplex, omit int) point {
		r := make(point, len(s[0]))
		for i := range r {
			c := 0.0
			for j := range s {
				if j == omit {
					continue
				} else {
					c += s[j][i]
				}
			}
			r[i] = c / float64((len(s) - 1))
		}
		return r
	}

	n := len(start)
	c := len(start[0])
	points := make([]point, 0)
	fv := make([]float64, n)

	for _, p := range start {
		points = append(points, point(p))
	}
	sx := simplex(points)
	if n != c+1 {
		panic("Can't optimize with too few starting points")
	}

	// Set up initial values
	for i := range fv {
		if cf != nil {
			cf(sx[i])
		}
		fv[i] = eval(f, sx[i])
	}

	k := 0
	for ; k < kMax; k++ {
		// Find the largest index
		vg := 0
		for i := range fv {
			if fv[i] > fv[vg] {
				vg = i
			}
		}

		// Find the smallest index
		vs := 0
		for i := range fv {
			if fv[i] < fv[vs] {
				vs = i
			}
		}

		// Second largest index
		vh := vs
		for i := range fv {
			if fv[i] > fv[vh] && fv[i] < fv[vg] {
				vh = i
			}
		}

		vm := centroid(sx, vg)

		vr := add(vm, scale(sub(vm, sx[vg]), α))
		if cf != nil {
			cf(vr)
		}
		fr := eval(f, vr)

		if fr < fv[vh] && fr >= fv[vs] {
			// Replace
			fv[vg] = fr
			sx[vg] = vr
		}

		// Investigate a step further
		if fr < fv[vs] {
			ve := add(vm, scale(sub(vr, vm), γ))
			if cf != nil {
				cf(ve)
			}

			fe := eval(f, ve)

			if fe < fr {
				sx[vg] = ve
				fv[vg] = fe
			} else {
				sx[vg] = vr
				fv[vg] = fr
			}
		}

		// Check contraction
		if fr >= fv[vh] {
			var vc point
			var fc float64
			if fr < fv[vg] && fr >= fv[vh] {
				// Outside contraction
				vc = add(vm, scale(sub(vr, vm), β))
			} else {
				// Inside contraction
				vc = sub(vm, scale(sub(vm, sx[vg]), β))
			}

			if cf != nil {
				cf(vc)
			}
			fc = eval(f, vc)

			if fc < fv[vg] {
				sx[vg] = vc
				fv[vg] = fc
			} else {
				for i := range sx {
					if i != vs {
						sx[i] = add(sx[vs], scale(sub(sx[i], sx[vs]), β))
					}
				}

				if cf != nil {
					cf(sx[vg])
				}
				fv[vg] = eval(f, sx[vg])

				if cf != nil {
					cf(sx[vh])
				}
				fv[vh] = eval(f, sx[vh])
			}
		}

		fsum := 0.0
		for _, v := range fv {
			fsum += v
		}

		favg := fsum / float64(len(fv))

		s := 0.0
		for _, v := range fv {
			s += math.Pow(v-favg, 2.0)
		}

		s = s * (1.0 / (float64(len(fv)) + 1.0))
		s = math.Sqrt(s)
		if s < ε {
			break
		}
	}

	vs := 0
	for i := range fv {
		if fv[i] < fv[vs] {
			vs = i
		}
	}

	return sx[vs], k, evaluations
}
