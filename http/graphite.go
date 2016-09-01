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

// Package http provides HTTP functionality for querying TS data as
// well as submitting data points to a receiver.
package http

import (
	"fmt"
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/misc"
	"github.com/tgres/tgres/receiver"
	"log"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"
)

func GraphiteMetricsFindHandler(rcvr *receiver.Receiver) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, "[\n")
		nodes := rcvr.Rcache.FsFind(r.FormValue("query"))
		for n, node := range nodes {
			parts := strings.Split(node.Name, ".")
			if node.Leaf {
				fmt.Fprintf(w, `{"leaf": 1, "context": {}, "text": "%s", "expandable": 0, "id": "%s", "allowChildren": 0}`, parts[len(parts)-1], node.Name)
			} else {
				fmt.Fprintf(w, `{"leaf": 0, "context": {}, "text": "%s", "expandable": 1, "id": "%s", "allowChildren": 1}`, parts[len(parts)-1], node.Name)
			}
			if n < len(nodes)-1 {
				fmt.Fprintf(w, ",\n")
			}
		}
		fmt.Fprintf(w, "\n]\n")
	}
}

func GraphiteRenderHandler(rcvr *receiver.Receiver) http.HandlerFunc {

	return func(w http.ResponseWriter, r *http.Request) {

		from, err := parseTime(r.FormValue("from"))
		if err != nil {
			log.Printf("RenderHandler(): (from) %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		to, err := parseTime(r.FormValue("until"))
		if err != nil {
			log.Printf("RenderHandler(): (unitl) %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		} else if to == nil {
			tmp := time.Now()
			to = &tmp
		}
		points, err := strconv.Atoi(r.FormValue("maxDataPoints"))
		if err != nil {
			log.Printf("RenderHandler(): (maxDataPoints) %v", err)
			w.WriteHeader(http.StatusBadRequest)
			return
		}

		fmt.Fprintf(w, "[")

		for tn, target := range r.Form["target"] {

			seriesMap, err := processTarget(rcvr, target, from.Unix(), to.Unix(), int64(points))

			if err != nil {
				log.Printf("RenderHandler(): %v", err)
				break // Graphite behaviour is empty list
			}

			nn := 0
			for _, name := range seriesMap.SortedKeys() {
				series := seriesMap[name]

				alias := series.Alias()
				if alias != "" {
					name = alias
				}

				fmt.Fprintf(w, "\n"+`{"target": "%s", "datapoints": [`+"\n", name)

				n := 0
				for series.Next() {
					if n > 0 {
						fmt.Fprintf(w, ",")
					}
					value := series.CurrentValue()
					ts := series.CurrentPosBeginsAfter().Unix() // NOTE: Graphite protocol marks the *beginning* of the point
					if ts > 0 {
						if math.IsNaN(value) || math.IsInf(value, 0) {
							fmt.Fprintf(w, "[null, %v]", ts)
						} else {
							fmt.Fprintf(w, "[%v, %v]", value, ts)
						}
						n++
					}
				}
				if nn < len(seriesMap)-1 || tn < len(r.Form["target"])-1 {
					fmt.Fprintf(w, "]},\n")
				} else {
					fmt.Fprintf(w, "]}")
				}
				series.Close()
				nn++
			}
		}
		fmt.Fprintf(w, "]\n")
	}
}

func parseTime(s string) (*time.Time, error) {

	if len(s) == 0 {
		return nil, nil
	}

	if s[0] == '-' { // relative
		if dur, err := misc.BetterParseDuration(s[1:len(s)]); err == nil {
			t := time.Now().Add(-dur)
			return &t, nil
		} else {
			return nil, fmt.Errorf("parseTime(): Error parsing relative time %q: %v", s, err)
		}
	} else { // absolute
		if s == "now" {
			t := time.Now()
			return &t, nil
		} else if i, err := strconv.ParseInt(s, 10, 64); err == nil {
			t := time.Unix(i, 0)
			return &t, nil
		} else {
			return nil, fmt.Errorf("parseTime(): Error parsing absolute time %q: %v", s, err)
		}
	}
}

func processTarget(rcvr *receiver.Receiver, target string, from, to, maxPoints int64) (dsl.SeriesMap, error) {
	// In our DSL everything must be a function call, so we wrap everything in group()
	query := fmt.Sprintf("group(%s)", target)
	dc := dsl.NewDslCtx(rcvr.Rcache, query, from, to, maxPoints)
	result, err := dc.ParseDsl()
	return result, err
}
