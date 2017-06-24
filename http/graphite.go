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
	"compress/gzip"
	"fmt"
	"io"
	"log"
	"math"
	"net/http"
	"regexp"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/misc"
)

const BATCH_LIMIT = 64

func GraphiteMetricsFindHandler(rcache dsl.NamedDSFetcher) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		// w.Header().Set("Access-Control-Allow-Origin", "*") // TODO Make me configurable

		start := time.Now()
		fmt.Fprintf(w, "[\n")
		nodes := rcache.FsFind(r.FormValue("query"))
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
		log.Printf("GraphiteMetricsFindHandler: finished in %v", time.Now().Sub(start))
	}
}

func GraphiteRenderHandler(rcache dsl.NamedDSFetcher) http.HandlerFunc {

	return makeGzipHandler(
		func(w http.ResponseWriter, r *http.Request) {
			// w.Header().Set("Access-Control-Allow-Origin", "*") // TODO Make me configurable
			w.Header().Set("Content-Type", "application/json")

			start := time.Now()
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

			var wg sync.WaitGroup

			targets := make([][]*graphiteSeries, len(r.Form["target"]))
			batchSize := 0
			for n, target := range r.Form["target"] {
				wg.Add(1)
				batchSize++
				go func(wg *sync.WaitGroup, target string, targets [][]*graphiteSeries, n int) {
					if sm, err := processTarget(rcache, target, from.Unix(), to.Unix(), int64(points)); err == nil {
						// sm may contain locked watched RRAs,
						// readDataPoints unlocks them in Close it's
						// important to not do anything that could
						// interrupt this, we MUST run readDataPoints
						targets[n] = readDataPoints(sm)
					} else {
						log.Printf("RenderHandler(): %v", err)
					}
					wg.Done()
				}(&wg, target, targets, n)
				if batchSize > BATCH_LIMIT { // limit concurrent processing
					wg.Wait()
					batchSize = 0
				}
			}
			wg.Wait()

			fmt.Fprintf(w, "[")

			for tn, target := range targets {

				nn := 0
				for _, series := range target {
					fmt.Fprintf(w, "\n"+`{"target": "%s", "datapoints": [`+"\n", series.name)
					n := 0
					for _, dp := range series.dps {
						if dp.t > 0 {
							if n > 0 {
								fmt.Fprintf(w, ",")
							}
							if math.IsNaN(dp.v) || math.IsInf(dp.v, 0) {
								fmt.Fprintf(w, "[null, %v]", dp.t)
							} else {
								fmt.Fprintf(w, "[%v, %v]", dp.v, dp.t)
							}
							n++
						}
					}

					if nn < len(target)-1 || tn < len(r.Form["target"])-1 {
						fmt.Fprintf(w, "]},\n")
					} else {
						fmt.Fprintf(w, "]}")
					}
					nn++
				}
			}
			fmt.Fprintf(w, "]\n")

			log.Printf("GraphiteRenderHandler: finished in %v", time.Now().Sub(start))
		},
	)
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

// This is not perfect, but it's better than nothing. It seeks
// identifiers containing a dot and surrounds them with quotes - this
// prevents errors for series names parts of which begin with a digit,
// which is not valid Go syntax.
func quoteIdentifiers(target string) string {
	result := target
	// Note that commas are only allowed inside {} (aka "value expression")
	parts := regexp.MustCompile(`("?[\w*][\w\-.*]*({[\w\-.*,]*})?[\w\-.*]*[\w*]"?)`).FindAllString(target, -1)
	for _, part := range parts {
		if strings.Contains(part, ".") && !strings.HasPrefix(part, "\"") {
			result = quoteIdentifiers(strings.Replace(result, part, fmt.Sprintf("%q", part), -1))
			break
		}
	}
	return result
}

func processTarget(rcache dsl.NamedDSFetcher, target string, from, to, maxPoints int64) (dsl.SeriesMap, error) {
	target = quoteIdentifiers(target)
	// In our DSL everything must be a function call, so we wrap everything in group()
	query := fmt.Sprintf("group(%s)", target)
	return dsl.ParseDsl(rcache, query, time.Unix(from, 0), time.Unix(to, 0), maxPoints)
}

// Graphite data points
type dataPoint struct {
	t int64
	v float64
}
type graphiteSeries struct {
	dps  []*dataPoint
	name string
}

func readDataPoints(sm dsl.SeriesMap) []*graphiteSeries {
	names := sm.SortedKeys()
	result := make([]*graphiteSeries, len(names))
	var (
		wg        sync.WaitGroup
		batchSize int
	)
	for n, name := range sm.SortedKeys() {
		series := sm[name]
		alias := series.Alias()
		if alias != "" {
			name = alias
		}
		wg.Add(1)
		batchSize++
		go func(wg *sync.WaitGroup, result []*graphiteSeries, n int, name string) {
			gs := &graphiteSeries{make([]*dataPoint, 0), name}
			for series.Next() {
				gs.dps = append(gs.dps, &dataPoint{series.CurrentTime().Unix(), series.CurrentValue()})
			}
			result[n] = gs
			series.Close()
			wg.Done()
		}(&wg, result, n, name)
		if batchSize > BATCH_LIMIT {
			wg.Wait()
			batchSize = 0
		}
	}
	wg.Wait()
	return result
}

// Gzip Compression
type gzipResponseWriter struct {
	io.Writer
	http.ResponseWriter
}

func (w gzipResponseWriter) Write(b []byte) (int, error) {
	return w.Writer.Write(b)
}

func makeGzipHandler(fn http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !strings.Contains(r.Header.Get("Accept-Encoding"), "gzip") {
			fn(w, r)
			return
		}
		w.Header().Set("Content-Encoding", "gzip")
		gz := gzip.NewWriter(w)
		defer gz.Close()
		gzr := gzipResponseWriter{Writer: gz, ResponseWriter: w}
		fn(gzr, r)
	}
}
