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

package serde

import (
	"database/sql"
	"fmt"
	"log"
	"time"
)

type dbSeries struct {
	ds  DbDataSourcer
	rra DbRoundRobinArchiver

	// Current Value
	value    float64
	posBegin time.Time // pos begins after
	posEnd   time.Time // pos ends on

	// Time boundary
	from time.Time
	to   time.Time

	// Db stuff
	db   *pgSerDe
	rows *sql.Rows

	// These are not the same:
	maxPoints int64 // max points we want
	groupByMs int64 // requested alignment

	latest time.Time

	// Alias
	alias string
}

func (dps *dbSeries) StepMs() int64 {
	return dps.rra.Step().Nanoseconds() / 1000000
}

func (dps *dbSeries) GroupByMs(ms ...int64) int64 {
	if len(ms) > 0 {
		defer func() { dps.groupByMs = ms[0] }()
	}
	if dps.groupByMs == 0 {
		return dps.StepMs()
	}
	return dps.groupByMs
}

func (dps *dbSeries) TimeRange(t ...time.Time) (time.Time, time.Time) {
	if len(t) == 1 {
		defer func() { dps.from = t[0] }()
	} else if len(t) == 2 {
		defer func() { dps.from, dps.to = t[0], t[1] }()
	}
	return dps.from, dps.to
}

func (dps *dbSeries) LastUpdate() time.Time {
	return dps.ds.LastUpdate()
}

func (dps *dbSeries) MaxPoints(n ...int64) int64 {
	if len(n) > 0 { // setter
		defer func() { dps.maxPoints = n[0] }()
	}
	return dps.maxPoints // getter
}

func (dps *dbSeries) Align() {}

func (dps *dbSeries) Alias(s ...string) string {
	if len(s) > 0 {
		dps.alias = s[0]
	}
	return dps.alias
}

func (dps *dbSeries) seriesQuerySqlUsingViewAndSeries() (*sql.Rows, error) {
	var (
		rows *sql.Rows
		err  error
	)

	var (
		finalGroupByMs int64
		rraStepMs      = dps.rra.Step().Nanoseconds() / 1000000
	)

	if dps.groupByMs != 0 {
		// Specific granularity was requested for alignment, we ignore maxPoints
		finalGroupByMs = finalGroupByMs/dps.groupByMs*dps.groupByMs + dps.groupByMs
	} else if dps.maxPoints != 0 {
		// If maxPoints was specified, then calculate group by interval
		finalGroupByMs = (dps.to.Unix() - dps.from.Unix()) * 1000 / dps.maxPoints
		finalGroupByMs = finalGroupByMs/rraStepMs*rraStepMs + rraStepMs
	} else {
		// Otherwise, group by will equal the rrastep
		finalGroupByMs = rraStepMs
	}

	if finalGroupByMs == 0 {
		finalGroupByMs = 1000 // TODO Why would this happen (it did)?
	}

	// Ensure that the true group by interval is reflected in the series.
	if finalGroupByMs != dps.groupByMs {
		dps.groupByMs = finalGroupByMs
	}

	// Ensure that we never return data beyond lastUpdate (it would
	// cause us to return bogus data because the RRD would wrap
	// around). We do this *after* calculating groupBy because groupBy
	// should be based on the screen resolution, not what we have in
	// the db.
	if dps.to.After(dps.LastUpdate()) {
		dps.to = dps.LastUpdate()
	}

	// TODO: support milliseconds?
	aligned_from := time.Unix(dps.from.Unix()/(finalGroupByMs/1000)*(finalGroupByMs/1000), 0)

	if debug {
		log.Printf("seriesQuerySqlUsingViewAndSeries() sql3 %v %v %v %v %v %v %v %v", aligned_from, dps.to, fmt.Sprintf("%d milliseconds", rraStepMs),
			dps.ds.Id(), dps.rra.Id(), dps.from, dps.to, finalGroupByMs)
	}
	rows, err = dps.db.sql3.Query(aligned_from, dps.to, fmt.Sprintf("%d milliseconds", rraStepMs), dps.ds.Id(), dps.rra.Id(), dps.from, dps.to, finalGroupByMs)

	if err != nil {
		log.Printf("seriesQuery(): error %v", err)
		return nil, err
	}

	return rows, nil
}

func (dps *dbSeries) Next() bool {

	if dps.rows == nil { // First Next()
		rows, err := dps.seriesQuerySqlUsingViewAndSeries()
		if err == nil {
			dps.rows = rows
		} else {
			log.Printf("dbSeries.Next(): database error: %v", err)
			return false
		}
	}

	if dps.rows.Next() {
		if ts, value, err := timeValueFromRow(dps.rows); err != nil {
			log.Printf("dbSeries.Next(): database error: %v", err)
			return false
		} else {
			dps.posBegin = dps.latest
			dps.posEnd = ts
			dps.value = value
			dps.latest = dps.posEnd
		}
		return true
	}

	return false
}

func (dps *dbSeries) CurrentValue() float64 {
	return dps.value
}

func (dps *dbSeries) CurrentPosBeginsAfter() time.Time {
	return dps.posBegin
}

func (dps *dbSeries) CurrentPosEndsOn() time.Time {
	return dps.posEnd
}

func (dps *dbSeries) Close() error {
	result := dps.rows.Close()
	dps.rows = nil // next Next() will re-open
	return result
}
