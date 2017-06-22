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
	"math"
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
	db   *pgvSerDe
	rows *sql.Rows

	// These are not the same:
	maxPoints int64         // max points we want
	groupBy   time.Duration // requested alignment

	latest time.Time

	// Alias
	alias string
}

func (dps *dbSeries) Step() time.Duration {
	return dps.rra.Step()
}

func (dps *dbSeries) GroupBy(td ...time.Duration) time.Duration {
	if len(td) > 0 {
		defer func() { dps.groupBy = td[0] }()
	}
	if dps.groupBy == 0 {
		return dps.Step()
	}
	return dps.groupBy
}

func (dps *dbSeries) TimeRange(t ...time.Time) (time.Time, time.Time) {
	if len(t) == 1 {
		defer func() { dps.from = t[0] }()
	} else if len(t) == 2 {
		defer func() { dps.from, dps.to = t[0], t[1] }()
	}
	return dps.from, dps.to
}

func (dps *dbSeries) Latest() time.Time {
	return dps.rra.Latest()
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
		groupByMs      = dps.groupBy.Nanoseconds() / 1e6
		rraStepMs      = dps.rra.Step().Nanoseconds() / 1e6
	)

	if dps.groupBy != 0 {
		// Specific granularity was requested for alignment, we ignore maxPoints
		finalGroupByMs = finalGroupByMs/groupByMs*groupByMs + groupByMs
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
	if finalGroupByMs != dps.groupBy.Nanoseconds()/1e6 {
		dps.groupBy = time.Duration(finalGroupByMs) * time.Millisecond
	}

	aligned_from := dps.from.Truncate(time.Duration(finalGroupByMs) * time.Millisecond)

	if debug {
		dbFormat := "2006-01-02 15:04:05 -0700"
		sqlStatement := fmt.Sprintf(
			"\nSELECT max(tg) mt, avg(r) ar\n"+
				"   FROM generate_series('%[2]s', '%[3]s', ('%[4]s')::interval) AS tg\n"+
				"   LEFT OUTER JOIN (SELECT t, r FROM %[1]stv tv WHERE ds_id = %[5]d AND rra_id = %[6]d\n"+
				"     AND t >= '%[7]s' AND t <= '%[8]s') s ON tg = s.t\n"+
				"   GROUP BY trunc((extract(epoch from tg)*1000-1))::bigint/%[9]d ORDER BY mt",
			dps.db.prefix,
			aligned_from.Format(dbFormat), dps.to.Format(dbFormat), fmt.Sprintf("%d milliseconds", rraStepMs),
			dps.ds.Id(), dps.rra.Id(), dps.from.Format(dbFormat), dps.to.Format(dbFormat),
			finalGroupByMs)
		log.Printf("seriesQuerySqlUsingViewAndSeries() sqlSelectSeries -- " + sqlStatement)
	}
	rows, err = dps.db.sqlSelectSeries.Query(aligned_from, dps.to, fmt.Sprintf("%d milliseconds", rraStepMs), dps.ds.Id(), dps.rra.Id(), dps.from, dps.to, finalGroupByMs)

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

func (dps *dbSeries) CurrentTime() time.Time {
	return dps.posEnd
}

func (dps *dbSeries) Close() error {
	if dps.rows == nil {
		return fmt.Errorf("Close() on dbSeries that isn not open.")
	}
	result := dps.rows.Close()
	dps.rows = nil // next Next() will re-open
	return result
}

func timeValueFromRow(rows *sql.Rows) (time.Time, float64, error) {
	var (
		value sql.NullFloat64
		ts    time.Time
	)
	if err := rows.Scan(&ts, &value); err == nil {
		if value.Valid {
			return ts, value.Float64, nil
		} else {
			return ts, math.NaN(), nil
		}
	} else {
		return time.Time{}, math.NaN(), err
	}
}
