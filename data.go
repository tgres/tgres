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
	"bytes"
	"database/sql"
	"fmt"
	"github.com/lib/pq"
	"log"
	"math"
	"path/filepath"
	"strconv"
	"strings"
	"time"
)

var (
	dbConn                             *sql.DB
	sql1, sql2, sql3, sql4, sql5, sql6 *sql.Stmt
)

var sqlOpen = func(a, b string) (*sql.DB, error) {
	return sql.Open(a, b)
}

var dbPing = func() error {
	return dbConn.Ping()
}

var initDbConnection = func(connect_string string) error {
	var err error
	if dbConn, err = sqlOpen("postgres", connect_string); err != nil {
		return err
	}
	if err = dbPing(); err != nil {
		return err
	}
	return nil
}

func prepareSqlStatements() error {
	var err error
	if sql1, err = dbConn.Prepare("UPDATE ts SET dp[$1:$2] = $3 WHERE rra_id = $4 AND n = $5"); err != nil {
		return err
	}
	if sql2, err = dbConn.Prepare("UPDATE rra SET value = $1, unknown_ms = $2, latest = $3 WHERE id = $4"); err != nil {
		return err
	}
	if sql3, err = dbConn.Prepare("SELECT max(tg) mt, avg(r) ar FROM generate_series($1, $2, ($3)::interval) AS tg " +
		"LEFT OUTER JOIN (SELECT t, r FROM tv WHERE ds_id = $4 AND rra_id = $5 " +
		" AND t >= $6 AND t <= $7) s ON tg = s.t GROUP BY trunc((extract(epoch from tg)*1000-1))::bigint/$8 ORDER BY mt"); err != nil {
		return err
	}
	if sql4, err = dbConn.Prepare("INSERT INTO ds (name, step_ms, heartbeat_ms) VALUES ($1, $2, $3) " +
		"RETURNING id, name, step_ms, heartbeat_ms, lastupdate, last_ds, value, unknown_ms"); err != nil {
		return err
	}
	if sql5, err = dbConn.Prepare("INSERT INTO rra (ds_id, cf, steps_per_row, size, xff) VALUES ($1, $2, $3, $4, $5) " +
		"RETURNING id, ds_id, cf, steps_per_row, size, width, xff, value, unknown_ms, latest"); err != nil {
		return err
	}
	if sql6, err = dbConn.Prepare("INSERT INTO ts (rra_id, n) VALUES ($1, $2)"); err != nil {
		return err
	}

	return nil
}

var createTablesIfNotExist = func() error {
	create_sql := `
       CREATE TABLE IF NOT EXISTS ds (
       id SERIAL NOT NULL PRIMARY KEY,
       name TEXT NOT NULL,
       step_ms BIGINT NOT NULL,
       heartbeat_ms BIGINT NOT NULL,
       lastupdate TIMESTAMPTZ,
       last_ds NUMERIC DEFAULT NULL,
       value DOUBLE PRECISION NOT NULL DEFAULT 'NaN',
       unknown_ms BIGINT NOT NULL DEFAULT 0);

       CREATE TABLE IF NOT EXISTS rra (
       id SERIAL NOT NULL PRIMARY KEY,
       ds_id INT NOT NULL,
       cf TEXT NOT NULL,
       steps_per_row INT NOT NULL,
       size INT NOT NULL,
       width INT NOT NULL DEFAULT 768,
       xff REAL NOT NULL,
       value DOUBLE PRECISION NOT NULL DEFAULT 'NaN',
       unknown_ms BIGINT NOT NULL DEFAULT 0,
       latest TIMESTAMPTZ DEFAULT NULL);

       CREATE TABLE IF NOT EXISTS ts (
       rra_id INT NOT NULL,
       n INT NOT NULL,
       dp DOUBLE PRECISION[] NOT NULL DEFAULT '{}');
    `
	if rows, err := dbConn.Query(create_sql); err != nil {
		log.Printf("ERROR: initial CREATE TABLE failed: %v", err)
		return err
	} else {
		rows.Close()
	}
	create_sql = `
       CREATE VIEW tv AS
       SELECT ds.id ds_id, rra.id rra_id, latest - interval '1 millisecond' * ds.step_ms * rra.steps_per_row *
            mod(rra.size + mod(extract(epoch from rra.latest)::bigint*1000/(ds.step_ms * rra.steps_per_row), size) + 1
           - (generate_subscripts(dp,1) + n * width), rra.size) AS t,
          UNNEST(dp) AS r
       FROM ds
       INNER JOIN rra ON rra.ds_id = ds.id
       INNER JOIN ts ON ts.rra_id = rra.id;

       CREATE VIEW tvd AS
       SELECT ds_id, rra_id, tstzrange(lag(t, 1) OVER (PARTITION BY ds_id, rra_id ORDER BY t), t, '(]') tr, r, step, row, row_n, abs_n, last_n, last_t, slot_distance FROM (
         SELECT ds.id ds_id, rra.id rra_id, latest - interval '1 millisecond' * ds.step_ms * rra.steps_per_row *
            mod(rra.size + mod(extract(epoch from rra.latest)::bigint*1000/(ds.step_ms * rra.steps_per_row), size) + 1
           - (generate_subscripts(dp,1) + n * width), rra.size) AS t,
          extract(epoch from (latest - interval '1 millisecond' * ds.step_ms * rra.steps_per_row *
          mod(rra.size + mod(extract(epoch from rra.latest)::bigint*1000/(ds.step_ms * rra.steps_per_row), size) + 1
           - (generate_subscripts(dp,1) + n * width), rra.size))) AS tu,
          UNNEST(dp) AS r,
          interval '1 millisecond' * ds.step_ms * rra.steps_per_row AS step,
          n AS row,
          generate_subscripts(dp,1) AS row_n,
          generate_subscripts(dp,1) + n * width AS abs_n,
          mod(extract(epoch from rra.latest)::bigint*1000/(ds.step_ms * rra.steps_per_row), size) + 1 AS last_n,
          extract(epoch from rra.latest)::bigint*1000 AS last_t,
            mod(rra.size + mod(extract(epoch from rra.latest)::bigint*1000/(ds.step_ms * rra.steps_per_row), size) + 1
           - (generate_subscripts(dp,1) + n * width), rra.size) AS slot_distance
       FROM ds
       INNER JOIN rra ON rra.ds_id = ds.id
       INNER JOIN ts ON ts.rra_id = rra.id) foo;
    `

	if rows, err := dbConn.Query(create_sql); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			log.Printf("ERROR: initial CREATE VIEW failed: %v", err)
			return err
		}
	} else {
		rows.Close()
	}

	create_sql = `
       CREATE UNIQUE INDEX idx_ds_name ON ds (name);
    `
	// There is no IF NOT EXISTS for CREATE INDEX until 9.5
	if rows, err := dbConn.Query(create_sql); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			log.Printf("ERROR: initial CREATE INDEX failed: %v", err)
			return err
		}
	} else {
		rows.Close()
	}

	create_sql = `
       CREATE UNIQUE INDEX idx_rra_rra_id_n ON ts (rra_id, n);
    `
	if rows, err := dbConn.Query(create_sql); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			log.Printf("ERROR: initial CREATE INDEX failed: %v", err)
			return err
		}
	} else {
		rows.Close()
	}
	return nil
}

// This implements the Series interface

type trDbSeries struct {
	ds  *trDataSource
	rra *trRoundRobinArchive

	// Current Value
	value    float64
	posBegin time.Time // pos begins after
	posEnd   time.Time // pos ends on

	// Time boundary
	from *time.Time
	to   *time.Time

	// Db stuff
	rows *sql.Rows

	// These are not the same:
	maxPoints int64 // max points we want
	groupByMs int64 // requested alignment

	latest time.Time

	// Alias
	alias *string
}

func (dps *trDbSeries) StepMs() int64 {
	return dps.ds.StepMs * int64(dps.rra.StepsPerRow)
}

func (dps *trDbSeries) SetGroupByMs(ms int64) {
	dps.groupByMs = ms
}

func (dps *trDbSeries) GroupByMs() int64 {
	if dps.groupByMs == 0 {
		return dps.StepMs()
	}
	return dps.groupByMs
}

func (dps *trDbSeries) Align() {}

func (dps *trDbSeries) Alias(s ...string) *string {
	if len(s) > 0 {
		dps.alias = &s[0]
	}
	return dps.alias
}

func (dps *trDbSeries) seriesQuerySqlUsingViewAndSeries() (*sql.Rows, error) {

	var (
		rows *sql.Rows
		err  error
	)

	var (
		finalGroupByMs int64
		rraStepMs      = dps.ds.StepMs * int64(dps.rra.StepsPerRow)
	)

	if dps.maxPoints != 0 {
		finalGroupByMs = (dps.to.Unix() - dps.from.Unix()) * 1000 / dps.maxPoints
		finalGroupByMs = finalGroupByMs/rraStepMs*rraStepMs + rraStepMs
	} else {
		finalGroupByMs = rraStepMs
	}

	if dps.groupByMs != 0 { // Specific granularity was requested for alignment
		finalGroupByMs = finalGroupByMs/dps.groupByMs*dps.groupByMs + dps.groupByMs
	}

	// TODO: support milliseconds?
	aligned_from := time.Unix(dps.from.Unix()/(finalGroupByMs/1000)*(finalGroupByMs/1000), 0)

	rows, err = sql3.Query(aligned_from, *dps.to, fmt.Sprintf("%d milliseconds", rraStepMs), dps.ds.Id, dps.rra.Id, *dps.from, *dps.to, finalGroupByMs)

	if err != nil {
		log.Printf("seriesQuery(): error %v", err)
		return nil, err
	}

	return rows, nil
}

func (dps *trDbSeries) Next() bool {

	if dps.rows == nil { // First Next()
		rows, err := dps.seriesQuerySqlUsingViewAndSeries()
		if err == nil {
			dps.rows = rows
		} else {
			log.Printf("trDbSeries.Next(): database error: %v", err)
			return false
		}
	}

	if dps.rows.Next() {
		if ts, value, err := timeValueFromRow(dps.rows); err != nil {
			log.Printf("trDbSeries.Next(): database error: %v", err)
			return false
		} else {
			dps.posBegin = dps.latest
			dps.posEnd = ts
			dps.value = value
			dps.latest = dps.posEnd
		}
		return true
	} else {
		// See if we have data points that haven't been synced yet
		if len(dps.rra.DPs) > 0 && dps.latest.Before(dps.rra.Latest) {
			// TODO this is kinda ugly?
			// Should RRA's implement Series interface perhaps?

			// because rra.DPs is a map there is no quick way to find the
			// earliest entry, we have to traverse the map. It seems
			// tempting to come with an alternative solution, but it's not
			// as simple as it seems, and given that this is mostly about
			// the tip of the series, this is good enough.

			// we do not provide averaging points here for the same reason

			for len(dps.rra.DPs) > 0 {

				earliest := dps.rra.Latest.Add(time.Millisecond)
				earliestSlotN := int64(-1)
				for n, _ := range dps.rra.DPs {
					ts := dps.rra.slotTimeStamp(dps.ds, n)
					if ts.Before(earliest) && ts.After(dps.latest) {
						earliest, earliestSlotN = ts, n
					}
				}
				if earliestSlotN != -1 {

					dps.posBegin = dps.latest
					dps.posEnd = earliest
					dps.value = dps.rra.DPs[earliestSlotN]
					dps.latest = earliest

					delete(dps.rra.DPs, earliestSlotN)

					var from, to time.Time

					if dps.from == nil {
						from = time.Unix(0, 0)
					} else {
						from = *dps.from
					}
					if dps.to == nil {
						to = dps.rra.Latest.Add(time.Millisecond)
					} else {
						to = *dps.to
					}
					if earliest.Add(time.Millisecond).After(from) && earliest.Before(to.Add(time.Millisecond)) {
						return true
					}
				} else {
					return false
				}
			}
		}
	}
	return false
}

func (dps *trDbSeries) CurrentValue() float64 {
	return dps.value
}

func (dps *trDbSeries) CurrentPosBeginsAfter() time.Time {
	return dps.posBegin
}

func (dps *trDbSeries) CurrentPosEndsOn() time.Time {
	return dps.posEnd
}

func (dps *trDbSeries) Close() error {
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

func dataSourceFromRow(rows *sql.Rows) (*trDataSource, error) {
	var (
		ds         trDataSource
		last_ds    sql.NullFloat64
		lastupdate pq.NullTime
	)
	err := rows.Scan(&ds.Id, &ds.Name, &ds.StepMs, &ds.HeartbeatMs, &lastupdate, &last_ds, &ds.Value, &ds.UnknownMs)
	if err != nil {
		log.Printf("dataSourceFromRow(): error scanning row: %v", err)
		return nil, err
	}
	if last_ds.Valid {
		ds.LastDs = last_ds.Float64
	} else {
		ds.LastDs = math.NaN()
	}
	if lastupdate.Valid {
		ds.LastUpdate = lastupdate.Time
	} else {
		ds.LastUpdate = time.Unix(0, 0)
	}
	return &ds, err
}

func roundRobinArchiveFromRow(rows *sql.Rows) (*trRoundRobinArchive, error) {
	var (
		latest pq.NullTime
		rra    trRoundRobinArchive
	)
	err := rows.Scan(&rra.Id, &rra.DsId, &rra.Cf, &rra.StepsPerRow, &rra.Size, &rra.Width, &rra.Xff, &rra.Value, &rra.UnknownMs, &latest)
	if err != nil {
		log.Printf("roundRoundRobinArchiveFromRow(): error scanning row: %v", err)
		return nil, err
	}
	if latest.Valid {
		rra.Latest = latest.Time
	} else {
		rra.Latest = time.Unix(0, 0)
	}
	rra.DPs = make(map[int64]float64)
	return &rra, err
}

func fetchDataSources() (map[string]*trDataSource, map[int64]*trDataSource, map[string]bool, error) {

	const sql = `SELECT id, name, step_ms, heartbeat_ms, lastupdate, last_ds, value, unknown_ms FROM ds`

	rows, err := dbConn.Query(sql)
	if err != nil {
		log.Printf("fetchDataSources(): error querying database: %v", err)
		return nil, nil, nil, err
	}
	defer rows.Close()

	byName := make(map[string]*trDataSource)
	byId := make(map[int64]*trDataSource)
	prefixes := make(map[string]bool)
	for rows.Next() {
		ds, err := dataSourceFromRow(rows)
		rras, err := fetchRoundRobinArchives(ds.Id)
		if err != nil {
			log.Printf("fetchDataSources(): error fetching RRAs: %v", err)
			return nil, nil, nil, err
		} else {
			ds.RRAs = rras
		}

		// TODO this replicated functionality in dss.insert()

		byName[ds.Name] = ds
		byId[ds.Id] = ds

		prefix := ds.Name
		for ext := filepath.Ext(prefix); ext != ""; {
			prefix = ds.Name[0 : len(prefix)-len(ext)]
			prefixes[prefix] = true
			ext = filepath.Ext(prefix)
		}
	}

	return byName, byId, prefixes, nil
}

func fetchRoundRobinArchives(ds_id int64) ([]*trRoundRobinArchive, error) {

	const sql = `SELECT id, ds_id, cf, steps_per_row, size, width, xff, value, unknown_ms, latest FROM rra WHERE ds_id = $1`

	rows, err := dbConn.Query(sql, ds_id)
	if err != nil {
		log.Printf("fetchRoundRobinArchives(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rras []*trRoundRobinArchive
	for rows.Next() {
		if rra, err := roundRobinArchiveFromRow(rows); err == nil {
			rras = append(rras, rra)
		} else {
			log.Printf("fetchRoundRobinArchives(): error: %v", err)
			return nil, err
		}
	}

	return rras, nil
}

func dpsAsString(dps map[int64]float64, start, end int64) string {
	var b bytes.Buffer
	b.WriteString("{")
	for i := start; i <= end; i++ {
		b.WriteString(strconv.FormatFloat(dps[int64(i)], 'f', -1, 64))
		if i != end {
			b.WriteString(",")
		}
	}
	b.WriteString("}")
	return b.String()
}

func flushRoundRobinArchive(rra *trRoundRobinArchive) error {

	var n int64
	rraSize := int64(rra.Size)
	if int32(len(rra.DPs)) == rra.Size { // The whole thing
		for n = 0; n < rra.slotRow(rraSize); n++ {
			end := rra.Width - 1
			if n == rraSize/int64(rra.Width) {
				end = (rraSize - 1) % rra.Width
			}
			dps := dpsAsString(rra.DPs, n*int64(rra.Width), n*int64(rra.Width)+rra.Width-1)
			if rows, err := sql1.Query(1, end+1, dps, rra.Id, n); err == nil {
				rows.Close()
			} else {
				return err
			}
		}
	} else if rra.start <= rra.end { // Single range
		for n = rra.start / int64(rra.Width); n < rra.slotRow(rra.end); n++ {
			start, end := int64(0), rra.Width-1
			if n == rra.start/rra.Width {
				start = rra.start % rra.Width
			}
			if n == rra.end/rra.Width {
				end = rra.end % rra.Width
			}
			dps := dpsAsString(rra.DPs, n*rra.Width+start, n*rra.Width+end)
			if rows, err := sql1.Query(start+1, end+1, dps, rra.Id, n); err == nil {
				rows.Close()
			} else {
				return err
			}
		}
	} else { // Double range (wrap-around, end < start)
		// range 1: 0 -> end
		for n = 0; n < rra.slotRow(rra.end); n++ {
			start, end := int64(0), rra.Width-1
			if n == rra.end/rra.Width {
				end = rra.end % rra.Width
			}
			dps := dpsAsString(rra.DPs, n*rra.Width+start, n*rra.Width+end)
			if rows, err := sql1.Query(start+1, end+1, dps, rra.Id, n); err == nil {
				rows.Close()
			} else {
				return err
			}
		}

		// range 2: start -> Size
		for n = rra.start / rra.Width; n < rra.slotRow(rraSize); n++ {
			start, end := int64(0), rra.Width-1
			if n == rra.start/rra.Width {
				start = rra.start % rra.Width
			}
			if n == rraSize/rra.Width {
				end = (rraSize - 1) % rra.Width
			}
			dps := dpsAsString(rra.DPs, n*rra.Width+start, n*rra.Width+end)
			if rows, err := sql1.Query(start+1, end+1, dps, rra.Id, n); err == nil {
				rows.Close()
			} else {
				return err
			}
		}
	}

	if rows, err := sql2.Query(rra.Value, rra.UnknownMs, rra.Latest, rra.Id); err == nil {
		rows.Close()
	} else {
		return err
	}

	return nil
}

func flushDataSource(ds *trDataSource) error {

	for _, rra := range ds.RRAs {
		if len(rra.DPs) > 0 {
			if err := flushRoundRobinArchive(rra); err != nil {
				log.Printf("flushDataSource(): error flushing RRA, probable data loss: %v", err)
				return err
			}
		}
	}

	const sql = "UPDATE ds SET lastupdate = $1, last_ds = $2, value = $3, unknown_ms = $4 WHERE id = $5"
	if rows, err := dbConn.Query(sql, ds.LastUpdate, ds.LastDs, ds.Value, ds.UnknownMs, ds.Id); err != nil {
		log.Printf("flushDataSource(): database error: %v", err)
	} else {
		rows.Close()
	}

	return nil
}

func createDataSource(name string, dsSpec *trDSSpec) (*trDataSource, error) {
	rows, err := sql4.Query(name, dsSpec.Step.Duration.Nanoseconds()/1000000, dsSpec.Heartbeat.Duration.Nanoseconds()/1000000)
	if err != nil {
		log.Printf("createDataSources(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()
	rows.Next()
	ds, err := dataSourceFromRow(rows)
	if err != nil {
		log.Printf("createDataSources(): error: %v", err)
		return nil, err
	}

	// RRAs
	for _, rraSpec := range dsSpec.RRAs {
		steps := rraSpec.Step.Nanoseconds() / (ds.StepMs * 1000000)
		size := rraSpec.Size.Nanoseconds() / rraSpec.Step.Nanoseconds()

		rraRows, err := sql5.Query(ds.Id, rraSpec.Function, steps, size, rraSpec.Xff)
		if err != nil {
			log.Printf("createDataSources(): error creating RRAs: %v", err)
			return nil, err
		}
		rraRows.Next()
		rra, err := roundRobinArchiveFromRow(rraRows)
		if err != nil {
			log.Printf("createDataSources(): error2: %v", err)
			return nil, err
		}
		ds.RRAs = append(ds.RRAs, rra)

		for n := int64(0); n <= (int64(rra.Size)/rra.Width + int64(rra.Size)%rra.Width/rra.Width); n++ {
			r, err := sql6.Query(rra.Id, n)
			if err != nil {
				log.Printf("createDataSources(): error creating TSs: %v", err)
				return nil, err
			}
			r.Close()
		}

		rraRows.Close()
	}

	return ds, nil
}

//
// Data layout notes.
//
// So we store datapoints as an array, and we know the latest
// timestamp. Every time we advance to the next point, so does the
// latest timestamp. By knowing the latest timestamp and the size of
// the array, we can identify which array element is last, it is:
// slots_since_epoch % slots
//
// If we take a slot with a number slot_n, it's distance from the
// latest slot can be calculated by this formula:
//
// distance = (total_slots + last_slot_n - slot_n) % total_slots
//
// E.g. with total_slots 100, slot_n 55 and last_slot_n 50:
//
// (100 + 50 - 55) % 100 => 95
//
// This means that if we advance forward from 55 by 95 slots, which
// means at step 45 we'll reach the end of the array, and start from
// the beginning, we'll arrive at 50.
//
// Or with total_slots 100, slot_n 45 and last_slot_n 50:
//
// (100 + 50 - 45) % 100 => 5
//

func seriesQuerySql(dsId, rraId int64, from, to *time.Time, interval int64) (*sql.Rows, error) {

	// TODO Document this, then delete this method.

	var (
		// Hopefully this will make it a little more decipherable
		lastSlotUnixTime       = "extract(epoch from rra.latest)::bigint*1000"
		slotDurationMs         = "(ds.step_ms * rra.steps_per_row)"
		lastSlotInSlots        = lastSlotUnixTime + "/" + slotDurationMs
		currentSlotN           = "(generate_subscripts(dp,1) + n * width)"
		lastSlotN              = "mod(" + lastSlotInSlots + ", size) + 1"
		distanceInSlots        = "mod(rra.size + " + lastSlotN + " - " + currentSlotN + ", rra.size)"
		slotDurationAsInterval = "interval '1 millisecond' * ds.step_ms * rra.steps_per_row"
		selectSql              = "SELECT latest - " + slotDurationAsInterval + " * " + distanceInSlots + " AS t, " +
			"UNNEST(dp) AS v " +
			"FROM ds, rra, ts WHERE rra.ds_id = ds.id AND ts.rra_id = rra.id AND ds.id = $1 AND rra.id = $2 "
		filterAndGroupSql = "SELECT MAX(t) mt, AVG(v) av FROM (" + selectSql + ") tv"
	)

	var (
		rows *sql.Rows
		err  error
	)
	if from == nil && to == nil {
		rows, err = dbConn.Query(filterAndGroupSql, dsId, rraId)
	} else if from != nil && to == nil {
		rows, err = dbConn.Query(filterAndGroupSql+" WHERE t >= $3 GROUP BY trunc((extract(epoch from t)-1)::bigint/$4) ORDER BY mt", dsId, rraId, *from, interval)
	} else if to != nil && from == nil {
		rows, err = dbConn.Query(filterAndGroupSql+" WHERE t <= $3 GROUP BY trunc((extract(epoch from t)-1)::bigint/$4) ORDER BY mt", dsId, rraId, *to, interval)
	} else {
		rows, err = dbConn.Query(filterAndGroupSql+" WHERE t >= $3 AND t <= $4 GROUP BY trunc((extract(epoch from t)-1)::bigint/$5) ORDER BY mt", dsId, rraId, *from, *to, interval)
	}

	if err != nil {
		log.Printf("seriesQuery(): error %v, sql: %v", err, filterAndGroupSql)
		return nil, err
	}

	return rows, nil
}

func seriesQuery(ds *trDataSource, from, to *time.Time, maxPoints int64) (Series, error) {

	rra := ds.bestRRA(*from, *to, maxPoints)

	// If from/to are nil - assign the rra boundaries
	rraEarliest := time.Unix(rra.getStartGivenEndMs(ds, rra.Latest.Unix()*1000)/1000, 0)

	if from == nil || rraEarliest.After(*from) {
		from = &rraEarliest
	}
	if to == nil || to.After(rra.Latest) {
		to = &rra.Latest
	}

	dps := &trDbSeries{ds: ds, rra: rra, from: from, to: to, maxPoints: maxPoints}
	return Series(dps), nil
}
