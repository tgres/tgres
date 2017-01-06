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

//
// Data layout notes.
//
// So we store datapoints as an array, and we know the latest
// timestamp. Every time we advance to the next point, so does the
// latest timestamp. By knowing the latest timestamp and the size of
// the array, we can identify which array element is last, it is:
// slots_since_epoch % slots
//
// If we take a slot with a number slot_n, its distance from the
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

package serde

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/series"
)

var debug bool

func init() {
	debug = os.Getenv("TGRES_SERDE_DEBUG") != ""
}

type pgSerDe struct {
	dbConn                                         *sql.DB
	sql1, sql2, sql3, sql4, sql5, sql6, sql7, sql8 *sql.Stmt
	prefix                                         string
}

func sqlOpen(a, b string) (*sql.DB, error) {
	return sql.Open(a, b)
}

func InitDb(connect_string, prefix string) (*pgSerDe, error) {
	if dbConn, err := sql.Open("postgres", connect_string); err != nil {
		return nil, err
	} else {
		p := &pgSerDe{dbConn: dbConn, prefix: prefix}
		if err := p.dbConn.Ping(); err != nil {
			return nil, err
		}
		if err := p.createTablesIfNotExist(); err != nil {
			return nil, err
		}
		if err := p.prepareSqlStatements(); err != nil {
			return nil, err
		}
		return p, nil
	}
}

func (p *pgSerDe) Fetcher() Fetcher         { return p }
func (p *pgSerDe) Flusher() Flusher         { return p }
func (p *pgSerDe) DbAddresser() DbAddresser { return p }

// A hack to use the DB to see who else is connected
func (p *pgSerDe) ListDbClientIps() ([]string, error) {
	const sql = "SELECT DISTINCT(client_addr) FROM pg_stat_activity"
	rows, err := p.dbConn.Query(sql)
	if err != nil {
		log.Printf("ListDbClientIps(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	result := make([]string, 0)
	for rows.Next() {
		var addr *string
		if err := rows.Scan(&addr); err != nil {
			log.Printf("ListDbClientIps(): error scanning row: %v", err)
			return nil, err
		}
		if addr != nil {
			result = append(result, *addr)
		}
	}
	return result, nil
}

func (p *pgSerDe) MyDbAddr() (*string, error) {
	hostname, _ := os.Hostname()
	randToken := fmt.Sprintf("%s%d", hostname, rand.Intn(1000000000))
	sql := fmt.Sprintf("SELECT client_addr FROM pg_stat_activity WHERE query LIKE '%%%s%%'", randToken)
	rows, err := p.dbConn.Query(sql)
	if err != nil {
		log.Printf("myPostgresAddr(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		var addr *string
		if err := rows.Scan(&addr); err != nil {
			log.Printf("myPostgresAddr(): error scanning row: %v", err)
			return nil, err
		}
		if addr != nil {
			log.Printf("myPostgresAddr(): %s", *addr)
			return addr, nil
		}
	}
	return nil, nil
}

func (p *pgSerDe) prepareSqlStatements() error {
	var err error

	if p.sql1, err = p.dbConn.Prepare(fmt.Sprintf("INSERT INTO %[1]sts AS ts (dp[$1:$2], rra_id, n) VALUES($3, $4, $5) "+
		"ON CONFLICT(rra_id, n) DO UPDATE SET dp[$1:$2] = $3 WHERE ts.rra_id = $4 AND ts.n = $5", p.prefix)); err != nil {
		return err
	}
	// legacy UPDATE version
	// if p.sql1, err = p.dbConn.Prepare(fmt.Sprintf("UPDATE %[1]sts ts SET dp[$1:$2] = $3 WHERE rra_id = $4 AND n = $5", p.prefix)); err != nil {
	// 	return err
	// }

	if p.sql2, err = p.dbConn.Prepare(fmt.Sprintf("UPDATE %[1]srra rra SET value = $1, duration_ms = $2, latest = $3 WHERE id = $4", p.prefix)); err != nil {
		return err
	}
	if p.sql3, err = p.dbConn.Prepare(fmt.Sprintf("SELECT max(tg) mt, avg(r) ar FROM generate_series($1, $2, ($3)::interval) AS tg "+
		"LEFT OUTER JOIN (SELECT t, r FROM %[1]stv tv WHERE ds_id = $4 AND rra_id = $5 "+
		" AND t >= $6 AND t <= $7) s ON tg = s.t GROUP BY trunc((extract(epoch from tg)*1000-1))::bigint/$8 ORDER BY mt",
		p.prefix)); err != nil {
		return err
	}
	if p.sql4, err = p.dbConn.Prepare(fmt.Sprintf("INSERT INTO %[1]sds AS ds (ident, step_ms, heartbeat_ms) VALUES ($1, $2, $3) "+
		// PG 9.5 required. NB: DO NOTHING causes RETURNING to return nothing, so we're using this dummy UPDATE to work around.
		"ON CONFLICT (ident) DO UPDATE SET step_ms = ds.step_ms "+
		"RETURNING id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms", p.prefix)); err != nil {
		return err
	}
	if p.sql5, err = p.dbConn.Prepare(fmt.Sprintf("INSERT INTO %[1]srra AS rra (ds_id, cf, steps_per_row, size, xff) VALUES ($1, $2, $3, $4, $5) "+
		"ON CONFLICT (ds_id, cf, steps_per_row, size) DO UPDATE SET ds_id = rra.ds_id "+
		"RETURNING id, ds_id, cf, steps_per_row, size, width, xff, value, duration_ms, latest", p.prefix)); err != nil {
		return err
	}
	// With sql1 UPSERT this is no longer necessary
	// if p.sql6, err = p.dbConn.Prepare(fmt.Sprintf("INSERT INTO %[1]sts (rra_id, n) VALUES ($1, $2) ON CONFLICT(rra_id, n) DO NOTHING",
	// 	p.prefix)); err != nil {
	// 	return err
	// }
	if p.sql7, err = p.dbConn.Prepare(fmt.Sprintf("UPDATE %[1]sds SET lastupdate = $1, value = $2, duration_ms = $3 WHERE id = $4", p.prefix)); err != nil {
		return err
	}
	if p.sql8, err = p.dbConn.Prepare(fmt.Sprintf("SELECT id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms FROM %[1]sds AS ds WHERE id = $1",
		p.prefix)); err != nil {
		return err
	}
	// // TODO This is WRONG, a "ident ->>" is NOT searchable using index, it must be ident @> '{"name":$1}'
	// if p.sql9, err = p.dbConn.Prepare(fmt.Sprintf("SELECT id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms FROM %[1]sds AS ds WHERE ident ->> 'name' = $1",
	// 	p.prefix)); err != nil {
	// 	return err
	// }

	return nil
}

func (p *pgSerDe) createTablesIfNotExist() error {
	create_sql := `
       CREATE TABLE IF NOT EXISTS %[1]sds (
       id SERIAL NOT NULL PRIMARY KEY,
       ident JSONB NOT NULL DEFAULT '{}' CONSTRAINT nonempty_ident CHECK (ident <> '{}'),
       step_ms BIGINT NOT NULL,
       heartbeat_ms BIGINT NOT NULL,
       lastupdate TIMESTAMPTZ,
       value DOUBLE PRECISION NOT NULL DEFAULT 'NaN',
       duration_ms BIGINT NOT NULL DEFAULT 0);

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_ds_ident_uniq ON %[1]sds (ident);
       CREATE INDEX IF NOT EXISTS %[1]sidx_ds_ident ON %[1]sds USING gin(ident);

       CREATE TABLE IF NOT EXISTS %[1]srra (
       id SERIAL NOT NULL PRIMARY KEY,
       ds_id INT NOT NULL REFERENCES %[1]sds(id) ON DELETE CASCADE,
       cf TEXT NOT NULL,
       steps_per_row INT NOT NULL,
       size INT NOT NULL,
       width INT NOT NULL DEFAULT 768,
       xff REAL NOT NULL,
       value DOUBLE PRECISION NOT NULL DEFAULT 'NaN',
       duration_ms BIGINT NOT NULL DEFAULT 0,
       latest TIMESTAMPTZ DEFAULT NULL);

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_rra_spec ON %[1]srra (ds_id, cf, steps_per_row, size);

       CREATE TABLE IF NOT EXISTS %[1]sts (
       rra_id INT NOT NULL REFERENCES %[1]srra(id) ON DELETE CASCADE,
       n INT NOT NULL,
       dp DOUBLE PRECISION[] NOT NULL DEFAULT '{}');

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]s_idx_ts_rra_id_n ON %[1]sts (rra_id, n);
    `
	if rows, err := p.dbConn.Query(fmt.Sprintf(create_sql, p.prefix)); err != nil {
		log.Printf("ERROR: initial CREATE TABLE failed: %v", err)
		return err
	} else {
		rows.Close()
	}
	create_sql = `
       CREATE VIEW %[1]stv AS
       SELECT ds.id ds_id, rra.id rra_id, latest - interval '1 millisecond' * ds.step_ms * rra.steps_per_row *
            mod(rra.size + mod(extract(epoch from rra.latest)::bigint*1000/(ds.step_ms * rra.steps_per_row), size) + 1
           - (generate_subscripts(dp,1) + n * width), rra.size) AS t,
          UNNEST(dp) AS r
       FROM %[1]sds ds
       INNER JOIN %[1]srra rra ON rra.ds_id = ds.id
       INNER JOIN %[1]sts ts ON ts.rra_id = rra.id;

       CREATE VIEW %[1]stvd AS
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
       FROM %[1]sds ds
       INNER JOIN %[1]srra rra ON rra.ds_id = ds.id
       INNER JOIN %[1]sts ts ON ts.rra_id = rra.id) foo;
    `
	if rows, err := p.dbConn.Query(fmt.Sprintf(create_sql, p.prefix)); err != nil {
		if !strings.Contains(err.Error(), "already exists") {
			log.Printf("ERROR: initial CREATE VIEW failed: %v", err)
			return err
		}
	} else {
		rows.Close()
	}

	return nil
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

func dataSourceFromRow(rows *sql.Rows) (*DbDataSource, error) {
	var (
		lastupdate               *time.Time
		durationMs, stepMs, hbMs int64
		value                    float64
		id                       int64
		identJson                []byte
		ident                    IdentTags
	)
	err := rows.Scan(&id, &identJson, &stepMs, &hbMs, &lastupdate, &value, &durationMs)
	if err != nil {
		log.Printf("dataSourceFromRow(): error scanning row: %v", err)
		return nil, err
	}

	if lastupdate == nil {
		lastupdate = &time.Time{}
	}

	err = json.Unmarshal(identJson, &ident)
	if err != nil {
		log.Printf("dataSourceFromRow(): error unmarshalling ident: %v", err)
		return nil, err
	}

	ds := NewDbDataSource(id, ident,
		rrd.NewDataSource(
			rrd.DSSpec{
				Step:       time.Duration(stepMs) * time.Millisecond,
				Heartbeat:  time.Duration(hbMs) * time.Millisecond,
				LastUpdate: *lastupdate,
				Value:      value,
				Duration:   time.Duration(durationMs) * time.Millisecond,
			},
		),
	)

	return ds, nil
}

func roundRobinArchiveFromRow(rows *sql.Rows, dsStep time.Duration) (*DbRoundRobinArchive, error) {
	var (
		latest *time.Time
		cf     string
		value  float64
		xff    float32

		id, dsId, durationMs, width, stepsPerRow, size int64
	)
	err := rows.Scan(&id, &dsId, &cf, &stepsPerRow, &size, &width, &xff, &value, &durationMs, &latest)
	if err != nil {
		log.Printf("roundRoundRobinArchiveFromRow(): error scanning row: %v", err)
		return nil, err
	}

	if latest == nil {
		latest = &time.Time{}
	}

	spec := rrd.RRASpec{
		Step:     time.Duration(stepsPerRow) * dsStep,
		Span:     time.Duration(stepsPerRow*size) * dsStep,
		Xff:      xff,
		Latest:   *latest,
		Value:    value,
		Duration: time.Duration(durationMs) * time.Millisecond,
	}

	switch cf {
	case "WMEAN":
		spec.Function = rrd.WMEAN
	case "MIN":
		spec.Function = rrd.MIN
	case "MAX":
		spec.Function = rrd.MAX
	case "LAST":
		spec.Function = rrd.LAST
	default:
		return nil, fmt.Errorf("roundRoundRobinArchiveFromRow(): Invalid cf: %q (valid funcs: wmean, min, max, last)", cf)
	}

	rra, err := NewDbRoundRobinArchive(id, width, spec)
	if err != nil {
		log.Printf("roundRoundRobinArchiveFromRow(): error creating rra: %v", err)
		return nil, err
	}

	return rra, nil
}

type pgSearchResult struct {
	rows  *sql.Rows
	err   error
	id    int64
	ident map[string]string
}

func (sr *pgSearchResult) Next() bool {
	if !sr.rows.Next() {
		return false
	}

	var b []byte
	sr.err = sr.rows.Scan(&sr.id, &b)
	if sr.err != nil {
		log.Printf("pgSearchResult.Next(): error scanning row: %v", sr.err)
		return false
	}
	sr.err = json.Unmarshal(b, &sr.ident)
	if sr.err != nil {
		log.Printf("Search(): error unmarshalling ident %q: %v", string(b), sr.err)
		return false
	}
	return true
}

func (sr *pgSearchResult) Id() int64        { return sr.id }
func (sr *pgSearchResult) Ident() IdentTags { return sr.ident }
func (sr *pgSearchResult) Close() error     { return sr.rows.Close() }

func buildSearchWhere(query map[string]string) (string, []interface{}) {
	var (
		where string
		args  []interface{}
	)

	n := 0
	for k, v := range query {
		if n > 0 {
			where += " AND "
		}
		// The "ident ?" ensures that we take advantage of the gin
		// index because a ->> cannot use it. If our only tag is
		// "name" and it exists for every DS, then this doesn't really
		// accomplish anyhting, but it will help somewhat when we have
		// varying tags.
		where += fmt.Sprintf("ident ? $%d AND ident ->> $%d ~* $%d", n+1, n+1, n+2)
		args = append(args, k, v)
		n += 2
	}

	return where, args
}

// Given a query in the form of ident keys and regular expressions for
// values, return all matching idents and the ds ids.
func (p *pgSerDe) Search(query SearchQuery) (SearchResult, error) {

	var (
		sql   = `SELECT id, ident FROM %[1]sds ds`
		where string
		args  []interface{}
	)

	if where, args = buildSearchWhere(query); len(args) > 0 {
		sql += fmt.Sprintf(" WHERE %s", where)
	}

	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix), args...)
	if err != nil {
		log.Printf("Search(): error querying database: %v", err)
		return nil, err
	}

	return &pgSearchResult{rows: rows}, nil
}

func (p *pgSerDe) FetchDataSourceById(id int64) (rrd.DataSourcer, error) {

	rows, err := p.sql8.Query(id)
	if err != nil {
		log.Printf("FetchDataSourceById(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		ds, err := dataSourceFromRow(rows)
		if err != nil {
			log.Printf("FetchDataSourceById(): error scanning DS: %v", err)
			return nil, err
		}
		rras, err := p.fetchRoundRobinArchives(ds)
		if err != nil {
			log.Printf("FetchDataSourceById(): error fetching RRAs: %v", err)
			return nil, err
		} else {
			ds.SetRRAs(rras)
		}
		return ds, nil
	}

	return nil, nil
}

// func (p *pgSerDe) FetchDataSourceByName(name string) (*DbDataSource, error) {

// 	rows, err := p.sql9.Query(name)
// 	if err != nil {
// 		log.Printf("FetchDataSourceByName(): error querying database: %v", err)
// 		return nil, err
// 	}
// 	defer rows.Close()

// 	if rows.Next() {
// 		ds, err := dataSourceFromRow(rows)
// 		rras, err := p.fetchRoundRobinArchives(ds)
// 		if err != nil {
// 			log.Printf("FetchDataSourceByName(): error fetching RRAs: %v", err)
// 			return nil, err
// 		} else {
// 			ds.SetRRAs(rras)
// 		}
// 		return ds, nil
// 	}

// 	return nil, nil
// }

func (p *pgSerDe) FetchDataSources() ([]rrd.DataSourcer, error) {

	const sql = `SELECT id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms FROM %[1]sds ds`

	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix))
	if err != nil {
		log.Printf("FetchDataSources(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	result := make([]rrd.DataSourcer, 0)
	for rows.Next() {
		ds, err := dataSourceFromRow(rows)
		rras, err := p.fetchRoundRobinArchives(ds)
		if err != nil {
			log.Printf("FetchDataSources(): error fetching RRAs: %v", err)
			return nil, err
		} else {
			ds.SetRRAs(rras)
		}
		result = append(result, ds)
	}

	return result, nil
}

func (p *pgSerDe) fetchRoundRobinArchives(ds *DbDataSource) ([]rrd.RoundRobinArchiver, error) {

	const sql = `SELECT id, ds_id, cf, steps_per_row, size, width, xff, value, duration_ms, latest FROM %[1]srra rra WHERE ds_id = $1`

	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix), ds.Id())
	if err != nil {
		log.Printf("fetchRoundRobinArchives(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rras []rrd.RoundRobinArchiver
	for rows.Next() {
		if rra, err := roundRobinArchiveFromRow(rows, ds.Step()); err == nil {
			rras = append(rras, rra)
		} else {
			log.Printf("fetchRoundRobinArchives(): error: %v", err)
			return nil, err
		}
	}

	return rras, nil
}

func (p *pgSerDe) flushRoundRobinArchive(rra DbRoundRobinArchiver) error {
	var n int64
	rraSize, rraWidth := rra.Size(), rra.Width()
	rraStart, rraEnd := rra.Start(), rra.End()
	if int64(rra.PointCount()) == rraSize { // The whole thing
		for n = 0; n < rra.SlotRow(rraSize); n++ {
			end := rraWidth - 1
			if n == rraSize/rraWidth {
				end = (rraSize - 1) % rraWidth
			}
			dps := rra.DPsAsPGString(n*rraWidth, n*rraWidth+rraWidth-1)
			if rows, err := p.sql1.Query(1, end+1, dps, rra.Id(), n); err == nil {
				if debug {
					log.Printf("flushRoundRobinArchive(1): rra.Id: %d rraStart: %d rra.End: %d params: s: %d e: %d len: %d n: %d", rra.Id(), rraStart, rraEnd, 1, end+1, len(dps), n)
				}
				rows.Close()
			} else {
				return err
			}
		}
	} else if rraStart <= rraEnd { // Single range
		for n = rraStart / rraWidth; n < rra.SlotRow(rraEnd); n++ {
			start, end := int64(0), rraWidth-1
			if n == rraStart/rraWidth {
				start = rraStart % rraWidth
			}
			if n == rraEnd/rraWidth {
				end = rraEnd % rraWidth
			}
			dps := rra.DPsAsPGString(n*rraWidth+start, n*rraWidth+end)
			if rows, err := p.sql1.Query(start+1, end+1, dps, rra.Id(), n); err == nil {
				if debug {
					log.Printf("flushRoundRobinArchive(2): rra.Id: %d rraStart: %d rra.End: %d params: s: %d e: %d len: %d n: %d", rra.Id(), rraStart, rraEnd, start+1, end+1, len(dps), n)
				}
				rows.Close()
			} else {
				return err
			}
		}
	} else { // Double range (wrap-around, end < start)
		// range 1: 0 -> end
		for n = 0; n < rra.SlotRow(rraEnd); n++ {
			start, end := int64(0), rraWidth-1
			if n == rraEnd/rraWidth {
				end = rraEnd % rraWidth
			}
			dps := rra.DPsAsPGString(n*rraWidth+start, n*rraWidth+end)
			if rows, err := p.sql1.Query(start+1, end+1, dps, rra.Id(), n); err == nil {
				if debug {
					log.Printf("flushRoundRobinArchive(3): rra.Id: %d rraStart: %d rra.End: %d params: s: %d e: %d len: %d n: %d", rra.Id, rraStart, rraEnd, start+1, end+1, len(dps), n)
				}
				rows.Close()
			} else {
				return err
			}
		}

		// range 2: start -> Size
		for n = rraStart / rraWidth; n < rra.SlotRow(rraSize); n++ {
			start, end := int64(0), rraWidth-1
			if n == rraStart/rraWidth {
				start = rraStart % rraWidth
			}
			if n == rraSize/rraWidth {
				end = (rraSize - 1) % rraWidth
			}
			dps := rra.DPsAsPGString(n*rraWidth+start, n*rraWidth+end)
			if rows, err := p.sql1.Query(start+1, end+1, dps, rra.Id(), n); err == nil {
				if debug {
					log.Printf("flushRoundRobinArchive(4): rra.Id: %d rraStart: %d rra.End: %d params: s: %d e: %d len: %d n: %d", rra.Id(), rraStart, rraEnd, start+1, end+1, len(dps), n)
				}
				rows.Close()
			} else {
				return err
			}
		}
	}

	if rows, err := p.sql2.Query(rra.Value(), rra.Duration().Nanoseconds()/1000000, rra.Latest(), rra.Id()); err == nil {
		rows.Close()
	} else {
		return err
	}

	return nil
}

func (p *pgSerDe) FlushDataSource(ds rrd.DataSourcer) error {
	dbds, ok := ds.(DbDataSourcer)
	if !ok {
		return fmt.Errorf("ds must be a DbDataSourcer to flush.")
	}

	for _, rra := range ds.RRAs() {
		// If this is not a DbRoundRobinArchive, we cannot flush
		drra, ok := rra.(DbRoundRobinArchiver)
		if !ok {
			return fmt.Errorf("rra must be a DbRoundRobinArchiver to flush.")
		}
		if drra.PointCount() > 0 {
			if err := p.flushRoundRobinArchive(drra); err != nil {
				log.Printf("FlushDataSource(): error flushing RRA, probable data loss: %v", err)
				return err
			}
		}
	}

	if debug {
		log.Printf("FlushDataSource(): Id %d: LastUpdate: %v, Value: %v, Duration: %v", dbds.Id(), ds.LastUpdate(), ds.Value(), ds.Duration())
	}
	durationMs := ds.Duration().Nanoseconds() / 1000000
	if rows, err := p.sql7.Query(ds.LastUpdate(), ds.Value(), durationMs, dbds.Id()); err != nil {
		// TODO Check number of rows updated - what if this DS does not exist in the DB?
		log.Printf("FlushDataSource(): database error: %v flushing data source %#v", err, ds)
		return err
	} else {
		rows.Close()
	}

	return nil
}

// FetchOrCreateDataSource loads or returns an existing DS. This is
// done by using upserts first on the ds table, then for each
// RRA. This method also attempt to create the TS empty rows with ON
// CONFLICT DO NOTHING. The returned DS contains no data, to get data
// use FetchSeries().
func (p *pgSerDe) FetchOrCreateDataSource(ident IdentTags, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	var (
		err  error
		rows *sql.Rows
	)
	rows, err = p.sql4.Query(ident.String(), dsSpec.Step.Nanoseconds()/1000000, dsSpec.Heartbeat.Nanoseconds()/1000000)
	if err != nil {
		log.Printf("FetchOrCreateDataSource(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()
	if !rows.Next() {
		log.Printf("FetchOrCreateDataSource(): unable to lookup/create")
		return nil, fmt.Errorf("unable to lookup/create")
	}
	ds, err := dataSourceFromRow(rows)
	if err != nil {
		log.Printf("FetchOrCreateDataSource(): error: %v", err)
		return nil, err
	}

	// RRAs
	var rras []rrd.RoundRobinArchiver
	for _, rraSpec := range dsSpec.RRAs {
		steps := int64(rraSpec.Step / ds.Step())
		size := rraSpec.Span.Nanoseconds() / rraSpec.Step.Nanoseconds()
		var cf string
		switch rraSpec.Function {
		case rrd.WMEAN:
			cf = "WMEAN"
		case rrd.MIN:
			cf = "MIN"
		case rrd.MAX:
			cf = "MAX"
		case rrd.LAST:
			cf = "LAST"
		}
		rraRows, err := p.sql5.Query(ds.Id(), cf, steps, size, rraSpec.Xff)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error creating RRAs: %v", err)
			return nil, err
		}
		rraRows.Next()
		rra, err := roundRobinArchiveFromRow(rraRows, ds.Step())
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error2: %v", err)
			return nil, err
		}
		rras = append(rras, rra)

		// sql1 UPSERT obsoletes the need for this
		// rraSize, rraWidth := rra.Size(), rra.Width()
		// for n := int64(0); n <= (rraSize/rraWidth + rraSize%rraWidth/rraWidth); n++ {
		// 	r, err := p.sql6.Query(rra.Id(), n)
		// 	if err != nil {
		// 		log.Printf("FetchOrCreateDataSource(): error creating TSs: %v", err)
		// 		return nil, err
		// 	}
		// 	r.Close()
		// }

		rraRows.Close()
	}
	ds.SetRRAs(rras)

	if debug {
		log.Printf("FetchOrCreateDataSource(): returning ds.id %d: LastUpdate: %v, %#v", ds.Id(), ds.LastUpdate(), ds)
	}
	return ds, nil
}

func (p *pgSerDe) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {

	rra := ds.BestRRA(from, to, maxPoints)

	// If from/to are nil - assign the rra boundaries
	rraEarliest := rra.Begins(rra.Latest())

	if from.IsZero() || rraEarliest.After(from) {
		from = rraEarliest
	}

	dbds, ok := ds.(DbDataSourcer)
	if !ok {
		return nil, fmt.Errorf("SeriesQuery: ds must be a DbDataSourcer")
	}

	dbrra, ok := rra.(DbRoundRobinArchiver)
	if !ok {
		return nil, fmt.Errorf("SeriesQuery: rra must be a DbRoundRobinArchive")
	}

	// Note that seriesQuerySqlUsingViewAndSeries() will modify "to"
	// to be the earliest of "to" or "LastUpdate".
	dps := &dbSeries{db: p, ds: dbds, rra: dbrra, from: from, to: to, maxPoints: maxPoints}
	return dps, nil
}
