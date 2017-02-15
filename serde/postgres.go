//
// Copyright 2017 Gregory Trubetskoy. All Rights Reserved.
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
// A vertical RRA implementation
//

package serde

import (
	"database/sql"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/series"
)

type pgvSerDe struct {
	dbConn *sql.DB
	prefix string

	sql2, sql3, sql6, sql7, sql8 *sql.Stmt
	sqlSelectDSByIdent           *sql.Stmt
	sqlInsertDS                  *sql.Stmt
	sqlSelectRRAsByDsId          *sql.Stmt
	sqlInsertRRA                 *sql.Stmt
	sqlInsertRRABundle           *sql.Stmt
	sqlSelectRRABundleByStepSize *sql.Stmt
	sqlSelectRRABundle           *sql.Stmt
	sqlInsertRRALatest           *sql.Stmt
	sqlSelectRRALatest           *sql.Stmt
	sqlUpdateRRALatest           *sql.Stmt
	sqlInsertTs                  *sql.Stmt
	sqlUpdateTs                  *sql.Stmt
}

func InitDb(connect_string, prefix string) (*pgvSerDe, error) {
	if dbConn, err := sql.Open("postgres", connect_string); err != nil {
		return nil, err
	} else {
		p := &pgvSerDe{dbConn: dbConn, prefix: prefix}
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

func (p *pgvSerDe) Fetcher() Fetcher                 { return p }
func (p *pgvSerDe) Flusher() Flusher                 { return p }
func (p *pgvSerDe) VerticalFlusher() VerticalFlusher { return p }
func (p *pgvSerDe) DbAddresser() DbAddresser         { return p }

// A hack to use the DB to see who else is connected
func (p *pgvSerDe) ListDbClientIps() ([]string, error) {
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

func (p *pgvSerDe) MyDbAddr() (*string, error) {
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

func (p *pgvSerDe) prepareSqlStatements() error {
	// PG 9.5+ required. DO NOTHING causes RETURNING to return
	// nothing, so we're using this dummy UPDATEs to work around. Note
	// that an INSERT bound to fail ON CONFLICT still increments the
	// sequence, which is not a big problem, but not ideal. To avoid
	// this, it is best to SELECT first, if this returns nothing, then
	// do the INSERT ... ON CONFLICT.

	var err error

	if p.sqlInsertTs, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]sts AS ts (rra_bundle_id, seg, i) VALUES ($1, $2, $3) ON CONFLICT(rra_bundle_id, seg, i) DO NOTHING",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlUpdateTs, err = p.dbConn.Prepare(fmt.Sprintf(
		"UPDATE %[1]sts AS ts SET dp[$4:$5] = $6 WHERE rra_bundle_id = $1 AND seg = $2 AND i = $3",
		p.prefix)); err != nil {
		return err
	}

	if p.sql2, err = p.dbConn.Prepare(fmt.Sprintf("UPDATE %[1]srra rra SET value = $1, duration_ms = $2 WHERE id = $3", p.prefix)); err != nil {
		return err
	}
	if p.sql3, err = p.dbConn.Prepare(fmt.Sprintf("SELECT max(tg) mt, avg(r) ar FROM generate_series($1, $2, ($3)::interval) AS tg "+
		"LEFT OUTER JOIN (SELECT t, r FROM %[1]stv tv WHERE ds_id = $4 AND rra_id = $5 "+
		" AND t >= $6 AND t <= $7) s ON tg = s.t GROUP BY trunc((extract(epoch from tg)*1000-1))::bigint/$8 ORDER BY mt",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectDSByIdent, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms FROM  %[1]sds WHERE ident = $1",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertDS, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]sds AS ds (ident, step_ms, heartbeat_ms) VALUES ($1, $2, $3) "+
			"ON CONFLICT (ident) DO UPDATE SET step_ms = ds.step_ms "+
			"RETURNING id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms", p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertRRA, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]srra AS rra (ds_id, rra_bundle_id, pos, seg, idx, cf, xff) VALUES ($1, $2, $3, $4, $5, $6, $7) "+
			"ON CONFLICT (ds_id, rra_bundle_id, cf) DO UPDATE SET ds_id = rra.ds_id "+
			"RETURNING id, ds_id, rra_bundle_id, pos, seg, idx, cf, xff, value, duration_ms", p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectRRAsByDsId, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT id, ds_id, rra_bundle_id, pos, seg, idx, cf, xff, value, duration_ms FROM %[1]srra rra WHERE ds_id = $1 ",
		p.prefix)); err != nil {
		return err
	}
	if p.sql7, err = p.dbConn.Prepare(fmt.Sprintf("UPDATE %[1]sds SET lastupdate = $1, value = $2, duration_ms = $3 WHERE id = $4", p.prefix)); err != nil {
		return err
	}
	if p.sql8, err = p.dbConn.Prepare(fmt.Sprintf("SELECT id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms FROM %[1]sds AS ds WHERE id = $1",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectRRABundleByStepSize, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT id, step_ms, size, width FROM %[1]srra_bundle AS rra_bundle WHERE step_ms = $1 AND size = $2",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertRRABundle, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]srra_bundle AS rra_bundle (step_ms, size) VALUES ($1, $2) "+
			"ON CONFLICT (step_ms, size) DO UPDATE SET size = rra_bundle.size "+
			"RETURNING id, step_ms, size, width", p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectRRABundle, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT id, step_ms, size, width FROM %[1]srra_bundle WHERE id = $1",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectRRALatest, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT latest[$3] AS latest FROM %[1]srra_latest AS rl WHERE rl.rra_bundle_id = $1 AND rl.seg = $2",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertRRALatest, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]srra_latest AS rra_latest (rra_bundle_id, seg) VALUES ($1, $2) ON CONFLICT(rra_bundle_id, seg) DO NOTHING",
		p.prefix)); err != nil {
		return err
	}
	return nil
}

func (p *pgvSerDe) createTablesIfNotExist() error {
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

       CREATE TABLE IF NOT EXISTS %[1]srra_bundle (
       id SERIAL NOT NULL PRIMARY KEY,
       step_ms INT NOT NULL,
       size INT NOT NULL,
       last_pos INT NOT NULL DEFAULT 0,
       width INT NOT NULL DEFAULT 200);

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_rra_bundle_spec ON %[1]srra_bundle (step_ms, size);

       CREATE TABLE IF NOT EXISTS %[1]srra_latest (
       rra_bundle_id INT NOT NULL REFERENCES %[1]srra_bundle(id) ON DELETE CASCADE,
       seg INT NOT NULL,
       latest TIMESTAMPTZ[] NOT NULL DEFAULT '{}');

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_rra_latest_bundle_id_seg ON %[1]srra_latest (rra_bundle_id, seg);

       CREATE TABLE IF NOT EXISTS %[1]srra (
       id SERIAL NOT NULL PRIMARY KEY,
       ds_id INT NOT NULL REFERENCES %[1]sds(id) ON DELETE CASCADE,
       rra_bundle_id INT NOT NULL REFERENCES %[1]srra_bundle(id) ON DELETE RESTRICT,
       cf TEXT NOT NULL,
       pos INT NOT NULL,
       seg INT NOT NULL,
       idx INT NOT NULL,
       xff REAL NOT NULL DEFAULT 0,
       value DOUBLE PRECISION NOT NULL DEFAULT 'NaN',
       duration_ms BIGINT NOT NULL DEFAULT 0);

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_rra_rra_bundle_id ON %[1]srra (ds_id, rra_bundle_id, cf);

       CREATE TABLE IF NOT EXISTS %[1]sts (
       rra_bundle_id INT NOT NULL REFERENCES %[1]srra_bundle(id) ON DELETE CASCADE,
       seg INT NOT NULL,
       i INT NOT NULL,
       dp DOUBLE PRECISION[] NOT NULL DEFAULT '{}');

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_ts_rra_bundle_id_seg_i ON %[1]sts (rra_bundle_id, seg, i);
    `
	if rows, err := p.dbConn.Query(fmt.Sprintf(create_sql, p.prefix)); err != nil {
		log.Printf("ERROR: initial CREATE TABLE failed: %v", err)
		return err
	} else {
		rows.Close()
	}
	create_sql = `
-- normal view
CREATE VIEW %[1]stv AS
  SELECT rra.ds_id AS ds_id, rra.id AS rra_id,
         rra_latest.latest[rra.idx] - INTERVAL '1 MILLISECOND' * rra_bundle.step_ms *
           MOD(rra_bundle.size + MOD(EXTRACT(EPOCH FROM rra_latest.latest[rra.idx])::BIGINT*1000/rra_bundle.step_ms, rra_bundle.size) - i, rra_bundle.size) AS t,
         dp[rra.idx] AS r
   FROM %[1]srra AS rra
   JOIN %[1]srra_bundle AS rra_bundle ON rra_bundle.id = rra.rra_bundle_id
   JOIN %[1]srra_latest AS rra_latest ON rra_latest.rra_bundle_id = rra_bundle.id AND rra_latest.seg = rra.seg
   JOIN %[1]sts AS ts ON ts.rra_bundle_id = rra_bundle.id AND ts.seg = rra.seg;
-- debug view
CREATE VIEW %[1]stvd AS
  SELECT
      ds_id
    , rra_id
    , tstzrange(lag(t, 1) OVER (PARTITION BY ds_id, rra_id ORDER BY t), t, '(]') AS tr
    , r
    , step
    , i
    , last_i
    , last_t
    , slot_distance
    , seg
    , idx
    , pos
    FROM (
     SELECT
        rra.ds_id AS ds_id
       ,rra.id AS rra_id
       ,rra_latest.latest[rra.idx] - '00:00:00.001'::interval * rra_bundle.step_ms::double precision *
          mod(rra_bundle.size + mod(date_part('epoch'::text, rra_latest.latest[rra.idx])::bigint * 1000 / rra_bundle.step_ms, rra_bundle.size::bigint) -
          ts.i, rra_bundle.size::bigint)::double precision AS t
       ,ts.dp[rra.idx] AS r
       ,'00:00:00.001'::interval * rra_bundle.step_ms::double precision AS step
       ,i AS i
       ,mod(date_part('epoch'::text, rra_latest.latest[rra.idx])::bigint * 1000 / rra_bundle.step_ms, rra_bundle.size::bigint) AS last_i
       ,date_part('epoch'::text, rra_latest.latest[rra.idx])::bigint * 1000 AS last_t
       ,mod(rra_bundle.size + mod(date_part('epoch'::text, rra_latest.latest[rra.idx])::bigint * 1000 / rra_bundle.step_ms, rra_bundle.size::bigint) -
                   ts.i, rra_bundle.size::bigint)::double precision AS slot_distance
       ,rra.seg AS seg
       ,rra.idx AS idx
       ,rra.pos AS pos
     FROM %[1]srra rra
     JOIN %[1]srra_bundle rra_bundle ON rra_bundle.id = rra.rra_bundle_id
     JOIN %[1]srra_latest rra_latest ON rra_latest.rra_bundle_id = rra_bundle.id AND rra_latest.seg = rra.seg
     JOIN %[1]sts ts ON ts.rra_bundle_id = rra_bundle.id AND ts.seg = rra.seg
  ) foo;
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

func rraBundleRecordFromRow(rows *sql.Rows) (*rraBundleRecord, error) {
	var bundle rraBundleRecord
	// id, step_ms, size, width
	err := rows.Scan(&bundle.id, &bundle.stepMs, &bundle.size, &bundle.width)
	if err != nil {
		log.Printf("rraBundleRecordFromRow(): error scanning row: %v", err)
		return nil, err
	}
	return &bundle, nil
}

func rraRecordFromRow(rows *sql.Rows) (*rraRecord, error) {

	var rra rraRecord
	err := rows.Scan(&rra.id, &rra.dsId, &rra.bundleId, &rra.pos, &rra.seg, &rra.idx, &rra.cf, &rra.xff, &rra.value, &rra.durationMs)
	if err != nil {
		log.Printf("rraRecordFromRow(): error scanning row: %v", err)
		return nil, err
	}

	return &rra, nil
}

func rraFromRRARecordAndBundle(rraRec *rraRecord, bundle *rraBundleRecord, latest time.Time) (*DbRoundRobinArchive, error) {

	spec := rrd.RRASpec{
		Step:     time.Duration(bundle.stepMs) * time.Millisecond,
		Span:     time.Duration(bundle.stepMs*bundle.size) * time.Millisecond,
		Xff:      rraRec.xff,
		Latest:   latest,
		Value:    rraRec.value,
		Duration: time.Duration(rraRec.durationMs) * time.Millisecond,
	}

	switch strings.ToUpper(rraRec.cf) {
	case "WMEAN":
		spec.Function = rrd.WMEAN
	case "MIN":
		spec.Function = rrd.MIN
	case "MAX":
		spec.Function = rrd.MAX
	case "LAST":
		spec.Function = rrd.LAST
	default:
		return nil, fmt.Errorf("rraFromRRARecordAndBundle(): Invalid cf: %q (valid funcs: wmean, min, max, last)", rraRec.cf)
	}

	rra, err := newDbRoundRobinArchive(rraRec.id, bundle.width, bundle.id, rraRec.pos, spec)
	if err != nil {
		log.Printf("rraFromRRARecordAndBundle(): error creating rra: %v", err)
		return nil, err
	}
	return rra, nil
}

// Given a query in the form of ident keys and regular expressions for
// values, return all matching idents and the ds ids.
func (p *pgvSerDe) Search(query SearchQuery) (SearchResult, error) {

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

func (p *pgvSerDe) FetchDataSourceById(id int64) (rrd.DataSourcer, error) {

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

func (p *pgvSerDe) FetchDataSources() ([]rrd.DataSourcer, error) {

	const sql = `
	SELECT ds.id, ds.ident, ds.step_ms, ds.heartbeat_ms, ds.lastupdate, ds.value, ds.duration_ms,
	       rra.id, rra.ds_id, rra.rra_bundle_id, rra.pos, rra.seg, rra.idx, rra.cf, rra.xff, rra.value, rra.duration_ms,
	       b.id, b.step_ms, b.size, b.width, rl.latest[rra.idx] AS latest
	FROM %[1]sds ds
	JOIN %[1]srra rra ON rra.ds_id = ds.id
	JOIN %[1]srra_bundle b ON b.id = rra.rra_bundle_id
	JOIN %[1]srra_latest AS rl ON rl.rra_bundle_id = b.id AND rl.seg = rra.seg
    ORDER BY ds.id, rra.id`

	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix))
	if err != nil {
		log.Printf("FetchDataSources(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	result := make([]rrd.DataSourcer, 0)
	var currentDs *DbDataSource
	var rras []rrd.RoundRobinArchiver
	for rows.Next() {
		var (
			err    error
			dsr    dsRecord
			rrar   rraRecord
			bundle rraBundleRecord
			latest *time.Time
		)

		err = rows.Scan(
			&dsr.id, &dsr.identJson, &dsr.stepMs, &dsr.hbMs, &dsr.lastupdate, &dsr.value, &dsr.durationMs, // DS
			&rrar.id, &rrar.dsId, &rrar.bundleId, &rrar.pos, &rrar.seg, &rrar.idx, &rrar.cf, &rrar.xff, &rrar.value, &rrar.durationMs, // RRA
			&bundle.id, &bundle.stepMs, &bundle.size, &bundle.width, // Bundle
			&latest) // latest
		if err != nil {
			return nil, fmt.Errorf("error scanning: %v", err)
		}

		if latest == nil {
			latest = &time.Time{}
		}

		if currentDs == nil || currentDs.id != dsr.id {

			if currentDs != nil && len(rras) > 0 {
				// this is fully baked, output it
				currentDs.SetRRAs(rras)
				result = append(result, currentDs)
			}

			if currentDs, err = dataSourceFromDsRec(&dsr); err != nil {
				return nil, fmt.Errorf("error scanning: %v", err)
			}

			rras = nil
		}

		var rra *DbRoundRobinArchive
		rra, err = rraFromRRARecordAndBundle(&rrar, &bundle, *latest)
		if err != nil {
			return nil, err
		}

		rras = append(rras, rra)
	}

	return result, nil
}

func (p *pgvSerDe) fetchOrCreateRRABundle(stepMs, size int64) (*rraBundleRecord, error) {
	rows, err := p.sqlSelectRRABundleByStepSize.Query(stepMs, size)
	if err != nil {
		log.Printf("fetchOrCreateRRABundle(): error querying database: %v", err)
		return nil, err
	}
	if !rows.Next() { // Needs to be created
		rows, err = p.sqlInsertRRABundle.Query(stepMs, size)
		if err != nil {
			log.Printf("fetchOrCreateRRABundle(): error inserting: %v", err)
			return nil, err
		}
		rows.Next()
	}
	defer rows.Close()

	var bundle *rraBundleRecord
	if bundle, err = rraBundleRecordFromRow(rows); err != nil {
		log.Printf("fetchOrCreateRRABundle(): error: %v", err)
		return nil, err
	}
	return bundle, nil
}

func (p *pgvSerDe) fetchRRABundle(id int64) (*rraBundleRecord, error) {
	rows, err := p.sqlSelectRRABundle.Query(id)
	if err != nil {
		log.Printf("fetchRRABundle(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		if bundle, err := rraBundleRecordFromRow(rows); err == nil {
			return bundle, nil
		} else {
			log.Printf("fetchRRABundle(): error: %v", err)
			return nil, err
		}
	}
	return nil, nil // not found
}

func (p *pgvSerDe) fetchRRALatest(bundleId, seg, idx int64) (time.Time, error) {
	rows, err := p.sqlSelectRRALatest.Query(bundleId, seg, idx)
	if err != nil {
		log.Printf("fetchRRALatest(): error querying database: %v", err)
		return time.Time{}, err
	}
	defer rows.Close()

	if rows.Next() {
		var latest *time.Time
		if err := rows.Scan(&latest); err != nil {
			log.Printf("fetchRRALatest(): error scanning: %v", err)
			return time.Time{}, err
		}
		if latest != nil {
			return *latest, nil
		}
	}
	return time.Time{}, nil // not found
}

func (p *pgvSerDe) fetchRoundRobinArchives(ds *DbDataSource) ([]rrd.RoundRobinArchiver, error) {
	var err error
	rows, err := p.sqlSelectRRAsByDsId.Query(ds.Id())
	if err != nil {
		log.Printf("fetchRoundRobinArchives(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	var rras []rrd.RoundRobinArchiver
	for rows.Next() {
		// rra
		var rraRec *rraRecord
		rraRec, err = rraRecordFromRow(rows)
		if err != nil {
			log.Printf("fetchRoundRobinArchives(): error: %v", err)
			return nil, err
		}
		// bundle
		var bundle *rraBundleRecord
		bundle, err = p.fetchRRABundle(rraRec.bundleId)
		if err != nil {
			log.Printf("fetchRoundRobinArchives(): error2: %v", err)
			return nil, err
		}
		// latest
		var latest time.Time
		latest, err = p.fetchRRALatest(bundle.id, rraRec.seg, rraRec.idx)
		if err != nil {
			log.Printf("fetchRoundRobinArchives(): error3: %v", err)
			return nil, err
		}
		// rra (finally)
		var rra *DbRoundRobinArchive
		rra, err = rraFromRRARecordAndBundle(rraRec, bundle, latest)
		if err != nil {
			log.Printf("fetchRoundRobinArchives(): error4: %v", err)
			return nil, err
		}
		// append
		rras = append(rras, rra)
	}
	return rras, nil
}

// Unlike the "horizontal" version, this does NOT flush the RRAs.
func (p *pgvSerDe) FlushDataSource(ds rrd.DataSourcer) error {
	dbds, ok := ds.(DbDataSourcer)
	if !ok {
		return fmt.Errorf("ds must be a DbDataSourcer to flush.")
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

func (p *pgvSerDe) VerticalFlushDPs(bundle_id, seg, i int64, dps map[int64]float64) error {

	chunks := arrayUpdateChunks(dps)

	if len(chunks) > 1 {
		//
		// Use single-statement update  // TODO make me a function!
		//
		dest, args := singleStmtUpdateArgs(chunks, "dp", 4, []interface{}{bundle_id, seg, i})
		stmt := fmt.Sprintf("UPDATE %[1]sts AS ts SET %s WHERE rra_bundle_id = $1 AND seg = $2 AND i = $3", p.prefix, dest)

		res, err := p.dbConn.Exec(stmt, args...)
		if err != nil {
			return err
		}

		if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
			if _, err = p.sqlInsertTs.Exec(bundle_id, seg, i); err != nil {
				return err
			}
			if res, err := p.dbConn.Exec(stmt, args...); err != nil {
				return err
			} else if affected, _ := res.RowsAffected(); affected == 0 {
				return fmt.Errorf("Unable to update row?")
			}
		}
		return nil
	} else {
		//
		// Use multi-statement update // TODO make me a funciton!
		//
		for _, args := range multiStmtUpdateArgs(chunks, []interface{}{bundle_id, seg, i}) {

			tx, err := p.dbConn.Begin()
			if err != nil {
				return err
			}
			defer tx.Commit() // TODO is this actually faster?

			res, err := tx.Stmt(p.sqlUpdateTs).Exec(args...)
			if err != nil {
				return err
			}

			if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
				if _, err = tx.Stmt(p.sqlInsertTs).Exec(bundle_id, seg, i); err != nil {
					return err
				}
				if res, err := tx.Stmt(p.sqlUpdateTs).Exec(args...); err != nil {
					return err
				} else if affected, _ := res.RowsAffected(); affected == 0 {
					return fmt.Errorf("Unable to update row?")
				}
			}
		}
		return nil
	}
}

func (p *pgvSerDe) VerticalFlushLatests(bundle_id, seg int64, latests map[int64]time.Time) error {
	ilatests := make(map[int64]interface{})
	for k, v := range latests {
		ilatests[k] = v
	}

	dest, args := arrayUpdateStatement_(ilatests, 3, "latest")
	args = append([]interface{}{bundle_id, seg}, args...)

	// TODO: Same as with VerticalFlushDPs - prepare and run multiple times?
	stmt := fmt.Sprintf("UPDATE %[1]srra_latest AS rra_latest SET %s WHERE rra_bundle_id = $1 AND seg = $2", p.prefix, dest)

	res, err := p.dbConn.Exec(stmt, args...)
	if err != nil {
		return err
	}

	if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
		if _, err = p.sqlInsertRRALatest.Exec(bundle_id, seg); err != nil {
			return err
		}
		if res, err := p.dbConn.Exec(stmt, args...); err != nil {
			return err
		} else if affected, _ := res.RowsAffected(); affected == 0 {
			return fmt.Errorf("Unable to update row?")
		}
	}
	return nil
}

// FetchOrCreateDataSource loads or returns an existing DS. This is
// done by using upserts first on the ds table, then for each
// RRA. This method also attempt to create the TS empty rows with ON
// CONFLICT DO NOTHING. The returned DS contains no data, to get data
// use FetchSeries().
func (p *pgvSerDe) FetchOrCreateDataSource(ident Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	var (
		err  error
		rows *sql.Rows
	)

	// Try SELECT first
	rows, err = p.sqlSelectDSByIdent.Query(ident.String())
	if err != nil {
		log.Printf("FetchOrCreateDataSource(): error querying database: %v", err)
		return nil, err
	}
	if !rows.Next() {
		// Now try INSERT
		rows, err = p.sqlInsertDS.Query(ident.String(), dsSpec.Step.Nanoseconds()/1000000, dsSpec.Heartbeat.Nanoseconds()/1000000)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error querying database: %v", err)
			return nil, err
		}
		if !rows.Next() {
			log.Printf("FetchOrCreateDataSource(): unable to lookup/create")
			return nil, fmt.Errorf("unable to lookup/create")
		}
	}
	defer rows.Close()
	ds, err := dataSourceFromRow(rows)
	if err != nil {
		log.Printf("FetchOrCreateDataSource(): error: %v", err)
		return nil, err
	}

	// RRAs
	var rras []rrd.RoundRobinArchiver
	for _, rraSpec := range dsSpec.RRAs {
		stepMs := rraSpec.Step.Nanoseconds() / 1000000
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

		// rra_bundle
		var bundle *rraBundleRecord
		bundle, err = p.fetchOrCreateRRABundle(stepMs, size)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error creating RRA bundle: %v", err)
			return nil, err
		}

		// Get the next position for this bundle WWW
		pos, err := p.rraBundleIncrPos(bundle.id)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error incrementing last_pos in RRA bundle: %v", err)
			return nil, err
		}

		// rra
		var rraRows *sql.Rows
		seg, idx := segIdxFromPosWidth(pos, bundle.width)
		rraRows, err = p.sqlInsertRRA.Query(ds.Id(), bundle.id, pos, seg, idx, cf, rraSpec.Xff)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error creating RRAs: %v", err)
			return nil, err
		}
		rraRows.Next()

		var rraRec *rraRecord
		rraRec, err = rraRecordFromRow(rraRows)
		rraRows.Close()
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error2: %v", err)
			return nil, err
		}

		latest := rraSpec.Latest

		var rra *DbRoundRobinArchive
		rra, err = rraFromRRARecordAndBundle(rraRec, bundle, latest)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error3: %v", err)
			return nil, err
		}

		rras = append(rras, rra)
	}
	ds.SetRRAs(rras)

	if debug {
		log.Printf("FetchOrCreateDataSource(): returning ds.id %d: LastUpdate: %v, %#v", ds.Id(), ds.LastUpdate(), ds)
	}
	return ds, nil
}

func (p *pgvSerDe) FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error) {

	dbds, ok := ds.(DbDataSourcer)
	if !ok {
		return nil, fmt.Errorf("FetchSeries: ds must be a DbDataSourcer")
	}

	rra := dbds.BestRRA(from, to, maxPoints)
	if rra == nil {
		return nil, fmt.Errorf("FetchSeries: No adequate RRA found for DS id: %v from: %v to: maxPoints: %v", dbds.Id(), from, to, maxPoints)
	}

	// If from/to are nil - assign the rra boundaries
	rraEarliest := rra.Begins(rra.Latest())

	if from.IsZero() || rraEarliest.After(from) {
		from = rraEarliest
	}

	dbrra, ok := rra.(DbRoundRobinArchiver)
	if !ok {
		return nil, fmt.Errorf("FetchSeries: rra must be a DbRoundRobinArchive")
	}

	// Note that seriesQuerySqlUsingViewAndSeries() will modify "to"
	// to be the earliest of "to" or "LastUpdate".
	dps := &dbSeriesV2{db: p, ds: dbds, rra: dbrra, from: from, to: to, maxPoints: maxPoints}
	return dps, nil
}

func (p *pgvSerDe) rraBundleIncrPos(id int64) (int64, error) {
	stmt := fmt.Sprintf("UPDATE %[1]srra_bundle SET last_pos = last_pos + 1 WHERE id = $1 RETURNING last_pos", p.prefix)
	rows, err := p.dbConn.Query(stmt, id)
	if err != nil {
		log.Printf("rraBundleIncrPos(): error querying database: %v", err)
		return 0, err
	}
	defer rows.Close()

	var pos int64
	if rows.Next() {
		if err := rows.Scan(&pos); err != nil {
			log.Printf("rraBundleIncrPos(): error scanning row: %v", err)
			return 0, err
		}
		return pos, nil
	}
	return 0, fmt.Errorf("rraBundleIncrPos: could not increment pos?")
}
