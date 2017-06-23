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

// TODO Data layout notes
//
// Versioning and Backfill problem
//
// When there is a large gap in data, it needs to filled in which
// causes a lot of database activity. To address this issue Tgres
// stores a version for each the data point. The data point is a
// number a smallint incrementing by 1 every time round-robin goes
// full circle. This means that a gap can be left without updating the
// data, the tv view will convert the values to NULL if the version
// does not match the expected version.

package serde

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"time"

	"github.com/lib/pq"
	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/series"
)

type pgvSerDe struct {
	dbConn  *sql.DB
	dbQConn *sql.DB // a separate connection for querying
	prefix  string
	listen  *pq.Listener

	sqlSelectSeries              *sql.Stmt
	sqlSelectDSByIdent           *sql.Stmt
	sqlInsertDS                  *sql.Stmt
	sqlInsertDSState             *sql.Stmt
	sqlSelectRRAsByDsId          *sql.Stmt
	sqlInsertRRA                 *sql.Stmt
	sqlInsertRRABundle           *sql.Stmt
	sqlSelectRRABundleByStepSize *sql.Stmt
	sqlSelectRRABundle           *sql.Stmt
	sqlInsertRRAState            *sql.Stmt
	sqlSelectRRAState            *sql.Stmt
	sqlInsertTs                  *sql.Stmt
	sqlUpdateTs                  *sql.Stmt
}

func InitDb(connect_string, prefix string) (*pgvSerDe, error) {
	if dbConn, err := sql.Open("postgres", connect_string); err != nil {
		return nil, err
	} else {
		// NB: disabling materialization (enable_material=off) speeds up
		// sqlSelectSeries.
		dbQConn, err := sql.Open("postgres", connect_string+" enable_material=off")
		if err != nil {
			return nil, err
		}
		l := pq.NewListener(connect_string, time.Second, 8*time.Second, nil)
		p := &pgvSerDe{dbConn: dbConn, dbQConn: dbQConn, listen: l, prefix: prefix}
		if err := p.dbConn.Ping(); err != nil {
			return nil, err
		}
		if err := p.createTablesIfNotExist(); err != nil {
			return nil, fmt.Errorf("createTablesIfNotExist: %v", err)
		}
		if err := p.prepareSqlStatements(); err != nil {
			return nil, fmt.Errorf("prepareSqlStatements: %v", err)
		}

		return p, nil
	}
}

func (p *pgvSerDe) Fetcher() Fetcher             { return p }
func (p *pgvSerDe) Flusher() Flusher             { return p }
func (p *pgvSerDe) EventListener() EventListener { return p }
func (p *pgvSerDe) DbAddresser() DbAddresser     { return p }

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
		"UPDATE %[1]sts AS ts SET dp[$4:$5] = $6, ver[$7:$8] = $9 WHERE rra_bundle_id = $1 AND seg = $2 AND i = $3",
		p.prefix)); err != nil {
		return err
	}
	// NB: dbQConn used here
	if p.sqlSelectSeries, err = p.dbQConn.Prepare(fmt.Sprintf(
		"SELECT max(tg) mt, avg(r) ar FROM generate_series($1, $2, ($3)::interval) AS tg "+
			"LEFT OUTER JOIN (SELECT t, r FROM %[1]stv tv WHERE ds_id = $4 AND rra_id = $5 "+
			" AND t >= $6 AND t <= $7) s ON tg = s.t GROUP BY trunc((extract(epoch from tg)*1000-1))::bigint/$8 ORDER BY mt",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectDSByIdent, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT id, ident, step_ms, heartbeat_ms, ds.seg, ds.idx, "+
			"dsst.lastupdate[ds.idx] AS lastupdate, dsst.value[ds.idx] AS value, dsst.duration_ms[ds.idx] AS duration_ms, "+
			"false AS created "+
			"FROM %[1]sds ds JOIN %[1]sds_state dsst ON ds.seg = dsst.seg "+
			"WHERE ident = $1",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertDS, err = p.dbConn.Prepare(fmt.Sprintf(
		// Here created is a trick to determine whether this was an INSERT or an UPDATE
		"INSERT INTO %[1]sds AS ds (ident, step_ms, heartbeat_ms) VALUES ($1, $2, $3) "+
			"ON CONFLICT (ident) DO UPDATE SET created = false "+
			"RETURNING id, ident, step_ms, heartbeat_ms, seg, idx, "+
			"NULL::TIMESTAMPTZ AS lastupdate, 'NaN'::DOUBLE PRECISION AS value, "+
			"0::BIGINT AS duration_ms, created", p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertDSState, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]sds_state AS dss (seg) VALUES ($1) ON CONFLICT(seg) DO NOTHING",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertRRA, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]srra AS rra (ds_id, rra_bundle_id, pos, seg, idx, cf, xff) VALUES ($1, $2, $3, $4, $5, $6, $7) "+
			"ON CONFLICT (ds_id, rra_bundle_id, cf) DO UPDATE SET ds_id = rra.ds_id "+
			"RETURNING id, ds_id, rra_bundle_id, pos, seg, idx, cf, xff", p.prefix)); err != nil {
		return err
	}
	if p.sqlSelectRRAsByDsId, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT id, ds_id, rra_bundle_id, pos, seg, idx, cf, xff FROM %[1]srra rra WHERE ds_id = $1 ",
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
	if p.sqlSelectRRAState, err = p.dbConn.Prepare(fmt.Sprintf(
		"SELECT latest[$3], value[$3], duration_ms[$3] AS latest FROM %[1]srra_state AS rl WHERE rl.rra_bundle_id = $1 AND rl.seg = $2",
		p.prefix)); err != nil {
		return err
	}
	if p.sqlInsertRRAState, err = p.dbConn.Prepare(fmt.Sprintf(
		"INSERT INTO %[1]srra_state AS rra_state (rra_bundle_id, seg) VALUES ($1, $2) ON CONFLICT(rra_bundle_id, seg) DO NOTHING",
		p.prefix)); err != nil {
		return err
	}
	return nil
}

var PgSegmentWidth int = 200

func (p *pgvSerDe) createTablesIfNotExist() error {
	create_sql := `
       -- NB: seg and idx are based on id, using lastval()
       CREATE TABLE IF NOT EXISTS %[1]sds (
       id SERIAL NOT NULL PRIMARY KEY,
       ident JSONB NOT NULL DEFAULT '{}' CONSTRAINT nonempty_ident CHECK (ident <> '{}'),
       step_ms BIGINT NOT NULL,
       heartbeat_ms BIGINT NOT NULL,
       seg INT NOT NULL DEFAULT (lastval()-1) / %[2]d,
       idx INT NOT NULL DEFAULT mod(lastval()-1, %[2]d)+1,
       created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
       created BOOL NOT NULL DEFAULT true);

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_ds_ident_uniq ON %[1]sds (ident);
       CREATE INDEX IF NOT EXISTS %[1]sidx_ds_ident ON %[1]sds USING gin(ident);

       CREATE TABLE IF NOT EXISTS %[1]sds_state (
       seg INT NOT NULL PRIMARY KEY,
       lastupdate TIMESTAMPTZ[] NOT NULL DEFAULT '{}',
       duration_ms BIGINT[] NOT NULL DEFAULT '{}',
       value DOUBLE PRECISION[] NOT NULL DEFAULT '{}');

       CREATE TABLE IF NOT EXISTS %[1]srra_bundle (
       id SERIAL NOT NULL PRIMARY KEY,
       step_ms INT NOT NULL,
       size INT NOT NULL,
       last_pos INT NOT NULL DEFAULT 0,
       width INT NOT NULL DEFAULT %[2]d);

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_rra_bundle_spec ON %[1]srra_bundle (step_ms, size);

       CREATE TABLE IF NOT EXISTS %[1]srra_state (
       rra_bundle_id INT NOT NULL REFERENCES %[1]srra_bundle(id) ON DELETE CASCADE,
       seg INT NOT NULL,
       latest TIMESTAMPTZ[] NOT NULL DEFAULT '{}',
       duration_ms BIGINT[] NOT NULL DEFAULT '{}',
       value DOUBLE PRECISION[] NOT NULL DEFAULT '{}');

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_rra_state_bundle_id_seg ON %[1]srra_state (rra_bundle_id, seg);

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
       dp DOUBLE PRECISION[] NOT NULL DEFAULT '{}',
       ver SMALLINT[] NOT NULL DEFAULT '{}');

       CREATE UNIQUE INDEX IF NOT EXISTS %[1]sidx_ts_rra_bundle_id_seg_i ON %[1]sts (rra_bundle_id, seg, i);
    `
	if _, err := p.dbConn.Exec(fmt.Sprintf(create_sql, p.prefix, PgSegmentWidth)); err != nil {
		log.Printf("ERROR: initial CREATE TABLE failed: %v", err)
		return err
	}

	// TODO: Remove this later. And we should consider a better migration mechanism.
	migrate_sql := `
DO $$
  DECLARE r RECORD;
  DECLARE q TEXT;
BEGIN
  IF (SELECT COUNT(1) FROM information_schema.columns WHERE table_name='%[1]sds' and column_name='created_at') > 0 THEN
    -- migration done already, nothing to do
    RETURN;
  END IF;

  -- DS STATE

  -- set seg and idx
  ALTER TABLE %[1]sds ADD COLUMN seg INT NOT NULL DEFAULT 0;
  ALTER TABLE %[1]sds ADD COLUMN idx INT NOT NULL DEFAULT 0;
  UPDATE %[1]sds SET seg = (id - 1) / %[2]d;
  UPDATE %[1]sds SET idx = mod(id - 1, %[2]d) + 1;
  ALTER TABLE %[1]sds ALTER COLUMN seg SET DEFAULT (lastval()-1) / %[2]d;
  ALTER TABLE %[1]sds ALTER COLUMN idx SET DEFAULT mod(lastval()-1, %[2]d)+1;

  -- good to have
  ALTER TABLE %[1]sds ADD COLUMN created_at TIMESTAMPTZ NOT NULL DEFAULT now();

  -- populate the data in ds_state
  INSERT INTO %[1]sds_state(seg) SELECT DISTINCT(seg) AS seg FROM %[1]sds;

  FOR r IN SELECT id, seg, idx, lastupdate, duration_ms, value FROM %[1]sds ORDER BY id
  LOOP
    q := 'UPDATE %[1]sds_state SET'
         || ' lastupdate[' || r.idx || '] = ' || quote_nullable(r.lastupdate)
         || ',duration_ms[' || r.idx || '] = ' || r.duration_ms
         || ',value[' || r.idx || '] = ' || quote_nullable(r.value::TEXT) || '::DOUBLE PRECISION'
         || ' WHERE seg = ' || r.seg;
    EXECUTE q;
  END LOOP;

  -- drop the columns
  ALTER TABLE %[1]sds DROP COLUMN lastupdate;
  ALTER TABLE %[1]sds DROP COLUMN duration_ms;
  ALTER TABLE %[1]sds DROP COLUMN value;

  -- RRA STATE

  -- drop the newly-created rra_state and use the already existing rra_latest
  DROP TABLE IF EXISTS %[1]srra_state;
  ALTER TABLE %[1]srra_latest RENAME TO %[1]srra_state;
  ALTER INDEX %[1]sidx_rra_latest_bundle_id_seg RENAME TO %[1]sidx_rra_state_bundle_id_seg;
  ALTER TABLE %[1]srra_state RENAME CONSTRAINT %[1]srra_latest_rra_bundle_id_fkey TO %[1]srra_state_rra_bundle_id_fkey;

  -- add columns for duration and value
  ALTER TABLE %[1]srra_state ADD COLUMN duration_ms BIGINT[] NOT NULL DEFAULT '{}';
  ALTER TABLE %[1]srra_state ADD COLUMN value DOUBLE PRECISION[] NOT NULL DEFAULT '{}';

  -- populate duration and value in rra_state
  FOR r IN SELECT id, rra_bundle_id, seg, idx, duration_ms, value FROM %[1]srra WHERE duration_ms > 0 ORDER BY id
  LOOP
    q := 'UPDATE %[1]srra_state SET'
         || ' duration_ms[' || r.idx || '] = ' || r.duration_ms
         || ',value[' || r.idx || '] = ' || quote_nullable(r.value::TEXT) || '::DOUBLE PRECISION'
         || ' WHERE rra_bundle_id = ' || r.rra_bundle_id || ' AND seg = ' || r.seg;
    EXECUTE q;
  END LOOP;

  -- drop the columns
  ALTER TABLE %[1]srra DROP COLUMN duration_ms;
  ALTER TABLE %[1]srra DROP COLUMN value;

END
$$;
`
	if _, err := p.dbConn.Exec(fmt.Sprintf(migrate_sql, p.prefix, PgSegmentWidth)); err != nil {
		log.Printf("ERROR: migrate failed: %v", err)
		return err
	}

	// NB: BEGIN > DROP > CREATE > COMMIT is the equivalent of CREATE OR REPLACE
	// See https://wiki.postgresql.org/wiki/Transactional_DDL_in_PostgreSQL:_A_Competitive_Analysis

	create_sql = `
BEGIN;

-- a view do simplify looking at DSs
DROP VIEW IF EXISTS %[1]sdsv;
CREATE VIEW %[1]sdsv AS
  SELECT id, ident, step_ms, heartbeat_ms, created_at,
         dss.lastupdate[ds.idx] AS lastupdate,
         dss.value[ds.idx] AS value,
         dss.duration_ms[ds.idx] AS duration_ms,
         ds.seg, idx
    FROM %[1]sds AS ds
    JOIN %[1]sds_state AS dss
      ON ds.seg = dss.seg;

-- a view to simplify looking at RRAs
DROP VIEW IF EXISTS %[1]srrav;
CREATE VIEW %[1]srrav AS
  SELECT rra.id, ds_id, cf, xff, size,
         '00:00:00.001'::interval * step_ms AS step,
         '00:00:00.001'::interval * step_ms * size AS span,
         rs.latest[rra.idx] AS latest,
         rs.duration_ms[rra.idx] AS duration_ms,
         rs.value[rra.idx] AS value,
         rra.seg, idx, rra.rra_bundle_id
    FROM %[1]srra AS rra
    JOIN %[1]srra_bundle AS rb ON rra.rra_bundle_id = rb.id
    JOIN %[1]srra_state AS rs ON rb.id = rs.rra_bundle_id AND rra.seg = rs.seg;

-- normal view
  -- sub-queries are for clarity, they do not affect performance here
  -- (as best i can tell explains are identical between this and non-nested)
DROP VIEW IF EXISTS %[1]stv;
CREATE VIEW %[1]stv AS
    SELECT ds_id, rra_id, step_ms, t, r
      FROM (
      SELECT ds_id, rra_id, step_ms, r
           , latest - '00:00:00.001'::interval * step_ms * mod(size + latest_i - i, size) AS t
           , ver
           , latest_ver - (i > latest_i)::INT AS expected_version
        FROM (
        SELECT ds_id, rra_id, step_ms, r
             , size, i, latest, ver
             , mod(latest_ms/step_ms, size) AS latest_i
             , mod(latest_ms / (step_ms::bigint * size), 32767)::smallint AS latest_ver
          FROM (
          SELECT rra.ds_id AS ds_id
               , rra.id AS rra_id
               , rra_bundle.step_ms AS step_ms
               , date_part('epoch'::text, rra_state.latest[rra.idx])::bigint * 1000 AS latest_ms
               , rra_state.latest[rra.idx] AS latest
               , rra_bundle.size AS size
               , ts.i AS i
               , dp[rra.idx] AS r
               , ver[rra.idx] AS ver
            FROM %[1]srra AS rra
            JOIN %[1]srra_bundle AS rra_bundle ON rra_bundle.id = rra.rra_bundle_id
            JOIN %[1]srra_state AS rra_state ON rra_state.rra_bundle_id = rra_bundle.id AND rra_state.seg = rra.seg
            JOIN %[1]sts AS ts ON ts.rra_bundle_id = rra_bundle.id AND ts.seg = rra.seg
          ) a
        ) b
      ) c
WHERE expected_version = coalesce(ver, expected_version);

-- debug view
-- TODO add version stuff to it
DROP VIEW IF EXISTS %[1]stvd;
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
       ,rra_state.latest[rra.idx] - '00:00:00.001'::interval * rra_bundle.step_ms::double precision *
          mod(rra_bundle.size + mod(date_part('epoch'::text, rra_state.latest[rra.idx])::bigint * 1000 / rra_bundle.step_ms, rra_bundle.size::bigint) -
          ts.i, rra_bundle.size::bigint)::double precision AS t
       ,ts.dp[rra.idx] AS r
       ,'00:00:00.001'::interval * rra_bundle.step_ms::double precision AS step
       ,i AS i
       ,mod(date_part('epoch'::text, rra_state.latest[rra.idx])::bigint * 1000 / rra_bundle.step_ms, rra_bundle.size::bigint) AS last_i
       ,date_part('epoch'::text, rra_state.latest[rra.idx])::bigint * 1000 AS last_t
       ,mod(rra_bundle.size + mod(date_part('epoch'::text, rra_state.latest[rra.idx])::bigint * 1000 / rra_bundle.step_ms, rra_bundle.size::bigint) -
                   ts.i, rra_bundle.size::bigint)::double precision AS slot_distance
       ,rra.seg AS seg
       ,rra.idx AS idx
       ,rra.pos AS pos
     FROM %[1]srra rra
     JOIN %[1]srra_bundle rra_bundle ON rra_bundle.id = rra.rra_bundle_id
     JOIN %[1]srra_state rra_state ON rra_state.rra_bundle_id = rra_bundle.id AND rra_state.seg = rra.seg
     JOIN %[1]sts ts ON ts.rra_bundle_id = rra_bundle.id AND ts.seg = rra.seg
  ) foo;

COMMIT;
`
	if _, err := p.dbConn.Exec(fmt.Sprintf(create_sql, p.prefix)); err != nil {
		//if !strings.Contains(err.Error(), "already exists") {
		log.Printf("ERROR: initial CREATE VIEW failed: %v", err)
		return err
		//}
	}

	create_sql = `
BEGIN;
DROP TRIGGER IF EXISTS %[1]sds_delete_trigger ON %[1]sds;

CREATE OR REPLACE FUNCTION %[1]sds_delete_notify() RETURNS TRIGGER AS
$body$
  BEGIN
    PERFORM pg_notify('%[1]sds_delete_event', OLD.ident::text);
    RETURN NULL;
  END;
$body$
LANGUAGE plpgsql;

CREATE TRIGGER %[1]sds_delete_trigger AFTER DELETE ON %[1]sds
  FOR EACH ROW
  EXECUTE PROCEDURE %[1]sds_delete_notify();

COMMIT;
`
	if _, err := p.dbConn.Exec(fmt.Sprintf(create_sql, p.prefix)); err != nil {
		log.Printf("ERROR: initial CREATE TRIGGER failed: %v", err)
		return err
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
	err := rows.Scan(&rra.id, &rra.dsId, &rra.bundleId, &rra.pos, &rra.seg, &rra.idx, &rra.cf, &rra.xff)
	if err != nil {
		log.Printf("rraRecordFromRow(): error scanning row: %v", err)
		return nil, err
	}

	return &rra, nil
}

func rraFromRRARecordStateAndBundle(rraRec *rraRecord, stateRec *rraStateRecord, bundle *rraBundleRecord) (*DbRoundRobinArchive, error) {

	if stateRec == nil {
		stateRec = &rraStateRecord{}
	}
	if stateRec.latest == nil {
		stateRec.latest = &time.Time{}
	}
	if stateRec.value == nil {
		stateRec.value = new(float64)
	}
	if stateRec.durationMs == nil {
		stateRec.durationMs = new(int64)
	}

	spec := rrd.RRASpec{
		Step:     time.Duration(bundle.stepMs) * time.Millisecond,
		Span:     time.Duration(bundle.stepMs*bundle.size) * time.Millisecond,
		Xff:      rraRec.xff,
		Latest:   *stateRec.latest,
		Value:    *stateRec.value,
		Duration: time.Duration(*stateRec.durationMs) * time.Millisecond,
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
		sql   = `SELECT ident FROM %[1]sds ds`
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

func (p *pgvSerDe) FetchDataSources(window time.Duration) ([]rrd.DataSourcer, error) {

	const sql = `
    SELECT ds.id, ds.ident, ds.step_ms, ds.heartbeat_ms, ds.seg, ds.idx,
           dsst.lastupdate[ds.idx] AS lastupdate,
           dsst.value[ds.idx] AS value,
           dsst.duration_ms[ds.idx] AS duration_ms,
           rra.id, rra.ds_id, rra.rra_bundle_id, rra.pos, rra.seg, rra.idx, rra.cf, rra.xff,
           b.id, b.step_ms, b.size, b.width,
           rs.latest[rra.idx] AS latest,
           rs.value[rra.idx] AS value,
           rs.duration_ms[rra.idx] AS duration_ms
    FROM %[1]sds ds
    LEFT OUTER JOIN %[1]sds_state dsst ON ds.seg = dsst.seg
    JOIN %[1]srra rra ON rra.ds_id = ds.id
    JOIN %[1]srra_bundle b ON b.id = rra.rra_bundle_id
    LEFT OUTER JOIN %[1]srra_state AS rs ON rs.rra_bundle_id = b.id AND rs.seg = rra.seg
    ORDER BY ds.id
`
	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix))
	if err != nil {
		log.Printf("FetchDataSources(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	type dsRecWithRRAs struct {
		dsr  *dsRecord
		rras []rrd.RoundRobinArchiver
	}

	dss := make([]*dsRecWithRRAs, 0)
	var (
		lastDsr       *dsRecord
		maxLastUpdate time.Time
		rras          []rrd.RoundRobinArchiver
	)
	for rows.Next() {
		var (
			err    error
			dsr    dsRecord
			rrar   rraRecord
			bundle rraBundleRecord
			state  rraStateRecord
		)

		err = rows.Scan(
			&dsr.id, &dsr.identJson, &dsr.stepMs, &dsr.hbMs, &dsr.seg, &dsr.idx, &dsr.lastupdate, &dsr.value, &dsr.durationMs, // DS
			&rrar.id, &rrar.dsId, &rrar.bundleId, &rrar.pos, &rrar.seg, &rrar.idx, &rrar.cf, &rrar.xff, // RRA
			&bundle.id, &bundle.stepMs, &bundle.size, &bundle.width, // Bundle
			&state.latest, &state.value, &state.durationMs) // RRA State
		if err != nil {
			return nil, fmt.Errorf("error scanning: %v", err)
		}

		if state.latest == nil {
			state.latest = &time.Time{}
		}

		if lastDsr == nil || lastDsr.id != dsr.id {

			if lastDsr != nil && len(rras) > 0 { // this is fully baked, output it

				if lastDsr.lastupdate == nil {
					lastDsr.lastupdate = &time.Time{}
				}

				dss = append(dss, &dsRecWithRRAs{lastDsr, rras})

				// Keep track of the absolute latest
				if maxLastUpdate.Before(*lastDsr.lastupdate) {
					maxLastUpdate = *lastDsr.lastupdate
				}
			}

			rras, lastDsr = nil, &dsr
		}

		var rra *DbRoundRobinArchive
		rra, err = rraFromRRARecordStateAndBundle(&rrar, &state, &bundle)
		if err != nil {
			return nil, err
		}

		rras = append(rras, rra)
	}

	// Weed out DSs that are older than some time period
	// TODO: max idle time should be configurable?
	skipped := 0
	result := make([]rrd.DataSourcer, 0, len(dss))
	var limit time.Time
	if window != 0 {
		limit = maxLastUpdate.Add(-window)
	}
	for i := 0; i < len(dss); i++ {
		// Always include DS with null lastupdate. TODO: perhaps rely
		// on created_at when lastupdate is null instead?
		if (*dss[i].dsr.lastupdate).IsZero() || limit.Before(*dss[i].dsr.lastupdate) {
			ds, err := dataSourceFromDsRec(dss[i].dsr)
			if err != nil {
				return nil, fmt.Errorf("error scanning: %v", err)
			}
			ds.SetRRAs(dss[i].rras)
			result = append(result, ds)
		} else {
			skipped++
		}
	}

	log.Printf("FetchDataSources: skipped %d data sources older than %v", skipped, limit)

	return result, nil
}

func (p *pgvSerDe) fetchOrCreateRRABundle(tx *sql.Tx, stepMs, size int64) (*rraBundleRecord, error) {
	rows, err := tx.Stmt(p.sqlSelectRRABundleByStepSize).Query(stepMs, size)
	if err != nil {
		log.Printf("fetchOrCreateRRABundle(): error querying database: %v", err)
		return nil, err
	}
	if !rows.Next() { // Needs to be created
		rows, err = tx.Stmt(p.sqlInsertRRABundle).Query(stepMs, size)
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

func (p *pgvSerDe) fetchRRAState(bundleId, seg, idx int64) (*rraStateRecord, error) {
	rows, err := p.sqlSelectRRAState.Query(bundleId, seg, idx)
	if err != nil {
		log.Printf("fetchRRAState(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		var state rraStateRecord
		if err := rows.Scan(&state.latest, &state.value, &state.durationMs); err != nil {
			log.Printf("fetchRRAState(): error scanning: %v", err)
			return nil, err
		}
		return &state, nil
	}
	return nil, nil // not found
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
		// state
		var stateRec *rraStateRecord
		stateRec, err = p.fetchRRAState(bundle.id, rraRec.seg, rraRec.idx)
		if err != nil {
			log.Printf("fetchRoundRobinArchives(): error3: %v", err)
			return nil, err
		}
		// rra (finally)
		var rra *DbRoundRobinArchive
		rra, err = rraFromRRARecordStateAndBundle(rraRec, stateRec, bundle)
		if err != nil {
			log.Printf("fetchRoundRobinArchives(): error4: %v", err)
			return nil, err
		}
		// append
		rras = append(rras, rra)
	}
	return rras, nil
}

func (p *pgvSerDe) FlushDSStates(seg int64, lastupdate, value, duration map[int64]interface{}) (sqlOps int, err error) {

	luChunks := arrayUpdateChunks(lastupdate)
	durChunks := arrayUpdateChunks(duration)
	valChunks := arrayUpdateChunks(value)

	offset := 2
	dest1, args := singleStmtUpdateArgs(luChunks, "lastupdate", offset, []interface{}{seg})
	offset += 3 * len(luChunks)
	dest2, args := singleStmtUpdateArgs(valChunks, "value", offset, args)
	offset += 3 * len(valChunks)
	dest3, args := singleStmtUpdateArgs(durChunks, "duration_ms", offset, args)

	stmt := fmt.Sprintf("UPDATE %[1]sds_state AS dss SET %s, %s, %s WHERE seg = $1", p.prefix, dest1, dest2, dest3)
	res, err := p.dbConn.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	sqlOps++

	if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
		insert := fmt.Sprintf("INSERT INTO %[1]sds_state AS dss (seg) VALUES ($1) ON CONFLICT(seg) DO NOTHING", p.prefix)
		if _, err = p.dbConn.Exec(insert, seg); err != nil {
			return 0, err
		}
		if res, err := p.dbConn.Exec(stmt, args...); err != nil {
			return 0, err
		} else if affected, _ := res.RowsAffected(); affected == 0 {
			return 0, fmt.Errorf("Unable to update row?")
		}
		sqlOps++
	}
	return sqlOps, nil
}

func (p *pgvSerDe) FlushDataPoints(bundle_id, seg, i int64, dps, vers map[int64]interface{}) (sqlOps int, err error) {
	// Due to the way PG array syntax works, we use two different
	// methods of updating data points. When the data points updated
	// are *one* contiguous chunk, we can use the form array[a:b] =
	// '{...}' and it appears in the statement once. Such a statement
	// can be prepared. When the data points are in *multiple*
	// chunks, then we use the form array[a:b] = '{...}', array[c:d] =
	// '{...}',.... This form cannot be prepared (or I don't know
	// how). The advantage of the single-statement (latter) is that it
	// is one statement, but it is not prepared. The multi-statement
	// (former) is more statements, but they are prepared.
	//
	// Our strategy is multi-statement is only used when there is just
	// one chunk, otherwise we use non-prepared single-statement
	// (which happens much more in the real world). Some testing
	// showed that this is most performant, though who knows.
	//
	// Summary (yes, confusing):
	//   1 chunk  => multi-stmt
	//   N chunks => single-stmt

	chunks := arrayUpdateChunks(dps)
	vchunks := arrayUpdateChunks(vers)

	if len(chunks) > 1 {
		//
		// Use single-statement update  // TODO make me a function!
		//
		dest1, args := singleStmtUpdateArgs(chunks, "dp", 4, []interface{}{bundle_id, seg, i})
		dest2, args := singleStmtUpdateArgs(vchunks, "ver", 4+3*len(chunks), args)

		stmt := fmt.Sprintf("UPDATE %[1]sts AS ts SET %s, %s WHERE rra_bundle_id = $1 AND seg = $2 AND i = $3", p.prefix, dest1, dest2)

		res, err := p.dbConn.Exec(stmt, args...)
		if err != nil {
			return 0, err
		}
		sqlOps++

		if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
			if _, err = p.sqlInsertTs.Exec(bundle_id, seg, i); err != nil {
				return 0, err
			}
			if res, err := p.dbConn.Exec(stmt, args...); err != nil {
				return 0, err
			} else if affected, _ := res.RowsAffected(); affected == 0 {
				return 0, fmt.Errorf("Unable to update row?")
			}
			sqlOps++
		}
		return sqlOps, nil
	} else {
		//
		// Use multi-statement update // TODO make me a funciton!
		//
		// NB: These loops are executes at most once because of the if / else we're in.
		for _, args := range multiStmtUpdateArgs(chunks, []interface{}{bundle_id, seg, i}) {
			for _, args := range multiStmtUpdateArgs(vchunks, args) {

				tx, err := p.dbConn.Begin()
				if err != nil {
					return 0, err
				}

				res, err := tx.Stmt(p.sqlUpdateTs).Exec(args...)
				if err != nil {
					tx.Rollback()
					return 0, err
				}
				sqlOps++

				if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
					if _, err = tx.Stmt(p.sqlInsertTs).Exec(bundle_id, seg, i); err != nil {
						tx.Rollback()
						return 0, err
					}
					if res, err := tx.Stmt(p.sqlUpdateTs).Exec(args...); err != nil {
						tx.Rollback()
						return 0, err
					} else if affected, _ := res.RowsAffected(); affected == 0 {
						tx.Rollback()
						return 0, fmt.Errorf("Unable to update row?")
					}
					sqlOps++
				}
				tx.Commit()
			}
		}
		return sqlOps, nil
	}
}

func (p *pgvSerDe) FlushRRAStates(bundle_id, seg int64, latests, value, duration map[int64]interface{}) (sqlOps int, err error) {

	latChunks := arrayUpdateChunks(latests)
	valChunks := arrayUpdateChunks(value)
	durChunks := arrayUpdateChunks(duration)

	offset := 3
	dest1, args := singleStmtUpdateArgs(latChunks, "latest", offset, []interface{}{bundle_id, seg})
	offset += 3 * len(latChunks)
	dest2, args := singleStmtUpdateArgs(valChunks, "value", offset, args)
	offset += 3 * len(valChunks)
	dest3, args := singleStmtUpdateArgs(durChunks, "duration_ms", offset, args)

	stmt := fmt.Sprintf("UPDATE %[1]srra_state AS rra_state SET %s, %s, %s WHERE rra_bundle_id = $1 AND seg = $2", p.prefix, dest1, dest2, dest3)
	res, err := p.dbConn.Exec(stmt, args...)
	if err != nil {
		return 0, err
	}
	sqlOps++

	if affected, _ := res.RowsAffected(); affected == 0 { // Insert and try again.
		if _, err = p.sqlInsertRRAState.Exec(bundle_id, seg); err != nil {
			return 0, err
		}
		if res, err := p.dbConn.Exec(stmt, args...); err != nil {
			return 0, err
		} else if affected, _ := res.RowsAffected(); affected == 0 {
			return 0, fmt.Errorf("Unable to update row?")
		}
		sqlOps++
	}
	return sqlOps, nil
}

func (p *pgvSerDe) fetchDataSource(ident Ident) (*DbDataSource, error) {

	rows, err := p.sqlSelectDSByIdent.Query(ident.String())
	if err != nil {
		log.Printf("fetchDataSource(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	if rows.Next() {
		ds, err := dataSourceFromRow(rows)
		if err != nil {
			log.Printf("fetchDataSource(): error scanning DS: %v", err)
			return nil, err
		}
		rras, err := p.fetchRoundRobinArchives(ds)
		if err != nil {
			log.Printf("fetchDataSource(): error fetching RRAs: %v", err)
			return nil, err
		} else {
			ds.SetRRAs(rras)
		}
		return ds, nil
	}

	return nil, nil
}

// FetchOrCreateDataSource loads or returns an existing DS. This is
// done by using upserts first on the ds table, then for each
// RRA. This method also attempt to create the TS empty rows with ON
// CONFLICT DO NOTHING. The returned DS contains no data, to get data
// use FetchSeries(). A nil dsSpec means fetch only, do not create.
func (p *pgvSerDe) FetchOrCreateDataSource(ident Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error) {
	var (
		err  error
		rows *sql.Rows
	)

	// Try SELECT first
	ds, err := p.fetchDataSource(ident)
	if err != nil {
		return nil, err
	}
	if ds != nil || dsSpec == nil {
		return ds, err
	}

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
	defer rows.Close()

	ds, err = dataSourceFromRow(rows)
	if err != nil {
		log.Printf("FetchOrCreateDataSource(): error 1: %v", err)
		return nil, err
	}
	if !ds.Created() { // UPSERT did not INSERT, nothing more to do here
		return ds, nil
	}

	// To avoid a condition where a DS is spread across segments when this is
	// executed in parallel, do this in a transaction.
	tx, err := p.dbConn.Begin()
	if err != nil {
		return nil, err
	}

	// Create DS State
	if _, err = tx.Stmt(p.sqlInsertDSState).Exec(ds.Seg()); err != nil {
		tx.Rollback()
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
		bundle, err = p.fetchOrCreateRRABundle(tx, stepMs, size)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error creating RRA bundle: %v", err)
			tx.Rollback()
			return nil, err
		}

		// Get the next position for this bundle TODO: If the DS was
		// not created (upsert), there is a possibity that we're
		// incrementing this in vain, the position will be wasted if
		// the rra already exists.
		pos, err := p.rraBundleIncrPos(tx, bundle.id)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error incrementing last_pos in RRA bundle: %v", err)
			tx.Rollback()
			return nil, err
		}

		// rra
		var rraRows *sql.Rows
		seg, idx := segIdxFromPosWidth(pos, bundle.width)
		rraRows, err = tx.Stmt(p.sqlInsertRRA).Query(ds.Id(), bundle.id, pos, seg, idx, cf, rraSpec.Xff)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error creating RRAs: %v", err)
			tx.Rollback()
			return nil, err
		}
		rraRows.Next()

		var rraRec *rraRecord
		rraRec, err = rraRecordFromRow(rraRows)
		rraRows.Close()
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error2: %v", err)
			tx.Rollback()
			return nil, err
		}

		dur := rraSpec.Duration.Nanoseconds() / 1e6
		rraState := &rraStateRecord{
			latest:     &rraSpec.Latest,
			durationMs: &dur,
			value:      &rraSpec.Value,
		}

		var rra *DbRoundRobinArchive
		rra, err = rraFromRRARecordStateAndBundle(rraRec, rraState, bundle)
		if err != nil {
			log.Printf("FetchOrCreateDataSource(): error3: %v", err)
			tx.Rollback()
			return nil, err
		}

		rras = append(rras, rra)
	}
	ds.SetRRAs(rras)

	if debug {
		log.Printf("FetchOrCreateDataSource(): returning ds.id %d: LastUpdate: %v, %#v", ds.Id(), ds.LastUpdate(), ds)
	}

	tx.Commit()
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

	dps := &dbSeries{db: p, ds: dbds, rra: dbrra, from: from, to: to, maxPoints: maxPoints}
	return dps, nil
}

// Returns a *new* RRA based on the one passed in, containing all the data.
// If the database is behind and data has not been saved yet, the version system
// will correct for it, latest does not have to be spot on accurate.
func (p *pgvSerDe) LoadRRAData(rra *DbRoundRobinArchive) (*DbRoundRobinArchive, error) {

	// the subselect apparently encourages index scan
	stmt := `
SELECT i, r
  FROM (SELECT i, dp[$1] AS r, ver[$1] AS v FROM %[1]sts ts WHERE rra_bundle_id = $2 and seg = $3) x
  WHERE (i <= $4) AND v = $5 OR (i > $4) AND v = $6
    AND r IS NOT NULL;
`

	// TODO There should be a centralized place for version calculation
	latest_i := rrd.SlotIndex(rra.Latest(), rra.Step(), rra.Size())
	span_ms := (rra.Step().Nanoseconds() / 1e6) * rra.Size()
	latest_ms := rra.Latest().UnixNano() / 1e6
	latestVer := int((latest_ms / span_ms) % 32767)
	prevVer := latestVer - 1
	if prevVer == -1 {
		prevVer = 32767
	}

	rows, err := p.dbConn.Query(fmt.Sprintf(stmt, p.prefix), rra.Idx(), rra.BundleId(), rra.Seg(), latest_i, latestVer, prevVer)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dps := make(map[int64]float64)
	for rows.Next() {
		var (
			i   int64
			val *float64
		)
		err = rows.Scan(&i, &val)
		if err != nil {
			return nil, err
		}
		if val != nil {
			dps[i] = *val
		}
	}

	spec := rra.Spec()
	spec.Latest = rra.Latest()
	spec.Value = rra.Value()
	spec.Duration = rra.Duration()
	spec.DPs = dps

	dbrra, err := newDbRoundRobinArchive(rra.id, rra.width, rra.bundleId, rra.pos, spec)
	if err != nil {
		return nil, err
	}

	return dbrra, nil
}

func (p *pgvSerDe) TsTableSize() (size, count int64, err error) {
	const stmt = `
  SELECT pg_total_relation_size(c.oid) AS total_bytes
         , c.reltuples AS row_estimate
    FROM pg_class c
    LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
  WHERE relname = '%[1]sts';`
	rows, err := p.dbConn.Query(fmt.Sprintf(stmt, p.prefix))
	if err != nil {
		return 0, 0, err
	}
	defer rows.Close()
	if rows.Next() {
		var fcnt float64
		err = rows.Scan(&size, &fcnt)
		if err != nil {
			return 0, 0, err
		}
		return size, int64(fcnt), nil
	}
	return 0, 0, nil
}

func (p *pgvSerDe) rraBundleIncrPos(tx *sql.Tx, id int64) (int64, error) {
	stmt := fmt.Sprintf("UPDATE %[1]srra_bundle SET last_pos = last_pos + 1 WHERE id = $1 RETURNING last_pos", p.prefix)
	rows, err := tx.Query(stmt, id)
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

// DS delete LISTEN/NOTIFY

func (p *pgvSerDe) RegisterDeleteListener(handler func(Ident)) error {
	err := p.listen.Listen(fmt.Sprintf("%[1]sds_delete_event", p.prefix))
	if err != nil {
		return err
	}

	go handleDeleteNotifications(p.listen, handler)
	return nil
}

func handleDeleteNotifications(l *pq.Listener, handler func(Ident)) {
	for {
		select {
		case n := <-l.Notify:
			// TODO: Should we be checking the n.Channel value to make sure
			// it is not some other event?
			var ident Ident
			err := json.Unmarshal([]byte(n.Extra), &ident)
			if err != nil {
				log.Printf("handleDeleteNotifications(): error unmarshalling ident: %v", err)
			}
			handler(ident)
		case <-time.After(30 * time.Second):
			// This is what the example code does, not sure we need it
			// https://godoc.org/github.com/lib/pq/listen_example
			go l.Ping()
		}
	}
}
