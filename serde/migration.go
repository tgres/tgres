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
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"sync"
	"time"

	"github.com/tgres/tgres/rrd"
)

func migration_lock(p *pgvSerDe) (*sql.Tx, error) {

	var tx *sql.Tx

	stmt := `CREATE TABLE %[1]smigration (foo INT)`
	_, err := p.dbConn.Exec(fmt.Sprintf(stmt, p.prefix))
	if err != nil {

		if strings.Contains(err.Error(), "already exists") {

			// If the table exists, then migration is either done or
			// in progress. In order to test that, we need to lock the
			// table.

			tx, err = p.dbConn.Begin()
			if err != nil {
				return nil, err // This is a fatal error
			}

			log.Printf("MIGRATION ALREADY COMPLETE, OR IN PROGRESS, CHECKING...")
			tx.Exec(fmt.Sprintf(`LOCK TABLE %[1]smigration IN EXCLUSIVE MODE`, p.prefix))

			// As soon as we're able to lock the table, this means
			// that migration is finished, and there is nothing more
			// to do here.

			tx.Commit()
			return nil, nil // No, error, no NOT run the migration
		} else {

			// Som other error, this is fatal
			return nil, err

		}
	}

	// We were able to create the table, and can run a migration. We
	// need to lock it first.

	if tx, err = p.dbConn.Begin(); err != nil {
		return nil, err // This is a fatal error
	}

	_, err = tx.Exec(fmt.Sprintf(`LOCK TABLE %[1]smigration IN EXCLUSIVE MODE`, p.prefix))
	if err != nil {
		return nil, err // fatal
	}

	// The table is locked. We return the transation, the caller must
	// now Commit it to unlock.

	return tx, nil
}

func Migrate(connect_string, oldPrefix, newPrefix string) error {

	log.Printf("Entering Migration()")

	log.Printf("Migration: creating a db connection with (old) prefix: %v", oldPrefix)
	db, err := migrate_InitDb(connect_string, oldPrefix)
	if err != nil {
		return err
	}

	tx, err := migration_lock(db)
	if err != nil {
		log.Printf("Migration: FATAL ERROR: %v", err)
		return err
	}

	if tx == nil {
		fmt.Printf("Migration: NO NEED TO MIGRATE.")
		return nil
	}

	defer tx.Commit()

	log.Printf("Migration: STARTING...")

	dss, err := migrate_FetchDataSources(db)
	if err != nil {
		return err
	}

	log.Printf("Migration: loaded %d DSs.", len(dss))

	log.Printf("Migration: loading RRA data...")
	for _, ds := range dss {
		if err := migrate_processDS(db, ds.(*DbDataSource)); err != nil {
			return err
		}
	}

	log.Printf("Migration: creating a db connection with (new) prefix: %v", newPrefix)
	db, err = InitDb(connect_string, newPrefix)
	if err != nil {
		return err
	}
	log.Printf("Migration: new db prefix initialied.")

	vcache := &verticalCache{
		Mutex: &sync.Mutex{},
		m:     make(map[bundleKey]*verticalCacheSegment),
	}

	// Now we need to flush all DSs into the vcache
	log.Printf("Migration: updating vcache and creating new DSs...")
	for _, ds := range dss {
		dbds, ok := ds.(*DbDataSource)
		if !ok {
			return fmt.Errorf("Migration: Not a *DbDataSource!\n")
		}
		newDs, err := migrate_createDS(db, dbds)
		if err != nil {
			log.Printf("Migration: error %v", err)
			return err
		}
		for _, rra := range newDs.RRAs() {
			if _rra, ok := rra.(*DbRoundRobinArchive); ok {
				log.Printf(" ... updating vcache with rra id %d size %d", _rra.Id(), len(_rra.DPs()))
				vcache.update(_rra)
			} else {
				return fmt.Errorf("Migration: ERROR: rra not a *serde.DbRoundRobinArchive!")
			}
		}
	}
	log.Printf("Migration: vcache fully populated.")

	// Now we need to flush the vacache, and we're done.
	log.Printf("Migration: flushing vcache\n")
	vcache.flush(db)
	log.Printf("Migration: flushing vcache DONE\n")

	log.Printf("Migration: SUCCESS!")
	return nil
}

func migrate_loadRRAData(p *pgvSerDe, rra_id int64, width int64) (map[int64]float64, error) {
	const stmt = `SELECT n, generate_subscripts(dp, 1) i, unnest(dp) v FROM %[1]sts WHERE rra_id = $1 ORDER BY n`

	rows, err := p.dbConn.Query(fmt.Sprintf(stmt, p.prefix), rra_id)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	dps := make(map[int64]float64)
	for rows.Next() {
		var (
			n, i int64
			v    sql.NullFloat64
		)

		if err := rows.Scan(&n, &i, &v); err != nil {
			return nil, err
		}

		if v.Valid {
			dps[n*width+i-1] = v.Float64 // DPs *are* 0-based
		} else {
			log.Printf(" ... ... skipping NULL value at %v", i)
		}
	}

	return dps, nil
}

func migrate_processDS(p *pgvSerDe, ds *DbDataSource) error {

	log.Printf(" ... loading data for DS: %v", ds.Ident())

	for _, rra := range ds.RRAs() {
		dbrra := rra.(*DbRoundRobinArchive)
		var (
			dps map[int64]float64
			err error
		)
		log.Printf(" ... ... loading RRA ID: %v", dbrra.Id())
		if dps, err = migrate_loadRRAData(p, dbrra.Id(), dbrra.Width()); err != nil {
			log.Printf("Error: %v\n", err)
			return err
		}
		rra.SetDPs(dps)
	}

	return nil
}

func migrate_createDS(p *pgvSerDe, ds *DbDataSource) (rrd.DataSourcer, error) {

	spec := rrd.DSSpec{
		Step:       ds.Step(),
		Heartbeat:  ds.Heartbeat(),
		LastUpdate: ds.LastUpdate(),
		Value:      ds.Value(),
		Duration:   ds.Duration(),
	}

	for _, rra := range ds.RRAs() {
		spec.RRAs = append(spec.RRAs,
			rrd.RRASpec{
				Step:     rra.Step(),
				Span:     rra.Step() * time.Duration(rra.Size()),
				Latest:   rra.Latest(),
				Value:    rra.Value(),
				Duration: rra.Duration(),
				Function: rrd.WMEAN,
			})
	}

	newDs, err := p.FetchOrCreateDataSource(ds.Ident(), &spec)
	if err != nil {
		return nil, err
	}

	for _, rra := range newDs.RRAs() {
		for _, _rra := range ds.RRAs() {
			if _rra.Size() == rra.Size() {
				rra.SetDPs(_rra.DPs())
			}
		}
	}

	return newDs, nil
}

func migrate_InitDb(connect_string, prefix string) (*pgvSerDe, error) {
	if dbConn, err := sql.Open("postgres", connect_string); err != nil {
		return nil, err
	} else {
		p := &pgvSerDe{dbConn: dbConn, prefix: prefix}
		if err := p.dbConn.Ping(); err != nil {
			return nil, err
		}
		return p, nil
	}
}

func migrate_fetchRoundRobinArchives(p *pgvSerDe, ds *DbDataSource) ([]rrd.RoundRobinArchiver, error) {

	const sql = `SELECT id, ds_id, cf, steps_per_row, size, width, xff, value, duration_ms, latest FROM %[1]srra rra WHERE ds_id = $1`

	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix), ds.Id())
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var rras []rrd.RoundRobinArchiver
	for rows.Next() {
		if rra, err := migrate_roundRobinArchiveFromRow(rows, ds.Step()); err == nil {
			rras = append(rras, rra)
		} else {
			return nil, err
		}
	}

	return rras, nil
}

func migrate_FetchDataSources(p *pgvSerDe) ([]rrd.DataSourcer, error) {

	const sql = `SELECT id, ident, step_ms, heartbeat_ms, lastupdate, value, duration_ms FROM %[1]sds ds`

	rows, err := p.dbConn.Query(fmt.Sprintf(sql, p.prefix))
	if err != nil {
		log.Printf("FetchDataSources(): error querying database: %v", err)
		return nil, err
	}
	defer rows.Close()

	result := make([]rrd.DataSourcer, 0)
	for rows.Next() {
		ds, err := migrate_dataSourceFromRow(rows)
		rras, err := migrate_fetchRoundRobinArchives(p, ds)
		if err != nil {
			return nil, err
		} else {
			ds.SetRRAs(rras)
		}
		result = append(result, ds)
	}

	return result, nil
}

func migrate_roundRobinArchiveFromRow(rows *sql.Rows, dsStep time.Duration) (*DbRoundRobinArchive, error) {
	var (
		latest *time.Time
		cf     string
		value  float64
		xff    float32

		id, dsId, durationMs, width, stepsPerRow, size int64
	)
	err := rows.Scan(&id, &dsId, &cf, &stepsPerRow, &size, &width, &xff, &value, &durationMs, &latest)
	if err != nil {
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

	switch strings.ToUpper(cf) {
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

	rra, err := newDbRoundRobinArchive(id, width, 0, 0, spec)
	if err != nil {
		log.Printf("roundRoundRobinArchiveFromRow(): error creating rra: %v", err)
		return nil, err
	}

	return rra, nil
}

func migrate_dataSourceFromDsRec(dsr *dsRecord) (*DbDataSource, error) {

	if dsr.lastupdate == nil {
		dsr.lastupdate = &time.Time{}
	}

	var ident Ident
	err := json.Unmarshal(dsr.identJson, &ident)
	if err != nil {
		log.Printf("dataSourceFromRow(): error unmarshalling ident: %v", err)
		return nil, err
	}

	ds := NewDbDataSource(dsr.id, ident,
		rrd.NewDataSource(
			rrd.DSSpec{
				Step:       time.Duration(dsr.stepMs) * time.Millisecond,
				Heartbeat:  time.Duration(dsr.hbMs) * time.Millisecond,
				LastUpdate: *dsr.lastupdate,
				Value:      dsr.value,
				Duration:   time.Duration(dsr.durationMs) * time.Millisecond,
			},
		),
	)

	return ds, nil
}

func migrate_dataSourceFromRow(rows *sql.Rows) (*DbDataSource, error) {
	var dsr dsRecord
	err := rows.Scan(&dsr.id, &dsr.identJson, &dsr.stepMs, &dsr.hbMs, &dsr.lastupdate, &dsr.value, &dsr.durationMs)
	if err != nil {
		log.Printf("dataSourceFromRow(): error scanning row: %v", err)
		return nil, err
	}
	return migrate_dataSourceFromDsRec(&dsr)
}

// Vertical cache caches data points from multiple series for the same
// period in the same "array", which is really a map.

// a bundle of data points from various RRAs, keyed by position
// within their segment, (RRA.pos % bundle.width).
type crossRRAPoints map[int64]float64

// map[time_index]map[series_index]value
type verticalCacheSegment struct {
	// data points keyed by index in the RRD, this key can be
	// converted to a timestamp if we know latest and the RRA
	// step/size.
	dps map[int64]crossRRAPoints
	// The latest timestamp for RRAs, keyed by RRA.pos.
	latests     map[int64]time.Time // rra.latest
	maxLatest   time.Time
	latestIndex int64
	lastFlushRT time.Time
	step        time.Duration
	size        int64
}

// The top level key for this cache is the combination of bundleId,
// (which is our link to step/size) and the segment number, which is a
// simple partitioning scheme to avoid the structure always growing
// sideways as the number of RRAs grows. The segment is the RRA.pos
// divided by the bundle width.
type bundleKey struct {
	bundleId, seg int64
}

type verticalCache struct {
	m map[bundleKey]*verticalCacheSegment
	*sync.Mutex
}

// Insert new data into the cache
func (bc *verticalCache) update(rra DbRoundRobinArchiver) {
	if rra.PointCount() == 0 {
		// Nothing for us to do. This can happen is other RRAs in the
		// DS have points, thus its getting flushed.
		return
	}

	bc.Lock()
	defer bc.Unlock()

	seg, idx := rra.Seg(), rra.Idx()
	key := bundleKey{rra.BundleId(), seg}

	if bc.m[key] == nil {
		bc.m[key] = &verticalCacheSegment{
			dps:         make(map[int64]crossRRAPoints),
			latests:     make(map[int64]time.Time),
			step:        rra.Step(),
			size:        rra.Size(),
			lastFlushRT: time.Now(), // Or else it will get sent to the flusher right away!
		}
	}

	entry := bc.m[key]
	for i, v := range rra.DPs() {
		if len(entry.dps[i]) == 0 {
			entry.dps[i] = map[int64]float64{idx: v}
		}
		entry.dps[i][idx] = v
	}

	entry.latests[idx] = rra.Latest()
}

func (bc *verticalCache) flush(db VerticalFlusher) {
	bc.Lock()
	for key, segment := range bc.m {
		if len(segment.dps) == 0 {
			continue
		}

		for i, dps := range segment.dps {

			if err := db.VerticalFlushDPs(key.bundleId, key.seg, i, dps); err != nil {
				fmt.Printf("vdbflusher: ERROR in VerticalFlushDps: %v", err)
			}
		}

		if len(segment.latests) > 0 {
			if err := db.VerticalFlushLatests(key.bundleId, key.seg, segment.latests); err != nil {
				fmt.Printf("verticalCache: ERROR in VerticalFlushLatests: %v", err)
			}
		}

		log.Printf("Migration: flushed key: %v", key)
		segment.lastFlushRT = time.Now()

	}
	bc.Unlock()
}
