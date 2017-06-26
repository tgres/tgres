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
	"time"

	"github.com/lib/pq"
	"github.com/tgres/tgres/rrd"
)

func dataSourceFromDsRec(dsr *dsRecord) (*DbDataSource, error) {

	if dsr.lastupdate == nil {
		dsr.lastupdate = &time.Time{}
	}
	if dsr.value == nil {
		dsr.value = new(float64)
	}
	if dsr.durationMs == nil {
		dsr.durationMs = new(int64)
	}

	var ident Ident
	err := json.Unmarshal(dsr.identJson, &ident)
	if err != nil {
		log.Printf("dataSourceFromRow(): error unmarshalling ident: %v", err)
		return nil, err
	}

	ds := NewDbDataSource(dsr.id, ident, dsr.seg, dsr.idx,
		rrd.NewDataSource(
			rrd.DSSpec{
				Step:       time.Duration(dsr.stepMs) * time.Millisecond,
				Heartbeat:  time.Duration(dsr.hbMs) * time.Millisecond,
				LastUpdate: *dsr.lastupdate,
				Value:      *dsr.value,
				Duration:   time.Duration(*dsr.durationMs) * time.Millisecond,
			},
		),
	)

	// This is so that we can know whether it was an INSERT or an UPDATE
	ds.created = dsr.created
	return ds, nil
}

func dsRecordFromRow(rows *sql.Rows) (*dsRecord, error) {
	var dsr dsRecord
	err := rows.Scan(&dsr.id, &dsr.identJson, &dsr.stepMs, &dsr.hbMs, &dsr.seg, &dsr.idx, &dsr.lastupdate, &dsr.value, &dsr.durationMs, &dsr.created)
	return &dsr, err
}

func dataSourceFromRow(rows *sql.Rows) (*DbDataSource, error) {
	dsr, err := dsRecordFromRow(rows)
	if err != nil {
		log.Printf("dataSourceFromRow(): error scanning row: %v", err)
		return nil, err
	}
	return dataSourceFromDsRec(dsr)
}

type pgSearchResult struct {
	rows  *sql.Rows
	err   error
	ident Ident
}

func (sr *pgSearchResult) Next() bool {
	if !sr.rows.Next() {
		return false
	}

	var b []byte
	sr.err = sr.rows.Scan(&b)
	if sr.err != nil {
		log.Printf("pgSearchResult.Next(): error scanning row: %v", sr.err)
		return false
	}
	var ident Ident // we want a new map created, not reuse the same one
	sr.err = json.Unmarshal(b, &ident)
	if sr.err != nil {
		log.Printf("Search(): error unmarshalling ident %q: %v", string(b), sr.err)
		return false
	}
	sr.ident = ident
	return true
}

func (sr *pgSearchResult) Ident() Ident { return sr.ident }
func (sr *pgSearchResult) Close() error { return sr.rows.Close() }

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

// Turn {3:0.1, 10:0.2, 9:0.3}
// to "dp[$1:$2] = $3, dp[$3:$4] = $5", {3, 3, "{0.1}", 9, 10, "{0.2,0.3}"}
// n is the beginning index, ($1 above), col is column name, e.g. "dp"
func arrayUpdateStatement_(m map[int64]interface{}, n int, col string) (dest string, args []interface{}) {

	if len(m) == 0 {
		return "", nil
	}

	// Find max and min indexes
	var min, max int64 = -1, -1
	for k, _ := range m {
		if min == -1 {
			min, max = k, k
		} else {
			if k < min {
				min = k
			}
			if k > max {
				max = k
			}
		}
	}

	dests := make([]string, 0)
	args = make([]interface{}, 0)

	vals := []interface{}{m[min]}
	begin, end := min, min

	for i := min + 1; i <= max; i++ {
		if v, ok := m[i]; ok {
			if begin == -1 {
				begin, end = i, i
			} else {
				end = i
			}
			vals = append(vals, v)
		} else if begin != -1 {
			// dump it
			dests = append(dests, fmt.Sprintf("%s[$%d:$%d]=$%d", col, n, n+1, n+2))
			n += 3
			args = append(args, begin, end, pq.Array(vals))

			// start fresh
			begin, vals = -1, make([]interface{}, 0)
		}
	}
	if begin != -1 {
		dests = append(dests, fmt.Sprintf("%s[$%d:$%d]=$%d", col, n, n+1, n+2))
		args = append(args, begin, end, pq.Array(vals))
	}

	return strings.Join(dests, ","), args
}

func arrayUpdateStatement(m map[int64]float64) [][]interface{} {

	if len(m) == 0 {
		return nil
	}

	// Find max and min indexes
	var min, max int64 = -1, -1
	for k, _ := range m {
		if min == -1 {
			min, max = k, k
		} else {
			if k < min {
				min = k
			}
			if k > max {
				max = k
			}
		}
	}

	args := make([][]interface{}, 0)

	vals := []interface{}{m[min]}
	begin, end := min, min

	for i := min + 1; i <= max; i++ {
		if v, ok := m[i]; ok {
			if begin == -1 {
				begin, end = i, i
			} else {
				end = i
			}
			vals = append(vals, v)
		} else if begin != -1 {
			// dump it
			args = append(args, []interface{}{begin, end, pq.Array(vals)})

			// start fresh
			begin, vals = -1, make([]interface{}, 0)
		}
	}
	if begin != -1 {
		args = append(args, []interface{}{begin, end, pq.Array(vals)})
	}

	return args
}

type arrayUpdateChunk struct {
	begin, end int64
	vals       []interface{}
}

func arrayUpdateChunks(m map[int64]interface{}) []*arrayUpdateChunk {

	if len(m) == 0 {
		return nil
	}

	// Find max and min indexes
	var min, max int64 = -1, -1
	for k, _ := range m {
		if min == -1 {
			min, max = k, k
		} else {
			if k < min {
				min = k
			}
			if k > max {
				max = k
			}
		}
	}

	var result []*arrayUpdateChunk

	vals := []interface{}{m[min]}
	begin, end := min, min

	for i := min + 1; i <= max; i++ {
		if v, ok := m[i]; ok {
			if begin == -1 {
				begin, end = i, i
			} else {
				end = i
			}
			vals = append(vals, v)
		} else if begin != -1 {
			// dump it
			result = append(result, &arrayUpdateChunk{begin, end, vals})

			// start fresh
			begin, vals = -1, make([]interface{}, 0)
		}
	}
	if begin != -1 {
		result = append(result, &arrayUpdateChunk{begin, end, vals})
	}

	return result
}

func singleStmtUpdateArgs(chunks []*arrayUpdateChunk, col string, n int, prefix []interface{}) (dest string, args []interface{}) {
	var dests []string
	args = append(args, prefix...)
	for _, chunk := range chunks {
		dests = append(dests, fmt.Sprintf("%s[$%d:$%d]=$%d", col, n, n+1, n+2))
		args = append(args, chunk.begin, chunk.end, pq.Array(chunk.vals))
		n += 3
	}
	if len(dests) == 0 {
		return fmt.Sprintf("%s[0:0]=NULL", col), args
	}
	return strings.Join(dests, ","), args
}

func multiStmtUpdateArgs(chunks []*arrayUpdateChunk, prefix []interface{}) (result [][]interface{}) {
	for _, chunk := range chunks {
		args := append([]interface{}{}, prefix...)
		args = append(args, chunk.begin, chunk.end, pq.Array(chunk.vals))
		result = append(result, args)
	}
	return result
}
