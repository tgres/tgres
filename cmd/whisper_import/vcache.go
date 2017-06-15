package main

import (
	"fmt"
	"sync"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

type crossRRAPoints map[int64]float64

type verticalCacheSegment struct {
	rows map[int64]crossRRAPoints
	// The latest timestamp for RRAs, keyed by RRA.pos.
	latests     map[int64]interface{} // rra.latest
	maxLatest   time.Time
	latestIndex int64
	step        time.Duration
	size        int64
}

type verticalCache struct {
	ts  int   // just some number for identification
	seg int64 // which segment this was for
	dps map[bundleKey]*verticalCacheSegment
	dss map[int64]map[int64]interface{}
}

type bundleKey struct {
	bundleId, seg int64
}

func (vc verticalCache) updateDps(rra serde.DbRoundRobinArchiver, origLatest time.Time) {

	seg, idx := rra.Seg(), rra.Idx()
	key := bundleKey{rra.BundleId(), seg}

	segment := vc.dps[key]
	if segment == nil {
		segment = &verticalCacheSegment{
			rows:    make(map[int64]crossRRAPoints),
			latests: make(map[int64]interface{}),
			step:    rra.Step(),
			size:    rra.Size(),
		}
		vc.dps[key] = segment
	}

	latest := rra.Latest()

	for i, v := range rra.DPs() {
		// It is possible for the actual (i.e. what was in the
		// database) latest to be ahead of us. If that is the case, we
		// need to make sure not to update "future" slots by accident.
		slotTime := rrd.SlotTime(i, origLatest, rra.Step(), rra.Size())
		if !slotTime.After(latest) {
			if len(segment.rows[i]) == 0 {
				segment.rows[i] = map[int64]float64{idx: v}
			}
			segment.rows[i][idx] = v
		}
	}

	// Only update latests if our latest is later than actual latest
	if latest.After(origLatest) {
		if segment.maxLatest.Before(latest) {
			segment.maxLatest = latest
			segment.latestIndex = rrd.SlotIndex(latest, rra.Step(), rra.Size())
		}
		segment.latests[idx] = latest
	} else {
		segment.latests[idx] = origLatest
	}

}

// Update DS state data
func (vc *verticalCache) updateDss(ds serde.DbDataSourcer) {

	seg, idx := ds.Seg(), ds.Idx()

	segment := vc.dss[seg]
	if segment == nil {
		segment = make(map[int64]interface{})
		vc.dss[seg] = segment
	}

	segment[idx] = ds.LastUpdate()
}

type vstats struct {
	*sync.Mutex
	pointCount, sqlOps int
}

func (vc verticalCache) flush(db serde.Flusher) error {
	var (
		wg sync.WaitGroup
		st = vstats{Mutex: &sync.Mutex{}}
	)

	if len(vc.dps) > 0 {
		fmt.Printf("[db] [%v] Starting vcache flush (%d segments)...\n", vc.ts, len(vc.dps))

		n, MAX, vl := 0, 64, len(vc.dps)
		for k, segment := range vc.dps {

			wg.Add(1)
			go flushSegment(db, &wg, &st, k, segment, vc.ts)
			delete(vc.dps, k)
			n++

			if n >= MAX {
				fmt.Printf("[db] [%v] ... ... waiting on %d of %d segment flushes ...\n", vc.ts, n, vl)
				wg.Wait()
				n = 0
			}

		}
		fmt.Printf("[db] [%v] ... ... waiting on remaining %d segment flushes ...\n", vc.ts, n)
		wg.Wait() // final wait
	}

	fmt.Printf("[db] [%v] Flushing %d DS states ...\n", vc.ts, len(vc.dss))
	for k, lu := range vc.dss {
		ops, err := db.FlushDSStates(k, lu, nil, nil)
		if err != nil {
			fmt.Printf("[db] [%v] EROR flushing DS state: %v\n", vc.ts, err)
		}
		st.sqlOps += ops
	}
	fmt.Printf("[db] [%v] Flushed %d DS states.\n", vc.ts, len(vc.dss))

	fmt.Printf("[db] [%v] Vcache flush complete, %d points in %d SQL ops.\n", vc.ts, st.pointCount, st.sqlOps)
	stats.Lock()
	st.Lock()
	stats.totalPoints += st.pointCount
	stats.totalSqlOps += st.sqlOps
	st.Unlock()
	stats.Unlock()
	return nil
}

func flushSegment(db serde.Flusher, wg *sync.WaitGroup, st *vstats, k bundleKey, segment *verticalCacheSegment, ts int) {
	defer wg.Done()

	if len(segment.rows) == 0 {
		return
	}

	fmt.Printf("[db] [%v] flushing %d rows for segment %v:%v...\n", ts, len(segment.rows), k.bundleId, k.seg)

	// Build a map of latest i and version according to flushLatests
	ivers := latestIVers(segment.latests, segment.step, segment.size)

	maxWidth, maxIdx := 0, 0

	for i, row := range segment.rows {
		idps, vers := dataPointsWithVersions(row, i, ivers)
		so, err := db.FlushDataPoints(k.bundleId, k.seg, i, idps, vers)
		if err != nil {
			fmt.Printf("[db] [%v] Error flushing DP segment %v:%v: %v\n", ts, k.bundleId, k.seg, err)
			return
		}
		st.Lock()
		st.sqlOps += so
		st.pointCount += len(row)
		st.Unlock()

		for j, _ := range row {
			if int(j) > maxIdx {
				maxIdx = int(j)
			}
		}
		if len(row) > maxWidth {
			maxWidth = len(row)
		}
	}

	if len(segment.latests) > 0 {
		fmt.Printf("[db] [%v] flushing RRA state for segment %v:%v...\n", ts, k.bundleId, k.seg)
		so, err := db.FlushRRAStates(k.bundleId, k.seg, segment.latests, nil, nil)
		if err != nil {
			fmt.Printf("[db] [%v] Error flushing RRA segment %v:%v: %v\n", ts, k.bundleId, k.seg, err)
			return
		}
		st.Lock()
		st.sqlOps += so
		st.Unlock()
	} else {
		fmt.Printf("[db] [%v] no latests to flush for segment %v:%v...\n", ts, k.bundleId, k.seg)
	}

	fmt.Printf("[db] [%v] DONE     %d rows (%d wide, max idx: %d) for segment %v:%v...\n", ts, len(segment.rows), maxWidth, maxIdx, k.bundleId, k.seg)
}

type iVer struct {
	i   int64
	ver int
}

func (iv *iVer) version(i int64) int {
	version := iv.ver
	if i > iv.i {
		version--
	}
	return version
}

func latestIVers(latests map[int64]interface{}, step time.Duration, size int64) map[int64]*iVer {
	result := make(map[int64]*iVer, len(latests))
	for idx, ilatest := range latests {
		latest := ilatest.(time.Time)
		i := rrd.SlotIndex(latest, step, size)
		span_ms := (step.Nanoseconds() / 1e6) * size
		latest_ms := latest.UnixNano() / 1e6
		ver := int((latest_ms / span_ms) % 32767)
		result[idx] = &iVer{i: i, ver: ver}
	}
	return result
}

func dataPointsWithVersions(in crossRRAPoints, i int64, ivs map[int64]*iVer) (dps, vers map[int64]interface{}) {
	dps = make(map[int64]interface{}, len(in))
	vers = make(map[int64]interface{}, len(in))
	for idx, dp := range in {
		dps[idx] = dp
		vers[idx] = ivs[idx].version(i)
	}
	return dps, vers
}
