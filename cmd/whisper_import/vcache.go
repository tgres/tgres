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
	latests     map[int64]time.Time // rra.latest
	maxLatest   time.Time
	latestIndex int64
	step        time.Duration
	size        int64
}

type verticalCache map[bundleKey]*verticalCacheSegment

type bundleKey struct {
	bundleId, seg int64
}

func (vc verticalCache) update(rra serde.DbRoundRobinArchiver, origLatest time.Time) {

	seg, idx := rra.Seg(), rra.Idx()
	key := bundleKey{rra.BundleId(), seg}

	segment := vc[key]
	if segment == nil {
		segment = &verticalCacheSegment{
			rows:    make(map[int64]crossRRAPoints),
			latests: make(map[int64]time.Time),
			step:    rra.Step(),
			size:    rra.Size(),
		}
		vc[key] = segment
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
	}
}

type stats struct {
	m                  *sync.Mutex
	pointCount, sqlOps int
}

func (vc verticalCache) flush(db serde.VerticalFlusher) error {
	var wg sync.WaitGroup
	fmt.Printf("[db] Starting vcache flush (%d segments)...\n", len(vc))

	st := stats{m: &sync.Mutex{}}

	n, MAX, vl := 0, 64, len(vc)
	for k, segment := range vc {

		wg.Add(1)
		go flushSegment(db, &wg, &st, k, segment)
		delete(vc, k)
		n++

		if n >= MAX {
			fmt.Printf("[db] ... ... waiting on %d of %d segment flushes ...\n", n, vl)
			wg.Wait()
			n = 0
		}

	}
	fmt.Printf("[db] ... ... waiting on %d segment flushes (final) ...\n", n)
	wg.Wait() // final wait

	fmt.Printf("[db] Vcache flush complete, %d points in %d SQL ops.\n", st.pointCount, st.sqlOps)
	totalPoints += st.pointCount
	totalSqlOps += st.sqlOps
	return nil
}

func flushSegment(db serde.VerticalFlusher, wg *sync.WaitGroup, st *stats, k bundleKey, segment *verticalCacheSegment) {
	defer wg.Done()

	fmt.Printf("[db]  flushing %d rows for segment %v:%v...\n", len(segment.rows), k.bundleId, k.seg)

	for i, row := range segment.rows {
		so, err := db.VerticalFlushDPs(k.bundleId, k.seg, i, row)
		if err != nil {
			fmt.Printf("[db] Error flushing segment %v:%v: %v\n", k.bundleId, k.seg, err)
			return
		}
		st.m.Lock()
		st.sqlOps += so
		st.pointCount += len(row)
		st.m.Unlock()
	}

	if len(segment.latests) > 0 {
		fmt.Printf("[db]  flushing latests for segment %v:%v...\n", k.bundleId, k.seg)
		so, err := db.VerticalFlushLatests(k.bundleId, k.seg, segment.latests)
		if err != nil {
			fmt.Printf("[db] Error flushing segment %v:%v: %v\n", k.bundleId, k.seg, err)
			return
		}
		st.m.Lock()
		st.sqlOps += so
		st.m.Unlock()
	} else {
		fmt.Printf("[db]  no latests to flush for segment %v:%v...\n", k.bundleId, k.seg)
	}

}
