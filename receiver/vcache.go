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

package receiver

import (
	"sync"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/serde"
)

// Vertical cache caches data points from multiple series for the same
// period in the same "array", which is really a map.

// a bundle of data points from various RRAs, keyed by position
// within their segment, (RRA.pos % bundle.width).
type crossRRAPoints map[int64]float64

// map[time_index]map[series_index]value
type verticalCacheSegment struct {
	*sync.Mutex
	// data points keyed by index in the RRD, this key can be
	// converted to a timestamp if we know latest and the RRA
	// step/size.
	rows map[int64]crossRRAPoints
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
	m       map[bundleKey]*verticalCacheSegment
	minStep time.Duration
	*sync.Mutex
}

// Insert new data into the cache
func (bc *verticalCache) update(rra serde.DbRoundRobinArchiver) {
	if rra.PointCount() == 0 {
		// Nothing for us to do. This can happen is other RRAs in the
		// DS have points, thus its getting flushed.
		return
	}

	seg, idx := rra.Seg(), rra.Idx()
	key := bundleKey{rra.BundleId(), seg}

	bc.Lock()

	segment := bc.m[key]
	if segment == nil {
		segment = &verticalCacheSegment{
			Mutex:       &sync.Mutex{},
			rows:        make(map[int64]crossRRAPoints),
			latests:     make(map[int64]time.Time),
			step:        rra.Step(),
			size:        rra.Size(),
			lastFlushRT: time.Now(), // Or else it will get sent to the flusher right away!
		}
		bc.m[key] = segment
	}

	segment.Lock()
	bc.Unlock()

	for i, v := range rra.DPs() {
		if len(segment.rows[i]) == 0 {
			segment.rows[i] = map[int64]float64{idx: v}
		}
		segment.rows[i][idx] = v
	}

	latest := rra.Latest()
	if segment.maxLatest.Before(latest) {
		segment.maxLatest = latest
		segment.latestIndex = rrd.SlotIndex(latest, rra.Step(), rra.Size())
	}
	segment.latests[idx] = latest

	segment.Unlock()
}

type vcStats struct {
	points         int
	segments       int
	rows           int // sum of all segments
	flushes        int
	flushedPoints  int
	flushedLatests int
	flushBlocked   int
}

func (bc *verticalCache) stats() *vcStats {
	bc.Lock()
	defer bc.Unlock()

	var st vcStats

	st.segments = len(bc.m)
	for _, segment := range bc.m {
		st.rows += len(segment.rows)
		for _, row := range segment.rows {
			st.points += len(row)
		}
	}

	return &st
}

func (bc *verticalCache) flush(ch chan *vDpFlushRequest, full bool) *vcStats {
	count, lcount, flushCount, blocked := 0, 0, 0, 0

	toFlush := make(map[bundleKey]*verticalCacheSegment, len(bc.m))

	bc.Lock()
	for key, segment := range bc.m {
		if len(segment.rows) == 0 {
			continue
		}

		now := time.Now()
		if !full && (now.Sub(segment.lastFlushRT) < bc.minStep) {
			continue
		}

		toFlush[key] = segment
	}
	bc.Unlock()

	// Now we have our own separate copy
	for key, segment := range toFlush {

		now := time.Now()

		// This is our version of latests, since if we're going to
		// possibly skip the latest segment row, the latests in the
		// cache are not what should be written to the database.
		flushLatests := make(map[int64]time.Time)

		segment.Lock()

		for i, dps := range segment.rows {

			// Do not flush entries that are at least 2 minStep "old", to make sure we're flushing "saturated" segments.
			if !full && !segment.maxLatest.IsZero() && i == segment.latestIndex && now.Sub(segment.maxLatest) < bc.minStep*2 {
				continue
			}

			if full { // insist, even if we block
				ch <- &vDpFlushRequest{key.bundleId, key.seg, i, dps, nil}
			} else { // just skip over if channel full
				select {
				default:
					// we're blocked
					blocked++
					continue
				case ch <- &vDpFlushRequest{key.bundleId, key.seg, i, dps, nil}:
				}
			}

			// delete the flushed segment row
			delete(segment.rows, i)

			// compute latests
			for idx, _ := range dps {
				l := rrd.SlotTime(i, segment.latests[idx], segment.step, segment.size)
				if flushLatests[idx].Before(l) { // no value is zero time
					flushLatests[idx] = l
				}
			}

			count += len(dps)
			flushCount += 1 // how many chunks get pushed to the channel => one or more SQL
		}

		if len(flushLatests) > 0 {
			ch <- &vDpFlushRequest{key.bundleId, key.seg, 0, nil, flushLatests}
			lcount += len(flushLatests)
			flushCount += 1
		}

		segment.lastFlushRT = time.Now()

		segment.Unlock()
	}

	st := bc.stats()
	st.flushes = flushCount
	st.flushedPoints = count
	st.flushedLatests = lcount
	st.flushBlocked = blocked

	return st
}
