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
	m       map[bundleKey]*verticalCacheSegment
	sr      statReporter
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

	latest := rra.Latest()
	if entry.maxLatest.Before(latest) {
		entry.maxLatest = latest
		entry.latestIndex = rrd.SlotIndex(latest, rra.Step(), rra.Size())
	}
	entry.latests[idx] = latest
}

func (bc *verticalCache) flush(ch chan *vDpFlushRequest, db serde.VerticalFlusher, full bool) int {
	rcount := 0
	scount := 0
	qcount := 0
	count := 0
	flushCount := 0

	bc.Lock()
	for key, segment := range bc.m {
		if len(segment.dps) == 0 {
			continue
		}

		for _, dps := range segment.dps {
			qcount += len(dps)
			rcount++
		}
		scount++

		now := time.Now()
		if !full && (now.Sub(segment.lastFlushRT) < bc.minStep) {
			continue
		}

		bc.sr.reportStatGauge("receiver.vcache.segment_rows", float64(len(segment.dps)))

		// This is our version of latests, since if we're going to
		// possibly skip the latest segment row, the latests in the
		// cache are not what should be written to the database.
		flushLatests := make(map[int64]time.Time)

		for i, dps := range segment.dps {

			// Do not flush entries that are at least 2 minStep "old", to make sure we're flushing "saturated" segments.
			if !full && !segment.maxLatest.IsZero() && i == segment.latestIndex && now.Sub(segment.maxLatest) < bc.minStep*2 {
				continue
			}

			if full { // insist, even if we block
				ch <- &vDpFlushRequest{key.bundleId, key.seg, i, dps, nil}
			} else { // just skip over if channel full
				select {
				default: // we're blocked
					continue
				case ch <- &vDpFlushRequest{key.bundleId, key.seg, i, dps, nil}:
				}
			}

			// delete the flushed segment row
			delete(segment.dps, i)

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
		}

		flushCount += 1
		segment.lastFlushRT = time.Now()

	}
	bc.Unlock()

	bc.sr.reportStatCount("receiver.vcache.points_flushed", float64(count))
	bc.sr.reportStatGauge("receiver.vcache.points", float64(qcount))
	bc.sr.reportStatGauge("receiver.vcache.segments", float64(scount))
	bc.sr.reportStatGauge("receiver.vcache.segment_rows", float64(rcount))
	bc.sr.reportStatGauge("receiver.vcache.points_per_rows", float64(qcount)/float64(scount)/float64(rcount))

	return flushCount
}
