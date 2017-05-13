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
	"math"
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

	bc.Unlock()

	segment.Lock()

	for i, v := range rra.DPs() {
		if len(segment.rows[i]) == 0 {
			segment.rows[i] = make(map[int64]float64, serde.PgSegmentWidth)
		}
		if !math.IsNaN(v) { // With versions NaNs can be ignored.
			segment.rows[i][idx] = v
		}
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
		segment.Lock()
		st.rows += len(segment.rows)
		for _, row := range segment.rows {
			st.points += len(row)
		}
		segment.Unlock()
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

		segment.Lock()

		// We need to iterate over the rows twice, the first time to
		// collect correct values for flushLatests which we will then
		// use to compute versions for the data points.

		// TODO: Should this be configurable, it seems a little lame
		// flushDelay ensures that we do not flush entries that are
		// incomplete because enough time has not passed for them to
		// become "saturated" with points.
		flushDelay := bc.minStep * 2
		if flushDelay > time.Minute {
			flushDelay = time.Minute
		}

		// This is our version of latests, since if we're going to
		// possibly skip the latest segment row, the latests in the
		// cache are not what should be written to the database.
		flushLatests := make(map[int64]time.Time, len(segment.latests))

		// First iteration: build flushLatests
		for i, dps := range segment.rows {

			// Translation: Unless a full flush is requested, if this
			// segment has a maxLatest value (precaution) and this
			// value is less than flushDelay in the past, continue.
			if !full && !segment.maxLatest.IsZero() && i == segment.latestIndex && now.Sub(segment.maxLatest) < flushDelay {
				continue
			}

			// latests - keep the highest value we come across
			for idx, _ := range dps {
				l := rrd.SlotTime(i, segment.latests[idx], segment.step, segment.size)
				if flushLatests[idx].Before(l) { // no value is zero time
					flushLatests[idx] = l
				}
			}
		}

		// Build a map of latest i and version according to flushLatests
		flushIVers := latestIVers(flushLatests, segment.step, segment.size)

		// Second iteration: datapoints and versions
		for i, dps := range segment.rows {

			if !full && !segment.maxLatest.IsZero() && i == segment.latestIndex && now.Sub(segment.maxLatest) < flushDelay {
				continue
			}

			idps, vers := dataPointsWithVersions(dps, i, flushIVers)

			if full { // insist, even if we block
				ch <- &vDpFlushRequest{key.bundleId, key.seg, i, idps, vers, nil}
			} else { // just skip over if channel full
				select {
				default:
					// we're blocked, we'll try again next time
					blocked++
					continue
				case ch <- &vDpFlushRequest{key.bundleId, key.seg, i, idps, vers, nil}:
				}
			}

			// delete the flushed segment row
			delete(segment.rows, i)

			count += len(dps)
			flushCount += 1 // how many chunks get pushed to the channel => one or more SQL
		}

		if len(flushLatests) > 0 {
			ch <- &vDpFlushRequest{key.bundleId, key.seg, 0, nil, nil, flushLatests}
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

func latestIVers(latests map[int64]time.Time, step time.Duration, size int64) map[int64]*iVer {
	result := make(map[int64]*iVer, len(latests))
	for idx, latest := range latests {
		i := rrd.SlotIndex(latest, step, size)
		span_ms := (step.Nanoseconds() / 1e6) * size
		latest_ms := latest.UnixNano() / 1e6
		ver := int((latest_ms / span_ms) % 32767)
		result[idx] = &iVer{i: i, ver: ver}
	}
	return result
}

// Convert data points map into two maps: (1) same dps, only as
// interface{}, (2) versions as interface{} (interface{} because this
// is what the postgres interface will eventually need.
func dataPointsWithVersions(in crossRRAPoints, i int64, ivs map[int64]*iVer) (dps, vers map[int64]interface{}) {
	dps = make(map[int64]interface{}, len(in))
	vers = make(map[int64]interface{}, len(in))
	for idx, dp := range in {
		dps[idx] = dp
		vers[idx] = ivs[idx].version(i)
	}
	return dps, vers
}
