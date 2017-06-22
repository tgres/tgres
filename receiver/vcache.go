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
	latests      map[int64]time.Time // rra.latest
	value        map[int64]float64
	duration     map[int64]int64
	maxLatest    time.Time
	latestIndex  int64
	lastFlushRT  time.Time
	lastSFlushRT time.Time // state flush
	step         time.Duration
	size         int64
}

type dsStateSegment struct {
	*sync.Mutex
	lastFlushRT time.Time
	lastupdate  map[int64]time.Time
	value       map[int64]float64
	duration    map[int64]int64
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
	dps     map[bundleKey]*verticalCacheSegment
	dss     map[int64]*dsStateSegment // keyed on seg
	minStep time.Duration
	*sync.Mutex
}

// Insert new data into the cache
func (vc *verticalCache) updateDps(rra serde.DbRoundRobinArchiver) {
	if rra.PointCount() == 0 {
		// Nothing for us to do. This can happen if other RRAs in the
		// DS have points, thus its getting flushed.
		return
	}

	seg, idx := rra.Seg(), rra.Idx()
	key := bundleKey{rra.BundleId(), seg}

	vc.Lock()

	segment := vc.dps[key]
	if segment == nil {
		segment = &verticalCacheSegment{
			Mutex:        &sync.Mutex{},
			rows:         make(map[int64]crossRRAPoints),
			latests:      make(map[int64]time.Time),
			value:        make(map[int64]float64),
			duration:     make(map[int64]int64),
			step:         rra.Step(),
			size:         rra.Size(),
			lastFlushRT:  time.Now(), // Or else it will get sent to the flusher right away!
			lastSFlushRT: time.Now(), // Or else it will get sent to the flusher right away!
		}
		vc.dps[key] = segment
	}

	vc.Unlock()

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
	segment.value[idx] = rra.Value()
	segment.duration[idx] = rra.Duration().Nanoseconds() / 1e6

	segment.Unlock()
}

// Update DS state data
func (vc *verticalCache) updateDss(ds serde.DbDataSourcer) {

	seg, idx := ds.Seg(), ds.Idx()

	vc.Lock()
	segment := vc.dss[seg]
	if segment == nil {
		segment = &dsStateSegment{
			Mutex:       &sync.Mutex{},
			lastFlushRT: time.Now(),
			lastupdate:  make(map[int64]time.Time),
			duration:    make(map[int64]int64), // milliseconds
			value:       make(map[int64]float64),
		}
		vc.dss[seg] = segment
	}
	vc.Unlock()

	segment.Lock()
	segment.lastupdate[idx] = ds.LastUpdate()
	segment.duration[idx] = ds.Duration().Nanoseconds() / 1e6
	segment.value[idx] = ds.Value()
	segment.Unlock()
}

type vcStats struct {
	// Currently in Vcache
	dpSegments int
	dpRows     int // sum of all dp segments
	dpPoints   int
	dsSegments int
	// Flush stats
	dpFlushes       int
	dpFlushedPoints int
	dpFlushBlocked  int
	rsFlushes       int
	dsFlushes       int
}

func (vc *verticalCache) stats() *vcStats {
	vc.Lock()
	defer vc.Unlock()

	var st vcStats

	st.dpSegments = len(vc.dps)
	for _, segment := range vc.dps {
		segment.Lock()
		st.dpRows += len(segment.rows)
		for _, row := range segment.rows {
			st.dpPoints += len(row)
		}
		segment.Unlock()
	}
	st.dsSegments = len(vc.dss)

	return &st
}

// When full is false (most of the time), all this does is queue up
// a bunch of flush requests. No actual DB requests happen here.
func (vc *verticalCache) flush(ch chan *vDpFlushRequest, full bool) *vcStats {

	dpFlushes, dpFlushedPoints, dpFlushBlocked, dsFlushes, rsFlushes := 0, 0, 0, 0, 0
	toFlush := make(map[bundleKey]*verticalCacheSegment, len(vc.dps))

	vc.Lock()
	for key, segment := range vc.dps {
		if len(segment.rows) == 0 {
			continue
		}
		now := time.Now()
		if !full && (now.Sub(segment.lastFlushRT) < vc.minStep) {
			continue
		}
		toFlush[key] = segment
	}
	vc.Unlock()

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
		flushDelay := vc.minStep * 2
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
		thisSegmentFlushes := 0
		for i, dps := range segment.rows {

			if !full && !segment.maxLatest.IsZero() && i == segment.latestIndex && now.Sub(segment.maxLatest) < flushDelay {
				continue
			}

			dfr := &vDpFlushRequest{key.bundleId, key.seg, i, dps, flushIVers, nil, nil, nil, nil}

			if full { // insist, even if we block
				ch <- dfr
			} else { // just skip over if channel full
				select {
				case ch <- dfr:
				default:
					// we're blocked, we'll try again next time
					dpFlushBlocked++
					continue
				}
			}
			dpFlushedPoints += len(dps)
			dpFlushes++ // how many chunks get pushed to the channel => one or more SQL
			thisSegmentFlushes++

			// delete the flushed segment row
			delete(segment.rows, i)
		}

		// RRA State
		if len(flushLatests) > 0 && thisSegmentFlushes > 0 {

			lat := make(map[int64]interface{}, len(flushLatests))
			for k, v := range flushLatests {
				lat[k] = interface{}(v)
			}

			dur := make(map[int64]interface{}, len(segment.duration))
			for k, v := range segment.duration {
				dur[k] = interface{}(v)
			}

			val := make(map[int64]interface{}, len(segment.value))
			for k, v := range segment.value {
				val[k] = interface{}(v)
			}

			// unlike dps, insist on a blocking operation
			ch <- &vDpFlushRequest{key.bundleId, key.seg, 0, nil, nil, lat, nil, dur, val}
			rsFlushes += 1
			segment.lastSFlushRT = time.Now()
		}

		// update lastFlushRT even if nothing was flushed above, we will only try
		// again in minStep time.
		segment.lastFlushRT = time.Now()
		segment.Unlock()
	}

	// DS States

	dssToFlush := make(map[int64]*dsStateSegment, len(vc.dss))

	vc.Lock()
	for seg, segment := range vc.dss {
		now := time.Now()
		if !full && (now.Sub(segment.lastFlushRT) < (vc.minStep * 2)) {
			continue
		}
		dssToFlush[seg] = segment
	}
	vc.Unlock()

	for seg, segment := range dssToFlush {
		segment.Lock()
		if len(segment.lastupdate) > 0 {

			lu := make(map[int64]interface{}, len(segment.lastupdate))
			for k, v := range segment.lastupdate {
				lu[k] = interface{}(v)
			}

			dur := make(map[int64]interface{}, len(segment.duration))
			for k, v := range segment.duration {
				dur[k] = interface{}(v)
			}

			val := make(map[int64]interface{}, len(segment.value))
			for k, v := range segment.value {
				val[k] = interface{}(v)
			}
			ch <- &vDpFlushRequest{0, seg, 0, nil, nil, nil, lu, dur, val}
			dsFlushes += 1

			// Clear out the segment
			segment.lastupdate = make(map[int64]time.Time)
			segment.duration = make(map[int64]int64)
			segment.value = make(map[int64]float64)
		}

		// even if there was nothing to flush consider it a flush
		segment.lastFlushRT = time.Now()
		segment.Unlock()
	}

	st := vc.stats()
	st.dpFlushes = dpFlushes
	st.dpFlushedPoints = dpFlushedPoints
	st.dpFlushBlocked = dpFlushBlocked
	st.rsFlushes = rsFlushes
	st.dsFlushes = dsFlushes

	return st
}

// This structure stores the slot index for the latest slot along with
// its version. With this information we can then compute the version
// for any slot index in the current iteration of the round-robin,
// which is generally just the version itself or version--.
type iVer struct {
	i   int64
	ver int
}

func (iv *iVer) version(i int64) int {
	version := iv.ver
	if i > iv.i {
		version--
		if version < 0 {
			version = 32767
		}
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
