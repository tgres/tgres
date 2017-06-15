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

// Package serde is the interface (and currently PostgreSQL
// implementaiton) for Serialization/Deserialization of data.
package serde

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/series"
)

var debug bool

func init() {
	debug = os.Getenv("TGRES_SERDE_DEBUG") != ""
}

// An iterator, similar to sql.Rows.
type SearchResult interface {
	Next() bool
	Close() error
	Ident() Ident
}

type SearchQuery map[string]string

type DataSourceSearcher interface {
	// Return a list od DS ids based on the query. How the query works
	// is up to the serde, it can even ignore the argumen, but the
	// general idea was a key: regex list where the underlying engine
	// would return all DSs whose ident tags named key match the
	// regex. Remember to Close() the SearchResult in the end.
	Search(query SearchQuery) (SearchResult, error)
}

type Fetcher interface {
	DataSourceSearcher
	// Fetch all the data sources (used to populate the cache on start)
	FetchDataSources(time.Duration) ([]rrd.DataSourcer, error)
	// Fetch or create a single DS. Passing a nil dsSpec disables creation.
	FetchOrCreateDataSource(ident Ident, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error)
	// FetchSeries is responsible for presenting a DS as a
	// series.Series. This may include selecting the most suitable RRA
	// of the DS to satisfy span and resolution requested, as well as
	// setting up a database cursor which will be used to iterate over
	// the series.
	FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error)
}

type EventListener interface {
	RegisterDeleteListener(func(Ident)) error
}

type Flusher interface {
	FlushDataPoints(bunlde_id, seg, i int64, dps, vers map[int64]interface{}) (int, error)
	FlushDSStates(seg int64, lastupdate, value, duration map[int64]interface{}) (int, error)
	FlushRRAStates(bundle_id, seg int64, latests, value, duration map[int64]interface{}) (int, error)
}

type SerDe interface {
	Fetcher() Fetcher
	Flusher() Flusher
	EventListener() EventListener
}

type DbAddresser interface {
	ListDbClientIps() ([]string, error) // Use the database to infer outside IPs of other connected clients
	MyDbAddr() (*string, error)
}

type DbSerDe interface {
	SerDe
	DbAddresser() DbAddresser
}

type Ident map[string]string

func (it Ident) String() string {

	// It's tempting to cache the resulting string in the receiver,
	// but given that most of what we do is look up newly arriving
	// data points, this cache wouldn't really be used that much and
	// only take up additional space. Also what happens if the ident
	// changes map?

	keys := make([]string, 0, len(it))
	for k, _ := range it {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf := bytes.NewBuffer(make([]byte, 0, 256))
	buf.WriteByte('{')
	for i, k := range keys {
		fmt.Fprintf(buf, `%q: %q`, k, it[k])
		if i < len(keys)-1 {
			buf.WriteByte(',')
		}
	}
	buf.WriteByte('}')
	return buf.String()
}
