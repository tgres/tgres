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
	"time"

	"github.com/tgres/tgres/rrd"
	"github.com/tgres/tgres/series"
)

type DataSourceNamesFetcher interface {
	FetchDataSourceNames() (map[string]int64, error)
}

type Fetcher interface {
	DataSourceNamesFetcher
	FetchDataSourceById(id int64) (rrd.DataSourcer, error)
	FetchDataSources() ([]rrd.DataSourcer, error)
	FetchOrCreateDataSource(name string, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error)
	FetchSeries(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (series.Series, error)
}

type Flusher interface {
	FlushDataSource(ds rrd.DataSourcer) error
}

type SerDe interface {
	Fetcher() Fetcher
	Flusher() Flusher
}

type DbAddresser interface {
	ListDbClientIps() ([]string, error) // Use the database to infer outside IPs of other connected clients
	MyDbAddr() (*string, error)
}

type DbSerDe interface {
	SerDe
	DbAddresser() DbAddresser
}
