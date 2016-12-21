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
)

type DataSourceNamesFetcher interface {
	FetchDataSourceNames() (map[string]int64, error)
}

type DataSourceFetcher interface {
	FetchDataSourceById(id int64) (rrd.DataSourcer, error)
}

type DataSourcesFetcher interface {
	FetchDataSources() ([]rrd.DataSourcer, error)
}

type DataSourcesFetchOrCreator interface {
	FetchOrCreateDataSource(name string, dsSpec *rrd.DSSpec) (rrd.DataSourcer, error)
}

type DataSourceFlusher interface {
	FlushDataSource(ds rrd.DataSourcer) error
}

type SeriesQuerier interface {
	SeriesQuery(ds rrd.DataSourcer, from, to time.Time, maxPoints int64) (rrd.Series, error)
}

type DataSourceSerDe interface {
	DataSourcesFetcher
	DataSourceFlusher
	DataSourcesFetchOrCreator
}

type DbAddresser interface {
	// Use the database to infer outside IPs of other connected clients
	ListDbClientIps() ([]string, error)
	MyDbAddr() (*string, error)
}

// This thing knows how to load/save series in some storage
type SerDe interface {
	DataSourceSerDe
	DataSourceFetcher
	DataSourceNamesFetcher
	SeriesQuerier
}

// This is a relational DB server
type DbSerDe interface {
	SerDe
	DbAddresser
}
