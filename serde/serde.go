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
	FetchDataSourceById(id int64) (*rrd.MetaDataSource, error)
}

type DataSourcesFetcher interface {
	FetchDataSources() ([]*rrd.MetaDataSource, error)
}

type DataSourcesFetchOrCreator interface {
	FetchOrCreateDataSource(name string, dsSpec *DSSpec) (*rrd.MetaDataSource, error)
}

type DataSourceFlusher interface {
	FlushDataSource(ds *rrd.MetaDataSource) error
}

type SeriesQuerier interface {
	SeriesQuery(ds *rrd.MetaDataSource, from, to time.Time, maxPoints int64) (Series, error)
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

// DSSpec describes a DataSource. DSSpec is a schema that is used to
// create the DataSource. This is necessary so that DS's can be crated
// on-the-fly.
type DSSpec struct {
	Step      time.Duration
	Heartbeat time.Duration
	RRAs      []*RRASpec
}

// RRASpec is the RRA definition part of DSSpec.
type RRASpec struct {
	Function rrd.Consolidation
	Step     time.Duration
	Size     time.Duration
	Xff      float64
}
