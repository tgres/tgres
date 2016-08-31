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

package serde

import (
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/rrd"
	"time"
)

// This thing knows how to load/save series in some storage
type SerDe interface {
	// Create a DS with name, and/or return it
	CreateOrReturnDataSource(name string, dsSpec *DSSpec) (*rrd.DataSource, error)

	FetchDataSource(id int64) (*rrd.DataSource, error)
	FetchDataSourceByName(name string) (*rrd.DataSource, error)

	FetchDataSources() ([]*rrd.DataSource, error)
	FetchDataSourceNames() (map[string]int64, error)

	// Flush a DS
	FlushDataSource(ds *rrd.DataSource) error
	// Query
	SeriesQuery(ds *rrd.DataSource, from, to time.Time, maxPoints int64) (dsl.Series, error)
	// Use the database to infer outside IPs of other connected clients
	ListDbClientIps() ([]string, error)
	MyDbAddr() (*string, error)
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
