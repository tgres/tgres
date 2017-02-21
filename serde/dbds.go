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
	"time"

	"github.com/tgres/tgres/rrd"
)

type DbDataSource struct {
	rrd.DataSourcer
	ident   Ident
	id      int64
	created bool
}

type DbDataSourcer interface {
	rrd.DataSourcer
	Ident() Ident
	Id() int64
	Created() bool
}

func (ds *DbDataSource) Ident() Ident  { return ds.ident }
func (ds *DbDataSource) Id() int64     { return ds.id }
func (ds *DbDataSource) Created() bool { return ds.created }

func NewDbDataSource(id int64, ident Ident, ds rrd.DataSourcer) *DbDataSource {
	return &DbDataSource{
		DataSourcer: ds,
		id:          id,
		ident:       ident,
	}
}

func (ds *DbDataSource) Copy() rrd.DataSourcer {
	result := &DbDataSource{
		id:    ds.id,
		ident: make(Ident, len(ds.ident)),
	}
	if ds.DataSourcer != nil {
		result.DataSourcer = ds.DataSourcer.Copy()
	}
	for k, v := range ds.ident {
		result.ident[k] = v
	}
	return result
}

type dsRecord struct {
	id         int64
	identJson  []byte
	stepMs     int64
	hbMs       int64
	lastupdate *time.Time
	value      float64
	durationMs int64
	created    bool
}
