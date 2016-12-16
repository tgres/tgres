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

import "github.com/tgres/tgres/rrd"

type DbDataSource struct {
	rrd.DataSourcer
	name string
	id   int64
}

type DbDataSourcer interface {
	rrd.DataSourcer
	Name() string
	Id() int64
}

func (ds *DbDataSource) Name() string { return ds.name }
func (ds *DbDataSource) Id() int64    { return ds.id }

func NewDbDataSource(id int64, name string, ds rrd.DataSourcer) *DbDataSource {
	return &DbDataSource{
		DataSourcer: ds,
		id:          id,
		name:        name,
	}
}

func (ds *DbDataSource) Copy() rrd.DataSourcer {
	return &DbDataSource{
		DataSourcer: ds.DataSourcer.Copy(),
		id:          ds.id,
		name:        ds.name,
	}
}
