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
	"bytes"
	"fmt"
	"sort"

	"github.com/tgres/tgres/rrd"
)

type DbDataSource struct {
	rrd.DataSourcer
	ident Ident
	id    int64
}

type DbDataSourcer interface {
	rrd.DataSourcer
	Ident() Ident
	Id() int64
}

func (ds *DbDataSource) Ident() Ident { return ds.ident }
func (ds *DbDataSource) Id() int64    { return ds.id }

func NewDbDataSource(id int64, ident Ident, ds rrd.DataSourcer) *DbDataSource {
	return &DbDataSource{
		DataSourcer: ds,
		id:          id,
		ident:       ident,
	}
}

func (ds *DbDataSource) Copy() rrd.DataSourcer {
	result := &DbDataSource{
		DataSourcer: ds.DataSourcer.Copy(),
		id:          ds.id,
		ident:       make(Ident, len(ds.ident)),
	}
	for k, v := range ds.ident {
		result.ident[k] = v
	}
	return result
}

type Ident map[string]string

func (it Ident) String() string {

	// It's tempting to cache the resulting string in the receiver,
	// but given that most of what we do is look up newly arriving
	// data points, this cache wouldn't really be used that much and
	// only take up additional space.

	keys := make([]string, 0, len(it))
	for k, _ := range it {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	buf := &bytes.Buffer{}
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
