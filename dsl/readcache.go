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

package dsl

import "github.com/tgres/tgres/serde"

type ReadCache struct {
	serde.SerDe
	dsns *DataSourceNames
}

func NewReadCache(db serde.SerDe) *ReadCache {
	return &ReadCache{SerDe: db, dsns: &DataSourceNames{}}
}

func (r *ReadCache) dsIdsFromIdent(ident string) map[string]int64 {
	result := r.dsns.dsIdsFromIdent(ident)
	if len(result) == 0 {
		r.dsns.reload(r)
		result = r.dsns.dsIdsFromIdent(ident)
	}
	return result
}

func (r *ReadCache) FsFind(pattern string) []*FsFindNode {
	r.dsns.reload(r)
	return r.dsns.fsFind(pattern)
}
