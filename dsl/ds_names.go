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

import (
	"path/filepath"
	"sort"
	"strings"
	"sync"
)

// for Graphite-like listings
type DataSourceNames struct {
	sync.RWMutex
	names    map[string]int64
	prefixes map[string]bool
}

type FsFindNode struct {
	Name string
	Leaf bool
	dsId int64
}

type fsNodes []*FsFindNode

// sort.Interface
func (fns fsNodes) Len() int {
	return len(fns)
}
func (fns fsNodes) Less(i, j int) bool {
	return fns[i].Name < fns[j].Name
}
func (fns fsNodes) Swap(i, j int) {
	fns[i], fns[j] = fns[j], fns[i]
}

// Add prefixes given a name:
// "abcd" => []
// "abcd.efg.hij" => ["abcd.efg", "abcd"]
func (dsns *DataSourceNames) addPrefixes(name string) {
	prefix := name
	for ext := filepath.Ext(prefix); ext != ""; {
		prefix = name[0 : len(prefix)-len(ext)]
		dsns.prefixes[prefix] = true
		ext = filepath.Ext(prefix)
	}
}

func (dsns *DataSourceNames) reload(db dataSourceFetcher) error {
	names, err := db.FetchDataSourceNames()
	if err != nil {
		return err
	}

	dsns.Lock()
	defer dsns.Unlock()

	dsns.names = make(map[string]int64)
	dsns.prefixes = make(map[string]bool)
	for name, id := range names {
		dsns.names[name] = id
		dsns.addPrefixes(name)
	}

	return nil
}

func (dsns *DataSourceNames) FsFind(pattern string) []*FsFindNode {

	dsns.RLock()
	defer dsns.RUnlock()

	dots := strings.Count(pattern, ".")

	set := make(map[string]*FsFindNode)
	for k, dsId := range dsns.names {
		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &FsFindNode{Name: k, Leaf: true, dsId: dsId}
		}
	}

	for k, _ := range dsns.prefixes {
		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &FsFindNode{Name: k, Leaf: false}
		}
	}

	// convert to array
	result := make(fsNodes, 0)
	for _, v := range set {
		result = append(result, v)
	}

	// so that results are consistently ordered, or Grafanas get confused
	sort.Sort(result)

	return result
}

func (dsns *DataSourceNames) DsIdsFromIdent(ident string) map[string]int64 {
	result := make(map[string]int64)
	for _, node := range dsns.FsFind(ident) {
		if node.Leaf { // only leaf nodes are series names
			result[node.Name] = node.dsId
		}
	}
	return result
}
