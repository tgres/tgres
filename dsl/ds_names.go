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
	"fmt"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"

	"github.com/tgres/tgres/serde"
)

// DataSourceNames provides a way of searching dot-separated names
// using same rules as filepath.Match, as well as comma-separated
// values in curly braces such as "foo.{bar,baz}".
type dataSourceNames struct {
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
func (dsns *dataSourceNames) addPrefixes(name string) {
	prefix := name
	for ext := filepath.Ext(prefix); ext != ""; {
		prefix = name[0 : len(prefix)-len(ext)]
		dsns.prefixes[prefix] = true
		ext = filepath.Ext(prefix)
	}
}

func (dsns *dataSourceNames) reload(db serde.DataSourceSearcher) error {
	sr, err := db.Search(map[string]string{"name": ".*"})
	defer sr.Close()
	if err != nil {
		return err
	}

	dsns.Lock()
	defer dsns.Unlock()

	dsns.names = make(map[string]int64)
	dsns.prefixes = make(map[string]bool)

	for sr.Next() {
		name := sr.Ident()["name"]
		if name == "" {
			return fmt.Errorf("reload(): 'name' tag missing for DS id: %d", sr.Id())
		}
		dsns.names[name] = sr.Id()
		dsns.addPrefixes(name)
	}

	return nil
}

func (dsns *dataSourceNames) fsFind(pattern string) []*FsFindNode {

	if strings.Count(pattern, ",") > 0 {
		subres := make(fsNodes, 0)

		parts := regexp.MustCompile("{[^{}]*}").FindAllString(pattern, -1)
		for _, part := range parts {
			subs := strings.Split(strings.Trim(part, "{}"), ",")

			for _, sub := range subs {
				subres = append(subres, dsns.fsFind(strings.Replace(pattern, part, sub, -1))...)
			}
		}

		return subres
	}

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

func (dsns *dataSourceNames) dsIdsFromIdent(ident string) map[string]int64 {
	result := make(map[string]int64)
	for _, node := range dsns.fsFind(ident) {
		if node.Leaf { // only leaf nodes are series names
			result[node.Name] = node.dsId
		}
	}
	return result
}
