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

// fsFindCache provides a way of searching dot-separated ident
// elements using same rules as filepath.Match, as well as
// comma-separated values in curly braces such as "foo.{bar,baz}".
type fsFindCache struct {
	sync.RWMutex
	key      string // name of the ident key, required
	names    map[string]serde.Ident
	prefixes map[string]bool
}

type FsFindNode struct {
	Name  string
	Leaf  bool
	ident serde.Ident
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
func (dsns *fsFindCache) addPrefixes(name string) {
	prefix := name
	for ext := filepath.Ext(prefix); ext != ""; {
		prefix = name[0 : len(prefix)-len(ext)]
		if prefix != "" {
			// Do not allow blank prefixes (malformed names like '.org.apache'), Grafana doesn't like blanks
			// TODO: Is there a better solution, e.g. not allowing datapoints from those?
			dsns.prefixes[prefix] = true
		}
		ext = filepath.Ext(prefix)
	}
}

func (dsns *fsFindCache) reload(db serde.DataSourceSearcher) error {
	sr, err := db.Search(map[string]string{dsns.key: ".*"})
	defer sr.Close()
	if err != nil {
		return err
	}

	dsns.Lock()
	defer dsns.Unlock()

	dsns.names = make(map[string]serde.Ident)
	dsns.prefixes = make(map[string]bool)

	for sr.Next() {
		name := sr.Ident()[dsns.key]
		if name == "" {
			return fmt.Errorf("reload(): '%s' tag missing for DS ident: %s", dsns.key, sr.Ident().String())
		}
		dsns.names[name] = sr.Ident()
		dsns.addPrefixes(name)
	}

	return nil
}

func (dsns *fsFindCache) fsFind(pattern string) []*FsFindNode {

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
	for k, ident := range dsns.names {
		if yes, _ := filepath.Match(pattern, k); yes && dots == strings.Count(k, ".") {
			set[k] = &FsFindNode{Name: k, Leaf: true, ident: ident}
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

func (dsns *fsFindCache) identsFromPattern(pattern string) map[string]serde.Ident {
	result := make(map[string]serde.Ident)
	for _, node := range dsns.fsFind(pattern) {
		if node.Leaf { // only leaf nodes are series names
			result[node.Name] = node.ident
		}
	}
	return result
}
