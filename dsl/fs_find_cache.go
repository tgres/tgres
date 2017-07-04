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
	*sync.RWMutex
	db  serde.DataSourceSearcher
	key string // name of the ident key, required
	*fsFindNode
}

type fsFindNode struct {
	ident serde.Ident // leaf node
	name  string      // my dot.name
	names map[string]*fsFindNode
}

func (n *fsFindNode) insert(parts []string, pos int, ident serde.Ident) {
	if pos >= len(parts) {
		return
	}

	if n.names == nil {
		n.names = make(map[string]*fsFindNode)
	}

	key := parts[pos]
	if _, ok := n.names[key]; !ok {
		n.names[key] = &fsFindNode{name: strings.Join(parts[0:pos+1], ".")}
	}
	node := n.names[key]

	// Note that a node can end up being both leaf and expandable, and
	// in theory there shouldn't be anyhting wrong with that, though
	// Grafana doesn't deal with it very well..
	if pos < len(parts)-1 {
		node.insert(parts, pos+1, ident)
	} else {
		node.ident = ident
	}
}

func (n *fsFindNode) search(pattern, key string, result map[string]*FsFindNode) {

	parts := strings.SplitN(pattern, ".", 2)
	prefix := parts[0]

	for k, child := range n.names {
		if yes, _ := filepath.Match(prefix, k); yes {

			parent := len(child.names) > 0
			leaf := child.ident != nil

			if parent {
				if len(parts) > 1 {
					child.search(parts[1], key, result)
				} else {
					// it's a prefix
					result[child.name] = &FsFindNode{Name: child.name, Leaf: leaf, Expandable: parent}
				}
			}
			if leaf {
				result[child.name] = &FsFindNode{Name: child.name, Leaf: leaf, Expandable: parent, ident: child.ident}
			}
		}
	}
}

func (f *fsFindCache) insert(ident serde.Ident) error {
	if name := ident[f.key]; name != "" {
		parts := strings.Split(name, ".")
		f.fsFindNode.insert(parts, 0, ident)
	} else {
		return fmt.Errorf("insert: '%s' tag missing for DS ident: %s", f.key, ident.String())
	}
	return nil
}

type FsFindNode struct {
	Name       string
	Leaf       bool
	Expandable bool
	ident      serde.Ident
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

func newFsFindCache(db serde.DataSourceSearcher, key string) *fsFindCache {
	return &fsFindCache{
		RWMutex:    &sync.RWMutex{},
		db:         db,
		key:        key,
		fsFindNode: &fsFindNode{},
	}
}

func (dsns *fsFindCache) reload() error {
	sr, err := dsns.db.Search(map[string]string{dsns.key: ".*"})
	if err != nil {
		return err
	}
	if sr == nil {
		return nil
	}
	defer sr.Close()

	dsns.Lock()
	defer dsns.Unlock()

	for sr.Next() {
		if err := dsns.insert(sr.Ident()); err != nil {
			return err
		}
	}

	return nil
}

func (dsns *fsFindCache) fsFind(pattern string) []*FsFindNode {
	if strings.Count(pattern, ",") > 0 {
		subres := make(fsNodes, 0)

		parts := regexp.MustCompile("{[^{}]*}").FindAllString(pattern, -1) // TODO pre-compile me
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

	set := make(map[string]*FsFindNode)
	dsns.search(pattern, dsns.key, set)

	// convert to array
	result := make(fsNodes, 0, len(set))
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
