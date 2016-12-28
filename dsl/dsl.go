//
// Copyright 2015 Gregory Trubetskoy. All Rights Reserved.
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

// Package dsl is the Domain Specific Language for the series
// query. Presently it mostly mimics the Graphite API functions. The
// parser used for the language is Go standard lib parser.ParseExpr(),
// which basically means the syntax is Go, and so the user of this
// package might need to ensure to wrap series names in quotes, etc.
package dsl

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"strconv"
	"strings"
	"time"
)

type dslCtx struct {
	src                 string
	escSrc              string
	from, to, maxPoints int64
	NamedDSFetcher
}

// Parse a DSL expression given by src and other params.
func ParseDsl(db NamedDSFetcher, src string, from, to, maxPoints int64) (SeriesMap, error) {
	return newDslCtx(db, src, from, to, maxPoints).parse()
}

func newDslCtx(db NamedDSFetcher, src string, from, to, maxPoints int64) *dslCtx {
	return &dslCtx{
		src:            fixQuotes(escapeBadChars(src)),
		from:           from,
		to:             to,
		maxPoints:      maxPoints,
		NamedDSFetcher: db}
}

// Parse a DSL context. Returns a SeriesMap or error.
func (dc *dslCtx) parse() (SeriesMap, error) {
	// parser.ParseExpr produces an AST in accordance with Go syntax,
	// which is just fine in our case.

	tr, err := parser.ParseExpr(dc.escSrc)
	if err != nil {
		return nil, fmt.Errorf("Error parsing %q: %v", dc.src, err)
	}

	fv := &FuncVisitor{dc, &callStack{}, nil, 0, -1, nil}

	ast.Walk(fv, tr)

	if fv.err != nil {
		return nil, fmt.Errorf("ParseDsl(): %v", fv.err)
	}

	return fv.ret, nil
}

func (dc *dslCtx) seriesFromSeriesOrIdent(what interface{}) (SeriesMap, error) {
	switch obj := what.(type) {
	case SeriesMap:
		return obj, nil
	case string:
		fromT, toT := time.Unix(dc.from, 0), time.Unix(dc.to, 0)
		series, err := dc.seriesFromIdent(obj, fromT, toT)
		return series, err
	}
	return nil, fmt.Errorf("seriesFromSeriesOrIdent(): unknown type: %T", what)
}

func (dc *dslCtx) seriesFromIdent(ident string, from, to time.Time) (SeriesMap, error) {
	ids := dc.dsIdsFromIdent(ident)
	result := make(SeriesMap)
	for name, id := range ids {
		ds, err := dc.FetchDataSourceById(id)
		if err != nil {
			return nil, fmt.Errorf("seriesFromIdent(): Error %v", err)
		}
		dps, err := dc.FetchSeries(ds, from, to, dc.maxPoints)
		if err != nil {
			return nil, fmt.Errorf("seriesFromIdent(): Error %v", err)
		}
		result[name] = &aliasSeries{Series: dps}
	}
	return result, nil
}

type FuncCall struct {
	ast  *ast.CallExpr
	args []interface{}
}

type callStack struct {
	nodes []*FuncCall
	count int
}

func (s *callStack) Push(n *FuncCall) {
	s.nodes = append(s.nodes[:s.count], n)
	s.count++
}

func (s *callStack) Pop() *FuncCall {
	if s.count == 0 {
		return nil
	}
	s.count--
	return s.nodes[s.count]
}

type FuncVisitor struct {
	dc           *dslCtx
	stack        *callStack
	ret          SeriesMap
	level        int
	processLevel int
	err          error
}

func (v *FuncVisitor) processStack() ast.Visitor {

	var (
		ret interface{}
	)

	c := v.stack.Pop()
	for c != nil && v.err == nil {
		for n, arg := range c.ast.Args {

			// We walk the arguments of this function. If an argument
			// happens to be another (nested/deeper) function call,
			// there should be a return value for it. If there isn't
			// one, the AST walk hasn't covered it yet, but will
			// eventually. This can happen when multiple args are
			// funciton cals, e.g. hello(one(...), two(...))

			switch tok := arg.(type) {
			case *ast.CallExpr:
				if c.args[n] == nil {
					if ret == nil {
						// There is nothing for this argument in the
						// list and there is no available return value
						// to use, this means we should push this call
						// back on the stack and return immediately to
						// continue the AST walk.
						v.stack.Push(c)
						return v
					} else {
						// We can use the return value to satisfy this arg
						c.args[n] = ret
						ret = nil
					}
				}
			case *ast.SelectorExpr, *ast.Ident:
				literal := unEscapeBadChars(v.dc.escSrc[tok.Pos()-1 : tok.End()-1])
				c.args[n] = literal
			case *ast.BasicLit:
				if tok.Kind == token.INT || tok.Kind == token.FLOAT {
					c.args[n], v.err = strconv.ParseFloat(tok.Value, 64)
				} else if tok.Kind == token.STRING {
					c.args[n] = unEscapeBadChars(tok.Value[1 : len(tok.Value)-1]) // remove surrounding quotes
				} else {
					v.err = fmt.Errorf("unsupported token type: %v", tok.Kind)
				}
			case *ast.UnaryExpr:
				expr := v.dc.escSrc[tok.Pos()-1 : tok.End()-1]
				c.args[n], v.err = strconv.ParseFloat(expr, 64)
			}
		}

		// if we got this far, all args for the function we are processing are satisfied
		name := v.dc.escSrc[c.ast.Fun.Pos()-1 : c.ast.Fun.End()-1]
		ret, v.err = seriesFromFunction(v.dc, name, c.args)
		c = v.stack.Pop()
	}

	v.ret = ret.(SeriesMap)
	return v
}

func (v *FuncVisitor) Visit(node ast.Node) ast.Visitor {

	if node == nil {
		v.level--
	} else {
		v.level++
	}

	if v.processLevel != -1 && v.processLevel == v.level {
		v.processLevel = -1
		return v.processStack()
	}

	switch t := node.(type) {
	case *ast.CallExpr:
		v.stack.Push(&FuncCall{t, make([]interface{}, len(t.Args))})

		// This ensures that we skip all the subsequent visits since
		// the CallExpr already contains all the information we need.
		v.processLevel = v.level - 1
	}
	return v
}

// Simple trick to avoid "*" which is not valid Go syntax

func escapeBadChars(target string) string {
	s := strings.Replace(target, "*", "__ASTERISK__", -1)
	s = strings.Replace(s, "=", "__ASSIGN__", -1)
	return strings.Replace(s, "-", "__DASH__", -1)
}

func unEscapeBadChars(target string) string {
	s := strings.Replace(target, "__ASTERISK__", "*", -1)
	s = strings.Replace(s, "__ASSIGN__", "=", -1)
	return strings.Replace(s, "__DASH__", "-", -1)
}

// Also - there are no single quoted strings in Go grammar
func fixQuotes(target string) string {
	// TODO if the string contains double quotes, they should be escaped
	// e.g. '"Foo"Bar"' is a problem, it becomes ""Foo"Bar"", but
	// should become "\"Foo\"Bar\""
	return strings.Replace(target, "'", "\"", -1)
}
