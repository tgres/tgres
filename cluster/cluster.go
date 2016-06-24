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

package cluster

import (
	"bytes"
	"compress/flate"
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
	"os"
	"sort"
	"strings"
)

type Cluster struct {
	*memberlist.Memberlist
	d *delegate
}

func (c *Cluster) Create() error {
	return c.CreateBind(os.Getenv("MLBIND"), 0)
}

func (c *Cluster) CreateBind(addr string, port int) (err error) {
	cfg := memberlist.DefaultLANConfig()
	if port != 0 {
		cfg.BindPort, cfg.AdvertisePort = port, port
	}
	if addr != "" {
		cfg.BindAddr, cfg.AdvertiseAddr = addr, addr
		cfg.Name = fmt.Sprintf("%s:%d", addr, port)
	}
	cfg.LogOutput = &logger{}
	c.d = &delegate{make([]chan *Msg, 0)}
	cfg.Delegate = c.d
	c.Memberlist, err = memberlist.Create(cfg)
	return err
}

func (c *Cluster) Join(existing []string) error {
	// NB: You can always join yourself
	if n, err := c.Memberlist.Join(existing); n <= 0 {
		return err
	}
	return nil
}

func (c *Cluster) SortedNodes() []*memberlist.Node {
	// Return in order of addr:port
	sn := sortableNodes{c.Members()}
	sort.Sort(sn)
	return sn.nl
}

type sortableNodes struct {
	nl []*memberlist.Node
}

func (sn sortableNodes) Len() int {
	return len(sn.nl)
}

func (sn sortableNodes) Less(i, j int) bool {
	istr := fmt.Sprintf("%s:%s", sn.nl[i].Addr, sn.nl[i].Port)
	jstr := fmt.Sprintf("%s:%s", sn.nl[j].Addr, sn.nl[i].Port)
	return istr < jstr
}

func (sn sortableNodes) Swap(i, j int) {
	sn.nl[i], sn.nl[j] = sn.nl[j], sn.nl[i]
}

func (c *Cluster) RegisterMsgType() (snd, rcv chan *Msg) {

	snd, rcv = make(chan *Msg, 16), make(chan *Msg, 16)

	d := c.d
	d.rcvChs = append(d.rcvChs, rcv)
	id := len(d.rcvChs) - 1

	go func(id int) {
		for {
			msg := <-snd
			msg.Src = c.LocalNode()
			msg.Id = id
			c.SendToTCP(msg.Dst, msg.bytes())
		}
	}(id)

	return snd, rcv
}

type Msg struct {
	Id       int
	Dst, Src *memberlist.Node
	Body     []byte
}

func NewMsg(dest *memberlist.Node, payload gob.GobEncoder) (*Msg, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return nil, err
	}
	return &Msg{Dst: dest, Body: buf.Bytes()}, nil
}

func (m *Msg) bytes() []byte {
	var buf bytes.Buffer
	z, _ := flate.NewWriter(&buf, -1)
	enc := gob.NewEncoder(z)
	if err := enc.Encode(m); err != nil {
		log.Printf("Msg.bytes(): Error encountered in encoding: %v", err)
		return nil
	}
	z.Close()
	return buf.Bytes()
}

func (m *Msg) Decode(dst interface{}) error {
	if err := gob.NewDecoder(bytes.NewBuffer(m.Body)).Decode(dst); err != nil {
		log.Printf("Msg.Decode() decoding error: %v", err)
		return err
	}
	return nil
}

type logger struct{}

// Ignore [DEBUG]
func (l *logger) Write(b []byte) (int, error) {
	s := string(b)
	if !strings.Contains(s, "[DEBUG]") {
		log.Printf(s)
	}
	return len(s), nil
}

// memberlist.Delegate interface
type delegate struct {
	rcvChs []chan *Msg
}

func (d *delegate) NodeMeta(limit int) []byte {
	return []byte{}
}

func (d *delegate) NotifyMsg(b []byte) {

	m := &Msg{}
	if err := gob.NewDecoder(flate.NewReader(bytes.NewBuffer(b))).Decode(m); err != nil {
		log.Printf("NotifyMsg(): error decoding: %#v", err)
	}

	if m.Id < len(d.rcvChs) {
		d.rcvChs[m.Id] <- m
	} else {
		log.Printf("NotifyMsg(): unknown msg Id: %d, dropping message", m.Id)
	}
}

func (d *delegate) GetBroadcasts(overhead, limit int) [][]byte {
	return [][]byte{}
}

func (d *delegate) LocalState(join bool) []byte {
	return []byte{}
}

func (d *delegate) MergeRemoteState(buf []byte, join bool) {}
