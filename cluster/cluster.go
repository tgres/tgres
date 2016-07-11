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
	"encoding/binary"
	"encoding/gob"
	"fmt"
	"github.com/hashicorp/memberlist"
	"log"
	"sort"
	"strings"
	"sync"
	"time"
)

var startTime time.Time

func init() {
	startTime = time.Now()
}

const updateNodeTO = 30 * time.Second

type ddEntry struct {
	dd   DistDatum
	node *Node
}

// Cluster is based on Memberlist and adds some functionality on top
// of it such as the notion of a node being "ready".
type Cluster struct {
	*memberlist.Memberlist
	sync.RWMutex
	rcvChs    []chan *Msg
	chgNotify []chan bool
	meta      []byte
	bcastq    *memberlist.TransmitLimitedQueue
	dds       map[int64]*ddEntry
	snd, rcv  chan *Msg // dds messages
	lastId    int64
}

// NewCluster creates a new Cluster with reasonable defaults.
func NewCluster() (*Cluster, error) {
	return NewClusterBind("", 0, "", 0, "")
}

// NewClusterBind creates a new Cluster while allowing for
// specification of the address/port to bind to, the address/port to
// advertize to the other nodes (use zero values for default) as well
// as the hostname. (This is useful if your app is running in a Docker
// container where it is impossible to figure out the outside IP
// addresses and the hostname can be the same).
func NewClusterBind(baddr string, bport int, aaddr string, aport int, name string) (*Cluster, error) {
	c := &Cluster{
		rcvChs:    make([]chan *Msg, 0),
		chgNotify: make([]chan bool, 0),
		dds:       make(map[int64]*ddEntry)}
	c.bcastq = &memberlist.TransmitLimitedQueue{NumNodes: func() int { return c.NumMembers() }}
	cfg := memberlist.DefaultLANConfig()
	cfg.PushPullInterval = 15 * time.Second
	if baddr != "" {
		cfg.BindAddr = baddr
	}
	if bport != 0 {
		cfg.BindPort = bport
	}
	if aaddr != "" {
		cfg.AdvertiseAddr = aaddr
	}
	if aport != 0 {
		cfg.AdvertisePort = aport
	}
	if name != "" {
		cfg.Name = name
	}
	cfg.LogOutput = &logger{}
	cfg.Delegate, cfg.Events = c, c
	var err error
	if c.Memberlist, err = memberlist.Create(cfg); err != nil {
		return nil, err
	}
	md := &nodeMeta{sortBy: startTime.UnixNano()}
	c.saveMeta(md)
	if err = c.UpdateNode(updateNodeTO); err != nil {
		log.Printf("NewClusterBind(): UpdateNode() failed: %v", err)
		return nil, err
	}

	c.snd, c.rcv = c.RegisterMsgType(false)

	return c, nil
}

// readyNodes get a list of nodes and returns only the ones that are
// ready.
func (c *Cluster) readyNodes() ([]*Node, error) {
	nodes, err := c.SortedNodes()
	if err != nil {
		return nil, err
	}
	readyNodes := make([]*Node, 0, len(nodes))
	for _, node := range nodes {
		if node.Ready() {
			readyNodes = append(readyNodes, node)
		}
	}
	return readyNodes, nil
}

// selectNode uses a simple module to assign a node given an integer
// id.
func selectNode(nodes []*Node, id int64) *Node {
	if len(nodes) == 0 {
		return nil
	}
	return nodes[int(id)%len(nodes)]
}

// LoadDistData will trigger a load of DistDatum's. Its argument is a
// function which performs the actual load and returns the list, while
// also providing the data to the application in whatever way is
// needed by the user-side. This action has to be triggered from the
// user-side. You should LoadDistData prior to marking your node as
// ready.
func (c *Cluster) LoadDistData(f func() ([]DistDatum, error)) error {
	dds, err := f()
	if err != nil {
		return err
	}

	readyNodes, err := c.readyNodes()
	if err != nil {
		return err
	}

	c.Lock()
	for _, dd := range dds {
		if dd.Id() == 0 {
			c.lastId++
			c.dds[c.lastId] = &ddEntry{dd: dd, node: selectNode(readyNodes, dd.Id(c.lastId))}
		} else {
			// There is nothing to do - we already have this id and its node.
		}
	}
	c.Unlock()
	return nil
}

// Join joins a cluster given at least one node address/port. NB: You
// can always join yourself if this is a cluster of one node.
func (c *Cluster) Join(existing []string) error {
	if n, err := c.Memberlist.Join(existing); n <= 0 {
		return err
	}
	return nil
}

// LocalNode returns a pointer to the local node.
func (c *Cluster) LocalNode() *Node {
	return &Node{Node: c.Memberlist.LocalNode()}
}

// Members lists cluster members (ready or not).
func (c *Cluster) Members() []*Node {
	nn := c.Memberlist.Members()
	result := make([]*Node, len(nn))
	for i, n := range nn {
		result[i] = &Node{Node: n}
	}
	return result
}

// SortedNodes returns nodes ordered by process start time
func (c *Cluster) SortedNodes() ([]*Node, error) {
	ms := c.Members()
	sn := sortableNodes{ms, make([]string, len(ms))}
	for i, _ := range sn.nl {
		md, err := sn.nl[i].extractMeta()
		if err != nil {
			return nil, err
		}
		sn.sortBy[i] = fmt.Sprintf("%d:%s", md.sortBy, sn.nl[i].Name()) // mix in Name for uniqueness
	}
	sort.Sort(sn)
	return sn.nl, nil
}

type sortableNodes struct {
	nl     []*Node
	sortBy []string
}

func (sn sortableNodes) Len() int {
	return len(sn.nl)
}

func (sn sortableNodes) Less(i, j int) bool {
	return sn.sortBy[i] < sn.sortBy[j]
}

func (sn sortableNodes) Swap(i, j int) {
	sn.nl[i], sn.nl[j] = sn.nl[j], sn.nl[i]
	sn.sortBy[i], sn.sortBy[j] = sn.sortBy[j], sn.sortBy[i]
}

type broadcast struct {
	msg []byte
}

// Implement the memberlist.Broadcast interface

func (b *broadcast) Invalidates(bc memberlist.Broadcast) bool {
	return false
}

func (b *broadcast) Message() []byte {
	return b.msg
}

func (b *broadcast) Finished() {
}

// RegisterMsgType makes sending messages across nodes simpler. It
// returns two channels, one to send the other to receive a *Msg
// structure. The nodes of the cluster must call RegisterMsgType in
// exact same order because that is what determines the internal
// message id and the channel to which it will be passed. If the bcast
// argument is true, the messages will be broadcast to all other
// nodes, otherwise the message is sent to the destination specified
// in Msg.Dst. Messages are compressed using flate.
func (c *Cluster) RegisterMsgType(bcast bool) (snd, rcv chan *Msg) {

	snd, rcv = make(chan *Msg, 16), make(chan *Msg, 16)

	c.rcvChs = append(c.rcvChs, rcv)
	id := len(c.rcvChs) - 1

	go func(id int) {
		for {
			msg := <-snd
			msg.Src = c.LocalNode()
			msg.Id = id
			if bcast && c.NumMembers() > 1 {
				c.bcastq.QueueBroadcast(&broadcast{msg.bytes()})
			} else if msg.Dst != nil {
				c.SendToTCP(msg.Dst.Node, msg.bytes())
			}
		}
	}(id)

	return snd, rcv
}

// NotifyClusterChanges returns a bool channel which will be sent true
// any time a cluster change happens (nodes join or leave, or node
// metadata changes).
func (c *Cluster) NotifyClusterChanges() chan bool {
	ch := make(chan bool, 1)
	c.chgNotify = append(c.chgNotify, ch)
	return ch
}

// This is what we store in Node metadata
type nodeMeta struct {
	ready  bool
	sortBy int64
	user   []byte
}

const minMdLen = 1 + binary.MaxVarintLen64

func (c *Cluster) extractMeta() (*nodeMeta, error) {
	return c.LocalNode().extractMeta()
}

func (c *Cluster) saveMeta(md *nodeMeta) {
	meta := make([]byte, minMdLen)
	if md.ready {
		meta[0] = 1
	} else {
		meta[0] = 0
	}
	binary.PutVarint(meta[1:], md.sortBy)
	meta = append(meta, md.user...)
	c.meta = meta
}

// Meta() will return the user part of the node metadata. (Cluster
// uses the beginning bytes to store its internal stuff such as the
// ready status of a node, trailed by user part).
func (n *Node) Meta() ([]byte, error) {
	var md *nodeMeta
	var err error
	if md, err = n.extractMeta(); err != nil {
		return nil, err
	}
	return md.user, nil
}

// Name returns the node name or "<nil>" if the pointer is nil.
func (n *Node) Name() string {
	// nil-resistant Name getter
	if n == nil {
		return "<nil>"
	}
	return n.Node.Name
}

// Sets the metadata and broadcasts an UpdateNode message to the
// cluster.
func (c *Cluster) SetMetaData(b []byte) error {
	// To set it, we must first get it.
	md, err := c.LocalNode().extractMeta()
	if err != nil {
		return err
	}
	md.user = b
	c.saveMeta(md)
	if err = c.UpdateNode(updateNodeTO); err != nil {
		log.Printf("Cluster.SetMetaData(): UpdateNode() failed: %v", err)
	}
	return err
}

// BEGIN memberlist.Delegate interface

func (c *Cluster) NodeMeta(limit int) []byte {
	return c.meta
}

func (c *Cluster) NotifyMsg(b []byte) {

	m := &Msg{}
	if err := gob.NewDecoder(flate.NewReader(bytes.NewBuffer(b))).Decode(m); err != nil {
		log.Printf("NotifyMsg(): error decoding: %#v", err)
	}

	if m.Id < len(c.rcvChs) {
		c.rcvChs[m.Id] <- m
	} else {
		log.Printf("NotifyMsg(): unknown msg Id: %d, dropping message", m.Id)
	}
}

func (c *Cluster) GetBroadcasts(overhead, limit int) [][]byte {
	return c.bcastq.GetBroadcasts(overhead, limit)
}

func (c *Cluster) LocalState(join bool) []byte            { return []byte{} }
func (c *Cluster) MergeRemoteState(buf []byte, join bool) {}

func (c *Cluster) NotifyJoin(n *memberlist.Node) {
	c.notifyAll()
}
func (c *Cluster) NotifyLeave(n *memberlist.Node) {
	c.notifyAll()
}
func (c *Cluster) NotifyUpdate(n *memberlist.Node) {
	c.notifyAll()
}

// END memberlist.Delegate interface

func (c *Cluster) notifyAll() {
	defer func() { recover() }() // in case ch is now closed
	for _, ch := range c.chgNotify {
		if len(ch) < cap(ch) {
			ch <- true
		}
	}
}

type Node struct {
	*memberlist.Node
}

func (n *Node) extractMeta() (*nodeMeta, error) {
	md := &nodeMeta{}
	if len(n.Node.Meta) < minMdLen {
		return nil, fmt.Errorf("Not enough bytes to extract metadata")
	}
	// ready
	md.ready = n.Node.Meta[0] == 1
	// sortBy
	var err error
	if md.sortBy, err = binary.ReadVarint(bytes.NewReader(n.Node.Meta[1:])); err != nil {
		return nil, fmt.Errorf("extractMeta(): sortBy: %v", err)
	}
	// user
	md.user = n.Node.Meta[minMdLen:]
	return md, nil
}

// Ready sets the Node status in the metadata and broadcasts a change
// notification to the cluster.
func (c *Cluster) Ready(status bool) error {
	md, err := c.extractMeta()
	if err != nil {
		return err
	}
	md.ready = status
	c.saveMeta(md)
	if err = c.UpdateNode(updateNodeTO); err != nil {
		log.Printf("Ready(): UpdateNode() failed: %v", err)
		return err
	}
	return nil
}

// Ready returns the status of a node.
func (n *Node) Ready() bool {
	md, err := n.extractMeta()
	if err != nil {
		return false
	}
	return md.ready
}

// Msg is the structure that should be passed to channels returned by
// c.RegisterMsgType().
type Msg struct {
	Id       int
	Dst, Src *Node
	Body     []byte
}

// NewMsgGob creates a Msg from a payload which implements
// gob.GobEncoder.
func NewMsgGob(dest *Node, payload gob.GobEncoder) (*Msg, error) {
	var buf bytes.Buffer
	enc := gob.NewEncoder(&buf)
	if err := enc.Encode(payload); err != nil {
		return nil, err
	}
	return &Msg{Dst: dest, Body: buf.Bytes()}, nil
}

// represent out message as bytes
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

// implement gob.GobDecoder interface.
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

// DistDatum is an interface for a piece of data distributed across
// the cluster. More preciesely, each DistDatum belongs to a node, and
// nodes are responsible for forwarding requests to the responsible
// node.
type DistDatum interface {
	// Id returns an integer that uniquely identifies this datum. The
	// id must originally have been provided by Cluster. If the datum
	// doesn't have an id yet, it should return 0.
	Id(ids ...int64) int64
	// Reqlinquish is a chance to persist the data before the datum
	// can be assigned to another node. On a cluater change that
	// requires a reassignment, the receiving node will wait for the
	// Relinquish operation to complete (up to a configurable
	// timeout).
	Relinquish() error
}

// NodeForDistDatum return the node responsible for this
// DistDatum. The nodes are cached, the call doesn't compute
// anything. The idea is that a NodeForDistDatum() should be pretty
// fast so that you can call it a lot, e.g. for every incoming data
// point.
func (c *Cluster) NodeForDistDatum(dd DistDatum) *Node {
	c.RLock()
	defer c.RUnlock()
	if dde, ok := c.dds[dd.Id()]; ok {
		return dde.node
	}
	return nil
}

// Transition() provides the transition on cluster
// changes. Transitions should be triggered by user-land after
// receiving a cluster change event from a channel returned by
// NotifyClusterChanges(). The transition will call Relinquish() on
// all DistDatums that are transferring to other nodes and wait for
// confirmation of Relinquish() from other nodes for DistDatums
// transferring to this node. Generally a node should be buffering all
// the data it receives during a transition.
func (c *Cluster) Transition(timeout time.Duration) error {
	var wg sync.WaitGroup

	c.Lock()
	defer c.Unlock()
	log.Printf("Transition(): Starting...")

	readyNodes, err := c.readyNodes()
	if err != nil {
		return err
	}

	waitIds := make(map[int64]bool)

	for _, dde := range c.dds {
		wg.Add(1)
		go func(dde *ddEntry) {
			defer wg.Done()
			newNode := selectNode(readyNodes, dde.dd.Id())
			if newNode == nil || dde.node.Name() != newNode.Name() {
				ln := c.LocalNode()
				if ln.Name() == dde.node.Name() { // we are the ex-node
					if newNode != nil {
						log.Printf("Transition(): Id %d is moving away to node %s", dde.dd.Id(), newNode.Name())
					}
					if err = dde.dd.Relinquish(); err != nil {
						log.Printf("Transition(): Warning: Relinquish() failed for id %d", dde.dd.Id())
					} else {
						// Notify the new node expecting this dd of Relinquish completion
						body := make([]byte, binary.MaxVarintLen64)
						binary.PutVarint(body, dde.dd.Id())
						m := &Msg{Dst: newNode, Body: []byte(body)}
						log.Printf("Transition(): Sending relinquish of id %d to node %s", dde.dd.Id(), newNode.Name())
						c.snd <- m
					}
				} else if newNode != nil && ln.Name() == newNode.Name() { // we are the new node
					log.Printf("Transition(): Id %d is moving to this node from node %s", dde.dd.Id(), dde.node.Name())
					// Add to the list of ids to wait on, but only if there existed nodes
					if dde.node.Name() != "<nil>" {
						waitIds[dde.dd.Id()] = true // ZZZ this is a problem for non-unique ids
					}
				}
			}
			dde.node = newNode // Assign the correct node in the end
		}(dde)
	}

	// Wait for this phase to finish
	wg.Wait()

	// Now wait on the reqinquishes
	wg.Add(1)
	go func() {
		defer wg.Done()

		log.Printf("Transition(): Waiting on %d relinquish messages... (timeout %v) %v", len(waitIds), timeout, waitIds)

		tmout := make(chan bool, 1)
		go func() {
			time.Sleep(timeout)
			tmout <- true
		}()

		for {
			if len(waitIds) == 0 {
				return
			}

			var m *Msg
			select {
			case m = <-c.rcv:
			case <-tmout:
				log.Printf("Transition(): WARNING: Relinquish wait timeout! Continuing. Some data is likely lost.")
				return
			}

			if id, err := binary.ReadVarint(bytes.NewReader(m.Body)); err == nil {
				log.Printf("Transition(): Got relinquish message for id %d from %s.", id, m.Src.Name())
				delete(waitIds, id)
			} else {
				log.Printf("Transition(): WARNING: Error decoding message from %s: %v", m.Src.Name(), err)
			}
			if len(waitIds) > 0 {
				log.Printf("Transition(): Still waiting on %d relinquish messages: %v", len(waitIds), waitIds)
			}
		}

	}()

	wg.Wait()
	log.Printf("Transition(): Complete!")
	return nil
}
