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
	"net"
	"net/rpc"
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
	dd    DistDatum
	nodes []*Node
}

// Cluster is based on Memberlist and adds some functionality on top
// of it such as the notion of a node being "ready".
type Cluster struct {
	*memberlist.Memberlist
	sync.RWMutex
	rcvChs    []chan *Msg
	chgNotify []chan bool
	meta      []byte
	dds       map[int64]*ddEntry
	snd, rcv  chan *Msg // dds messages
	copies    int
	lastId    int64
	rpcPort   int
	rpc       net.Listener
}

// NewCluster creates a new Cluster with reasonable defaults.
func NewCluster() (*Cluster, error) {
	return NewClusterBind("", 0, "", 0, 0, "")
}

// NewClusterBind creates a new Cluster while allowing for
// specification of the address/port to bind to, the address/port to
// advertize to the other nodes (use zero values for default) as well
// as the hostname. (This is useful if your app is running in a Docker
// container where it is impossible to figure out the outside IP
// addresses and the hostname can be the same).
func NewClusterBind(baddr string, bport int, aaddr string, aport int, rpcport int, name string) (*Cluster, error) {
	c := &Cluster{
		rcvChs:    make([]chan *Msg, 0),
		chgNotify: make([]chan bool, 0),
		dds:       make(map[int64]*ddEntry),
		copies:    1}
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

	if rpcport == 0 {
		c.rpcPort = 12354
	} else {
		c.rpcPort = rpcport
	}

	c.snd, c.rcv = c.RegisterMsgType()

	rpc.Register(&ClusterRPC{c})
	if c.rpc, err = net.Listen("tcp", fmt.Sprintf("%s:%d", baddr, c.rpcPort)); err != nil {
		return nil, err
	}

	// Serve RPC Requests
	go func() {
		for {
			rpc.Accept(c.rpc)
		}
	}()

	return c, nil
}

type ClusterRPC struct {
	c *Cluster
}

func (rpc *ClusterRPC) Message(msg Msg, reply *Msg) error {

	if msg.Id < len(rpc.c.rcvChs) {
		rpc.c.rcvChs[msg.Id] <- &msg
	} else {
		log.Printf("Cluster.Message() (via RPC): unknown msg Id: %d, dropping message.", msg.Id)
	}

	*reply = Msg{Id: 495, Body: []byte("HELLO")}
	return nil
}

// Set the number of copies of DistDatims that the Cluster will
// keep. The default is 1. You can only set it while the cluster is
// empty.
func (c *Cluster) Copies(n ...int) int {
	if len(n) > 0 && len(c.dds) == 0 {
		// only allow setting copies when the cluster is still empty
		c.copies = n[0]
	}
	return c.copies
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

// selectNodes uses a simple module to assign a node given an integer
// id.
func selectNodes(nodes []*Node, id int64, n int) []*Node {
	if len(nodes) == 0 {
		return nil
	}
	result := make([]*Node, n)
	for i := 0; i < n; i++ {
		result[i] = nodes[(int(id)+i)%len(nodes)]
	}
	return result
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
			c.dds[c.lastId] = &ddEntry{dd: dd, nodes: selectNodes(readyNodes, dd.Id(c.lastId), c.copies)}
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

// RegisterMsgType makes sending messages across nodes simpler. It
// returns two channels, one to send the other to receive a *Msg
// structure. The nodes of the cluster must call RegisterMsgType in
// exact same order because that is what determines the internal
// message id and the channel to which it will be passed. The message
// is sent to the destination specified in Msg.Dst. Messages are
// compressed using flate.
func (c *Cluster) RegisterMsgType() (snd, rcv chan *Msg) {

	snd, rcv = make(chan *Msg, 16), make(chan *Msg, 16)

	c.rcvChs = append(c.rcvChs, rcv)
	id := len(c.rcvChs) - 1

	go func(id int) {
		for {
			msg := <-snd

			if msg.Dst == nil {
				log.Printf("Cluster: cannot send message when Dst is not set, ignoring.")
				continue
			}

			if msg.Dst.rpc == nil {
				addr := fmt.Sprintf("%s:%d", msg.Dst.Addr, c.rpcPort)
				log.Printf("Cluster: establishing RPC connection to node %s via %s", msg.Dst.Name(), addr)
				conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
				if err != nil {
					log.Printf("Cluster: cannot establish connection to %s: %v, dropping this message.", addr, err)
					continue
				}
				msg.Dst.rpc = rpc.NewClient(conn)
			}

			msg.Src = c.LocalNode()
			msg.Id = id

			var resp Msg
			msg.Dst.rpc.Call("ClusterRPC.Message", msg, &resp)
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
	return nil
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
	rpc *rpc.Client
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
	GetName() string
}

// NodesForDistDatum returns the nodes responsible for this
// DistDatum. The first node is the one responsible for Relinquish(),
// the rest are up to the user to decide. The nodes are cached, the
// call doesn't compute anything. The idea is that a
// NodesForDistDatum() should be pretty fast so that you can call it a
// lot, e.g. for every incoming data point.
func (c *Cluster) NodesForDistDatum(dd DistDatum) []*Node {
	c.RLock()
	defer c.RUnlock()
	if dde, ok := c.dds[dd.Id()]; ok {
		return dde.nodes
	}
	return nil
}

func (c *Cluster) List() map[int64]*ddEntry {
	return c.dds
}

func (dde *ddEntry) Node() *Node {
	if len(dde.nodes) == 0 {
		return nil
	}
	return dde.nodes[0]
}

func (dde *ddEntry) DD() DistDatum {
	return dde.dd
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

			// The idea is that the first node in the list is the
			// "lead" responsible for saving the data. What happens
			// with the rest is up to the userland to deal with.
			var newNode, oldNode *Node
			newNodes := selectNodes(readyNodes, dde.dd.Id(), c.copies)
			if len(newNodes) > 0 {
				newNode = newNodes[0]
			}
			if len(dde.nodes) > 0 {
				oldNode = dde.nodes[0]
			}
			if newNode == nil || oldNode.Name() != newNode.Name() {
				ln := c.LocalNode()
				if ln.Name() == oldNode.Name() { // we are the ex-node
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
					log.Printf("Transition(): Id %d is moving to this node from node %s", dde.dd.Id(), oldNode.Name())
					// Add to the list of ids to wait on, but only if there existed nodes
					if oldNode.Name() != "<nil>" {
						waitIds[dde.dd.Id()] = true
					}
				}
			}
			dde.nodes = newNodes // Assign the correct nodes in the end
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
