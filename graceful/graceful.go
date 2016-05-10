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

package graceful

import (
	"net"
	"os"
	"sync"
	"syscall"
)

var (
	TcpWg sync.WaitGroup
)

type gracefulConn struct {
	net.Conn
}

func (w gracefulConn) Close() error {
	err := w.Conn.Close()
	if err == nil {
		TcpWg.Done()
	}
	return err
}

type Listener struct {
	net.Listener
	stop    chan error
	stopped bool
}

func NewListener(l net.Listener) (gl *Listener) {
	gl = &Listener{Listener: l, stop: make(chan error)}
	go func() {
		_ = <-gl.stop
		gl.stopped = true
		gl.stop <- gl.Listener.Close()
	}()
	return
}

func (gl *Listener) Close() error {
	if gl.stopped {
		return syscall.EINVAL
	}
	gl.stop <- nil
	return <-gl.stop
}

func (gl *Listener) Accept() (c net.Conn, err error) {
	c, err = gl.Listener.Accept()
	if err != nil {
		return
	}

	c = gracefulConn{Conn: c}

	TcpWg.Add(1)
	return
}

func (gl *Listener) File() *os.File {
	tl := gl.Listener.(*net.TCPListener)
	fl, _ := tl.File()
	return fl
}
