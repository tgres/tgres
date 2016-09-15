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

// Package daemon is the tgres command line server.
package daemon

import (
	"flag"
	"fmt"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/serde"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"strings"
	"syscall"
)

var (
	serviceMgr       *ServiceManager
	logFile          *os.File
	cycleLogCh            = make(chan int)
	quitting         bool = false
	gracefulChildPid int
)

func parseFlags() (textCfgPath, gracefulProtos, join string) {

	// Parse the flags, if any
	flag.StringVar(&textCfgPath, "c", "./etc/tgres.conf", "path to config file")
	flag.StringVar(&join, "join", "", "List of add:port,addr:port,... of nodes to join")
	flag.StringVar(&gracefulProtos, "graceful", "", "list of fds (internal use only)")
	flag.Parse()

	return
}

func savePid(PidPath string) {
	f, err := os.Create(PidPath)
	if err != nil {
		logFatalf("Unable to create pid file '%s': (%v)", PidPath, err)
	}
	fmt.Fprintf(f, "%d\n", os.Getpid())
	log.Printf("Pid saved in %s.", PidPath)
}

func Init() { // not to be confused with init()

	runtime.GOMAXPROCS(runtime.NumCPU())

	// TODO this should be in log.go
	log.SetPrefix(fmt.Sprintf("[%d] ", os.Getpid()))
	log.Printf("Tgres starting.")

	cfgPath, gracefulProtos, join := parseFlags()

	// This creates the Cfg variable
	if err := ReadConfig(cfgPath); err != nil {
		log.Fatal("Exiting.")
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	if err := processConfig(configer(Cfg), wd); err != nil { // This validates the config
		log.Fatalf("Error in config file %s: %v", cfgPath, err)
	}

	savePid(Cfg.PidPath)

	// Initialize Database
	db, err := serde.InitDb(Cfg.DbConnectString, "")
	if err != nil {
		log.Fatalf("Error connecting to the DB: %v", err)
		return
	}

	log.Printf("Initialized DB connection.")

	var (
		c         *cluster.Cluster
		tgresBind = os.Getenv("TGRES_BIND")
		aaddr     *string
	)
	if os.Getenv("TGRES_ADDRFROMDB") != "" {
		aaddr, err = db.MyDbAddr()
		if err != nil {
			log.Printf("Database error: %v", err)
			return
		}
		log.Printf("Using %v as advertized address for the cluster.", *aaddr)
	} else {
		aaddr = &tgresBind
	}

	c, err = cluster.NewClusterBind(tgresBind, 0, *aaddr, 0, 0, tgresBind)
	if err != nil {
		log.Printf("Unable to create cluster: %v", err)
		return
	}

	// Join other nodes, if any
	var ips []string
	if join != "" {
		ips = strings.Split(join, ",")
	} else if os.Getenv("TGRES_ADDRFROMDB") != "" {
		if ips, err = db.ListDbClientIps(); err != nil {
			log.Printf("Error from db.ListDbClientIps(): %v", err)
			return
		}
	}
	if err := c.Join(ips); err != nil {
		log.Printf("Joining nodes: %s", strings.Join(ips, ","))
		log.Printf("Unable to join any cluster members: %v", err)
		return
	}

	// Create the receiver
	rcvr := receiver.New(c, db)
	rcvr.NWorkers = Cfg.Workers
	rcvr.MaxCacheDuration = Cfg.MaxCache.Duration
	rcvr.MinCacheDuration = Cfg.MinCache.Duration
	rcvr.MaxCachedPoints = Cfg.MaxCachedPoints
	rcvr.StatFlushDuration = Cfg.StatFlush.Duration
	rcvr.StatsNamePrefix = Cfg.StatsNamePrefix
	rcvr.DSSpecs = receiver.MatchingDSSpecFinder(Cfg)

	// Create and run the Service Manager
	serviceMgr = newServiceManager(rcvr)
	if err := serviceMgr.run(gracefulProtos); err != nil {
		log.Printf("Could not run the service manager: %v", err)
		return
	}

	if gracefulProtos != "" {
		// Do the graceful dance - tell the parent to die, then
		// wait for it to signal us back that the data has been
		// flushed correctly, at which point it is OK for us to
		// start the receiver.

		parent := syscall.Getppid()
		log.Printf("start(): Killing parent pid: %v", parent)
		syscall.Kill(parent, syscall.SIGTERM)
		log.Printf("start(): Waiting for the parent to signal that flush is complete...")
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGUSR1)
		s := <-ch
		log.Printf("start(): Received %v, proceeding to load the data", s)
	}

	// *finally* start the receiver (because graceful restart, parent must save first)
	rcvr.Start()

	// TODO - there should be a -f(oreground) flag?
	// Also see not below about saving the starting working directory
	//std2DevNull()
	//os.Chdir("/")

	// TODOthis too could be in the receiver for consistency?
	for {
		// Wait for a SIGINT or SIGTERM.
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		s := <-ch
		log.Printf("Got signal: %v", s)
		if s == syscall.SIGHUP {
			if gracefulChildPid == 0 {
				gracefulRestart(rcvr, cfgPath)
			}
		} else {
			gracefulExit(rcvr)
			break
		}
	}
}

func Finish() {
	quitting = true
	log.Printf("main: Waiting for all other goroutines to finish...")
	log.Println("main: All goroutines finished, exiting.")

	// Close log
	log.SetOutput(os.Stderr)
	if logFile != nil {
		logFile.Close()
	}

	os.Remove(Cfg.PidPath)
}

func gracefulRestart(rcvr *receiver.Receiver, cfgPath string) {

	if !filepath.IsAbs(os.Args[0]) {
		log.Printf("ERROR: Graceful restart only possible when %q started with absolute path, ignoring this request.", os.Args[0])
		return
	}

	files, protos := serviceMgr.listenerFilesAndProtocols()

	log.Printf("gracefulRestart(): Beginning graceful restart with sockets: %v and protos: %q", files, protos)

	mypath, _ := filepath.Abs(os.Args[0]) // TODO we should really be the starting working directory
	args := []string{
		"-c", cfgPath,
		"-graceful", protos}

	cmd := exec.Command(mypath, args...)
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = files

	// The new process will kill -TERM us when it's ready
	err := cmd.Start()
	if err != nil {
		log.Printf("gracefulRestart(): Failed to launch, error: %v", err)
	} else {
		gracefulChildPid = cmd.Process.Pid
		log.Printf("gracefulRestart(): Forked child, waiting to be killed...")
	}
}

func gracefulExit(rcvr *receiver.Receiver) {

	log.Printf("Gracefully exiting...")

	quitting = true

	rcvr.ClusterReady(false)

	log.Printf("Waiting for all TCP connections to finish...")
	serviceMgr.closeListeners()
	log.Printf("TCP connections finished.")

	// Stop the receiver
	rcvr.Stop()

	if gracefulChildPid != 0 {
		// let the child know the data is flushed
		syscall.Kill(gracefulChildPid, syscall.SIGUSR1)
	}
}

var std2DevNull = func() error {
	f, e := os.OpenFile("/dev/null", os.O_RDWR, 0)
	if e == nil {
		fd := int(f.Fd())
		syscall.Dup2(fd, int(os.Stdin.Fd()))
		syscall.Dup2(fd, int(os.Stdout.Fd()))
		syscall.Dup2(fd, int(os.Stderr.Fd()))
		return nil
	} else {
		return e
	}
}
