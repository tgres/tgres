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
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/tgres/tgres/blaster"
	"github.com/tgres/tgres/cluster"
	"github.com/tgres/tgres/dsl"
	"github.com/tgres/tgres/receiver"
	"github.com/tgres/tgres/serde"
)

var (
	logFile          *os.File
	cycleLogCh       = make(chan int)
	gracefulChildPid int
)

var getCwd = func() string {
	// blank means wd could not be established
	// it might be okay if paths are absolute
	wd, _ := os.Getwd()
	return wd
}

var savePid = func(pidPath string) error {
	f, err := os.Create(pidPath)
	if err == nil {
		fmt.Fprintf(f, "%d\n", os.Getpid())
		return f.Close()
	}
	return err
}

var initDb = func(connectString string) (serde.DbSerDe, error) {
	prefix := os.Getenv("TGRES_DB_PREFIX")
	return serde.InitDb(connectString, prefix)
}

// Figure out which address to bind to and which to advertize for the
// cluster. (The two are not always the same e.g. in a container).
var determineClusterBindAddress = func(db serde.DbAddresser) (bindAddr, advAddr string, err error) {
	bindAddr = os.Getenv("TGRES_BIND")
	if os.Getenv("TGRES_ADDRFROMDB") != "" {
		var a *string
		a, err = db.MyDbAddr()
		if err != nil {
			return "", "", err
		}
		if a == nil {
			return "", "", fmt.Errorf("Database returned nil address.")
		}
		advAddr = *a
	} else {
		advAddr = bindAddr
	}
	return
}

var determineClusterJoinAddress = func(join string, db serde.DbAddresser) (ips []string, err error) {
	if join != "" {
		ips = strings.Split(join, ",")
	} else if os.Getenv("TGRES_ADDRFROMDB") != "" {
		if ips, err = db.ListDbClientIps(); err != nil {
			return nil, err
		}
	}
	return ips, err
}

var initCluster = func(bindAddr, advAddr string, joinIps []string) (c *cluster.Cluster, err error) {
	c, err = cluster.NewClusterBind(bindAddr, 0, advAddr, 0, 0, bindAddr)
	if err != nil {
		return nil, err
	}

	if err := c.Join(joinIps); err != nil {
		return nil, fmt.Errorf("Unable to join cluster members: %q, %v", strings.Join(joinIps, ","), err)
	}

	return c, nil
}

var createReceiver = func(cfg *Config, c *cluster.Cluster, db serde.SerDe) *receiver.Receiver {
	r := receiver.NewWithMaxQueue(db, receiver.MatchingDSSpecFinder(cfg), cfg.MaxReceiverQueueSize)
	r.MinStep = cfg.MinStep.Duration
	r.StatFlushDuration = cfg.StatFlush.Duration
	r.StatsNamePrefix = cfg.StatsNamePrefix
	r.MaxReceiverQueueSize = cfg.MaxReceiverQueueSize
	r.MaxMemoryBytes = uint64(cfg.MaxMemoryBytes)
	r.ReportStats = true
	r.NWorkers = cfg.Workers
	r.SetCluster(c)
	return r
}

var startReceiver = func(r *receiver.Receiver) {
	r.Start()
}

var waitForSignal = func(r *receiver.Receiver, sm *serviceManager, cfgPath, join string) {
	for {
		// Wait for a SIGINT or SIGTERM.
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
		s := <-ch
		log.Printf("Got signal: %v", s)
		if s == syscall.SIGHUP {
			if gracefulChildPid == 0 {
				gracefulRestart(r, sm, cfgPath, join)
			}
		} else {
			gracefulExit(r, sm)
			break
		}
	}
}

func Init(cfgPath, gracefulProtos, join string) (cfg *Config) { // not to be confused with init()

	log.Printf("Tgres starting.")

	// Read the config
	cfg, err := readConfig(cfgPath)
	if err != nil {
		log.Printf("Unable to read config %q, exiting: %s.", cfgPath, err)
		return
	}
	log.Printf("Using config file: '%s'.", cfgPath)

	// Get current directory
	wd := getCwd() // a separate function for testability
	if wd == "" {
		log.Printf("WARNING: Could not determine current working directory, this only works if all paths in config are absolute.")
	}

	// Validate the configuration
	if err := processConfig(cfg, wd); err != nil { // This validates the config
		log.Printf("Error in config file %s, exiting: %v", cfgPath, err)
		return
	}

	// Connect to the DB (and create tables if needed, etc)
	db, err := initDb(cfg.DbConnectString)
	if err != nil {
		log.Printf("Error connecting to the DB, exiting: %v", err)
		return
	}
	log.Printf("Initialized DB connection.")

	// Determine cluster bind address
	var bindAddr, advAddr string
	bindAddr, advAddr, err = determineClusterBindAddress(db.DbAddresser())
	if err != nil {
		log.Printf("Cannot determine cluster bind / advertise addresses, exiting: %v", err)
		return
	}

	// Determine ips of other nodes to join
	var joinIps []string
	joinIps, err = determineClusterJoinAddress(join, db.DbAddresser())
	if err != nil {
		log.Printf("Cannot determine cluster node addresses to join, exiting: %v", err)
		return
	}

	// Create Receiver (with nil cluster, because if graceful, then we must wait for parent to shutdown)
	rcvr := createReceiver(cfg, nil, db)

	// Is there a blaster?
	if os.Getenv("TGRES_BLASTER") != "" {
		log.Printf("Creating a blaster instance.")
		rcvr.Blaster = blaster.New(rcvr)
	}

	// Create and run the Service Manager
	rcache := dsl.NewNamedDSFetcher(db.Fetcher())
	serviceMgr := newServiceManager(rcvr, rcache, cfg)
	if err := serviceMgr.run(gracefulProtos); err != nil {
		log.Printf("Could not run the service manager: %v", err)
		return
	}

	// Handle graceful file descriptors
	if gracefulProtos != "" {
		// Do the graceful dance - tell the parent to die, then
		// wait for it to signal us back that the data has been
		// flushed correctly, at which point it is OK for us to
		// start the receiver.

		log.Printf("start(): All listeners are listening.")
		parent := syscall.Getppid()
		log.Printf("start(): Killing parent pid: %v", parent)
		syscall.Kill(parent, syscall.SIGTERM)
		log.Printf("start(): Waiting for the parent to signal that flush is complete...")
		ch := make(chan os.Signal)
		signal.Notify(ch, syscall.SIGUSR1)
		s := <-ch
		log.Printf("start(): Received %v, proceeding to load the data", s)
	} else {
		log.Printf("start(): Proceeding with initialization.") // i.e. this is not graceful
	}

	// Initialize cluster
	// We had to wait until after graceful, so that the new cluster can bind to sockets
	var c *cluster.Cluster
	const (
		clusterPause = time.Second
		attempts     = 30
	)
	for i := 0; i < attempts; i++ {
		c, err = initCluster(bindAddr, advAddr, joinIps)
		if err != nil {
			log.Printf("Error initializing cluster, will try again in %v (up to %v times): %v", clusterPause, attempts, err)
			time.Sleep(clusterPause)
			continue
		}
		break
	}
	if err != nil {
		log.Printf("Error initializing cluster, giving up and exiting: %v", err)
		return
	}
	rcvr.SetCluster(c)

	// Save PID (by now the graceful parent pid can be overwritten)
	if err := savePid(cfg.PidPath); err != nil {
		// This is not good, but isn't fatal
		log.Printf("WARNING: Unable to create pid file '%s', exiting: (%v)", cfg.PidPath, err)
	} else {
		log.Printf("Pid saved in %q.", cfg.PidPath)
	}

	// *finally* start the receiver (because graceful restart, parent must save data first)
	startReceiver(rcvr)
	log.Printf("Receiver started, Tgres is ready.")

	// Wait for HUP or TERM, etc.
	waitForSignal(rcvr, serviceMgr, cfgPath, join)

	return
}

// Only remove pid if it matches ours
var checkRemovePid = func(pidPath string) bool {
	bpid, err := ioutil.ReadFile(pidPath)
	if err != nil {
		return false
	}
	spid := string(bpid)
	if strings.HasSuffix(spid, "\n") {
		spid = spid[:len(spid)-1]
	}
	npid, err := strconv.ParseInt(spid, 10, 64)
	if err != nil {
		return false
	}

	pid := os.Getpid()

	if int(npid) != pid {
		return false // not our pid
	}

	err = os.Remove(pidPath)
	return err == nil
}

func Finish(cfg *Config) {
	log.Printf("main: Waiting for all other goroutines to finish...")
	log.Println("main: All goroutines finished, exiting.")

	if checkRemovePid(cfg.PidPath) {
		log.Printf("Removed pid-file %q", cfg.PidPath)
	}

	// Close log
	log.SetOutput(os.Stderr)
	if logFile != nil {
		logFile.Close()
	}
}

func gracefulRestart(rcvr *receiver.Receiver, serviceMgr *serviceManager, cfgPath, join string) {

	if !filepath.IsAbs(os.Args[0]) {
		log.Printf("ERROR: Graceful restart only possible when %q started with absolute path, ignoring this request.", os.Args[0])
		return
	}

	files, protos := serviceMgr.listenerFilesAndProtocols()
	log.Printf("gracefulRestart(): Beginning graceful restart with sockets: %v and protos: %q", files, protos)

	mypath, _ := filepath.Abs(os.Args[0]) // TODO we should really be the starting working directory
	args := []string{
		"-c", cfgPath}

	if join != "" {
		args = append(args, "-join", join)
	}

	os.Unsetenv("TGRES_PROTOS")
	env := append(os.Environ(), fmt.Sprintf("TGRES_PROTOS=%s", protos))

	cmd := exec.Command(mypath, args...)
	cmd.Env = env
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	cmd.ExtraFiles = files

	err := cmd.Start()
	if err != nil {
		log.Printf("gracefulRestart(): Failed to launch, error: %v", err)
	} else {
		gracefulChildPid = cmd.Process.Pid
		log.Printf("gracefulRestart(): Forked child, waiting to be killed...")
	}

	// The new process will kill -TERM us when it's ready to accept
	// connections which will trigger gracefulExit().  If it does not
	// (because it failed), it's business as ususal we just carry on.
}

func gracefulExit(rcvr *receiver.Receiver, serviceMgr *serviceManager) {

	log.Printf("Gracefully exiting...")

	log.Printf("Closing TCP Listeners...")
	serviceMgr.closeListeners(false)
	log.Printf("TCP listeners closed.")

	rcvr.ClusterReady(false) // triggers a transition
	// Allow enough time for a transition to start
	time.Sleep(500 * time.Millisecond) // TODO This is a hack

	log.Printf("Waiting for all TCP connections to finish...")
	serviceMgr.closeListeners(true)
	log.Printf("TCP connections finished.")

	// Stop the receiver
	rcvr.Stop()

	if gracefulChildPid != 0 {
		// let the child know the data is flushed
		syscall.Kill(gracefulChildPid, syscall.SIGUSR1)
	}
}
