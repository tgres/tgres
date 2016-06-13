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

package daemon

import (
	"flag"
	"fmt"
	"github.com/tgres/tgres/serde"
	x "github.com/tgres/tgres/transceiver"
	"log"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"runtime"
	"syscall"
)

var (
	serviceMgr       *ServiceManager
	logFile          *os.File
	cycleLogCh            = make(chan int)
	quitting         bool = false
	gracefulChildPid int
)

func parseFlags() (string, string) {

	var (
		textCfgPath, gracefulProtos string
	)

	// Parse the flags, if any
	flag.StringVar(&textCfgPath, "c", "./etc/tgres.conf", "path to config file")
	flag.StringVar(&gracefulProtos, "graceful", "", "list of fds (internal use only)")
	flag.Parse()

	return textCfgPath, gracefulProtos
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

	cfgPath, gracefulProtos := parseFlags()

	// This creates the Cfg variable
	if err := ReadConfig(cfgPath); err != nil {
		log.Fatal("Exiting.")
	}

	wd, err := os.Getwd()
	if err != nil {
		log.Fatal(err)
	}

	// TODO This should like inside config?
	if err := processConfig(configer(Cfg), wd); err != nil { // This validates the config
		log.Fatalf("Error in config file %s: %v", cfgPath, err)
	}

	savePid(Cfg.PidPath)

	// Initialize Database
	if db, err := serde.InitDb(Cfg.DbConnectString); err != nil {
		log.Fatalf("Error connecting to the DB: %v", err)
	} else {
		log.Printf("Initialized DB connection.")

		// Create the transceiver
		t := x.New(db)
		t.NWorkers = Cfg.Workers
		t.MaxCacheDuration = Cfg.MaxCache.Duration
		t.MinCacheDuration = Cfg.MinCache.Duration
		t.MaxCachedPoints = Cfg.MaxCachedPoints
		t.StatFlushDuration = Cfg.StatFlush.Duration
		t.StatsNamePrefix = Cfg.StatsNamePrefix
		t.DSSpecs = x.MatchingDSSpecFinder(Cfg)

		// Create and run the Service Manager
		serviceMgr = newServiceManager(t)
		if err := serviceMgr.run(gracefulProtos); err != nil {
			log.Printf("Could not run the service manager: %v", err)
			return
		}

		if gracefulProtos != "" {
			// Do the graceful dance - tell the parent to die, then
			// wait for it to signal us back that the data has been
			// flushed correctly, at which point it is OK for us to
			// start the transceiver.

			parent := syscall.Getppid()
			log.Printf("start(): Killing parent pid: %v", parent)
			syscall.Kill(parent, syscall.SIGTERM)
			log.Printf("start(): Waiting for the parent to signal that flush is complete...")
			ch := make(chan os.Signal)
			signal.Notify(ch, syscall.SIGUSR1)
			s := <-ch
			log.Printf("start(): Received %v, proceeding to load the data", s)
		}

		// *finally* start the transceiver (because graceful restart, parent must save first)
		if err := t.Start(); err != nil {
			log.Printf("Could not start the transceiver: %v", err)
			return
		}

		// TODO - there should be a -f(oreground) flag?
		// Also see not below about saving the starting working directory
		//std2DevNull()
		//os.Chdir("/")

		// TODOthis too could be in the transceiver for consistency?
		for {
			// Wait for a SIGINT or SIGTERM.
			ch := make(chan os.Signal)
			signal.Notify(ch, syscall.SIGINT, syscall.SIGTERM, syscall.SIGHUP)
			s := <-ch
			log.Printf("Got signal: %v", s)
			if s == syscall.SIGHUP {
				if gracefulChildPid == 0 {
					gracefulRestart(t, cfgPath)
				}
			} else {
				gracefulExit(t)
				break
			}
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

func gracefulRestart(t *x.Transceiver, cfgPath string) {

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

func gracefulExit(t *x.Transceiver) {

	log.Printf("Gracefully exiting...")

	quitting = true

	log.Printf("Waiting for all TCP connections to finish...")
	serviceMgr.closeListeners()
	log.Printf("TCP connections finished.")

	// Stop the transceiver
	t.Stop()

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
