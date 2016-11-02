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

// Tgres is a tool for receiving and reporting on simple time series
// written in Go which uses PostgreSQL for storage.
package main

import (
	"flag"
	"github.com/tgres/tgres/daemon"
	"log"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
)

func parseFlags() (textCfgPath, gracefulProtos, join string, bg bool) {

	// Parse the flags, if any
	flag.StringVar(&textCfgPath, "c", "./etc/tgres.conf", "path to config file")
	flag.StringVar(&join, "join", "", "List of add:port,addr:port,... of nodes to join")
	flag.StringVar(&gracefulProtos, "graceful", "", "list of fds (internal use only)")
	flag.BoolVar(&bg, "bg", false, "Immediately background itself")
	flag.Parse()

	return
}

func main() {

	textCfgPath, gracefulProtos, join, bg := parseFlags()

	if bg {
		if !filepath.IsAbs(textCfgPath) {
			log.Fatalf("ERROR: Background only possible when config path is absolute (cfg path: %q).", textCfgPath)
		}
		if !filepath.IsAbs(os.Args[0]) {
			log.Fatalf("ERROR: Background only possible when %q started with absolute path (path: %q).", os.Args[0])
		}
		log.Printf("Backgrounding...")
		if err := std2DevNull(); err != nil {
			log.Fatalf("Error: %v", err)
		}
		os.Chdir("/")
		background(textCfgPath, join)
		return
	}

	if cfg := daemon.Init(textCfgPath, gracefulProtos, join); cfg != nil {
		daemon.Finish(cfg)
	}
}

func background(cp, join string) {
	mypath, _ := filepath.Abs(os.Args[0])
	args := []string{"-c", cp}
	if join != "" {
		args = append(args, "-join", join)
	}
	cmd := exec.Command(mypath, args...)
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Fatalf("Error: %v", err)
	}
}

func std2DevNull() error {
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
