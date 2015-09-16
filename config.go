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

package timeriver

import (
	"errors"
	"fmt"
	"github.com/BurntSushi/toml"
	"log"
	"os"
	"path/filepath"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var config *trConfig

type trConfig struct {
	PidPath                  string   `toml:"pid-file"`
	LogPath                  string   `toml:"log-file"`
	LogCycle                 duration `toml:"log-cycle-interval"`
	DbConnectString          string   `toml:"db-connect-string"`
	MaxCachedPoints          int      `toml:"max-cached-points"`
	MaxCache                 duration `toml:"max-cache-duration"`
	MinCache                 duration `toml:"min-cache-duration"`
	GraphiteTextListenSpec   string   `toml:"graphite-text-listen-spec"`
	GraphiteUdpListenSpec    string   `toml:"graphite-udp-listen-spec"`
	GraphitePickleListenSpec string   `toml:"graphite-pickle-listen-spec"`
	HttpListenSpec           string   `toml:"http-listen-spec"`
	Workers                  int
	DSs                      []trDSSpec `toml:"ds"`
}

type duration struct {
	time.Duration
}

func (d *duration) UnmarshalText(text []byte) error {
	var err error
	d.Duration, err = time.ParseDuration(string(text))
	return err
}

type regex struct {
	*regexp.Regexp
	Text string
}

func (r *regex) UnmarshalText(text []byte) error {
	var err error
	r.Text = string(text)
	r.Regexp, err = regexp.Compile(string(text))
	return err
}

type trDSSpec struct {
	Regexp    regex
	Type      string
	Step      duration
	Heartbeat duration
	RRAs      []trRRASpec
}

type trRRASpec struct {
	Function string
	Step     time.Duration
	Size     time.Duration
	Xff      float64
}

func betterParseDuration(s string) (time.Duration, error) {
	if strings.HasSuffix(s, "min") {
		s = s[0 : len(s)-2] // min -> m
	} else if strings.HasSuffix(s, "hour") {
		s = s[0 : len(s)-3] // hour -> h
	}
	if d, err := time.ParseDuration(s); err != nil {
		if strings.HasPrefix(err.Error(), "time: unknown unit ") {
			d, _ := strconv.ParseInt(s[0:len(s)-1], 10, 64)
			if strings.HasPrefix(err.Error(), "time: unknown unit d in") {
				return time.Duration(d*24) * time.Hour, nil
			} else if strings.HasPrefix(err.Error(), "time: unknown unit w in") {
				return time.Duration(d*168) * time.Hour, nil
			} else if strings.HasPrefix(err.Error(), "time: unknown unit y in") {
				return time.Duration(d*8760) * time.Hour, nil
			}
		}
		return d, err
	} else {
		return d, nil
	}
}

func (r *trRRASpec) UnmarshalText(text []byte) error {
	r.Xff = 0.5
	parts := strings.SplitN(string(text), ":", 4)
	if len(parts) < 3 || len(parts) > 4 {
		return fmt.Errorf("Invalid RRA specification (not enough or too many elements): %q", string(text))
	}
	r.Function = parts[0]
	if strings.ToLower(r.Function) != "average" {
		return fmt.Errorf("Invalid function: %q", r.Function)
	}
	var err error
	if r.Step, err = betterParseDuration(parts[1]); err != nil {
		return fmt.Errorf("Invalid Step: %q (%v)", parts[1], err)
	}
	if r.Size, err = betterParseDuration(parts[2]); err != nil {
		return fmt.Errorf("Invalid Size: %q (%v)", parts[2], err)
	}
	if (r.Size.Nanoseconds() % r.Step.Nanoseconds()) != 0 {
		newSize := time.Duration(r.Size.Nanoseconds()/r.Step.Nanoseconds()*r.Step.Nanoseconds()) * time.Nanosecond
		log.Printf("Size (%q) is not a multiple of Step (%q), auto adjusting Size to %v.", parts[2], parts[1], newSize)
		r.Size = newSize
		if newSize.Nanoseconds() == 0 {
			return fmt.Errorf("invalid Size (%v)", newSize)
		}
	}
	if len(parts) == 4 {
		var err error
		if r.Xff, err = strconv.ParseFloat(parts[3], 64); err != nil {
			return fmt.Errorf("Invalid XFF: %q (%v)", parts[3], err)
		}
	}
	return nil
}

func readConfig(cfgPath string) (*trConfig, error) {
	config = &trConfig{}
	_, err := toml.DecodeFile(cfgPath, config)
	if err != nil {
		log.Printf("Unable to read config: %s.", err)
		return nil, err
	} else {
		log.Printf("Read config file: '%s'.", cfgPath)
	}
	return config, nil
}

func (c *trConfig) processConfigPidFile(wd string) error {
	if c.PidPath == "" {
		return fmt.Errorf("pid-file setting empty")
	}
	if !filepath.IsAbs(c.PidPath) {
		c.PidPath = filepath.Join(wd, c.PidPath)
	}
	pidDir, _ := filepath.Split(c.PidPath)
	if err := os.MkdirAll(pidDir, 0755); err != nil {
		return errors.New(fmt.Sprintf("Unable to create directory: '%s' (%v).", pidDir, err))
	}
	return nil
}

func (c *trConfig) processConfigLogFile(wd string) error {
	if c.LogPath == "" {
		return fmt.Errorf("log-file setting empty")
	}
	if !filepath.IsAbs(c.LogPath) {
		c.LogPath = filepath.Join(wd, c.LogPath)
	}
	logDir, _ := filepath.Split(c.LogPath)
	if err := os.MkdirAll(logDir, 0755); err != nil {
		return errors.New(fmt.Sprintf("Unable to create directory: '%s' (%v).", logDir, err))
	}

	log.Printf("Logs will be written to '%s'.", c.LogPath)
	return nil
}

func (c *trConfig) processConfigLogCycleInterval() error {
	if c.LogCycle.Duration == 0 {
		return fmt.Errorf("log-cycle-interval setting empty")
	}
	log.Printf("Will cycle logs every %v (log-cycle-interval).", c.LogCycle.Duration)

	logDir, _ := filepath.Split(c.LogPath)
	log.Printf("All further status messages will be written to log file(s) in '%s'.", logDir)
	logFileCycler()
	log.Print("Server starting.")

	return nil
}

func (c *trConfig) processDbConnectString() error {
	if c.DbConnectString == "" {
		return fmt.Errorf("db-connect-string empty")
	}
	if err := initDbConnection(c.DbConnectString); err == nil {
		log.Printf("Initialized DB connection.")
	} else {
		log.Printf("Error connecting to the DB: %v", err)
		return err
	}
	return nil
}

func (c *trConfig) processMaxCachedPoints() error {
	if c.MaxCachedPoints == 0 {
		return fmt.Errorf("max-cached-points missing, must be integer")
	}
	log.Printf("Data Sources will be flushed when cached points exceeds %d (max-cached-points).", c.MaxCachedPoints)
	return nil
}

func (c *trConfig) processMaxCacheDuration() error {
	if c.MaxCache.Duration == 0 {
		return fmt.Errorf("max-cache-duration is missing")
	} else {
		log.Printf("Data Sources will be flushed after (approximately) %v (max-cache-duration).", c.MaxCache.Duration)
	}
	return nil
}

func (c *trConfig) processMinCacheDuration() error {
	if c.MinCache.Duration == 0 {
		return fmt.Errorf("min-cache-duration is missing")
	} else {
		log.Printf("A Data Source will be flushed at most once per %v (min-cache-duration).", c.MinCache.Duration)
	}
	return nil
}

func (c *trConfig) processWorkers() error {
	if c.Workers == 0 {
		return fmt.Errorf("workers missing, must be an integer")
	}
	log.Printf("Number of workers (and flushers) will be %d.", c.Workers)
	return nil
}

func (c *trConfig) processDSSpec() error {
	// TODO validate function, regular expression, all that
	for _, ds := range c.DSs {
		for _, rra := range ds.RRAs {
			if (rra.Step.Nanoseconds() % ds.Step.Duration.Nanoseconds()) != 0 {
				newStep := time.Duration(rra.Step.Nanoseconds()/ds.Step.Duration.Nanoseconds()*ds.Step.Duration.Nanoseconds()) * time.Nanosecond
				log.Printf("DS %q: RRA step (%v) is not a multiple of DS Step (%v), auto adjusting Step to %v.", ds.Regexp.Text, rra.Step, ds.Step.Duration, newStep)
				if newStep.Nanoseconds() == 0 {
					return fmt.Errorf("DS %q: invalid Step (%v)", ds.Regexp.Text, newStep)
				}
				rra.Step = newStep
			}
		}
	}
	// TODO xff?
	return nil
}

func (c *trConfig) findMatchingDsSpec(name string) *trDSSpec {
	for _, dsSpec := range c.DSs {
		if dsSpec.Regexp.Regexp.MatchString(name) {
			return &dsSpec
		}
	}
	return nil
}

type configer interface {
	processConfigPidFile(string) error
	processConfigLogFile(string) error
	processConfigLogCycleInterval() error
	processDbConnectString() error
	processMaxCachedPoints() error
	processMaxCacheDuration() error
	processMinCacheDuration() error
	processWorkers() error
	processDSSpec() error
}

func processConfig(c configer, wd string) error {

	if err := c.processConfigPidFile(wd); err != nil {
		return err
	}
	if err := c.processConfigLogFile(wd); err != nil {
		return err
	}
	if err := c.processConfigLogCycleInterval(); err != nil {
		return err
	}
	if err := c.processDbConnectString(); err != nil {
		return err
	}
	if err := c.processMaxCachedPoints(); err != nil {
		return err
	}
	if err := c.processMaxCacheDuration(); err != nil {
		return err
	}
	if err := c.processMinCacheDuration(); err != nil {
		return err
	}
	if err := c.processWorkers(); err != nil {
		return err
	}
	if err := c.processDSSpec(); err != nil {
		return err
	}
	return nil
}
