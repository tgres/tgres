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

// Package misc is misc stuff.
package misc

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"
)

var (
	sanitizeRegexSpace       = regexp.MustCompile("\\s+")
	sanitizeRegexSlash       = regexp.MustCompile("/")
	sanitizeRegexNonAlphaNum = regexp.MustCompile("[^a-zA-Z_\\-0-9\\.]")
)

func SanitizeName(name string) string {
	name = sanitizeRegexSpace.ReplaceAllString(name, "_")
	name = sanitizeRegexSlash.ReplaceAllString(name, "-")
	return sanitizeRegexNonAlphaNum.ReplaceAllString(name, "")
}

func BetterParseDuration(s string) (time.Duration, error) {

	if strings.HasSuffix(s, "min") {
		s = s[0 : len(s)-2] // min -> m
	} else if strings.HasSuffix(s, "hour") {
		s = s[0 : len(s)-3] // hour -> h
	} else if strings.HasSuffix(s, "mon") {
		fd, err := strconv.ParseFloat(s[0:len(s)-3], 64)
		if err != nil {
			return 0, err
		}
		s = fmt.Sprintf("%vh", fd*30*24)
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
