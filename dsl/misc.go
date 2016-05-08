package dsl

import (
	"fmt"
	"strconv"
	"strings"
	"time"
)

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
