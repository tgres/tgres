package dsl

import (
	"testing"
	"time"
)

func Test_funcs_dsl(t *testing.T) {
	DBTime := "2006-01-02 15:04:05"
	when, _ := time.Parse(DBTime, "2017-03-16 09:41:00")

	// averageSeries
	sm, err := ParseDsl(nil, "averageSeries(constantLine(10), constantLine(20), constantLine(30))", when.Unix(), when.Add(-time.Hour).Unix(), 100)
	if err != nil {
		t.Error(err)
	}

	n := 0
	for _, s := range sm {
		for s.Next() {
			v := s.CurrentValue()
			if v != 20 {
				t.Errorf("s.CurrentValue != 20: %v", v)
			}
			n++
		}
		if n != 2 {
			t.Errorf("n != 2")
		}
	}

}
