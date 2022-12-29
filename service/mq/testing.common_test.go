package mq_test

import (
	"testing"
)

func assertError(t testing.TB, e error) {
	if e != nil {
		t.Fatal("assertError:", e)
	}
}
