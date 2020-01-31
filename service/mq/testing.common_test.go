package mq

import (
	"testing"
)

func assertError(t testing.TB, e error) {
	if e != nil {
		t.Fatal("assertError:", e)
	}
}

func AssertError(t testing.TB, e error) {
	if e != nil {
		t.Fatal("assertError:", e)
	}
}
