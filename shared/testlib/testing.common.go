package testlib

import (
	"testing"
)

func AssertError(t testing.TB, e error) {
	if e != nil {
		t.Fatal("assertError:", e)
	}
}
