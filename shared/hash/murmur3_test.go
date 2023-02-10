package hash_test

import (
	"testing"

	"github.com/meidoworks/nekoq/shared/hash"
)

func TestMurmur3String(t *testing.T) {
	if hash.DoMurmur3([]byte("hello")) != 613153351 {
		t.Fatal("murmur3 mismatch")
	}
}

func BenchmarkMurmur3String(b *testing.B) {
	for i := 0; i < b.N; i++ {
		hash.DoMurmur3([]byte("hello"))
	}
}
