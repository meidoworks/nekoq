package mq

import (
	"testing"
)

func TestStartTime(t *testing.T) {
	idgen := NewIdGen(1, 1)
	t.Log(idgen.Next())
	t.Log(idgen.Next())
	t.Log(idgen.Next())
	t.Log(idgen.Next())
	i1, err := idgen.Next()
	assertError(t, err)
	i2, err := idgen.Next()
	assertError(t, err)
	if i1 == i2 {
		t.Fatal("id should not equals")
	}
}

func BenchmarkIdGen_Next(b *testing.B) {
	idgen := NewIdGen(1, 1)
	id, err := idgen.Next()
	if err != nil {
		b.Fatal(err)
	}
	b.Log(id)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		idgen.Next()
	}
}

func BenchmarkIdGen_Next_Parallel(b *testing.B) {
	idgen := NewIdGen(1, 1)
	id, err := idgen.Next()
	if err != nil {
		b.Fatal(err)
	}
	b.Log(id)

	b.ResetTimer()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			idgen.Next()
		}
	})
}
