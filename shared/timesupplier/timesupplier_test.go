package timesupplier

import (
	"math/rand"
	"testing"
	"time"
)

func TestCheckRealtime(t *testing.T) {
	ti := CachedTime()
	tinow := time.Now()
	t.Log(ti, tinow)
	sub := tinow.Sub(ti)
	if sub < 0 {
		sub = -sub
	}
	t.Log(sub, sub.Milliseconds())
	if sub > 2*ResolutionInMillis*time.Millisecond {
		t.Fatal("should less than 2*ResolutionInMillis")
	} else {
		t.Log("actual time gap:", sub.Milliseconds())
	}

	for i := 0; i < 10; i++ {
		time.Sleep(time.Duration(rand.Intn(500)+500) * time.Millisecond)

		ti2 := CachedTime()
		ti2now := time.Now()
		t.Log(ti2, ti2now)
		sub2 := ti2.Sub(ti2now)
		if sub2 < 0 {
			sub2 = -sub2
		}
		t.Log(sub2, sub2.Milliseconds())
		if sub2 > 2*ResolutionInMillis*time.Millisecond {
			t.Fatal("should less than 2*ResolutionInMillis")
		} else {
			t.Log("actual time gap:", sub2.Milliseconds())
		}
	}
}
