package priorityqueue_test

import (
	"testing"

	"github.com/meidoworks/nekoq/shared/priorityqueue"
)

func TestRandomOrder(t *testing.T) {
	p := priorityqueue.NewMinPriorityQueue[string]()

	p.Push("C", 3)
	p.Push("A", 1)
	p.Push("E", 5)
	p.Push("B", 2)
	p.Push("D", 4)

	if "A" != p.Peak() {
		t.Fatal("expected A to", p.Peak())
	}
	if val := p.Pop(); "A" != val {
		t.Fatal("expected A to", val)
	}
	if val := p.Pop(); "B" != val {
		t.Fatal("expected B to", val)
	}
	if val := p.Pop(); "C" != val {
		t.Fatal("expected C to", val)
	}
	if val := p.Pop(); "D" != val {
		t.Fatal("expected D to", val)
	}
	if val := p.Pop(); "E" != val {
		t.Fatal("expected E to", val)
	}
	if !p.IsEmpty() {
		t.Fatal("expected empty")
	}
}
