package priorityqueue

import (
	"container/heap"
)

type PriorityQueue[T any] struct {
	priorityQueue priorityQueue[T]
}

func NewMinPriorityQueue[T any]() *PriorityQueue[T] {
	p := new(PriorityQueue[T])
	heap.Init(&p.priorityQueue)
	return p
}

func (p *PriorityQueue[T]) Push(value T, priority int) {
	item := &Item[T]{
		value:    value,
		priority: priority,
	}
	heap.Push(&p.priorityQueue, item)
}

func (p *PriorityQueue[T]) Pop() T {
	item := heap.Pop(&p.priorityQueue).(*Item[T])
	return item.value
}

func (p *PriorityQueue[T]) Peak() T {
	return p.priorityQueue[0].value
}

func (p *PriorityQueue[T]) IsEmpty() bool {
	return p.priorityQueue.Len() == 0
}

type Item[T any] struct {
	value    T
	priority int
}

type priorityQueue[T any] []*Item[T]

func (pq priorityQueue[T]) Len() int { return len(pq) }

func (pq priorityQueue[T]) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq priorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
}

func (pq *priorityQueue[T]) Push(x any) {
	item := x.(*Item[T])
	*pq = append(*pq, item)
}

func (pq *priorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	old[n-1] = nil // avoid memory leak
	*pq = old[0 : n-1]
	return item
}
