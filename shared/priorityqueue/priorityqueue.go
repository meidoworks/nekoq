package priorityqueue

import (
	"container/heap"
)

type Option[T any] func(queue *PriorityQueue[T])

type PriorityQueue[T any] struct {
	priorityQueue priorityQueue[T]
}

func WithPreallocateSize[T any](n int) Option[T] {
	return func(queue *PriorityQueue[T]) {
		queue.priorityQueue = make(priorityQueue[T], 0, n)
	}
}

func NewMinPriorityQueue[T any](options ...Option[T]) *PriorityQueue[T] {
	p := new(PriorityQueue[T])
	for _, option := range options {
		option(p)
	}
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

func (p *PriorityQueue[T]) Size() int {
	return p.priorityQueue.Len()
}

type Item[T any] struct {
	value    T
	priority int

	index int
}

type priorityQueue[T any] []*Item[T]

func (pq priorityQueue[T]) Len() int { return len(pq) }

func (pq priorityQueue[T]) Less(i, j int) bool {
	return pq[i].priority < pq[j].priority
}

func (pq priorityQueue[T]) Swap(i, j int) {
	pq[i], pq[j] = pq[j], pq[i]
	pq[i].index = i
	pq[j].index = j
}

func (pq *priorityQueue[T]) Push(x any) {
	item := x.(*Item[T])
	item.index = len(*pq)
	*pq = append(*pq, item)
}

func (pq *priorityQueue[T]) Pop() any {
	old := *pq
	n := len(old)
	item := old[n-1]
	item.index = -1 // for safety
	old[n-1] = nil  // avoid memory leak
	*pq = old[0 : n-1]
	return item
}

func (pq *priorityQueue[T]) update(item *Item[T], value T, priority int) {
	item.value = value
	item.priority = priority
	heap.Fix(pq, item.index)
}
