package mux

import (
	"sync"
)

type Broadcast[T any] struct {
	sync.Mutex
	subs []chan T
	ack  chan bool
}

func NewBroadcast[T any]() *Broadcast[T] {
	return &Broadcast[T]{
		subs: []chan T{},
		ack:  make(chan bool),
	}
}

func (b *Broadcast[T]) Send(msg T) {
	b.Lock()
	defer b.Unlock()

	for _, sub := range b.subs {
		sub <- msg
	}
	for range b.subs {
		<-b.ack
	}
}

func (b *Broadcast[T]) Subscribe() <-chan T {
	b.Lock()
	defer b.Unlock()

	ch := make(chan T)
	b.subs = append(b.subs, ch)
	return ch
}

func (b *Broadcast[T]) Unsubscribe(ch <-chan T) {
	b.Lock()
	defer b.Unlock()

	idx := -1
	for i, sub := range b.subs {
		if sub == ch {
			idx = i
			close(sub)
		}
	}
	if idx >= 0 {
		b.subs[idx] = b.subs[len(b.subs)-1]
		b.subs = b.subs[:len(b.subs)-1]
	}
}

func (b *Broadcast[T]) Close() {
	b.Lock()
	defer b.Unlock()

	for _, sub := range b.subs {
		close(sub)
	}
	b.subs = []chan T{}

	close(b.ack)
}
