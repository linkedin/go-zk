package zk

import (
	"context"
	"errors"
	"sync"
)

var ErrEventQueueClosed = errors.New("zk: event queue closed")

type EventQueue interface {
	// Next waits for a new Event to be received until the context expires or the queue is closed.
	Next(ctx context.Context) (Event, error)
	// Push adds the given event to the queue and notifies any in-flight calls to Next that a new event is available.
	Push(e Event)
	// Close functions like closing a channel. Subsequent calls to Next will drain whatever events remain in the buffer
	// while subsequent calls to Push will panic. Once remaining events are drained, Next will return
	// ErrEventQueueClosed.
	Close()
}

type chanEventQueue chan Event

func (c chanEventQueue) Next(ctx context.Context) (Event, error) {
	select {
	case <-ctx.Done():
		return Event{}, ctx.Err()
	case e, ok := <-c:
		if !ok {
			return Event{}, ErrEventQueueClosed
		} else {
			return e, nil
		}
	}
}

func (c chanEventQueue) Push(e Event) {
	c <- e
}

func (c chanEventQueue) Close() {
	close(c)
}

func newChanEventChannel() chanEventQueue {
	return make(chan Event, 1)
}

type unlimitedEventQueue struct {
	lock     sync.Mutex
	newEvent chan struct{}
	events   []Event
}

func newUnlimitedEventQueue() *unlimitedEventQueue {
	return &unlimitedEventQueue{
		newEvent: make(chan struct{}),
	}
}

func (q *unlimitedEventQueue) Push(e Event) {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.newEvent == nil {
		// Panic like a closed channel
		panic("send on closed unlimited channel")
	}

	q.events = append(q.events, e)
	close(q.newEvent)
	q.newEvent = make(chan struct{})
}

func (q *unlimitedEventQueue) Close() {
	q.lock.Lock()
	defer q.lock.Unlock()

	if q.newEvent == nil {
		// Panic like a closed channel
		panic("close of closed EventQueue")
	}

	close(q.newEvent)
	q.newEvent = nil
}

func (q *unlimitedEventQueue) Next(ctx context.Context) (Event, error) {
	for {
		q.lock.Lock()
		if len(q.events) > 0 {
			e := q.events[0]
			q.events = q.events[1:]
			q.lock.Unlock()
			return e, nil
		}

		ch := q.newEvent
		if ch == nil {
			q.lock.Unlock()
			return Event{}, ErrEventQueueClosed
		}
		q.lock.Unlock()

		select {
		case <-ctx.Done():
			return Event{}, ctx.Err()
		case <-ch:
			continue
		}
	}
}
