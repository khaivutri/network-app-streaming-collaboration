package events

import (
	"sync"
	"sync/atomic"
	"time"
)

// Bus fans out bounded, non-blocking domain events to local UI subscribers.
type Bus struct {
	mu          sync.Mutex
	nextID      atomic.Uint64
	nextSubID   uint64
	subscribers map[uint64]chan Event
	closed      bool
}

// NewBus creates an empty event bus.
func NewBus() *Bus {
	return &Bus{
		subscribers: make(map[uint64]chan Event),
	}
}

// Subscribe registers a bounded subscriber channel.
func (b *Bus) Subscribe(buffer int) (uint64, <-chan Event) {
	if buffer <= 0 {
		buffer = 32
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.nextSubID++
	id := b.nextSubID
	channel := make(chan Event, buffer)
	if b.closed {
		close(channel)
		return id, channel
	}
	b.subscribers[id] = channel
	return id, channel
}

// Unsubscribe removes and closes one subscriber channel.
func (b *Bus) Unsubscribe(id uint64) {
	b.mu.Lock()
	defer b.mu.Unlock()
	channel, ok := b.subscribers[id]
	if !ok {
		return
	}
	delete(b.subscribers, id)
	close(channel)
}

// Publish emits an event without blocking the producer.
func (b *Bus) Publish(name string, data any) Event {
	event := Event{
		ID:        b.nextID.Add(1),
		Name:      name,
		Data:      data,
		CreatedAt: time.Now(),
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return event
	}
	for _, channel := range b.subscribers {
		select {
		case channel <- event:
		default:
		}
	}
	return event
}

// Close closes all current subscribers and prevents future deliveries.
func (b *Bus) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for id, channel := range b.subscribers {
		close(channel)
		delete(b.subscribers, id)
	}
}
