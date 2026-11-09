package events

import "time"

// Event is an immutable notification emitted after client state changes.
type Event struct {
	ID        uint64
	Name      string
	Data      any
	CreatedAt time.Time
}
