package dispatcher

import (
	"fmt"
	"log/slog"
	"sync"

	"github.com/khaivutri/network-app-streaming-collaboration/pkg/transport/rudp"
)

// HandlerFunc handles one application message.
type HandlerFunc func(sess *rudp.Session, streamID uint8, payload []byte) error

// Router safely maps message types to handlers and isolates panics.
type Router struct {
	handlers     map[uint8]HandlerFunc
	mu           sync.RWMutex
	fallback     HandlerFunc
	panicHandler func(*rudp.Session, uint8, error)
}

// NewRouter creates an empty router.
func NewRouter() *Router { return &Router{handlers: make(map[uint8]HandlerFunc)} }

// Register installs or replaces a message handler.
func (r *Router) Register(msgType uint8, fn HandlerFunc) {
	r.mu.Lock()
	r.handlers[msgType] = fn
	r.mu.Unlock()
}

// SetFallback installs the handler for unknown message types.
func (r *Router) SetFallback(fn HandlerFunc) { r.mu.Lock(); r.fallback = fn; r.mu.Unlock() }

// SetPanicHandler installs a callback used to send a protocol error after recovery.
func (r *Router) SetPanicHandler(fn func(*rudp.Session, uint8, error)) {
	r.mu.Lock()
	r.panicHandler = fn
	r.mu.Unlock()
}

// Dispatch invokes a handler with panic recovery and error logging.
func (r *Router) Dispatch(sess *rudp.Session, streamID uint8, msgType uint8, payload []byte) {
	defer func() {
		if recovered := recover(); recovered != nil {
			slog.Error("dispatcher recovered handler panic", "msg_type", msgType, "panic", recovered)
			r.mu.RLock()
			panicHandler := r.panicHandler
			r.mu.RUnlock()
			if panicHandler != nil {
				panicHandler(sess, msgType, fmt.Errorf("handler panic: %v", recovered))
			}
		}
	}()
	r.mu.RLock()
	fn, ok := r.handlers[msgType]
	fallback := r.fallback
	r.mu.RUnlock()
	if !ok || fn == nil {
		fn = fallback
	}
	if fn == nil {
		return
	}
	if err := fn(sess, streamID, payload); err != nil {
		slog.Warn("dispatcher handler failed", "msg_type", msgType, "error", err)
	}
}
