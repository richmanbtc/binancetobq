package watchdog

import (
	"sync/atomic"
	"time"
)

type Watchdog struct {
	start      time.Time
	lastPing   atomic.Int64
	timeout    atomic.Int64
	stop, done chan struct{}
}

// Exit when no ping arrives within the timeout.
// No logging, application locks, or cleanup are allowed on the hard-exit path.
func New(timeout time.Duration, exit func()) *Watchdog {
	w := &Watchdog{start: time.Now(), stop: make(chan struct{}), done: make(chan struct{})}
	w.SetTimeout(timeout)
	ticker := time.NewTicker(time.Second)
	go func() {
		defer close(w.done)
		defer ticker.Stop()
		for {
			select {
			case <-w.stop:
				return
			case <-ticker.C:
				now := time.Since(w.start).Nanoseconds()
				if now-w.lastPing.Load() >= w.timeout.Load() {
					exit()
					return
				}
			}
		}
	}()
	return w
}
func (w *Watchdog) Close() { close(w.stop); <-w.done }
func (w *Watchdog) SetTimeout(timeout time.Duration) {
	if w != nil {
		w.timeout.Store(int64(timeout))
	}
}
func (w *Watchdog) Ping() {
	if w != nil {
		w.lastPing.Store(time.Since(w.start).Nanoseconds())
	}
}
