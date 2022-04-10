package cdc

import (
	"sync"

	"6.824/logging"
	"github.com/sirupsen/logrus"
)

type DataChangeCapturer struct {
	mu     sync.RWMutex
	notify map[string]*KeyListener
	logger *logrus.Logger
}

type KeyListener struct {
	listeners map[chan string]struct{}
	mu        sync.RWMutex
}
type WatchResult struct {
	Key      string
	Notify   chan string
	Callback func()
}

func NewKeyListener() *KeyListener {
	return &KeyListener{listeners: make(map[chan string]struct{})}
}

func NewDCC() *DataChangeCapturer {
	return &DataChangeCapturer{
		notify: make(map[string]*KeyListener),
		logger: logging.GetLogger("dcc", logrus.DebugLevel),
	}
}

func (dc *DataChangeCapturer) CaptureDataChange(key string, newVal string) {
	dc.mu.RLock()
	listener, ok := dc.notify[key]
	dc.mu.RUnlock()
	if ok {
		listener.mu.RLock()
		for notifyChan := range listener.listeners {
			if len(notifyChan) != 0 {
				// For watch operations, etcd guarantees to return the same value
				// for the same key across all members for the same revision.
				continue
			}
			// select {
			// case oldVal := <-notifyChan:
			// 	dc.logger.WithField("oldval", oldVal).Debug("removing old vals")
			// default:
			// }
			notifyChan <- newVal
		}
		listener.mu.RUnlock()
	}
}

// returns new value, watch must return immediately
func (dc *DataChangeCapturer) Watch(key string) *WatchResult {
	changedChan := make(chan string, 1)
	dc.mu.Lock()
	listener, ok := dc.notify[key]
	if !ok {
		listener = NewKeyListener()
		dc.notify[key] = listener
	}
	dc.mu.Unlock()

	listener.mu.Lock()
	listener.listeners[changedChan] = struct{}{}
	listener.mu.Unlock()

	callback := func() {
		listener.mu.Lock()
		delete(listener.listeners, changedChan)
		listener.mu.Unlock()
	}
	return &WatchResult{Key: key, Callback: callback, Notify: changedChan}
}
