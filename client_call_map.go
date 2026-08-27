package thriftobs

import (
	"sync"
	"time"
)

type ClientCall struct {
	SeqID     int32
	Method    string
	Start     time.Time
	BytesSent uint64
}

type ClientCallMap struct {
	calls map[int32]ClientCall
	mu    sync.RWMutex
}

func NewClientCallMap() *ClientCallMap {
	return &ClientCallMap{
		calls: map[int32]ClientCall{},
		mu:    sync.RWMutex{},
	}
}

func (c *ClientCallMap) Add(call ClientCall) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.calls[call.SeqID] = call
}

func (c *ClientCallMap) Remove(seqID int32) (ClientCall, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()

	call, ok := c.calls[seqID]
	if ok {
		delete(c.calls, seqID)
	}
	return call, ok
}

func (c *ClientCallMap) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.calls)
}
