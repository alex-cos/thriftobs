package thriftobs

import (
	"runtime"
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
	calls      map[int32]ClientCall
	expiration time.Duration
	interval   time.Duration
	stop       chan struct{}
	mu         sync.RWMutex
}

func NewClientCallMap(expiration, interval time.Duration) *ClientCallMap {
	ccm := &ClientCallMap{
		expiration: expiration,
		interval:   interval,
		calls:      map[int32]ClientCall{},
		stop:       make(chan struct{}),
		mu:         sync.RWMutex{},
	}

	go ccm.run()

	runtime.SetFinalizer(ccm, func(c *ClientCallMap) {
		c.abort()
	})

	return ccm
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

func (c *ClientCallMap) cleanup() {
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()

	for k, v := range c.calls {
		exptime := v.Start.Add(c.expiration)
		if exptime.UnixNano() > 0 && now.After(exptime) {
			delete(c.calls, k)
		}
	}
}

func (c *ClientCallMap) abort() {
	if c.stop != nil {
		close(c.stop)
	}
}

func (c *ClientCallMap) run() {
	tick := time.Tick(c.interval)
	for {
		select {
		case <-tick:
			c.cleanup()
		case <-c.stop:
			return
		}
	}
}
