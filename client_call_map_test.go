package thriftobs_test

import (
	"sync"
	"testing"
	"time"

	"github.com/alex-cos/thriftobs"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClientCallMap(t *testing.T) {
	t.Parallel()

	cm := thriftobs.NewClientCallMap()
	assert.NotNil(t, cm)
}

func TestClientCallMap_AddAndRemove(t *testing.T) {
	t.Parallel()

	cm := thriftobs.NewClientCallMap()
	call := thriftobs.ClientCall{
		SeqID:     1,
		Method:    "echo",
		Start:     time.Now(),
		BytesSent: 100,
	}

	cm.Add(call)
	removed, ok := cm.Remove(1)

	require.True(t, ok)
	assert.Equal(t, call, removed)

	// Second remove should fail
	_, ok = cm.Remove(1)
	assert.False(t, ok)
}

func TestClientCallMap_RemoveNonExistent(t *testing.T) {
	t.Parallel()

	cm := thriftobs.NewClientCallMap()
	_, ok := cm.Remove(999)
	assert.False(t, ok)
}

func TestClientCallMap_ConcurrentAccess(t *testing.T) {
	t.Parallel()

	cm := thriftobs.NewClientCallMap()
	var wg sync.WaitGroup
	const numGoroutines = 100
	const callsPerGoroutine = 100

	// Add calls concurrently
	for i := range numGoroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := range callsPerGoroutine {
				seqID := int32(base + j)
				cm.Add(thriftobs.ClientCall{
					SeqID:     seqID,
					Method:    "test",
					Start:     time.Now(),
					BytesSent: uint64(j),
				})
			}
		}(i * callsPerGoroutine)
	}
	wg.Wait()

	// Remove calls concurrently
	for i := range numGoroutines {
		wg.Add(1)
		go func(base int) {
			defer wg.Done()
			for j := range callsPerGoroutine {
				seqID := int32(base + j)
				_, _ = cm.Remove(seqID)
			}
		}(i * callsPerGoroutine)
	}
	wg.Wait()

	// All calls should be removed
	assert.Equal(t, 0, cm.Len())
}

func TestClientCallMap_OverwriteSameSeqID(t *testing.T) {
	t.Parallel()

	cm := thriftobs.NewClientCallMap()
	call1 := thriftobs.ClientCall{SeqID: 1, Method: "first", Start: time.Now(), BytesSent: 10}
	call2 := thriftobs.ClientCall{SeqID: 1, Method: "second", Start: time.Now(), BytesSent: 20}

	cm.Add(call1)
	cm.Add(call2) // overwrite

	removed, ok := cm.Remove(1)
	require.True(t, ok)
	assert.Equal(t, call2, removed) // should get the latest
}
