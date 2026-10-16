package thriftobs_test

import (
	"context"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type poolTestClient struct {
	id int
}

type poolTestError struct{}

func (*poolTestError) Error() string {
	return "compiled failure"
}

func (*poolTestError) TExceptionType() thrift.TExceptionType {
	return thrift.TExceptionTypeCompiled
}

type poolMockTransport struct {
	thrift.TTransport

	live     *atomic.Int64
	closed   atomic.Bool
	closes   atomic.Int32
	closeErr error
}

func (m *poolMockTransport) Open() error {
	return nil
}

func (m *poolMockTransport) IsOpen() bool {
	return !m.closed.Load()
}

func (m *poolMockTransport) Close() error {
	m.closed.Store(true)
	m.closes.Add(1)

	if m.live != nil {
		m.live.Add(-1)
	}

	return m.closeErr
}

type poolTestFactory struct {
	mu         sync.Mutex
	calls      int
	transports []*poolMockTransport

	failCalls  int
	panicCalls int
	delay      time.Duration
	closeErr   error

	live atomic.Int64
	peak atomic.Int64
}

func (f *poolTestFactory) create(_ context.Context) (poolTestClient, thrift.TTransport, error) {
	f.mu.Lock()

	f.calls++

	id := f.calls

	if f.panicCalls > 0 {
		f.panicCalls--
		f.mu.Unlock()

		panic("pool factory panic")
	}

	if f.failCalls > 0 {
		f.failCalls--
		f.mu.Unlock()

		return poolTestClient{}, nil, assert.AnError
	}

	delay := f.delay
	f.mu.Unlock()

	if delay > 0 {
		time.Sleep(delay)
	}

	transport := &poolMockTransport{live: &f.live, closeErr: f.closeErr}

	f.mu.Lock()
	f.transports = append(f.transports, transport)
	f.mu.Unlock()

	current := f.live.Add(1)

	for {
		peak := f.peak.Load()
		if current <= peak || f.peak.CompareAndSwap(peak, current) {
			break
		}
	}

	return poolTestClient{id: id}, transport, nil
}

func (f *poolTestFactory) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.calls
}

func (f *poolTestFactory) transportCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()

	return len(f.transports)
}

func (f *poolTestFactory) transport(index int) *poolMockTransport {
	f.mu.Lock()
	defer f.mu.Unlock()

	return f.transports[index]
}

func newPoolTestPool(
	t *testing.T,
	factory *poolTestFactory,
	opts ...thriftobs.PoolOption,
) *thriftobs.Pool[poolTestClient] {
	t.Helper()

	pool, err := thriftobs.NewPool(nil, factory.create, opts...)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Close())
	})

	return pool
}

func gaugeValue(t *testing.T, gauge prometheus.Gauge) float64 {
	t.Helper()

	var metric dto.Metric

	require.NoError(t, gauge.Write(&metric))

	return metric.GetGauge().GetValue()
}

func newPoolTestGauges() *thriftobs.PoolMetrics {
	return &thriftobs.PoolMetrics{
		Total: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_pool_connections",
			Help: "Total number of pooled connections.",
		}),
		InUse: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_pool_in_use",
			Help: "Number of connections currently in use.",
		}),
		Idle: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_pool_idle",
			Help: "Number of idle connections.",
		}),
		Invalid: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_pool_invalid",
			Help: "Number of invalid connections.",
		}),
		Open: prometheus.NewGauge(prometheus.GaugeOpts{
			Name: "test_pool_open",
			Help: "Number of open connections.",
		}),
	}
}

func TestNewPool_NilFactory(t *testing.T) {
	t.Parallel()

	pool, err := thriftobs.NewPool[poolTestClient](nil, nil)

	require.ErrorIs(t, err, thriftobs.ErrNilFactory)
	assert.Nil(t, pool)
}

func TestNewPool_CreatesMinConnections(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}

	newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(3))

	assert.Equal(t, 3, rec.callCount())
	assert.Equal(t, 3, rec.transportCount())
}

func TestNewPool_ClampsMinConnectionsToMax(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}

	newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(10),
		thriftobs.WithPoolMaxConnections(2),
	)

	assert.Equal(t, 2, rec.callCount())
}

func TestNewPool_BestEffortWhenInitialConnectionsFail(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{failCalls: 2}

	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(2))

	assert.Equal(t, 2, rec.callCount())

	err := pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 3, rec.callCount())
}

func TestPool_Do_Success(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(1))

	var got poolTestClient

	err := pool.Do(t.Context(), func(client poolTestClient) error {
		got = client

		return nil
	})

	require.NoError(t, err)
	assert.NotZero(t, got.id)
	assert.Equal(t, 1, rec.callCount())

	err = pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 1, rec.callCount())
}

func TestPool_Do_NilOperation(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(0))

	err := pool.Do(t.Context(), nil)

	require.ErrorIs(t, err, thriftobs.ErrNilOperation)
}

func TestPool_Do_ContextCanceled(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(0))

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	err := pool.Do(ctx, func(poolTestClient) error {
		return nil
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Zero(t, rec.callCount())
}

func TestPool_Do_NilTransportFromFactory(t *testing.T) {
	t.Parallel()

	pool, err := thriftobs.NewPool(
		nil,
		func(_ context.Context) (poolTestClient, thrift.TTransport, error) {
			return poolTestClient{}, nil, nil
		},
		thriftobs.WithPoolMinConnections(0),
	)
	require.NoError(t, err)

	t.Cleanup(func() {
		require.NoError(t, pool.Close())
	})

	err = pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	})

	require.ErrorIs(t, err, thriftobs.ErrNilTransport)
}

func TestPool_Do_Retries(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		retries  int
		expected int32
	}{
		{name: "retries disabled", retries: 0, expected: 1},
		{name: "two retries", retries: 2, expected: 3},
		{name: "negative uses default", retries: -1, expected: 2},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			rec := &poolTestFactory{}
			pool := newPoolTestPool(t, rec,
				thriftobs.WithPoolMinConnections(1),
				thriftobs.WithPoolMaxRetries(tc.retries),
			)

			var calls atomic.Int32

			err := pool.Do(t.Context(), func(poolTestClient) error {
				calls.Add(1)

				return assert.AnError
			})

			require.ErrorIs(t, err, assert.AnError)
			assert.Equal(t, tc.expected, calls.Load())
		})
	}
}

func TestPool_Do_ApplicationException_NoRetry(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(1),
		thriftobs.WithPoolMaxRetries(2),
	)

	appErr := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "it failed, no luck")

	var calls int

	err := pool.Do(t.Context(), func(poolTestClient) error {
		calls++

		return appErr
	})

	require.ErrorIs(t, err, appErr)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, rec.callCount())
	assert.Equal(t, 1, rec.transportCount())
	assert.False(t, rec.transport(0).closed.Load())

	// The connection was released, not invalidated: the next call reuses it.
	require.NoError(t, pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	}))
	assert.Equal(t, 1, rec.callCount())
}

func TestPool_Do_CompiledException_NoRetry(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(1),
		thriftobs.WithPoolMaxRetries(2),
	)

	compiledErr := &poolTestError{}

	var calls int

	err := pool.Do(t.Context(), func(poolTestClient) error {
		calls++

		return compiledErr
	})

	require.ErrorIs(t, err, compiledErr)
	assert.Equal(t, 1, calls)
	assert.False(t, rec.transport(0).closed.Load())
}

func TestPool_Do_DesynchronizedApplicationException(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(1),
		thriftobs.WithPoolMaxRetries(1),
	)

	var calls int

	err := pool.Do(t.Context(), func(poolTestClient) error {
		calls++

		return thrift.NewTApplicationException(thrift.WRONG_METHOD_NAME, "wrong method name")
	})

	var appEx thrift.TApplicationException

	require.ErrorAs(t, err, &appEx)
	assert.Equal(t, int32(thrift.WRONG_METHOD_NAME), appEx.TypeId())
	assert.Equal(t, 2, calls)
	assert.Equal(t, 2, rec.callCount())
	assert.True(t, rec.transport(0).closed.Load())
}

func TestPool_Do_ContextCanceledDuringCall(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(1),
		thriftobs.WithPoolMaxRetries(2),
	)

	ctx, cancel := context.WithCancel(t.Context())

	var calls int

	err := pool.Do(ctx, func(poolTestClient) error {
		calls++
		cancel()

		return context.Canceled
	})

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, 1, calls)
	assert.Equal(t, 1, rec.callCount())
	assert.True(t, rec.transport(0).closed.Load())
}

func TestPool_Do_PanickingFactoryReleasesReservation(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{panicCalls: 1}

	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(0),
		thriftobs.WithPoolMaxConnections(1),
		thriftobs.WithPoolHealthCheck(time.Hour),
	)

	require.Panics(t, func() {
		_ = pool.Do(t.Context(), func(poolTestClient) error {
			return nil
		})
	})

	ctx, cancel := context.WithTimeout(t.Context(), time.Second)
	defer cancel()

	err := pool.Do(ctx, func(poolTestClient) error {
		return nil
	})

	require.NoError(t, err)
	assert.Equal(t, 2, rec.callCount())
}

func TestPool_Do_ClosedPool(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec)

	require.NoError(t, pool.Close())

	err := pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	})

	require.ErrorIs(t, err, thriftobs.ErrPoolClosed)
}

func TestPool_Do_WaitsForAvailableConnection(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(1),
		thriftobs.WithPoolMaxConnections(1),
		thriftobs.WithPoolHealthCheck(time.Hour),
	)

	started := make(chan struct{})
	release := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- pool.Do(t.Context(), func(poolTestClient) error {
			close(started)
			<-release

			return nil
		})
	}()

	<-started

	secondDone := make(chan error, 1)

	go func() {
		secondDone <- pool.Do(t.Context(), func(poolTestClient) error {
			return nil
		})
	}()

	select {
	case <-secondDone:
		require.Fail(t, "second Do completed while the only connection was in use")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)

	require.NoError(t, <-firstDone)
	require.NoError(t, <-secondDone)
}

func TestPool_Do_CapacityInvariant(t *testing.T) {
	t.Parallel()

	const (
		maxConns = 4
		workers  = 32
		ops      = 10
	)

	rec := &poolTestFactory{delay: 2 * time.Millisecond}

	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(0),
		thriftobs.WithPoolMaxConnections(maxConns),
		thriftobs.WithPoolMaxIdleTime(time.Hour),
		thriftobs.WithPoolMaxLifetime(time.Hour),
		thriftobs.WithPoolHealthCheck(time.Hour),
		thriftobs.WithPoolConnectTimeout(5*time.Second),
	)

	ctx := t.Context()

	var wg sync.WaitGroup

	errCh := make(chan error, workers)

	for range workers {
		wg.Add(1)

		go func() {
			defer wg.Done()

			for range ops {
				err := pool.Do(ctx, func(poolTestClient) error {
					time.Sleep(time.Millisecond)

					return nil
				})
				if err != nil {
					errCh <- err

					return
				}
			}
		}()
	}

	wg.Wait()
	close(errCh)

	for err := range errCh {
		require.NoError(t, err)
	}

	assert.Positive(t, rec.peak.Load())
	assert.LessOrEqual(t, rec.peak.Load(), int64(maxConns))

	require.NoError(t, pool.Close())

	for i := range rec.transportCount() {
		assert.Equal(t, int32(1), rec.transport(i).closes.Load())
	}

	assert.Equal(t, int64(0), rec.live.Load())
}

func TestPool_Close(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}
	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(2))

	require.NoError(t, pool.Close())

	require.Equal(t, int32(1), rec.transport(0).closes.Load())
	require.Equal(t, int32(1), rec.transport(1).closes.Load())
	assert.False(t, rec.transport(0).IsOpen())

	require.NoError(t, pool.Close())
	assert.Equal(t, int32(1), rec.transport(0).closes.Load())
}

func TestPool_Close_TransportError(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{closeErr: assert.AnError}

	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(1))

	require.ErrorIs(t, pool.Close(), assert.AnError)
}

func TestPool_HealthCheck_RemovesExpiredConnections(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}

	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(0),
		thriftobs.WithPoolMaxIdleTime(time.Hour),
		thriftobs.WithPoolMaxLifetime(10*time.Millisecond),
		thriftobs.WithPoolHealthCheck(10*time.Millisecond),
	)

	require.NoError(t, pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	}))
	require.Equal(t, 1, rec.transportCount())

	first := rec.transport(0)

	require.Eventually(t, first.closed.Load, time.Second, 5*time.Millisecond)
	assert.Equal(t, int32(1), first.closes.Load())

	require.NoError(t, pool.Do(t.Context(), func(poolTestClient) error {
		return nil
	}))
	assert.Equal(t, 2, rec.transportCount())
}

func TestPool_HealthCheck_RecreatesMinConnections(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}

	pool := newPoolTestPool(t, rec,
		thriftobs.WithPoolMinConnections(1),
		thriftobs.WithPoolMaxConnections(2),
		thriftobs.WithPoolMaxIdleTime(time.Hour),
		thriftobs.WithPoolMaxLifetime(10*time.Millisecond),
		thriftobs.WithPoolHealthCheck(10*time.Millisecond),
	)

	require.Eventually(t, func() bool {
		return rec.transportCount() >= 2
	}, time.Second, 5*time.Millisecond)

	first := rec.transport(0)
	assert.True(t, first.closed.Load())
	assert.Equal(t, int32(1), first.closes.Load())

	require.NoError(t, pool.Close())

	for i := range rec.transportCount() {
		assert.Equal(t, int32(1), rec.transport(i).closes.Load())
	}
}

func TestPool_SetMetrics(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}

	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(2))

	metrics := newPoolTestGauges()

	pool.SetMetrics(metrics)

	assert.Equal(t, float64(2), gaugeValue(t, metrics.Total))
	assert.Zero(t, gaugeValue(t, metrics.InUse))
	assert.Equal(t, float64(2), gaugeValue(t, metrics.Idle))
	assert.Zero(t, gaugeValue(t, metrics.Invalid))
	assert.Equal(t, float64(2), gaugeValue(t, metrics.Open))

	require.NoError(t, pool.Do(t.Context(), func(poolTestClient) error {
		require.Equal(t, float64(1), gaugeValue(t, metrics.InUse))
		require.Equal(t, float64(1), gaugeValue(t, metrics.Idle))

		return nil
	}))

	assert.Zero(t, gaugeValue(t, metrics.InUse))
	assert.Equal(t, float64(2), gaugeValue(t, metrics.Idle))

	require.NoError(t, pool.Close())

	assert.Zero(t, gaugeValue(t, metrics.Total))
	assert.Zero(t, gaugeValue(t, metrics.Idle))
	assert.Zero(t, gaugeValue(t, metrics.Open))
}

func TestPool_SetMetrics_OnDemandCreation(t *testing.T) {
	t.Parallel()

	rec := &poolTestFactory{}

	pool := newPoolTestPool(t, rec, thriftobs.WithPoolMinConnections(0))

	metrics := newPoolTestGauges()

	pool.SetMetrics(metrics)

	assert.Zero(t, gaugeValue(t, metrics.Total))

	require.NoError(t, pool.Do(t.Context(), func(poolTestClient) error {
		require.Equal(t, float64(1), gaugeValue(t, metrics.Total))
		require.Equal(t, float64(1), gaugeValue(t, metrics.InUse))
		require.Equal(t, float64(1), gaugeValue(t, metrics.Open))

		return nil
	}))

	assert.Zero(t, gaugeValue(t, metrics.InUse))
	assert.Equal(t, float64(1), gaugeValue(t, metrics.Idle))
}

func TestThriftClientFactory_ContextCanceled(t *testing.T) {
	t.Parallel()

	factory := thriftobs.ThriftClientFactory(
		func(thrift.TTransport, thrift.TProtocolFactory) poolTestClient {
			return poolTestClient{id: 1}
		},
		thrift.NewTBinaryProtocolFactoryConf(nil),
		"127.0.0.1",
		1,
		time.Second,
		time.Second,
	)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()

	client, transport, err := factory(ctx)

	require.ErrorIs(t, err, context.Canceled)
	assert.Equal(t, poolTestClient{}, client)
	assert.Nil(t, transport)
}

func TestThriftClientFactory_Connect(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)

	assertThriftClientFactoryConnect(t, listener, "127.0.0.1")
}

func TestThriftClientFactory_ConnectIPv6(t *testing.T) {
	t.Parallel()

	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Skip("IPv6 loopback unavailable")
	}

	assertThriftClientFactoryConnect(t, listener, "::1")
}

func assertThriftClientFactoryConnect(t *testing.T, listener net.Listener, host string) {
	t.Helper()

	t.Cleanup(func() {
		_ = listener.Close()
	})

	accepted := make(chan net.Conn, 1)

	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}

		accepted <- conn
	}()

	protocolFactory := thrift.NewTBinaryProtocolFactoryConf(nil)

	var received thrift.TProtocolFactory

	factory := thriftobs.ThriftClientFactory(
		func(_ thrift.TTransport, pf thrift.TProtocolFactory) poolTestClient {
			received = pf

			return poolTestClient{id: 42}
		},
		protocolFactory,
		host,
		listener.Addr().(*net.TCPAddr).Port,
		time.Second,
		time.Second,
	)

	client, transport, err := factory(t.Context())

	require.NoError(t, err)
	require.NotNil(t, transport)
	assert.True(t, transport.IsOpen())
	assert.Equal(t, 42, client.id)
	assert.Same(t, protocolFactory, received)

	require.NoError(t, transport.Close())

	select {
	case conn := <-accepted:
		_ = conn.Close()
	case <-time.After(time.Second):
		require.Fail(t, "connection was not accepted")
	}
}
