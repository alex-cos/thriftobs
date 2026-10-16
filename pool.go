package thriftobs

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	ErrPoolClosed   = errors.New("pool is closed")
	ErrNilFactory   = errors.New("pool factory is nil")
	ErrNilTransport = errors.New("pool factory returned nil transport")
	ErrPoolFull     = errors.New("pool is full")
	ErrNilOperation = errors.New("pool operation is nil")
)

// poolMetricsSnapshot holds the connection state counters.
type poolMetricsSnapshot struct {
	total   float64
	inUse   float64
	invalid float64
	open    float64
}

// PoolMetrics holds the optional Prometheus gauges updated by the pool.
// Every field may be nil; nil gauges are ignored.
type PoolMetrics struct {
	Total   prometheus.Gauge // pooled connections
	InUse   prometheus.Gauge // connections currently checked out
	Idle    prometheus.Gauge // connections not in use
	Invalid prometheus.Gauge // connections marked invalid, awaiting removal
	Open    prometheus.Gauge // connections whose transport is open
}

// PoolFactory creates a new client and transport.
// The factory must honor ctx cancellation whenever possible.
// ctx may be canceled because the caller canceled the operation or
// because the pool is being closed.
type PoolFactory[T any] func(context.Context) (T, thrift.TTransport, error)

type pooledConn[T any] struct {
	mu         sync.Mutex
	client     T
	transport  thrift.TTransport
	inUse      bool
	invalid    bool
	createdAt  time.Time
	lastUsedAt time.Time
}

// Pool is a generic connection pool for Thrift clients.
type Pool[T any] struct {
	config  PoolConfig
	factory PoolFactory[T]
	logger  *slog.Logger

	metricsMu sync.Mutex
	metrics   PoolMetrics

	connsMu sync.Mutex
	conns   []*pooledConn[T]
	// nextIdx is protected by connsMu.
	nextIdx  uint64
	creating int

	notifyCh chan struct{}

	closed atomic.Bool

	lifecycleMu sync.Mutex
	active      sync.WaitGroup

	healthStopCh chan struct{}
	healthWg     sync.WaitGroup
}

// NewPool creates a new connection pool.
func NewPool[T any](
	logger *slog.Logger,
	factory PoolFactory[T],
	opts ...PoolOption,
) (*Pool[T], error) {
	if factory == nil {
		return nil, ErrNilFactory
	}

	if logger == nil {
		logger = slog.Default()
	}

	config := DefaultPoolConfig()
	for _, opt := range opts {
		opt(&config)
	}
	config = normalizePoolConfig(config)

	p := &Pool[T]{
		config:       config,
		factory:      factory,
		logger:       logger.With("component", "thrift_pool"),
		conns:        make([]*pooledConn[T], 0, config.maxConnections),
		notifyCh:     make(chan struct{}),
		healthStopCh: make(chan struct{}),
	}

	// Best effort: the pool can start even if some initial connections fail.
	for range config.minConnections {
		ctx, cancel := context.WithTimeout(context.Background(), config.connectTimeout)
		err := p.createAndAddConn(ctx)
		cancel()

		if err != nil {
			p.logger.Warn("failed to create initial connection, will retry on demand",
				"error", err,
			)
		}
	}

	p.healthWg.Add(1)
	go p.healthChecker()

	p.computeMetrics()

	return p, nil
}

func (p *Pool[T]) SetMetrics(metrics *PoolMetrics) {
	p.metricsMu.Lock()

	if metrics != nil {
		p.metrics = *metrics
	}

	p.metricsMu.Unlock()

	p.computeMetrics()
}

// Close closes the pool and waits for active operations/background work.
func (p *Pool[T]) Close() error {
	var lastErr error

	if !p.beginClose() {
		return nil
	}

	close(p.healthStopCh)

	p.connsMu.Lock()
	p.signalLocked()
	p.connsMu.Unlock()

	p.healthWg.Wait()

	p.active.Wait()

	p.connsMu.Lock()
	conns := p.conns
	p.conns = nil
	p.signalLocked()
	p.connsMu.Unlock()

	for _, conn := range conns {
		conn.mu.Lock()

		transport := conn.transport
		conn.transport = nil
		conn.invalid = true
		conn.inUse = false
		conn.client = *new(T)

		conn.mu.Unlock()

		if transport != nil {
			if err := transport.Close(); err != nil {
				lastErr = err
			}
		}
	}

	p.computeMetrics()

	return lastErr
}

// Do executes fn with a pooled client.
//
// Application exceptions returned by the server (thrift.TApplicationException
// or generated exception types) are returned without retrying and leave the
// connection usable. Transport and protocol errors cause the connection to be
// invalidated and the operation to be retried. The number of retries is
// controlled by WithPoolMaxRetries (0 disables retries).
func (p *Pool[T]) Do(ctx context.Context, fn func(T) error) error {
	var lastErr error

	if fn == nil {
		return ErrNilOperation
	}

	if err := p.beginOperation(); err != nil {
		return err
	}
	defer p.endOperation()

	for range p.config.maxRetries + 1 {
		conn, idx, err := p.acquire(ctx)
		if err != nil {
			return err
		}

		conn.mu.Lock()
		client := conn.client
		transport := conn.transport
		invalid := conn.invalid
		conn.mu.Unlock()

		if invalid || transport == nil {
			p.invalidate(conn)
			lastErr = ErrNilTransport
			continue
		}

		err = fn(client)

		if err == nil {
			p.release(conn)
			return nil
		}

		lastErr = err

		if isApplicationError(err) {
			p.release(conn)

			return fmt.Errorf("pool: operation failed: %w", err)
		}

		p.logger.Warn(
			"transport error, invalidating connection and retrying",
			"conn_idx", idx,
			"error", err,
		)

		p.invalidate(conn)

		if ctx.Err() != nil {
			return fmt.Errorf("pool: operation failed: %w", lastErr)
		}
	}

	return fmt.Errorf("pool: operation failed: %w", lastErr)
}

// isApplicationError reports whether err is an application-level exception
// produced by the server and fully read from the wire, in which case the
// connection remains usable.
func isApplicationError(err error) bool {
	var appEx thrift.TApplicationException

	if errors.As(err, &appEx) {
		switch appEx.TypeId() {
		case thrift.INVALID_MESSAGE_TYPE_EXCEPTION,
			thrift.WRONG_METHOD_NAME,
			thrift.BAD_SEQUENCE_ID:
			// The response body was not consumed: the connection is desynchronized.
			return false
		default:
			return true
		}
	}

	var tex thrift.TException

	return errors.As(err, &tex) && tex.TExceptionType() == thrift.TExceptionTypeCompiled
}

// createConnection creates a connection using the supplied context.
func (p *Pool[T]) createConnection(ctx context.Context) (*pooledConn[T], error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	client, transport, err := p.factory(ctx)
	if err != nil {
		return nil, err
	}

	if transport == nil {
		return nil, ErrNilTransport
	}

	// The factory may return successfully just after cancellation.
	// Do not leak the transport in that case.
	if err := ctx.Err(); err != nil {
		_ = transport.Close()
		return nil, err
	}

	now := time.Now()

	return &pooledConn[T]{
		client:     client,
		transport:  transport,
		createdAt:  now,
		lastUsedAt: now,
	}, nil
}

// reserveCreation reserves one connection slot.
func (p *Pool[T]) reserveCreation() bool {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	if p.closed.Load() {
		return false
	}

	if len(p.conns)+p.creating >= p.config.maxConnections {
		return false
	}

	p.creating++

	return true
}

// releaseCreation releases one reserved connection slot.
func (p *Pool[T]) releaseCreation() {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	p.releaseCreationLocked()
	p.signalLocked()
}

// releaseCreationLocked releases one reserved slot; connsMu must be held.
func (p *Pool[T]) releaseCreationLocked() {
	if p.creating > 0 {
		p.creating--
	}
}

// createReservedConn creates a connection for an already-reserved slot.
// On failure, the reservation is released, including when the factory
// panics. On success, the caller owns the reservation and must either
// convert it with addReservedConn or release it with releaseCreation.
func (p *Pool[T]) createReservedConn(ctx context.Context) (*pooledConn[T], error) {
	created := false

	defer func() {
		if r := recover(); r != nil {
			p.releaseCreation()
			panic(r)
		}

		if !created {
			p.releaseCreation()
		}
	}()

	conn, err := p.createConnection(ctx)
	if err != nil {
		return nil, err
	}

	created = true

	return conn, nil
}

// addReservedConn converts a reserved slot into a pooled connection. The
// reservation is always consumed: it is either replaced by the added
// connection or released when the pool is closed. It returns the index of
// the added connection and whether it was added.
func (p *Pool[T]) addReservedConn(conn *pooledConn[T]) (int, bool) {
	p.connsMu.Lock()
	defer p.connsMu.Unlock()

	if p.closed.Load() {
		p.releaseCreationLocked()
		p.signalLocked()

		return -1, false
	}

	p.releaseCreationLocked()

	p.conns = append(p.conns, conn)
	idx := len(p.conns) - 1

	p.signalLocked()

	return idx, true
}

// createAndAddConn creates a connection and adds it to the pool.
func (p *Pool[T]) createAndAddConn(ctx context.Context) error {
	if p.closed.Load() {
		return ErrPoolClosed
	}

	if !p.reserveCreation() {
		if p.closed.Load() {
			return ErrPoolClosed
		}
		return ErrPoolFull
	}

	conn, err := p.createReservedConn(ctx)
	if err != nil {
		return err
	}

	if _, ok := p.addReservedConn(conn); !ok {
		p.closeTransport(conn.transport)

		return ErrPoolClosed
	}

	p.computeMetrics()

	return nil
}

// acquire gets an available connection, creating one if possible.
func (p *Pool[T]) acquire(ctx context.Context) (*pooledConn[T], int, error) {
	if err := ctx.Err(); err != nil {
		return nil, -1, err
	}

	for {
		if p.closed.Load() {
			return nil, -1, ErrPoolClosed
		}

		p.connsMu.Lock()

		if p.closed.Load() {
			p.connsMu.Unlock()
			return nil, -1, ErrPoolClosed
		}

		if conn, idx := p.findAvailableLocked(); conn != nil {
			snapshot := p.snapshotMetricsLocked()
			p.connsMu.Unlock()

			p.setMetricsValues(snapshot)

			return conn, idx, nil
		}

		if len(p.conns)+p.creating < p.config.maxConnections {
			p.creating++
			p.connsMu.Unlock()

			conn, err := p.createReservedConn(ctx)
			if err != nil {
				return nil, -1, err
			}

			// createConnection already checks ctx after the factory returns,
			// but keep this check here as a final ownership safeguard.
			if err := ctx.Err(); err != nil {
				p.releaseCreation()
				p.closeTransport(conn.transport)

				return nil, -1, err
			}

			conn.mu.Lock()
			conn.inUse = true
			conn.lastUsedAt = time.Now()
			conn.mu.Unlock()

			idx, ok := p.addReservedConn(conn)

			if !ok {
				p.closeTransport(conn.transport)

				return nil, -1, ErrPoolClosed
			}

			p.computeMetrics()

			return conn, idx, nil
		}

		// The pool is currently full. Capture the current notification
		// channel while holding connsMu, then wait for a state change.
		waitCh := p.notifyCh
		p.connsMu.Unlock()

		select {
		case <-ctx.Done():
			return nil, -1, ctx.Err()
		case <-waitCh:
			// State changed; retry.
		}

		p.computeMetrics()
	}
}

func (p *Pool[T]) findAvailableLocked() (*pooledConn[T], int) {
	n := len(p.conns)
	if n == 0 {
		return nil, -1
	}

	start := p.nextIdx % uint64(n)
	p.nextIdx++

	now := time.Now()

	for i := range n {
		idx := int((start + uint64(i)) % uint64(n)) //nolint: gosec
		conn := p.conns[idx]

		conn.mu.Lock()

		if !conn.inUse &&
			!conn.invalid &&
			conn.transport != nil {
			conn.inUse = true
			conn.lastUsedAt = now

			conn.mu.Unlock()
			return conn, idx
		}

		conn.mu.Unlock()
	}

	return nil, -1
}

// release marks a connection as available again.
func (p *Pool[T]) release(conn *pooledConn[T]) {
	if conn == nil {
		return
	}

	conn.mu.Lock()

	if !conn.invalid {
		conn.inUse = false
		conn.lastUsedAt = time.Now()
	}

	conn.mu.Unlock()

	p.connsMu.Lock()
	p.signalLocked()

	snapshot := p.snapshotMetricsLocked()
	p.connsMu.Unlock()

	p.setMetricsValues(snapshot)
}

// invalidate removes and closes a connection.
func (p *Pool[T]) invalidate(conn *pooledConn[T]) {
	var transport thrift.TTransport

	if conn == nil {
		return
	}

	p.connsMu.Lock()

	for i, candidate := range p.conns {
		if candidate == conn {
			p.conns = append(p.conns[:i], p.conns[i+1:]...)
			break
		}
	}

	conn.mu.Lock()
	conn.invalid = true
	conn.inUse = false
	transport = conn.transport
	conn.transport = nil
	conn.client = *new(T)
	conn.mu.Unlock()

	p.signalLocked()
	p.connsMu.Unlock()

	p.closeTransport(transport)

	p.computeMetrics()
}

// healthChecker runs periodic health checks.
func (p *Pool[T]) healthChecker() {
	defer p.healthWg.Done()

	ticker := time.NewTicker(p.config.healthCheck)
	defer ticker.Stop()

	for {
		select {
		case <-p.healthStopCh:
			return

		case <-ticker.C:
			if p.closed.Load() {
				return
			}

			p.runHealthChecks()
		}
	}
}

func (p *Pool[T]) runHealthChecks() {
	if p.closed.Load() {
		return
	}

	now := time.Now()
	var toClose []thrift.TTransport

	p.connsMu.Lock()

	if p.closed.Load() {
		p.connsMu.Unlock()
		return
	}

	validConns := make([]*pooledConn[T], 0, len(p.conns))

	for _, conn := range p.conns {
		conn.mu.Lock()

		inUse := conn.inUse
		invalid := conn.invalid
		transport := conn.transport
		idleTime := now.Sub(conn.lastUsedAt)
		lifetime := now.Sub(conn.createdAt)

		conn.mu.Unlock()

		if inUse {
			validConns = append(validConns, conn)
			continue
		}

		remove := false

		switch {
		case invalid:
			remove = true

		case lifetime > p.config.maxLifetime:
			p.logger.Debug(
				"closing connection: max lifetime exceeded",
				"lifetime", lifetime,
			)
			remove = true

		case idleTime > p.config.maxIdleTime:
			p.logger.Debug(
				"closing connection: max idle time exceeded",
				"idle_time", idleTime,
			)
			remove = true

		case transport == nil:
			remove = true

		case !transport.IsOpen():
			p.logger.Debug("closing connection: transport not open")
			remove = true
		}

		if remove {
			conn.mu.Lock()
			transport = conn.transport
			conn.transport = nil
			conn.invalid = true
			conn.client = *new(T)
			conn.mu.Unlock()

			if transport != nil {
				toClose = append(toClose, transport)
			}

			continue
		}

		validConns = append(validConns, conn)
	}

	p.conns = validConns

	needed := max(p.config.minConnections-(len(p.conns)+p.creating), 0)

	// Reserve all required slots while holding the pool lock.
	p.creating += needed

	p.signalLocked()
	p.connsMu.Unlock()

	for _, transport := range toClose {
		p.closeTransport(transport)
	}

	p.createNeededConnections(needed)

	p.computeMetrics()
}

// Create the reserved connections outside the pool lock.
func (p *Pool[T]) createNeededConnections(needed int) {
	for range needed {
		if p.closed.Load() {
			p.releaseCreation()
			continue
		}

		ctx, cancel := context.WithTimeout(
			context.Background(),
			p.config.connectTimeout,
		)

		conn, err := p.createConnection(ctx)
		cancel()

		if err != nil {
			p.releaseCreation()

			if !p.closed.Load() {
				p.logger.Error(
					"failed to recreate minimum connection",
					"error", err,
				)
			}

			continue
		}

		if _, ok := p.addReservedConn(conn); !ok {
			p.closeTransport(conn.transport)
			continue
		}
	}
}

func (p *Pool[T]) beginOperation() error {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	if p.closed.Load() {
		return ErrPoolClosed
	}

	p.active.Add(1)
	return nil
}

func (p *Pool[T]) endOperation() {
	p.active.Done()
}

func (p *Pool[T]) beginClose() bool {
	p.lifecycleMu.Lock()
	defer p.lifecycleMu.Unlock()

	return p.closed.CompareAndSwap(false, true)
}

func (p *Pool[T]) signalLocked() {
	close(p.notifyCh)
	p.notifyCh = make(chan struct{})
}

func (p *Pool[T]) closeTransport(transport thrift.TTransport) {
	if transport == nil {
		return
	}

	if err := transport.Close(); err != nil {
		p.logger.Warn("failed to close transport", "error", err)
	}
}

func (p *Pool[T]) computeMetrics() {
	p.connsMu.Lock()

	snapshot := p.snapshotMetricsLocked()

	p.connsMu.Unlock()

	p.setMetricsValues(snapshot)
}

// snapshotMetricsLocked counts the connection states; connsMu must be held.
func (p *Pool[T]) snapshotMetricsLocked() poolMetricsSnapshot {
	snapshot := poolMetricsSnapshot{total: float64(len(p.conns))}

	for _, conn := range p.conns {
		conn.mu.Lock()

		if conn.inUse {
			snapshot.inUse++
		}
		if conn.invalid {
			snapshot.invalid++
		}
		if conn.transport != nil && conn.transport.IsOpen() {
			snapshot.open++
		}

		conn.mu.Unlock()
	}

	return snapshot
}

// setMetricsValues publishes the snapshot to the configured gauges.
func (p *Pool[T]) setMetricsValues(snapshot poolMetricsSnapshot) {
	p.metricsMu.Lock()
	defer p.metricsMu.Unlock()

	if p.metrics.Total != nil {
		p.metrics.Total.Set(snapshot.total)
	}
	if p.metrics.InUse != nil {
		p.metrics.InUse.Set(snapshot.inUse)
	}
	if p.metrics.Invalid != nil {
		p.metrics.Invalid.Set(snapshot.invalid)
	}
	if p.metrics.Open != nil {
		p.metrics.Open.Set(snapshot.open)
	}
	if p.metrics.Idle != nil {
		p.metrics.Idle.Set(snapshot.total - snapshot.inUse)
	}
}

// ThriftClientFactory creates a Thrift client factory using the provided
// protocol factory, with independent connect and socket timeouts.
func ThriftClientFactory[T any](
	newClientFunc func(thrift.TTransport, thrift.TProtocolFactory) T,
	protocolFactory thrift.TProtocolFactory,
	host string,
	port int,
	connectTimeout time.Duration,
	socketTimeout time.Duration,
) PoolFactory[T] {
	return func(ctx context.Context) (T, thrift.TTransport, error) {
		var zero T

		if err := ctx.Err(); err != nil {
			return zero, nil, err
		}

		addr := net.JoinHostPort(host, strconv.Itoa(port))
		socket := thrift.NewTSocketConf(
			addr,
			&thrift.TConfiguration{
				ConnectTimeout: connectTimeout,
				SocketTimeout:  socketTimeout,
			},
		)

		transportFactory := thrift.NewTTransportFactory()

		transport, err := transportFactory.GetTransport(socket)
		if err != nil {
			return zero, nil, fmt.Errorf(
				"failed to get transport: %w",
				err,
			)
		}

		if transport == nil {
			return zero, nil, ErrNilTransport
		}

		if err := ctx.Err(); err != nil {
			_ = transport.Close()
			return zero, nil, err
		}

		if err := transport.Open(); err != nil {
			_ = transport.Close()
			return zero, nil, fmt.Errorf("failed to open transport: %w", err)
		}

		// Open may complete after cancellation.
		if err := ctx.Err(); err != nil {
			_ = transport.Close()
			return zero, nil, err
		}

		client := newClientFunc(transport, protocolFactory)

		return client, transport, nil
	}
}
