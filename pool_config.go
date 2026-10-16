package thriftobs

import "time"

// PoolConfig holds configuration for the connection pool.
type PoolConfig struct {
	minConnections int
	maxConnections int
	maxRetries     int
	maxIdleTime    time.Duration
	maxLifetime    time.Duration
	healthCheck    time.Duration
	connectTimeout time.Duration
}

// DefaultPoolConfig returns a PoolConfig with sensible defaults.
func DefaultPoolConfig() PoolConfig {
	return PoolConfig{
		minConnections: 2,
		maxConnections: 10,
		maxRetries:     1,
		maxIdleTime:    5 * time.Minute,
		maxLifetime:    1 * time.Hour,
		healthCheck:    1 * time.Minute,
		connectTimeout: 10 * time.Second,
	}
}

func normalizePoolConfig(config PoolConfig) PoolConfig {
	defaults := DefaultPoolConfig()

	if config.minConnections < 0 {
		config.minConnections = 0
	}
	if config.maxConnections <= 0 {
		config.maxConnections = defaults.maxConnections
	}
	if config.minConnections > config.maxConnections {
		config.minConnections = config.maxConnections
	}
	if config.maxRetries < 0 {
		config.maxRetries = defaults.maxRetries
	}
	if config.maxIdleTime <= 0 {
		config.maxIdleTime = defaults.maxIdleTime
	}
	if config.maxLifetime <= 0 {
		config.maxLifetime = defaults.maxLifetime
	}
	if config.healthCheck <= 0 {
		config.healthCheck = defaults.healthCheck
	}
	if config.connectTimeout <= 0 {
		config.connectTimeout = defaults.connectTimeout
	}

	return config
}

type PoolOption func(*PoolConfig)

// WithPoolMinConnections sets the minimum number of connections maintained
// in the pool (default 2).
func WithPoolMinConnections(nb int) PoolOption {
	return func(c *PoolConfig) {
		c.minConnections = nb
	}
}

// WithPoolMaxConnections sets the maximum number of connections the pool can
// create (default 10).
func WithPoolMaxConnections(nb int) PoolOption {
	return func(c *PoolConfig) {
		c.maxConnections = nb
	}
}

// WithPoolMaxRetries sets the number of retries performed by Do after the
// first attempt; 0 disables retries (default 1). A negative value restores
// the default.
func WithPoolMaxRetries(nb int) PoolOption {
	return func(c *PoolConfig) {
		c.maxRetries = nb
	}
}

// WithPoolMaxIdleTime sets how long a connection may stay idle before it is
// closed (default 5m).
func WithPoolMaxIdleTime(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		c.maxIdleTime = d
	}
}

// WithPoolMaxLifetime sets the maximum age of a connection before it is
// recycled (default 1h).
func WithPoolMaxLifetime(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		c.maxLifetime = d
	}
}

// WithPoolHealthCheck sets the interval between health checks (default 1m).
func WithPoolHealthCheck(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		c.healthCheck = d
	}
}

// WithPoolConnectTimeout sets the timeout used when creating connections
// (default 10s).
func WithPoolConnectTimeout(d time.Duration) PoolOption {
	return func(c *PoolConfig) {
		c.connectTimeout = d
	}
}
