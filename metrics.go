package thriftobs

import "github.com/prometheus/client_golang/prometheus"

const method = "method"

type Metrics struct {
	Requests      *prometheus.CounterVec
	Errors        *prometheus.CounterVec
	Duration      *prometheus.HistogramVec
	BytesReceived *prometheus.CounterVec
	BytesSent     *prometheus.CounterVec
}

func GetMetrics() *Metrics {
	return &Metrics{
		Requests: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thrift_requests_total",
				Help: "Total number of Thrift requests.",
			},
			[]string{method},
		),
		Errors: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thrift_errors_total",
				Help: "Total number of Thrift errors.",
			},
			[]string{method},
		),
		Duration: prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name: "thrift_request_duration_seconds",
				Help: "Thrift request duration.",
			},
			[]string{method},
		),
		BytesReceived: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thrift_bytes_received_total",
				Help: "Total number of total received bytes.",
			},
			[]string{method},
		),
		BytesSent: prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "thrift_bytes_sent_total",
				Help: "Total number of total sent bytes.",
			},
			[]string{method},
		),
	}
}

func GetPoolMetrics() *PoolMetrics {
	return &PoolMetrics{
		Total: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thrift_pool_connections",
				Help: "Total number of Thrift Pool connections.",
			},
		),
		InUse: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thrift_pool_in_use_connections",
				Help: "Number of used Thrift Pool connections.",
			},
		),
		Idle: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thrift_pool_idle_connections",
				Help: "Number of idle Thrift Pool connections.",
			},
		),
		Invalid: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thrift_pool_invalid_connections",
				Help: "Number of invalid Thrift Pool connections.",
			},
		),
		Open: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Name: "thrift_pool_open_connections",
				Help: "Number of opened Thrift Pool connections.",
			},
		),
	}
}

func NewThriftCollectors(m *Metrics) []prometheus.Collector {
	return []prometheus.Collector{
		m.Requests,
		m.Errors,
		m.Duration,
		m.BytesReceived,
		m.BytesSent,
	}
}

func NewThriftPoolCollectors(m *PoolMetrics) []prometheus.Collector {
	return []prometheus.Collector{
		m.Total,
		m.InUse,
		m.Idle,
		m.Invalid,
		m.Open,
	}
}
