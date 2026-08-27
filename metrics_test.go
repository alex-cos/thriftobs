package thriftobs_test

import (
	"testing"

	"github.com/alex-cos/thriftobs"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGetMetrics(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	assert.NotNil(t, metrics)
	assert.NotNil(t, metrics.Requests)
	assert.NotNil(t, metrics.Errors)
	assert.NotNil(t, metrics.Duration)
	assert.NotNil(t, metrics.BytesReceived)
	assert.NotNil(t, metrics.BytesSent)
}

func TestGetMetrics_MetricNames(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	// Test that metrics can be registered and gathered
	reg := prometheus.NewRegistry()
	reg.MustRegister(metrics.Requests)

	// Use the metric first
	metrics.Requests.WithLabelValues("test").Inc()

	metricFamilies, err := reg.Gather()
	require.NoError(t, err)

	// Just verify we got some metric families back
	assert.NotEmpty(t, metricFamilies)
}

func TestNewThriftCollectors(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	collectors := thriftobs.NewThriftCollectors(metrics)

	assert.Len(t, collectors, 5)
	assert.Contains(t, collectors, metrics.Requests)
	assert.Contains(t, collectors, metrics.Errors)
	assert.Contains(t, collectors, metrics.Duration)
	assert.Contains(t, collectors, metrics.BytesReceived)
	assert.Contains(t, collectors, metrics.BytesSent)
}

func TestMetrics_Requests_LabelValues(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	counter := metrics.Requests.WithLabelValues("echo")
	assert.NotNil(t, counter)

	counter.Inc()
}

func TestMetrics_Duration_Observe(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	histogram := metrics.Duration.WithLabelValues("echo")
	assert.NotNil(t, histogram)

	histogram.Observe(1.5)
}

func TestMetrics_Bytes_Observe(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	received := metrics.BytesReceived.WithLabelValues("echo")
	sent := metrics.BytesSent.WithLabelValues("echo")

	assert.NotNil(t, received)
	assert.NotNil(t, sent)

	received.Add(100)
	sent.Add(200)
}

func TestMetrics_Errors(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()

	counter := metrics.Errors.WithLabelValues("echo")
	assert.NotNil(t, counter)

	counter.Inc()
}

func TestMultipleCalls_GetMetrics_ReturnsNewInstance(t *testing.T) {
	t.Parallel()

	m1 := thriftobs.GetMetrics()
	m2 := thriftobs.GetMetrics()

	assert.NotSame(t, m1, m2)
	assert.NotSame(t, m1.Requests, m2.Requests)
}
