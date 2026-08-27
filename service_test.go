package thriftobs_test

import (
	"context"
	"crypto/tls"
	"testing"
	"time"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

type svcMockProcessor struct {
	thrift.TProcessor
	processCalled bool
}

func (m *svcMockProcessor) Process(ctx context.Context, in, out thrift.TProtocol) (bool, thrift.TException) {
	m.processCalled = true
	return true, nil
}

func TestNewService(t *testing.T) {
	t.Parallel()

	processor := &svcMockProcessor{}
	metrics := thriftobs.GetMetrics()

	svc := thriftobs.NewService(processor, metrics)

	assert.NotNil(t, svc)
	assert.Equal(t, thriftobs.Binary, svc.GetProtocol())
	assert.Equal(t, time.Duration(0), svc.GetConnectTimeout())
	assert.Equal(t, time.Duration(0), svc.GetSocketTimeout())
	assert.Nil(t, svc.GetProtocolFactory())
	assert.Nil(t, svc.GetCfg())
	assert.Empty(t, svc.GetCertFile())
	assert.Empty(t, svc.GetKeyFile())
}

func TestNewService_WithOptions(t *testing.T) {
	t.Parallel()

	processor := &svcMockProcessor{}
	metrics := thriftobs.GetMetrics()
	customFactory := &thrift.TBinaryProtocolFactory{}
	tlsConfig := &tls.Config{}

	svc := thriftobs.NewService(processor, metrics,
		thriftobs.WithProtocol(thriftobs.Compact),
		thriftobs.WithConnectTimeout(10*time.Second),
		thriftobs.WithSocketTimeout(20*time.Second),
		thriftobs.WithProtocolFactory(customFactory),
		thriftobs.WithTLSConfig(tlsConfig),
		thriftobs.WithSSL("cert.pem", "key.pem"),
	)

	assert.Equal(t, thriftobs.Compact, svc.GetProtocol())
	assert.Equal(t, 10*time.Second, svc.GetConnectTimeout())
	assert.Equal(t, 20*time.Second, svc.GetSocketTimeout())
	assert.Equal(t, customFactory, svc.GetProtocolFactory())
	assert.Equal(t, tlsConfig, svc.GetCfg())
	assert.Equal(t, "cert.pem", svc.GetCertFile())
	assert.Equal(t, "key.pem", svc.GetKeyFile())
}

func TestService_IsRunning(t *testing.T) {
	t.Parallel()

	processor := &svcMockProcessor{}
	metrics := thriftobs.GetMetrics()
	svc := thriftobs.NewService(processor, metrics)

	assert.False(t, svc.IsRunning())
}

func TestService_Stop_NotRunning(t *testing.T) {
	t.Parallel()

	processor := &svcMockProcessor{}
	metrics := thriftobs.GetMetrics()
	svc := thriftobs.NewService(processor, metrics)

	err := svc.Stop()

	assert.NoError(t, err)
}

func TestWithProtocol(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	opt := thriftobs.WithProtocol(thriftobs.Compact)
	opt(svc)
	assert.Equal(t, thriftobs.Compact, svc.GetProtocol())
}

func TestWithConnectTimeout(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	opt := thriftobs.WithConnectTimeout(15 * time.Second)
	opt(svc)
	assert.Equal(t, 15*time.Second, svc.GetConnectTimeout())
}

func TestWithSocketTimeout(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	opt := thriftobs.WithSocketTimeout(25 * time.Second)
	opt(svc)
	assert.Equal(t, 25*time.Second, svc.GetSocketTimeout())
}

func TestWithSSL(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	opt := thriftobs.WithSSL("cert.pem", "key.pem")
	opt(svc)
	assert.Equal(t, "cert.pem", svc.GetCertFile())
	assert.Equal(t, "key.pem", svc.GetKeyFile())
}

func TestWithTLSConfig(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	cfg := &tls.Config{}
	opt := thriftobs.WithTLSConfig(cfg)
	opt(svc)
	assert.Equal(t, cfg, svc.GetCfg())
}

func TestWithTLSConfig_Nil(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	opt := thriftobs.WithTLSConfig(nil)
	opt(svc)
	assert.Nil(t, svc.GetCfg())
}

func TestWithProtocolFactory(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	factory := &thrift.TBinaryProtocolFactory{}
	opt := thriftobs.WithProtocolFactory(factory)
	opt(svc)
	assert.Equal(t, factory, svc.GetProtocolFactory())
}

func TestWithProtocolFactory_Nil(t *testing.T) {
	t.Parallel()

	svc := &thriftobs.Service{}
	opt := thriftobs.WithProtocolFactory(nil)
	opt(svc)
	assert.Nil(t, svc.GetProtocolFactory())
}
