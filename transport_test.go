package thriftobs_test

import (
	"testing"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type transMockTransport struct {
	thrift.TTransport
	readData  []byte
	writeData []byte
	readErr   error
	writeErr  error
	closed    bool
}

func (m *transMockTransport) Read(p []byte) (int, error) {
	if len(m.readData) == 0 {
		return 0, m.readErr
	}
	n := copy(p, m.readData)
	m.readData = m.readData[n:]
	return n, m.readErr
}

func (m *transMockTransport) Write(p []byte) (int, error) {
	if m.writeErr != nil {
		return 0, m.writeErr
	}
	m.writeData = append(m.writeData, p...)
	return len(p), nil
}

func (m *transMockTransport) Close() error {
	m.closed = true
	return nil
}

func (m *transMockTransport) IsOpen() bool {
	return !m.closed
}

func TestNewMetricsTransport(t *testing.T) {
	t.Parallel()

	underlying := &transMockTransport{}
	mt := thriftobs.NewMetricsTransport(underlying)

	assert.NotNil(t, mt)
	assert.Equal(t, underlying, mt.TTransport)
	assert.Equal(t, uint64(0), mt.GetBytesRead())
	assert.Equal(t, uint64(0), mt.GetBytesWritten())
}

func TestMetricsTransport_Read(t *testing.T) {
	t.Parallel()

	underlying := &transMockTransport{readData: []byte("hello")}
	mt := thriftobs.NewMetricsTransport(underlying)

	buf := make([]byte, 5)
	n, err := mt.Read(buf)

	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, "hello", string(buf))
	assert.Equal(t, uint64(5), mt.GetBytesRead())
	assert.Equal(t, uint64(0), mt.GetBytesWritten())
}

func TestMetricsTransport_Write(t *testing.T) {
	t.Parallel()

	underlying := &transMockTransport{}
	mt := thriftobs.NewMetricsTransport(underlying)

	n, err := mt.Write([]byte("world"))

	require.NoError(t, err)
	assert.Equal(t, 5, n)
	assert.Equal(t, uint64(0), mt.GetBytesRead())
	assert.Equal(t, uint64(5), mt.GetBytesWritten())
	assert.Equal(t, []byte("world"), underlying.writeData)
}

func TestMetricsTransport_ReadError(t *testing.T) {
	t.Parallel()

	underlying := &transMockTransport{readErr: assert.AnError}
	mt := thriftobs.NewMetricsTransport(underlying)

	buf := make([]byte, 5)
	n, err := mt.Read(buf)

	assert.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, uint64(0), mt.GetBytesRead())
}

func TestMetricsTransport_WriteError(t *testing.T) {
	t.Parallel()

	underlying := &transMockTransport{writeErr: assert.AnError}
	mt := thriftobs.NewMetricsTransport(underlying)

	n, err := mt.Write([]byte("test"))

	assert.Error(t, err)
	assert.Equal(t, 0, n)
	assert.Equal(t, uint64(0), mt.GetBytesWritten())
}

func TestMetricsTransportFactory(t *testing.T) {
	t.Parallel()

	factory := thriftobs.NewMetricsTransportFactory()
	underlying := &transMockTransport{}

	mt, err := factory.GetTransport(underlying)

	require.NoError(t, err)
	assert.NotNil(t, mt)
	assert.IsType(t, &thriftobs.MetricsTransport{}, mt)
	assert.Equal(t, underlying, mt.(*thriftobs.MetricsTransport).TTransport)
}
