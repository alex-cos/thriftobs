package thriftobs_test

import (
	"context"
	"testing"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
)

type procMockProtocol struct {
	thrift.TProtocol
	transport *thriftobs.MetricsTransport
	readErr   error
	writeErr  error
	method    string
}

func (m *procMockProtocol) Transport() thrift.TTransport {
	return m.transport
}

func (m *procMockProtocol) ReadMessageBegin(ctx context.Context) (string, thrift.TMessageType, int32, error) {
	if m.readErr != nil {
		return "", 0, 0, m.readErr
	}
	return m.method, thrift.CALL, 1, nil
}

func (m *procMockProtocol) ReadMessageEnd(ctx context.Context) error {
	return nil
}

func (m *procMockProtocol) WriteMessageBegin(
	ctx context.Context,
	name string,
	messageType thrift.TMessageType,
	seqID int32,
) error {
	m.method = name
	return m.writeErr
}

func (m *procMockProtocol) WriteMessageEnd(ctx context.Context) error {
	return nil
}

func (m *procMockProtocol) Flush(ctx context.Context) error {
	return nil
}

// ----------------------------------------------------------------------------

type procMockProcessor struct {
	thrift.TProcessor
	processFunc func(ctx context.Context, in, out thrift.TProtocol) (bool, thrift.TException)
}

func (m *procMockProcessor) Process(ctx context.Context, in, out thrift.TProtocol) (bool, thrift.TException) {
	if m.processFunc != nil {
		return m.processFunc(ctx, in, out)
	}
	return true, nil
}

// ----------------------------------------------------------------------------

type procMockTransport struct {
	thrift.TTransport
	readData  []byte
	writeData []byte
	readErr   error
	writeErr  error
	closed    bool
}

func (m *procMockTransport) Read(p []byte) (int, error) {
	if len(m.readData) == 0 {
		return 0, m.readErr
	}
	n := copy(p, m.readData)
	m.readData = m.readData[n:]
	return n, m.readErr
}

func (m *procMockTransport) Write(p []byte) (int, error) {
	m.writeData = append(m.writeData, p...)
	return len(p), m.writeErr
}

func (m *procMockTransport) Close() error {
	m.closed = true
	return nil
}

func (m *procMockTransport) IsOpen() bool {
	return !m.closed
}

// ----------------------------------------------------------------------------

func TestNewMetricsProcessor(t *testing.T) {
	t.Parallel()

	underlying := &procMockProcessor{}
	metrics := thriftobs.GetMetrics()

	mp := thriftobs.NewMetricsProcessor(underlying, metrics)

	assert.NotNil(t, mp)
	// MetricsProcessor embeds TProcessor, can't directly compare
	// Just verify it was created successfully
}

func TestMetricsProcessor_Process_Success(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	inTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	outTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	inProto := &procMockProtocol{transport: inTransport, method: "echo"}
	outProto := &procMockProtocol{transport: outTransport}

	underlying := &procMockProcessor{}
	mp := thriftobs.NewMetricsProcessor(underlying, metrics)

	ok, err := mp.Process(context.Background(), inProto, outProto)

	assert.True(t, ok)
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), inTransport.GetBytesRead())
	assert.Equal(t, uint64(0), outTransport.GetBytesWritten())
}

func TestMetricsProcessor_Process_RecordsMetrics(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	inTransport := thriftobs.NewMetricsTransport(&procMockTransport{readData: []byte("hello")})
	outTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	inProto := &procMockProtocol{transport: inTransport, method: "echo"}
	outProto := &procMockProtocol{transport: outTransport}

	underlying := &procMockProcessor{}
	mp := thriftobs.NewMetricsProcessor(underlying, metrics)

	ok, err := mp.Process(context.Background(), inProto, outProto)

	assert.True(t, ok)
	assert.NoError(t, err)

	// Check metrics were recorded
	// Note: We can't easily inspect internal prometheus metrics without a gatherer
	// but we can verify the code path executes without panic
}

func TestMetricsProcessor_Process_Error(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	inTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	outTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	inProto := &procMockProtocol{transport: inTransport, method: "echo"}
	outProto := &procMockProtocol{transport: outTransport}

	testErr := thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "test error")
	underlying := &procMockProcessor{
		processFunc: func(ctx context.Context, in, out thrift.TProtocol) (bool, thrift.TException) {
			return false, testErr
		},
	}
	mp := thriftobs.NewMetricsProcessor(underlying, metrics)

	ok, err := mp.Process(context.Background(), inProto, outProto)

	assert.False(t, ok)
	assert.Error(t, err)
}

func TestMetricsProcessor_Process_NoMethod(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	inTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	outTransport := thriftobs.NewMetricsTransport(&procMockTransport{})
	inProto := &procMockProtocol{transport: inTransport, method: ""}
	outProto := &procMockProtocol{transport: outTransport}

	underlying := &procMockProcessor{}
	mp := thriftobs.NewMetricsProcessor(underlying, metrics)

	ok, err := mp.Process(context.Background(), inProto, outProto)

	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestMetricsProcessor_Process_NilTransport(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	inProto := &procMockProtocol{transport: nil, method: "echo"}
	outProto := &procMockProtocol{transport: nil}

	underlying := &procMockProcessor{}
	mp := thriftobs.NewMetricsProcessor(underlying, metrics)

	ok, err := mp.Process(context.Background(), inProto, outProto)

	assert.True(t, ok)
	assert.NoError(t, err)
}

func TestMetricsProcessor_ObserveRequests(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	mp := thriftobs.NewMetricsProcessor(&procMockProcessor{}, metrics)

	mp.ObserveRequests("testMethod")
}

func TestMetricsProcessor_ObserveDuration(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	mp := thriftobs.NewMetricsProcessor(&procMockProcessor{}, metrics)

	mp.ObserveDuration("testMethod", 1.5)
}

func TestMetricsProcessor_ObserveErrors(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	mp := thriftobs.NewMetricsProcessor(&procMockProcessor{}, metrics)

	mp.ObserveErrors("testMethod", thrift.NewTApplicationException(thrift.INTERNAL_ERROR, "test"))
	mp.ObserveErrors("testMethod", nil) // should not record
}

func TestMetricsProcessor_ObserveBytes(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	mp := thriftobs.NewMetricsProcessor(&procMockProcessor{}, metrics)

	mp.ObserveBytesReceived("testMethod", 100, 200)
	mp.ObserveBytesSent("testMethod", 50, 150)
}
