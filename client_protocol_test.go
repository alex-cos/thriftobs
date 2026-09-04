package thriftobs_test

import (
	"context"
	"testing"
	"time"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const expiration = 10 * time.Second
const interval = 10 * time.Second

type cliMockProtocol struct {
	thrift.TProtocol
	transport   *thriftobs.MetricsTransport
	readMethod  string
	readType    thrift.TMessageType
	readSeqID   int32
	readErr     error
	writeErr    error
	writeMethod string
	writeType   thrift.TMessageType
	writeSeqID  int32
}

func (m *cliMockProtocol) Transport() thrift.TTransport {
	return m.transport
}

func (m *cliMockProtocol) ReadMessageBegin(ctx context.Context) (string, thrift.TMessageType, int32, error) {
	return m.readMethod, m.readType, m.readSeqID, m.readErr
}

func (m *cliMockProtocol) ReadMessageEnd(ctx context.Context) error {
	return nil
}

func (m *cliMockProtocol) WriteMessageBegin(
	ctx context.Context,
	name string,
	messageType thrift.TMessageType,
	seqID int32,
) error {
	m.writeMethod = name
	m.writeType = messageType
	m.writeSeqID = seqID
	return m.writeErr
}

func (m *cliMockProtocol) WriteMessageEnd(ctx context.Context) error {
	return nil
}

func (m *cliMockProtocol) Flush(ctx context.Context) error {
	return nil
}

// ----------------------------------------------------------------------------

type cliMockProtocolFactory struct {
	thrift.TProtocolFactory
	createdProtocol *cliMockProtocol
}

func (f *cliMockProtocolFactory) GetProtocol(trans thrift.TTransport) thrift.TProtocol {
	if f.createdProtocol != nil {
		f.createdProtocol.transport = trans.(*thriftobs.MetricsTransport)
	}
	return f.createdProtocol
}

// ----------------------------------------------------------------------------

type cliMockTransport struct {
	thrift.TTransport
	closed bool
}

func (m *cliMockTransport) Close() error {
	m.closed = true
	return nil
}

func (m *cliMockTransport) IsOpen() bool {
	return !m.closed
}

func (m *cliMockTransport) Read(p []byte) (int, error) {
	return 0, nil
}

func (m *cliMockTransport) Write(p []byte) (int, error) {
	return len(p), nil
}

// ----------------------------------------------------------------------------

func TestNewClientProtocol(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	underlying := &cliMockProtocol{}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	assert.NotNil(t, cp)
	assert.Equal(t, underlying, cp.TProtocol)
}

func TestClientProtocol_WriteMessageBegin_Call(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{transport: transport}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	err := cp.WriteMessageBegin(context.Background(), "echo", thrift.CALL, 1)

	require.NoError(t, err)
	assert.Equal(t, "echo", underlying.writeMethod)
	assert.Equal(t, thrift.CALL, underlying.writeType)
	assert.Equal(t, int32(1), underlying.writeSeqID)

	// Check call was recorded
	call, ok := calls.Remove(1)
	require.True(t, ok)
	assert.Equal(t, "echo", call.Method)
	assert.Equal(t, int32(1), call.SeqID)
	assert.True(t, call.Start.After(time.Now().Add(-time.Second)))
}

func TestClientProtocol_WriteMessageBegin_OneWay(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{transport: transport}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	err := cp.WriteMessageBegin(context.Background(), "oneway", thrift.ONEWAY, 2)

	require.NoError(t, err)
	call, ok := calls.Remove(2)
	require.True(t, ok)
	assert.Equal(t, "oneway", call.Method)
}

func TestClientProtocol_WriteMessageBegin_Reply_NotRecorded(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{transport: transport}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	err := cp.WriteMessageBegin(context.Background(), "echo", thrift.REPLY, 3)

	require.NoError(t, err)
	_, ok := calls.Remove(3)
	assert.False(t, ok)
}

func TestClientProtocol_WriteMessageBegin_Exception_NotRecorded(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{transport: transport}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	err := cp.WriteMessageBegin(context.Background(), "echo", thrift.EXCEPTION, 4)

	require.NoError(t, err)
	_, ok := calls.Remove(4)
	assert.False(t, ok)
}

func TestClientProtocol_ReadMessageBegin_Reply(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{
		transport:  transport,
		readMethod: "echo",
		readType:   thrift.REPLY,
		readSeqID:  1,
	}

	// Pre-add call
	calls.Add(thriftobs.ClientCall{
		SeqID:     1,
		Method:    "echo",
		Start:     time.Now(),
		BytesSent: 10,
	})

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	method, msgType, seqID, err := cp.ReadMessageBegin(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "echo", method)
	assert.Equal(t, thrift.REPLY, msgType)
	assert.Equal(t, int32(1), seqID)
}

func TestClientProtocol_ReadMessageBegin_Exception(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{
		transport:  transport,
		readMethod: "echo",
		readType:   thrift.EXCEPTION,
		readSeqID:  2,
	}

	calls.Add(thriftobs.ClientCall{
		SeqID:     2,
		Method:    "echo",
		Start:     time.Now(),
		BytesSent: 10,
	})

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	method, msgType, seqID, err := cp.ReadMessageBegin(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "echo", method)
	assert.Equal(t, thrift.EXCEPTION, msgType)
	assert.Equal(t, int32(2), seqID)
}

func TestClientProtocol_ReadMessageBegin_Call_NoMatch(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{
		transport:  transport,
		readMethod: "echo",
		readType:   thrift.CALL,
		readSeqID:  3,
	}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	method, _, _, err := cp.ReadMessageBegin(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "echo", method)
}

func TestClientProtocol_ReadMessageBegin_Error(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{
		transport: transport,
		readErr:   assert.AnError,
	}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	_, _, _, err := cp.ReadMessageBegin(context.Background())

	assert.Error(t, err)
}

func TestClientProtocol_ReadMessageBegin_NoMatchingCall(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	calls := thriftobs.NewClientCallMap(expiration, interval)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})
	underlying := &cliMockProtocol{
		transport:  transport,
		readMethod: "echo",
		readType:   thrift.REPLY,
		readSeqID:  999,
	}

	cp := thriftobs.NewClientProtocol(underlying, calls, metrics)

	method, _, _, err := cp.ReadMessageBegin(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "echo", method)
	// No panic, just no metrics recorded
}

func TestNewClientProtocolFactory(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	underlyingFactory := &cliMockProtocolFactory{}

	factory := thriftobs.NewClientProtocolFactory(underlyingFactory, metrics)

	assert.NotNil(t, factory)
	// factory is thrift.TProtocolFactory interface, can't access embedded fields directly
	assert.NotNil(t, factory)
}

func TestClientProtocolFactory_GetProtocol(t *testing.T) {
	t.Parallel()

	metrics := thriftobs.GetMetrics()
	underlyingProto := &cliMockProtocol{}
	factory := thriftobs.NewClientProtocolFactory(&cliMockProtocolFactory{createdProtocol: underlyingProto}, metrics)
	transport := thriftobs.NewMetricsTransport(&cliMockTransport{})

	proto := factory.GetProtocol(transport)

	assert.IsType(t, &thriftobs.ClientProtocol{}, proto)
	assert.Equal(t, underlyingProto, proto.(*thriftobs.ClientProtocol).TProtocol)
	// Can't access unexported fields from outside package
}
