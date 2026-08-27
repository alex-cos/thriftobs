package thriftobs_test

import (
	"context"
	"testing"

	"github.com/alex-cos/thriftobs"
	"github.com/apache/thrift/lib/go/thrift"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type srvMockProtocol struct {
	thrift.TProtocol
	readMethod string
	readType   thrift.TMessageType
	readSeqID  int32
	readErr    error
}

func (m *srvMockProtocol) ReadMessageBegin(ctx context.Context) (string, thrift.TMessageType, int32, error) {
	return m.readMethod, m.readType, m.readSeqID, m.readErr
}

func (m *srvMockProtocol) ReadMessageEnd(ctx context.Context) error {
	return nil
}

func (m *srvMockProtocol) WriteMessageBegin(
	ctx context.Context,
	name string,
	messageType thrift.TMessageType,
	seqID int32,
) error {
	return nil
}

func (m *srvMockProtocol) WriteMessageEnd(ctx context.Context) error {
	return nil
}

func (m *srvMockProtocol) Flush(ctx context.Context) error {
	return nil
}

func (m *srvMockProtocol) Transport() thrift.TTransport {
	return nil
}

// ----------------------------------------------------------------------------

type srvMockProtocolFactory struct {
	thrift.TProtocolFactory
	createdProtocol thrift.TProtocol
}

func (f *srvMockProtocolFactory) GetProtocol(trans thrift.TTransport) thrift.TProtocol {
	return f.createdProtocol
}

// ----------------------------------------------------------------------------

type srvMockTransport struct {
	thrift.TTransport
	closed bool
}

func (m *srvMockTransport) Close() error {
	m.closed = true
	return nil
}

func (m *srvMockTransport) IsOpen() bool {
	return !m.closed
}

func (m *srvMockTransport) Read(p []byte) (int, error) {
	return 0, nil
}

func (m *srvMockTransport) Write(p []byte) (int, error) {
	return len(p), nil
}

// ----------------------------------------------------------------------------

func TestNewServerProtocol(t *testing.T) {
	t.Parallel()

	underlying := &srvMockProtocol{}
	sp := thriftobs.NewServerProtocol(underlying)

	assert.NotNil(t, sp)
	assert.Equal(t, underlying, sp.TProtocol)
	assert.Empty(t, sp.GetMethod())
}

func TestServerProtocol_ReadMessageBegin_Success(t *testing.T) {
	t.Parallel()

	underlying := &srvMockProtocol{
		readMethod: "echo",
		readType:   thrift.CALL,
		readSeqID:  1,
	}
	sp := thriftobs.NewServerProtocol(underlying)

	method, msgType, seqID, err := sp.ReadMessageBegin(context.Background())

	require.NoError(t, err)
	assert.Equal(t, "echo", method)
	assert.Equal(t, thrift.CALL, msgType)
	assert.Equal(t, int32(1), seqID)
	assert.Equal(t, "echo", sp.GetMethod())
}

func TestServerProtocol_ReadMessageBegin_Error(t *testing.T) {
	t.Parallel()

	underlying := &srvMockProtocol{
		readErr: assert.AnError,
	}
	sp := thriftobs.NewServerProtocol(underlying)

	method, msgType, seqID, err := sp.ReadMessageBegin(context.Background())

	assert.Error(t, err)
	assert.Empty(t, method)
	assert.Equal(t, thrift.TMessageType(0), msgType)
	assert.Equal(t, int32(0), seqID)
	assert.Empty(t, sp.GetMethod())
}

func TestServerProtocol_GetMethod_ThreadSafe(t *testing.T) {
	t.Parallel()

	underlying := &srvMockProtocol{
		readMethod: "test",
	}
	sp := thriftobs.NewServerProtocol(underlying)

	// Read to set method
	_, _, _, _ = sp.ReadMessageBegin(context.Background())

	// Concurrent reads should be safe
	done := make(chan struct{})
	for range 100 {
		go func() {
			for {
				select {
				case <-done:
					return
				default:
					_ = sp.GetMethod()
				}
			}
		}()
	}

	// Write concurrently
	for range 100 {
		go func() {
			underlying.readMethod = "concurrent"
			_, _, _, _ = sp.ReadMessageBegin(context.Background())
		}()
	}

	close(done)
	// If no race detected, test passes
}

func TestNewServerProtocolFactory(t *testing.T) {
	t.Parallel()

	underlyingFactory := &srvMockProtocolFactory{}
	factory := thriftobs.NewServerProtocolFactory(underlyingFactory)

	assert.NotNil(t, factory)
	// factory is thrift.TProtocolFactory interface, can't access embedded field directly
}

func TestServerProtocolFactory_GetProtocol(t *testing.T) {
	t.Parallel()

	underlyingProto := &srvMockProtocol{}
	factory := thriftobs.NewServerProtocolFactory(&srvMockProtocolFactory{createdProtocol: underlyingProto})
	transport := &srvMockTransport{}

	proto := factory.GetProtocol(transport)

	assert.IsType(t, &thriftobs.ServerProtocol{}, proto)
	assert.Equal(t, underlyingProto, proto.(*thriftobs.ServerProtocol).TProtocol)
}
