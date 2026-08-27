package thriftobs

import (
	"sync/atomic"

	"github.com/apache/thrift/lib/go/thrift"
)

type MetricsTransport struct {
	thrift.TTransport

	bytesRead    atomic.Uint64
	bytesWritten atomic.Uint64
}

func NewMetricsTransport(t thrift.TTransport) *MetricsTransport {
	return &MetricsTransport{
		TTransport:   t,
		bytesRead:    atomic.Uint64{},
		bytesWritten: atomic.Uint64{},
	}
}

func (t *MetricsTransport) Read(p []byte) (int, error) {
	n, err := t.TTransport.Read(p)
	t.bytesRead.Add(uint64(n)) //nolint: gosec

	return n, err
}

func (t *MetricsTransport) Write(p []byte) (int, error) {
	n, err := t.TTransport.Write(p)
	t.bytesWritten.Add(uint64(n)) //nolint: gosec

	return n, err
}

func (t *MetricsTransport) GetBytesRead() uint64 {
	return t.bytesRead.Load()
}

func (t *MetricsTransport) GetBytesWritten() uint64 {
	return t.bytesWritten.Load()
}

// ----------------------------------------------------------------------------

type MetricsTransportFactory struct {
}

func NewMetricsTransportFactory() thrift.TTransportFactory {
	return &MetricsTransportFactory{}
}

func (f *MetricsTransportFactory) GetTransport(
	t thrift.TTransport,
) (thrift.TTransport, error) {
	return NewMetricsTransport(t), nil
}
