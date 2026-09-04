package thriftobs

import (
	"context"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type ClientProtocol struct {
	thrift.TProtocol

	calls   *ClientCallMap
	metrics *Metrics
}

func NewClientProtocol(
	protocol thrift.TProtocol,
	calls *ClientCallMap,
	metrics *Metrics,
) *ClientProtocol {
	return &ClientProtocol{
		TProtocol: protocol,
		calls:     calls,
		metrics:   metrics,
	}
}

func (p *ClientProtocol) WriteMessageBegin(
	ctx context.Context,
	name string,
	messageType thrift.TMessageType,
	seqID int32,
) error {
	var bytesBefore, bytesAfter uint64

	transport := ConvertTransport(p.Transport())
	if transport != nil {
		bytesBefore = transport.GetBytesWritten()
	}

	err := p.TProtocol.WriteMessageBegin(
		ctx,
		name,
		messageType,
		seqID,
	)
	if err != nil {
		return err
	}

	if transport != nil {
		bytesAfter = transport.GetBytesWritten()
	}

	if messageType == thrift.CALL || messageType == thrift.ONEWAY {
		p.calls.Add(ClientCall{
			SeqID:     seqID,
			Method:    name,
			Start:     time.Now(),
			BytesSent: bytesAfter - bytesBefore,
		})
	}

	return nil
}

func (p *ClientProtocol) ReadMessageBegin(
	ctx context.Context,
) (string, thrift.TMessageType, int32, error) {
	var bytesBefore, bytesAfter uint64

	transport := ConvertTransport(p.Transport())
	if transport != nil {
		bytesBefore = transport.GetBytesRead()
	}

	name, messageType, seqID, err :=
		p.TProtocol.ReadMessageBegin(ctx)

	if err != nil {
		return name, messageType, seqID, err
	}

	if transport != nil {
		bytesAfter = transport.GetBytesRead()
	}

	if messageType == thrift.REPLY ||
		messageType == thrift.EXCEPTION {
		p.finishCall(seqID, messageType, bytesAfter-bytesBefore)
	}

	return name, messageType, seqID, nil
}

func (p *ClientProtocol) finishCall(
	seqID int32,
	messageType thrift.TMessageType,
	bytesRead uint64,
) {
	call, ok := p.calls.Remove(seqID)
	if !ok {
		return
	}

	duration := time.Since(call.Start)

	if p.metrics != nil {
		method := call.Method
		p.ObserveRequests(method)
		p.ObserveDuration(method, duration.Seconds())
		p.ObserveErrors(method, messageType)
		p.ObserveBytesSent(method, call.BytesSent)
		p.ObserveBytesReceived(method, bytesRead)
	}
}

func (p *ClientProtocol) ObserveRequests(method string) {
	if p.metrics.Requests != nil {
		SafeObserve(func() {
			p.metrics.Requests.WithLabelValues(method).Inc()
		})
	}
}

func (p *ClientProtocol) ObserveDuration(method string, duration float64) {
	if p.metrics.Duration != nil {
		SafeObserve(func() {
			p.metrics.Duration.WithLabelValues(method).Observe(duration)
		})
	}
}

func (p *ClientProtocol) ObserveErrors(method string, messageType thrift.TMessageType) {
	if messageType == thrift.EXCEPTION {
		SafeObserve(func() {
			p.metrics.Errors.WithLabelValues(method).Inc()
		})
	}
}

func (p *ClientProtocol) ObserveBytesReceived(method string, bytes uint64) {
	if p.metrics.BytesReceived != nil {
		SafeObserve(func() {
			p.metrics.BytesReceived.WithLabelValues(method).Add(
				float64(bytes),
			)
		})
	}
}

func (p *ClientProtocol) ObserveBytesSent(method string, bytes uint64) {
	if p.metrics.BytesSent != nil {
		SafeObserve(func() {
			p.metrics.BytesSent.WithLabelValues(method).Add(
				float64(bytes),
			)
		})
	}
}

// ----------------------------------------------------------------------------

type ClientProtocolFactory struct {
	thrift.TProtocolFactory

	calls   *ClientCallMap
	metrics *Metrics
}

func NewClientProtocolFactory(f thrift.TProtocolFactory, metrics *Metrics) thrift.TProtocolFactory {
	return &ClientProtocolFactory{
		TProtocolFactory: f,
		metrics:          metrics,
		calls:            NewClientCallMap(15*time.Minute, 20*time.Minute),
	}
}

func (f *ClientProtocolFactory) GetProtocol(trans thrift.TTransport) thrift.TProtocol {
	return NewClientProtocol(f.TProtocolFactory.GetProtocol(trans), f.calls, f.metrics)
}
