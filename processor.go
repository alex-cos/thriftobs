package thriftobs

import (
	"context"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type MetricsProcessor struct {
	thrift.TProcessor
	metrics *Metrics
}

func NewMetricsProcessor(t thrift.TProcessor, metrics *Metrics) *MetricsProcessor {
	return &MetricsProcessor{
		TProcessor: t,
		metrics:    metrics,
	}
}

func (p *MetricsProcessor) Process(
	ctx context.Context,
	in thrift.TProtocol,
	out thrift.TProtocol,
) (bool, thrift.TException) {
	var (
		method                                                     string
		bytesInBefore, bytesOutBefore, bytesInAfter, bytesOutAfter uint64
	)
	inTransport := ConvertTransport(in.Transport())
	outTransport := ConvertTransport(out.Transport())

	if inTransport != nil && outTransport != nil {
		bytesInBefore = inTransport.GetBytesRead()
		bytesOutBefore = outTransport.GetBytesWritten()
	}

	start := time.Now()

	ok, err := p.TProcessor.Process(ctx, in, out)

	duration := time.Since(start).Seconds()
	if inTransport != nil && outTransport != nil {
		bytesInAfter = inTransport.GetBytesRead()
		bytesOutAfter = outTransport.GetBytesWritten()
	}
	mProtocol, matched := in.(*ServerProtocol)
	if matched {
		method = mProtocol.GetMethod()
	}

	if method != "" && p.metrics != nil {
		p.ObserveRequests(method)
		p.ObserveDuration(method, duration)
		p.ObserveErrors(method, err)
		p.ObserveBytesReceived(method, bytesInBefore, bytesInAfter)
		p.ObserveBytesSent(method, bytesOutBefore, bytesOutAfter)
	}

	return ok, err
}

func (p *MetricsProcessor) ObserveRequests(method string) {
	if p.metrics.Requests != nil {
		SafeObserve(func() {
			p.metrics.Requests.WithLabelValues(method).Inc()
		})
	}
}

func (p *MetricsProcessor) ObserveDuration(method string, duration float64) {
	if p.metrics.Duration != nil {
		SafeObserve(func() {
			p.metrics.Duration.WithLabelValues(method).Observe(duration)
		})
	}
}

func (p *MetricsProcessor) ObserveErrors(method string, err error) {
	if err != nil && p.metrics.Errors != nil {
		SafeObserve(func() {
			p.metrics.Errors.WithLabelValues(method).Inc()
		})
	}
}

func (p *MetricsProcessor) ObserveBytesReceived(method string, bytesInBefore, bytesInAfter uint64) {
	if p.metrics.BytesReceived != nil {
		SafeObserve(func() {
			p.metrics.BytesReceived.WithLabelValues(method).Add(
				float64(bytesInAfter - bytesInBefore),
			)
		})
	}
}

func (p *MetricsProcessor) ObserveBytesSent(method string, bytesOutBefore, bytesOutAfter uint64) {
	if p.metrics.BytesSent != nil {
		SafeObserve(func() {
			p.metrics.BytesSent.WithLabelValues(method).Add(
				float64(bytesOutAfter - bytesOutBefore),
			)
		})
	}
}
