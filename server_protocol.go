package thriftobs

import (
	"context"
	"sync"

	"github.com/apache/thrift/lib/go/thrift"
)

type ServerProtocol struct {
	thrift.TProtocol

	method string
	mu     sync.RWMutex
}

func NewServerProtocol(t thrift.TProtocol) *ServerProtocol {
	return &ServerProtocol{
		TProtocol: t,
		method:    "",
		mu:        sync.RWMutex{},
	}
}

func (p *ServerProtocol) ReadMessageBegin(
	ctx context.Context,
) (string, thrift.TMessageType, int32, error) {
	method, messageType, seqID, err :=
		p.TProtocol.ReadMessageBegin(ctx)

	p.mu.Lock()
	if err == nil {
		p.method = method
	} else {
		p.method = ""
	}
	p.mu.Unlock()

	return method, messageType, seqID, err
}

func (p *ServerProtocol) GetMethod() string {
	p.mu.RLock()
	defer p.mu.RUnlock()

	return p.method
}

// ----------------------------------------------------------------------------

type ServerProtocolFactory struct {
	thrift.TProtocolFactory
}

func NewServerProtocolFactory(f thrift.TProtocolFactory) *ServerProtocolFactory {
	return &ServerProtocolFactory{
		TProtocolFactory: f,
	}
}

func (f *ServerProtocolFactory) GetProtocol(trans thrift.TTransport) thrift.TProtocol {
	return NewServerProtocol(f.TProtocolFactory.GetProtocol(trans))
}
