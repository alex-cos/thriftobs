package thriftobs

import (
	"crypto/tls"
	"errors"
	"fmt"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

func CreateClientTransport(
	host string,
	port int,
	connectTimeout time.Duration,
	socketTimeout time.Duration,
	tls *tls.Config,
) (thrift.TTransport, error) {
	var (
		transport thrift.TTransport
		err       error
	)

	addr := fmt.Sprintf("%s:%d", host, port)
	cfg := &thrift.TConfiguration{
		ConnectTimeout: connectTimeout,
		SocketTimeout:  socketTimeout,
	}
	if tls != nil {
		cfg.TLSConfig = tls
	}

	transport = thrift.NewTSocketConf(addr, cfg)

	transportFactory := NewMetricsTransportFactory()

	transport, err = transportFactory.GetTransport(transport)
	if err != nil {
		return nil, err
	}
	if transport == nil {
		return nil, errors.New("error from GetTransport, got nil transport")
	}
	err = transport.Open()
	if err != nil {
		return nil, err
	}

	return transport, nil
}
