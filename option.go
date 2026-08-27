package thriftobs

import (
	"crypto/tls"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type Option func(*Service)

func WithProtocol(protocol ProtocolType) Option {
	return func(c *Service) {
		c.protocol = protocol
	}
}

func WithConnectTimeout(connectTimeout time.Duration) Option {
	return func(c *Service) {
		c.connectTimeout = connectTimeout
	}
}

func WithSocketTimeout(socketTimeout time.Duration) Option {
	return func(c *Service) {
		c.socketTimeout = socketTimeout
	}
}

func WithSSL(certFile, keyFile string) Option {
	return func(c *Service) {
		c.certFile = certFile
		c.keyFile = keyFile
	}
}

func WithTLSConfig(cfg *tls.Config) Option {
	return func(c *Service) {
		if cfg != nil {
			c.cfg = cfg
		}
	}
}

func WithProtocolFactory(protocolFactory thrift.TProtocolFactory) Option {
	return func(c *Service) {
		if protocolFactory != nil {
			c.protocolFactory = protocolFactory
		}
	}
}
