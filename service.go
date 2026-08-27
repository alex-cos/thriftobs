package thriftobs

import (
	"crypto/tls"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/apache/thrift/lib/go/thrift"
)

type Service struct {
	protocol        ProtocolType
	processor       thrift.TProcessor
	metrics         *Metrics
	connectTimeout  time.Duration
	socketTimeout   time.Duration
	protocolFactory thrift.TProtocolFactory
	cfg             *tls.Config
	certFile        string
	keyFile         string
	server          *thrift.TSimpleServer
	mu              sync.RWMutex
}

func NewService(
	processor thrift.TProcessor,
	metrics *Metrics,
	opts ...Option,
) *Service {
	service := &Service{
		protocol:        Binary,
		processor:       processor,
		metrics:         metrics,
		connectTimeout:  0,
		socketTimeout:   0,
		protocolFactory: nil,
		cfg:             nil,
		certFile:        "",
		keyFile:         "",
		server:          nil,
		mu:              sync.RWMutex{},
	}

	for _, opt := range opts {
		opt(service)
	}

	return service
}

func (s *Service) GetProtocol() ProtocolType {
	return s.protocol
}

func (s *Service) GetSocketTimeout() time.Duration {
	return s.socketTimeout
}

func (s *Service) GetConnectTimeout() time.Duration {
	return s.connectTimeout
}

func (s *Service) GetProtocolFactory() thrift.TProtocolFactory {
	return s.protocolFactory
}

func (s *Service) GetCfg() *tls.Config {
	return s.cfg
}

func (s *Service) GetCertFile() string {
	return s.certFile
}

func (s *Service) GetKeyFile() string {
	return s.keyFile
}

func (s *Service) Start(host string, port int) error {
	s.mu.Lock()

	addr := fmt.Sprintf("%s:%d", host, port)
	err := s.runServer(addr)
	if err != nil {
		s.server = nil
		s.mu.Unlock()
		return err
	}
	s.mu.Unlock()

	err = s.server.Serve()

	s.mu.Lock()
	s.server = nil
	s.mu.Unlock()

	return err
}

func (s *Service) Stop() error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.server != nil {
		err := s.server.Stop()
		if err != nil {
			return err
		}
		err = s.server.ServerTransport().Close()
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) IsRunning() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return (s.server != nil)
}

// ----------------------------------------------------------------------------

func (s *Service) runServer(addr string) error {
	var (
		protocolFactory thrift.TProtocolFactory
		transport       thrift.TServerTransport
		err             error
	)

	cfg := s.cfg

	if cfg == nil && s.certFile != "" && s.keyFile != "" {
		cfg = new(tls.Config)
		if cert, err := tls.LoadX509KeyPair(s.certFile, s.keyFile); err == nil {
			cfg.Certificates = append(cfg.Certificates, cert)
		} else {
			return err
		}
	}
	if cfg != nil {
		transport, err = thrift.NewTSSLServerSocket(addr, cfg)
	} else {
		transport, err = thrift.NewTServerSocket(addr)
	}
	if err != nil {
		return err
	}

	if transport == nil {
		return errors.New("error when building transport, got nil transport")
	}

	processor := NewMetricsProcessor(s.processor, s.metrics)

	transportFactory := NewMetricsTransportFactory()

	if s.protocolFactory != nil {
		protocolFactory = s.protocolFactory
	} else {
		protocolFactory, err = BuildProtocolFactory(s.protocol,
			&thrift.TConfiguration{
				ConnectTimeout: s.connectTimeout,
				SocketTimeout:  s.socketTimeout,
			},
		)
		if err != nil {
			return err
		}
	}

	protocolFactory = NewServerProtocolFactory(protocolFactory)

	s.server = thrift.NewTSimpleServer4(
		processor,
		transport,
		transportFactory,
		protocolFactory,
	)

	return nil
}
