# thriftobs

[![Go Version](https://img.shields.io/badge/go-1.25-blue)](https://go.dev)
[![Test Status](https://github.com/alex-cos/thriftobs/actions/workflows/test.yml/badge.svg)](https://github.com/alex-cos/thriftobs/actions/workflows/test.yml)
[![Codecov](https://codecov.io/gh/alex-cos/thriftobs/branch/main/graph/badge.svg)](https://codecov.io/gh/alex-cos/thriftobs)
[![Lint Status](https://github.com/alex-cos/thriftobs/actions/workflows/lint.yml/badge.svg)](https://github.com/alex-cos/thriftobs/actions/workflows/lint.yml)
[![License](https://img.shields.io/badge/License-MIT-green)](LICENSE)
[![Thrift](https://img.shields.io/badge/thrift-v0.23.0-orange)](https://thrift.apache.org)

Prometheus observability wrapper for [Apache Thrift](https://thrift.apache.org/) in Go. Automatically instruments Thrift servers with per-method metrics (requests, errors, latency, bytes) via decorator implementations of `TProcessor`, `TProtocol`, and `TTransport`.

## Features

- **Zero-code instrumentation** — wrap your existing `TProcessor` without changing business logic.
- **5 Prometheus metrics** out of the box, labeled by `method`.
- **Thread-safe** — atomic counters and `sync.RWMutex` for method capture and server lifecycle.
- **Panic-safe** — `safeObserve` recovers from Prometheus panics and logs to `stderr`.
- **Functional options** — configure protocol, timeouts, TLS and custom factories via `Option`.
- **TLS / SSL support** — `WithTLSConfig` and `WithSSL` for `TSSLServerSocket`, client-side `tls.Config`.
- **Multiple protocols** — `binary` (default), `compact`, `json`, `simplejson`.
- **Client helper** — instrumented transport factory for Thrift clients.
- **Simple server lifecycle** — `Service` wraps `TSimpleServer` with `Start`/`Stop`/`IsRunning`.

## Installation

```bash
go get github.com/alex-cos/thriftobs
```

Requirements:

- Go >= 1.25
- Apache Thrift 0.23.0

### Install Apache Thrift

The Apache Thrift framework combines a software stack with a code generation engine to build cross-language services (C++, Java, Python, Go, etc.).

Install version **0.23.0**:

- Linux: [https://archive.apache.org/dist/thrift/0.23.0/thrift-0.23.0.tar.gz](https://archive.apache.org/dist/thrift/0.23.0/thrift-0.23.0.tar.gz)
- Windows: [https://archive.apache.org/dist/thrift/0.23.0/thrift-0.23.0.exe](https://archive.apache.org/dist/thrift/0.23.0/thrift-0.23.0.exe)

## Quick Start

### Server

The server API uses functional options. `NewService` takes `processor`, `metrics` and a variadic list of `Option`:

```go
package main

import (
  "time"

  "github.com/alex-cos/thriftobs"
  "github.com/alex-cos/thriftobs/example/gen/example"
  "github.com/prometheus/client_golang/prometheus"
  "github.com/prometheus/client_golang/prometheus/collectors"
)

func main() {
  // 1. Create metrics
  metrics := thriftobs.GetMetrics()

  // 2. Wrap your Thrift processor
  processor := example.NewExempleProcessor(NewHandler())

  // 3. Create the instrumented service
  svc := thriftobs.NewService(
    processor,
    metrics,
    thriftobs.WithProtocol(thriftobs.Binary),
    thriftobs.WithConnectTimeout(10*time.Second),
    thriftobs.WithSocketTimeout(10*time.Second),
  )

  // 4. Register Prometheus collectors
  reg := prometheus.NewRegistry()
  reg.MustRegister(thriftobs.NewThriftCollectors(metrics)...)

  // 5. Expose /metrics on :8001
  go StartPromServer("", 8001, reg)

  // 6. Serve Thrift on :9000 (blocking)
  if err := svc.Start("", 9000); err != nil {
    panic(err)
  }
}
```

### Client

```go
package main

import (
  "context"
  "crypto/tls"
  "time"

  "github.com/alex-cos/thriftobs"
  "github.com/alex-cos/thriftobs/example/gen/example"
)

func main() {
  metrics := thriftobs.GetMetrics()

  transport, err := thriftobs.CreateClientTransport(
    "localhost",
    9000,
    10*time.Second,
    10*time.Second,
    nil, // no TLS configuration
  )
  if err != nil {
    panic(err)
  }
  defer transport.Close()

  protocolFactory, err := thriftobs.BuildClientProtocolFactory(thriftobs.Binary, nil, metrics)
  if err != nil {
    panic(err)
  }

  reg := prometheus.NewRegistry()
  reg.MustRegister(thriftobs.NewThriftCollectors(metrics)...)

  go StartPromServer("", 8002, reg)

  client := example.NewExempleClientFactory(transport, protocolFactory)
  resp, err := client.Echo(context.Background(), "hello")
}
```

## Metrics Exposed

All metrics are `*prometheus.CounterVec` or `*prometheus.HistogramVec` with a single label `method`.

| Metric | Type | Labels | Help |
| --- | --- | --- | --- |
| `thrift_requests_total` | CounterVec | `method` | Total number of Thrift requests. |
| `thrift_errors_total` | CounterVec | `method` | Total number of Thrift errors. |
| `thrift_request_duration_seconds` | HistogramVec | `method` | Thrift request duration. Uses default Prometheus buckets. |
| `thrift_bytes_received_total` | CounterVec | `method` | Total number of received bytes. |
| `thrift_bytes_sent_total` | CounterVec | `method` | Total number of sent bytes. |

Example PromQL:

```promql
# Request rate per method (5m)
sum by (method) (rate(thrift_requests_total[5m]))

# Error rate
sum by (method) (rate(thrift_errors_total[5m])) / sum by (method) (rate(thrift_requests_total[5m]))

# p95 latency per method
histogram_quantile(0.95, sum by (le, method) (rate(thrift_request_duration_seconds_bucket[5m])))

# Throughput
sum by (method) (rate(thrift_bytes_received_total[5m]))
sum by (method) (rate(thrift_bytes_sent_total[5m]))
```

Register collectors with any registry:

```go
reg.MustRegister(thriftobs.NewThriftCollectors(metrics)...)
```

## Service Options

Configure the server via functional options:

| Option | Signature | Description |
| --- | --- | --- |
| `WithProtocol` | `WithProtocol(protocol string) Option` | Thrift protocol: `binary` (default), `compact`, `json`, `simplejson`. Ignored if `WithProtocolFactory` is set. |
| `WithConnectTimeout` | `WithConnectTimeout(d time.Duration) Option` | Timeout for establishing connections. |
| `WithSocketTimeout` | `WithSocketTimeout(d time.Duration) Option` | Socket read/write timeout. |
| `WithSSL` | `WithSSL(certFile, keyFile string) Option` | Load X509 key pair and serve via `TSSLServerSocket`. |
| `WithTLSConfig` | `WithTLSConfig(cfg *tls.Config) Option` | Provide custom `tls.Config` directly. Takes precedence over `WithSSL`. |
| `WithProtocolFactory` | `WithProtocolFactory(f TProtocolFactory) Option` | Supply a fully custom `TProtocolFactory`, bypassing `BuildProtocolFactory`. |

Example combining options:

```go
svc := thriftobs.NewService(
  processor,
  metrics,
  thriftobs.WithProtocol("compact"),
  thriftobs.WithConnectTimeout(5*time.Second),
  thriftobs.WithSocketTimeout(5*time.Second),
  thriftobs.WithTLSConfig(tlsConfig),
)
```

### TLS / SSL

## Protocol Configuration

`BuildProtocolFactory` selects the Thrift protocol:

| Name | Factory | Notes |
| --- | --- | --- |
| `binary`, `""` | `TBinaryProtocolFactoryConf` | Default. Most efficient. |
| `compact` | `TCompactProtocolFactoryConf` | More compact than binary. |
| `json` | `TJSONProtocolFactory` | Human-readable. |
| `simplejson` | `TSimpleJSONProtocolFactoryConf` | Simplified JSON. |

```go
factory, err := thriftobs.BuildProtocolFactory("json", &thrift.TConfiguration{
  ConnectTimeout: 5 * time.Second,
  SocketTimeout:  5 * time.Second,
})
```

## Example

The repository includes a complete example:

```thrift
service Exemple {
  string echo(1:string text)
}
```

Build and run:

```bash
# Generate bindings + build binaries
make build

# Terminal 1: start server (Thrift :9000, Prometheus :8001)
./example/bin/server

# Terminal 2: check server metrics
curl http://localhost:8001/metrics | grep thrift_

# Terminal 3: start client (interactive) (Prometheus :8002)
./example/bin/client
hello

# Terminal 4: check client metrics
curl http://localhost:8002/metrics | grep thrift_
```

## Development

```bash
# Vendor dependencies
go mod vendor

# Build example binaries
make build

# Run tests
make test

# Lint
make lint
```
