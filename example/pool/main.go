package main

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/alex-cos/thriftobs"
	"github.com/alex-cos/thriftobs/example/gen/example"
	"github.com/alex-cos/thriftobs/example/prom"
	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

const timeout = 10 * time.Second

func main() {
	mode := os.Getenv("mode")
	if mode == "DEV" {
		gin.SetMode(gin.DebugMode)
	} else {
		gin.SetMode(gin.ReleaseMode)
	}

	metrics := thriftobs.GetMetrics()
	poolMetrics := thriftobs.GetPoolMetrics()

	protocolFactory, err := thriftobs.BuildClientProtocolFactory(thriftobs.Binary, nil, metrics)
	if err != nil {
		panic(err)
	}

	pool, err := thriftobs.NewPool(
		nil, // *slog.Logger, nil uses slog.Default()
		thriftobs.ThriftClientFactory(
			example.NewExempleClientFactory,
			protocolFactory,
			"localhost",
			9000,
			timeout, // connect timeout
			timeout, // socket timeout
		),
		thriftobs.WithPoolMinConnections(2),
		thriftobs.WithPoolMaxConnections(10),
		thriftobs.WithPoolMaxRetries(1),
		thriftobs.WithPoolMaxIdleTime(5*time.Minute),
		thriftobs.WithPoolMaxLifetime(time.Hour),
		thriftobs.WithPoolHealthCheck(time.Minute),
		thriftobs.WithPoolConnectTimeout(10*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer pool.Close()
	pool.SetMetrics(poolMetrics)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	reg.MustRegister(
		thriftobs.NewThriftCollectors(metrics)...,
	)
	reg.MustRegister(
		thriftobs.NewThriftPoolCollectors(poolMetrics)...,
	)
	prometheus.DefaultRegisterer = reg

	go func() {
		err := prom.StartPromServer("", 8003, reg)
		if err != nil {
			panic(err)
		}
	}()

	process(pool)
}

func process(pool *thriftobs.Pool[*example.ExempleClient]) {
	var input string

	for {
		n, err := fmt.Scanln(&input)
		if err != nil {
			if strings.Contains(err.Error(), "unexpected newline") {
				continue
			}
			panic(err)
		}
		if n == 0 {
			continue
		}
		if input == "exit" || input == "quit" {
			return
		}
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		err = pool.Do(ctx, func(client *example.ExempleClient) error {
			output, err := client.Echo(ctx, input)
			if err != nil {
				return err
			}
			fmt.Fprintln(os.Stdout, output)

			return nil
		})
		cancel()
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
	}
}
