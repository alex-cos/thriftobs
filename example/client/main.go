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
	transport, err := thriftobs.CreateClientTransport(
		"localhost",
		9000,
		timeout,
		timeout,
		nil,
	)
	if err != nil {
		panic(err)
	}
	defer transport.Close()

	protocolFactory, err := thriftobs.BuildClientProtocolFactory(thriftobs.Binary, nil, metrics)
	if err != nil {
		panic(err)
	}
	client := example.NewExempleClientFactory(transport, protocolFactory)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	reg.MustRegister(thriftobs.NewThriftCollectors(metrics)...)
	prometheus.DefaultRegisterer = reg

	go func() {
		err := prom.StartPromServer("", 8002, reg)
		if err != nil {
			panic(err)
		}
	}()

	process(client)
}

func process(client *example.ExempleClient) {
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
		output, err := client.Echo(ctx, input)
		if err != nil {
			fmt.Fprintln(os.Stderr, err.Error())
		}
		cancel()

		fmt.Fprintln(os.Stdout, output)
	}
}
