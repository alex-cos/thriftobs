package main

import (
	"os"
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

	processor := example.NewExempleProcessor(NewHandler())

	svc := thriftobs.NewService(
		processor,
		metrics,
		thriftobs.WithConnectTimeout(timeout),
		thriftobs.WithSocketTimeout(timeout),
	)

	reg := prometheus.NewRegistry()
	reg.MustRegister(
		collectors.NewGoCollector(),
		collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
	)
	reg.MustRegister(thriftobs.NewThriftCollectors(metrics)...)
	prometheus.DefaultRegisterer = reg

	go func() {
		err := prom.StartPromServer("", 8001, reg)
		if err != nil {
			panic(err)
		}
	}()

	err := svc.Start("", 9000)
	if err != nil {
		panic(err)
	}
}
