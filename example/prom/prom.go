package prom

import (
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const timeout = 10 * time.Second

func engine(reg *prometheus.Registry) *gin.Engine {
	metrics := gin.New()
	metrics.Use(
		gin.Recovery(),
	)
	metrics.GET("/metrics", gin.WrapH(
		promhttp.HandlerFor(reg, promhttp.HandlerOpts{Registry: reg})),
	)

	return metrics
}

func StartPromServer(host string, port int, reg *prometheus.Registry) error {
	addr := fmt.Sprintf("%s:%d", host, port)
	srv := &http.Server{
		Addr:              addr,
		Handler:           engine(reg),
		TLSConfig:         nil,
		ReadHeaderTimeout: timeout,
	}

	err := srv.ListenAndServe()
	if err != nil && !errors.Is(err, http.ErrServerClosed) {
		return err
	}

	return nil
}
