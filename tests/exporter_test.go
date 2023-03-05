package tests

import (
	"testing"
	"time"

	"github.com/BitTraceProject/BitTrace-Exporter/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
)

func TestExporterServer(t *testing.T) {
	var (
		conf = config.ExporterConfig{
			ReceiverServerAddr: "http://localhost:8080",
			ExporterTag:        "exporter-1",
			PollInterval:       3000,
			FileKeepingDays:    7,
		}
		stopCh = make(chan struct{})
	)
	conf.Complete()
	s := server.NewExporterServer(conf, stopCh)
	go func() {
		time.Sleep(60 * time.Minute)
		stopCh <- struct{}{}
	}()
	s.Run()
}
