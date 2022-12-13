package tests

import (
	"github.com/BitTraceProject/BitTrace-Exporter/server"
	"testing"
	"time"

	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
)

func TestExporterServer(t *testing.T) {
	var (
		conf = &config.ExporterConfig{
			ReceiverServerAddr: "http://localhost:8080",
			Tag:                "exporter-1",
			StartDay:           "2022-12-04",
			StartSeq:           15,
			PollInterval:       3000,
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
