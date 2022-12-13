package main

import (
	"time"

	"github.com/BitTraceProject/BitTrace-Exporter/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
	"github.com/BitTraceProject/BitTrace-Types/pkg/env"
)

// TODO 使用运行参数的方式代替环境变量
// TODO 不需要 NewExporterConfig 的方法？

var (
	envPairs = map[string]string{
		"CONTAINER_NAME":    "",
		"BITTRACE_ROOT_DIR": "",
		"BITTRACE_LOG_DIR":  "",
	}
)

func main() {
	err := env.LookupEnvPairs(&envPairs)
	if err != nil {
		panic(err)
	}
	var (
		loggerName = envPairs["CONTAINER_NAME"]
		conf       = &config.ExporterConfig{
			ReceiverServerAddr: "http://localhost:8080",
			Tag:                loggerName,
			StartDay:           "2022-12-04",
			StartSeq:           15,
			PollInterval:       100,
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
