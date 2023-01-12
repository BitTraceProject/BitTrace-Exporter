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
		"CONTAINER_NAME": "",
	}
)

func main() {
	// TODO 老是失败，加上 sudo
	// TODO docker 日志没权限读，加上 sudo 根目录变了
	err := env.LookupEnvPairs(&envPairs)
	if err != nil {
		panic(err)
	}
	var (
		loggerName = envPairs["CONTAINER_NAME"]
		//loggerName = "exporter-1"
		conf = config.ExporterConfig{
			ReceiverServerAddr: "http://localhost:8080",
			Tag:                loggerName,
			StartDay:           "2022-12-15",
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
