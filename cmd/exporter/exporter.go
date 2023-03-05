package main

import (
	"flag"
	"log"
	"time"

	"github.com/BitTraceProject/BitTrace-Exporter/server"
	"github.com/BitTraceProject/BitTrace-Types/pkg/common"
	"github.com/BitTraceProject/BitTrace-Types/pkg/config"
)

var (
	envKey      = "CONTAINER_NAME"
	runTimeFlag = flag.Int64("t", 0, "the time to run exporter.(minutes)")
)

func main() {
	flag.Parse()
	if *runTimeFlag == 0 {
		panic("error:run time flag is zero or not set")
	}
	loggerName, err := common.LookupEnv(envKey)
	if err != nil {
		panic(err)
	}
	var (
		conf = config.ExporterConfig{
			ReceiverServerAddr: "http://receiver.receiver.bittrace.proj:8080",
			ExporterTag:        loggerName,
			PollInterval:       100, // 100ms
			FileKeepingDays:    7,   // 7days
		}
		stopCh = make(chan struct{})
	)
	conf.Complete()
	s := server.NewExporterServer(conf, stopCh)
	go func() {
		time.Sleep(time.Duration(*runTimeFlag) * time.Minute)
		stopCh <- struct{}{}
	}()
	log.Printf("running exporter, stop after %d minute", *runTimeFlag)
	s.Run()
}
