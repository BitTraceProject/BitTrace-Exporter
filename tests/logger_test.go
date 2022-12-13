package tests

import (
	"github.com/BitTraceProject/BitTrace-Exporter/common"
	"testing"
	"time"
)

func TestGetLogger(t *testing.T) {
	l := common.GetLogger("test_logger")
	l.Info("test info %d", 0)
	l.Warn("test warn %d", 0)
	l.Fatal("test fatal %d", 0)
	l.Error("test error %d", 0)
	time.Sleep(3 * time.Second)
	l.Error("test error %d", 1)
	l.Error("test error %d", 2)
	l.Error("test error %d", 3)
}
