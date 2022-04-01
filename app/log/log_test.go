package log

import (
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"testing"
)

func TestNewLog(t *testing.T) {
	log := NewLog("./newlog.log", zapcore.InfoLevel)
	log.Info("hello world")
	log.Debug("hello world")
	log.Error("error world")
}

func TestNewStatus(t *testing.T) {
	log := NewStatus("./newstatus.log")
	log.Info("hello world",
		zap.Int("count", 100),
		zap.Int("cost", 200))
}
