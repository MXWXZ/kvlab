package utils

import (
	"runtime"

	"github.com/samuel/go-zookeeper/zk"

	log "github.com/sirupsen/logrus"
)

func init() {
	zk.DefaultLogger = log.StandardLogger()
}

func Fatal(args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	log.WithFields(log.Fields{
		"file": file,
		"line": line,
	}).Fatal(args...)
}

func Warn(args ...interface{}) {
	_, file, line, _ := runtime.Caller(1)
	log.WithFields(log.Fields{
		"file": file,
		"line": line,
	}).Warn(args...)
}

func Info(args ...interface{}) {
	log.Info(args...)
}
