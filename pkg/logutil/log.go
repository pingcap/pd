// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package logutil

import (
	"fmt"
	"os"
	"path"

	log "github.com/Sirupsen/logrus"
	"github.com/coreos/pkg/capnslog"
	"github.com/juju/errors"
	config "github.com/pingcap/pd/pkg/config"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultMaxSize    = 500 // MB
	defaultMaxBackups = 3
	defaultMaxAge     = 28 // days
	defaultLogLevel   = log.InfoLevel

	logDirPerm = 0755
)

type redirectFormatter struct{}

// Format translate etcd capnslog logs to logrus logs.
func (f *redirectFormatter) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	logger := log.WithFields(log.Fields{
		"subsystem": "etcd",
		"package":   pkg,
	})

	logStr := fmt.Sprint(entries)

	switch level {
	case capnslog.CRITICAL:
		logger.Fatalf(logStr)
	case capnslog.ERROR:
		logger.Errorf(logStr)
	case capnslog.WARNING:
		logger.Warningf(logStr)
	case capnslog.NOTICE:
		logger.Infof(logStr)
	case capnslog.INFO:
		logger.Infof(logStr)
	case capnslog.DEBUG:
		logger.Debugf(logStr)
	case capnslog.TRACE:
		logger.Debugf(logStr)
	}
}

// Flush only for implementing Formatter.
func (f *redirectFormatter) Flush() {}

// setLogOutput sets output path for all logs.
func setLogOutput(logFile string) error {
	// PD log
	dir := path.Dir(logFile)
	err := os.MkdirAll(dir, logDirPerm)
	if err != nil {
		return errors.Trace(err)
	}

	log.SetOutput(&lumberjack.Logger{
		Filename:   logFile,
		MaxSize:    defaultMaxSize, // megabytes
		MaxBackups: defaultMaxBackups,
		MaxAge:     defaultMaxAge, // days
	})

	return nil
}

func stringToLogLevel(level string) log.Level {
	switch level {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn":
		return log.WarnLevel
	case "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// InitLogger initializes logging configuration
func InitLogger(cfg *config.ServerConfig) error {
	log.SetLevel(stringToLogLevel(cfg.LogLevel))
	log.SetFormatter(&log.TextFormatter{FullTimestamp: true, TimestampFormat: "2006-01-02 15:04:05"})

	// redirect etcd log
	capnslog.SetFormatter(&redirectFormatter{})

	// Force redirect etcd log to stderr.
	if len(cfg.LogFile) == 0 {
		return nil
	}

	err := setLogOutput(cfg.LogFile)
	if err != nil {
		return errors.Trace(err)
	}

	return nil
}
