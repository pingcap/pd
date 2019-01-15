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
	"bytes"
	"fmt"
	"io"
	"os"
	"path"
	"runtime"
	"runtime/debug"
	"strings"
	"sync"
	"time"

	"github.com/coreos/etcd/raft"
	"github.com/coreos/pkg/capnslog"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
	"go.uber.org/zap"
	"go.uber.org/zap/zapcore"
	"google.golang.org/grpc/grpclog"
	lumberjack "gopkg.in/natefinch/lumberjack.v2"
)

const (
	defaultLogTimeFormat = "2006/01/02 15:04:05.000"
	defaultLogMaxSize    = 300 // MB
	defaultLogFormat     = "text"
	defaultLogLevel      = log.InfoLevel
)

// FileLogConfig serializes file log related config in toml/json.
type FileLogConfig struct {
	// Log filename, leave empty to disable file log.
	Filename string `toml:"filename" json:"filename"`
	// Is log rotate enabled. TODO.
	LogRotate bool `toml:"log-rotate" json:"log-rotate"`
	// Max size for a single file, in MB.
	MaxSize int `toml:"max-size" json:"max-size"`
	// Max log keep days, default is never deleting.
	MaxDays int `toml:"max-days" json:"max-days"`
	// Maximum number of old log files to retain.
	MaxBackups int `toml:"max-backups" json:"max-backups"`
}

// LogConfig serializes log related config in toml/json.
type LogConfig struct {
	// Log level.
	Level string `toml:"level" json:"level"`
	// Log format. one of json, text, or console.
	Format string `toml:"format" json:"format"`
	// Disable automatic timestamps in output.
	DisableTimestamp bool `toml:"disable-timestamp" json:"disable-timestamp"`
	// File log config.
	File FileLogConfig `toml:"file" json:"file"`
	// Development
	Development       bool
	DisableCaller     bool
	DisableStacktrace bool
	// SamplingConfig sets a sampling strategy for the logger. Sampling caps the
	// global CPU and I/O load that logging puts on your process while attempting
	// to preserve a representative subset of your logs.
	//
	// Values configured here are per-second. See zapcore.NewSampler for details.
	Sampling *zap.SamplingConfig `toml:"sampling" json:"sampling"`
}

// redirectFormatter will redirect etcd logs to logrus logs.
type redirectFormatter struct{}

// Format implements capnslog.Formatter hook.
func (rf *redirectFormatter) Format(pkg string, level capnslog.LogLevel, depth int, entries ...interface{}) {
	if pkg != "" {
		pkg = fmt.Sprint(pkg, ": ")
	}

	logStr := fmt.Sprint(pkg, entries)

	switch level {
	case capnslog.CRITICAL:
		log.Fatalf(logStr)
	case capnslog.ERROR:
		log.Errorf(logStr)
	case capnslog.WARNING:
		log.Warningf(logStr)
	case capnslog.NOTICE:
		log.Infof(logStr)
	case capnslog.INFO:
		log.Infof(logStr)
	case capnslog.DEBUG, capnslog.TRACE:
		log.Debugf(logStr)
	}
}

// Flush only for implementing Formatter.
func (rf *redirectFormatter) Flush() {}

// isSKippedPackageName tests wether path name is on log library calling stack.
func isSkippedPackageName(name string) bool {
	return strings.Contains(name, "github.com/sirupsen/logrus") ||
		strings.Contains(name, "github.com/coreos/pkg/capnslog")
}

// modifyHook injects file name and line pos into log entry.
type contextHook struct{}

// Fire implements logrus.Hook interface
// https://github.com/sirupsen/logrus/issues/63
func (hook *contextHook) Fire(entry *log.Entry) error {
	pc := make([]uintptr, 4)
	cnt := runtime.Callers(6, pc)

	for i := 0; i < cnt; i++ {
		fu := runtime.FuncForPC(pc[i] - 1)
		name := fu.Name()
		if !isSkippedPackageName(name) {
			file, line := fu.FileLine(pc[i] - 1)
			entry.Data["file"] = path.Base(file)
			entry.Data["line"] = line
			break
		}
	}
	return nil
}

// Levels implements logrus.Hook interface.
func (hook *contextHook) Levels() []log.Level {
	return log.AllLevels
}

// StringToLogLevel translates log level string to log level.
func StringToLogLevel(level string) log.Level {
	switch strings.ToLower(level) {
	case "fatal":
		return log.FatalLevel
	case "error":
		return log.ErrorLevel
	case "warn", "warning":
		return log.WarnLevel
	case "debug":
		return log.DebugLevel
	case "info":
		return log.InfoLevel
	}
	return defaultLogLevel
}

// textFormatter is for compatibility with ngaut/log
type textFormatter struct {
	DisableTimestamp bool
}

// Format implements logrus.Formatter
func (f *textFormatter) Format(entry *log.Entry) ([]byte, error) {
	var b *bytes.Buffer
	if entry.Buffer != nil {
		b = entry.Buffer
	} else {
		b = &bytes.Buffer{}
	}
	if !f.DisableTimestamp {
		fmt.Fprintf(b, "%s ", entry.Time.Format(defaultLogTimeFormat))
	}
	if file, ok := entry.Data["file"]; ok {
		fmt.Fprintf(b, "%s:%v:", file, entry.Data["line"])
	}
	fmt.Fprintf(b, " [%s] %s", entry.Level.String(), entry.Message)
	for k, v := range entry.Data {
		if k != "file" && k != "line" {
			fmt.Fprintf(b, " %v=%v", k, v)
		}
	}
	b.WriteByte('\n')
	return b.Bytes(), nil
}

// StringToLogFormatter uses the different log formatter according to a given format name.
func StringToLogFormatter(format string, disableTimestamp bool) log.Formatter {
	switch strings.ToLower(format) {
	case "text":
		return &textFormatter{
			DisableTimestamp: disableTimestamp,
		}
	case "json":
		return &log.JSONFormatter{
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	case "console":
		return &log.TextFormatter{
			FullTimestamp:    true,
			TimestampFormat:  defaultLogTimeFormat,
			DisableTimestamp: disableTimestamp,
		}
	default:
		return &textFormatter{}
	}
}

// InitFileLog initializes file based logging options.
func InitFileLog(cfg *FileLogConfig) (*lumberjack.Logger, error) {
	if st, err := os.Stat(cfg.Filename); err == nil {
		if st.IsDir() {
			return nil, errors.New("can't use directory as log file name")
		}
	}
	if cfg.MaxSize == 0 {
		cfg.MaxSize = defaultLogMaxSize
	}

	// use lumberjack to logrotate
	return &lumberjack.Logger{
		Filename:   cfg.Filename,
		MaxSize:    cfg.MaxSize,
		MaxBackups: cfg.MaxBackups,
		MaxAge:     cfg.MaxDays,
		LocalTime:  true,
	}, nil
}

type wrapLogrus struct {
	*log.Logger
}

// V provides the functionality that returns whether a particular log level is at
// least l - this is needed to meet the LoggerV2 interface.  GRPC's logging levels
// are: https://github.com/grpc/grpc-go/blob/master/grpclog/loggerv2.go#L71
// 0=info, 1=warning, 2=error, 3=fatal
// logrus's are: https://github.com/sirupsen/logrus/blob/master/logrus.go
// 0=panic, 1=fatal, 2=error, 3=warn, 4=info, 5=debug
func (lg *wrapLogrus) V(l int) bool {
	// translate to logrus level
	logrusLevel := 4 - l
	return int(lg.Logger.Level) <= logrusLevel
}

var once sync.Once

// InitLogger initializes PD's logger.
func InitLogger(cfg *LogConfig) error {
	var (
		err    error
		output io.Writer
	)

	once.Do(func() {
		log.SetLevel(StringToLogLevel(cfg.Level))
		log.AddHook(&contextHook{})

		if cfg.Format == "" {
			cfg.Format = defaultLogFormat
		}
		log.SetFormatter(StringToLogFormatter(cfg.Format, cfg.DisableTimestamp))

		// etcd log
		capnslog.SetFormatter(&redirectFormatter{})
		// grpc log
		lg := &wrapLogrus{log.StandardLogger()}
		grpclog.SetLoggerV2(lg)
		// raft log
		raft.SetLogger(lg)

		if len(cfg.File.Filename) == 0 {
			return
		}

		output, err = InitFileLog(&cfg.File)
		if err == nil {
			log.SetOutput(output)
		}

	})
	return err
}

// InitZapLogger initializes the zap logger.
func InitZapLogger(cfg *LogConfig) (*zap.Logger, error) {
	var output zapcore.WriteSyncer
	if len(cfg.File.Filename) > 0 {
		lg, err := InitFileLog(&cfg.File)
		if err != nil {
			return nil, err
		}
		output = zapcore.AddSync(lg)
	} else {
		stdOut, close, err := zap.Open([]string{"stdout"}...)
		if err != nil {
			close()
			return nil, err
		}
		output = stdOut
	}
	level := zap.NewAtomicLevel()
	err := level.UnmarshalText([]byte(cfg.Level))
	if err != nil {
		return nil, err
	}
	core := zapcore.NewCore(newZapTextEncoder(cfg), output, level)
	lg := zap.New(core, cfg.buildOptions(output)...)

	return lg, nil
}

func newZapTextEncoder(cfg *LogConfig) zapcore.Encoder {
	cc := zapcore.EncoderConfig{
		// Keys can be anything except the empty string.
		TimeKey:        "time",
		LevelKey:       "level",
		NameKey:        "name",
		CallerKey:      "caller",
		MessageKey:     "message",
		StacktraceKey:  "stack",
		LineEnding:     zapcore.DefaultLineEnding,
		EncodeLevel:    zapcore.CapitalLevelEncoder,
		EncodeTime:     DefaultTimeEncoder,
		EncodeDuration: zapcore.StringDurationEncoder,
		EncodeCaller:   ShortCallerEncoder,
	}
	if cfg.DisableTimestamp {
		cc.TimeKey = ""
	}
	return NewTextEncoder(cc)
}

func (cfg *LogConfig) buildOptions(errSink zapcore.WriteSyncer) []zap.Option {
	opts := []zap.Option{zap.ErrorOutput(errSink)}

	if cfg.Development {
		opts = append(opts, zap.Development())
	}

	if !cfg.DisableCaller {
		opts = append(opts, zap.AddCaller())
	}

	stackLevel := zap.ErrorLevel
	if cfg.Development {
		stackLevel = zap.WarnLevel
	}
	if !cfg.DisableStacktrace {
		opts = append(opts, zap.AddStacktrace(stackLevel))
	}

	if cfg.Sampling != nil {
		opts = append(opts, zap.WrapCore(func(core zapcore.Core) zapcore.Core {
			return zapcore.NewSampler(core, time.Second, int(cfg.Sampling.Initial), int(cfg.Sampling.Thereafter))
		}))
	}
	return opts
}

// LogPanic logs the panic reason and stack, then exit the process.
// Commonly used with a `defer`.
func LogPanic() {
	if e := recover(); e != nil {
		log.Fatalf("panic: %v, stack: %s", e, string(debug.Stack()))
	}
}
