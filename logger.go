// Copyright 2015 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package xorm

import (
	"fmt"
	"io"
	"log"

	phoenixormcore "github.com/yongjacky/phoenix-go-orm-core"
)

// default log options
const (
	DEFAULT_LOG_PREFIX = "[xorm]"
	DEFAULT_LOG_FLAG   = log.Ldate | log.Lmicroseconds
	DEFAULT_LOG_LEVEL  = phoenixormcore.LOG_DEBUG
)

var _ phoenixormcore.ILogger = DiscardLogger{}

// DiscardLogger don't log implementation for phoenixormcore.ILogger
type DiscardLogger struct{}

// Debug empty implementation
func (DiscardLogger) Debug(v ...interface{}) {}

// Debugf empty implementation
func (DiscardLogger) Debugf(format string, v ...interface{}) {}

// Error empty implementation
func (DiscardLogger) Error(v ...interface{}) {}

// Errorf empty implementation
func (DiscardLogger) Errorf(format string, v ...interface{}) {}

// Info empty implementation
func (DiscardLogger) Info(v ...interface{}) {}

// Infof empty implementation
func (DiscardLogger) Infof(format string, v ...interface{}) {}

// Warn empty implementation
func (DiscardLogger) Warn(v ...interface{}) {}

// Warnf empty implementation
func (DiscardLogger) Warnf(format string, v ...interface{}) {}

// Level empty implementation
func (DiscardLogger) Level() phoenixormcore.LogLevel {
	return phoenixormcore.LOG_UNKNOWN
}

// SetLevel empty implementation
func (DiscardLogger) SetLevel(l phoenixormcore.LogLevel) {}

// ShowSQL empty implementation
func (DiscardLogger) ShowSQL(show ...bool) {}

// IsShowSQL empty implementation
func (DiscardLogger) IsShowSQL() bool {
	return false
}

// SimpleLogger is the default implment of phoenixormcore.ILogger
type SimpleLogger struct {
	DEBUG   *log.Logger
	ERR     *log.Logger
	INFO    *log.Logger
	WARN    *log.Logger
	level   phoenixormcore.LogLevel
	showSQL bool
}

var _ phoenixormcore.ILogger = &SimpleLogger{}

// NewSimpleLogger use a special io.Writer as logger output
func NewSimpleLogger(out io.Writer) *SimpleLogger {
	return NewSimpleLogger2(out, DEFAULT_LOG_PREFIX, DEFAULT_LOG_FLAG)
}

// NewSimpleLogger2 let you customrize your logger prefix and flag
func NewSimpleLogger2(out io.Writer, prefix string, flag int) *SimpleLogger {
	return NewSimpleLogger3(out, prefix, flag, DEFAULT_LOG_LEVEL)
}

// NewSimpleLogger3 let you customrize your logger prefix and flag and logLevel
func NewSimpleLogger3(out io.Writer, prefix string, flag int, l phoenixormcore.LogLevel) *SimpleLogger {
	return &SimpleLogger{
		DEBUG: log.New(out, fmt.Sprintf("%s [debug] ", prefix), flag),
		ERR:   log.New(out, fmt.Sprintf("%s [error] ", prefix), flag),
		INFO:  log.New(out, fmt.Sprintf("%s [info]  ", prefix), flag),
		WARN:  log.New(out, fmt.Sprintf("%s [warn]  ", prefix), flag),
		level: l,
	}
}

// Error implement phoenixormcore.ILogger
func (s *SimpleLogger) Error(v ...interface{}) {
	if s.level <= phoenixormcore.LOG_ERR {
		s.ERR.Output(2, fmt.Sprint(v...))
	}
	return
}

// Errorf implement phoenixormcore.ILogger
func (s *SimpleLogger) Errorf(format string, v ...interface{}) {
	if s.level <= phoenixormcore.LOG_ERR {
		s.ERR.Output(2, fmt.Sprintf(format, v...))
	}
	return
}

// Debug implement phoenixormcore.ILogger
func (s *SimpleLogger) Debug(v ...interface{}) {
	if s.level <= phoenixormcore.LOG_DEBUG {
		s.DEBUG.Output(2, fmt.Sprint(v...))
	}
	return
}

// Debugf implement phoenixormcore.ILogger
func (s *SimpleLogger) Debugf(format string, v ...interface{}) {
	if s.level <= phoenixormcore.LOG_DEBUG {
		s.DEBUG.Output(2, fmt.Sprintf(format, v...))
	}
	return
}

// Info implement phoenixormcore.ILogger
func (s *SimpleLogger) Info(v ...interface{}) {
	if s.level <= phoenixormcore.LOG_INFO {
		s.INFO.Output(2, fmt.Sprint(v...))
	}
	return
}

// Infof implement phoenixormcore.ILogger
func (s *SimpleLogger) Infof(format string, v ...interface{}) {
	if s.level <= phoenixormcore.LOG_INFO {
		s.INFO.Output(2, fmt.Sprintf(format, v...))
	}
	return
}

// Warn implement phoenixormcore.ILogger
func (s *SimpleLogger) Warn(v ...interface{}) {
	if s.level <= phoenixormcore.LOG_WARNING {
		s.WARN.Output(2, fmt.Sprint(v...))
	}
	return
}

// Warnf implement phoenixormcore.ILogger
func (s *SimpleLogger) Warnf(format string, v ...interface{}) {
	if s.level <= phoenixormcore.LOG_WARNING {
		s.WARN.Output(2, fmt.Sprintf(format, v...))
	}
	return
}

// Level implement phoenixormcore.ILogger
func (s *SimpleLogger) Level() phoenixormcore.LogLevel {
	return s.level
}

// SetLevel implement phoenixormcore.ILogger
func (s *SimpleLogger) SetLevel(l phoenixormcore.LogLevel) {
	s.level = l
	return
}

// ShowSQL implement phoenixormcore.ILogger
func (s *SimpleLogger) ShowSQL(show ...bool) {
	if len(show) == 0 {
		s.showSQL = true
		return
	}
	s.showSQL = show[0]
}

// IsShowSQL implement phoenixormcore.ILogger
func (s *SimpleLogger) IsShowSQL() bool {
	return s.showSQL
}
