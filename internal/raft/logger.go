package raft

import (
	"fmt"
	"log"
	"os"
)

type Logger interface {
	Debug(v ...interface{})
	Debugf(format string, v ...interface{})
	Info(v ...interface{})
	Infof(format string, v ...interface{})
	Warning(v ...interface{})
	Warningf(format string, v ...interface{})
	Error(v ...interface{})
	Errorf(format string, v ...interface{})
	Fatal(v ...interface{})
	Fatalf(format string, v ...interface{})
	Panic(v ...interface{})
	Panicf(format string, v ...interface{})
}

type defaultLogger struct {
	*log.Logger
	debug bool
}

var DefaultLogger Logger = &defaultLogger{
	Logger: log.New(os.Stderr, "raft ", log.LstdFlags),
	debug:  false,
}

func (l *defaultLogger) Debug(v ...interface{}) {
	if l.debug {
		l.Output(2, fmt.Sprint(v...))
	}
}
func (l *defaultLogger) Debugf(format string, v ...interface{}) {
	if l.debug {
		l.Output(2, fmt.Sprintf(format, v...))
	}
}
func (l *defaultLogger) Info(v ...interface{})  { l.Output(2, fmt.Sprint(v...)) }
func (l *defaultLogger) Infof(f string, v ...interface{}) { l.Output(2, fmt.Sprintf(f, v...)) }
func (l *defaultLogger) Warning(v ...interface{}) { l.Output(2, "WARNING: "+fmt.Sprint(v...)) }
func (l *defaultLogger) Warningf(f string, v ...interface{}) {
	l.Output(2, "WARNING: "+fmt.Sprintf(f, v...))
}
func (l *defaultLogger) Error(v ...interface{}) { l.Output(2, "ERROR: "+fmt.Sprint(v...)) }
func (l *defaultLogger) Errorf(f string, v ...interface{}) {
	l.Output(2, "ERROR: "+fmt.Sprintf(f, v...))
}
func (l *defaultLogger) Fatal(v ...interface{}) {
	l.Output(2, fmt.Sprint(v...))
	os.Exit(1)
}
func (l *defaultLogger) Fatalf(f string, v ...interface{}) {
	l.Output(2, fmt.Sprintf(f, v...))
	os.Exit(1)
}
func (l *defaultLogger) Panic(v ...interface{}) {
	s := fmt.Sprint(v...)
	l.Output(2, s)
	panic(s)
}
func (l *defaultLogger) Panicf(f string, v ...interface{}) {
	s := fmt.Sprintf(f, v...)
	l.Output(2, s)
	panic(s)
}

type discardLogger struct{}

func (discardLogger) Debug(...interface{})          {}
func (discardLogger) Debugf(string, ...interface{}) {}
func (discardLogger) Info(...interface{})           {}
func (discardLogger) Infof(string, ...interface{})  {}
func (discardLogger) Warning(...interface{})        {}
func (discardLogger) Warningf(string, ...interface{}) {}
func (discardLogger) Error(...interface{})          {}
func (discardLogger) Errorf(string, ...interface{}) {}
func (discardLogger) Fatal(...interface{})          {}
func (discardLogger) Fatalf(string, ...interface{}) {}
func (discardLogger) Panic(v ...interface{})        { panic(fmt.Sprint(v...)) }
func (discardLogger) Panicf(f string, v ...interface{}) { panic(fmt.Sprintf(f, v...)) }

var DiscardLogger Logger = discardLogger{}
