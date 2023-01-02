package common

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/BitTraceProject/BitTrace-Types/pkg/constants"
)

type (
	Logger interface {
		Msg(data string)
		Info(format string, a ...interface{})
		Warn(format string, a ...interface{})
		Error(format string, a ...interface{})
		Fatal(format string, a ...interface{})
	}
	// defaultLogger 维护两个 logger，根据 height 来切换
	defaultLogger struct {
		sync.Mutex

		// 当前 logger 的 唯一标识
		loggerName string

		// 当前日志文件的日志数量和限制
		currentN, limitN int64
		// 当前日志文件的 id
		currentFileID int64
		// 当前日志文件的 day
		currentDay string
		// filepath = LogFileBasePath + "/" + loggerName + "/" + currentDay + "/" + currentFileID + ".log"
		currentFilepath string

		log  log.Logger
		logf *os.File
	}
)

var (
	loggers map[string]Logger
	mux     sync.RWMutex
)

func init() {
	loggers = map[string]Logger{}
}

func GetLogger(loggerName string) Logger {
	mux.RLock()
	l, ok := loggers[loggerName]
	mux.RUnlock()
	if !ok {
		mux.Lock()
		if l, ok = loggers[loggerName]; !ok {
			l = newLogger(loggerName)
			loggers[loggerName] = l
		}
		mux.Unlock()
	}
	return l
}

func newLogger(loggerName string) Logger {
	// new logger
	now := time.Now()
	currentDay := CurrentDay(now)
	l := &defaultLogger{
		loggerName:      loggerName,
		currentN:        0,
		limitN:          constants.RECEIVE_DATA_PACKAGE_MAXN,
		currentFileID:   0,
		currentDay:      currentDay,
		currentFilepath: CurrentIDLogFilepath(LogFileBasePath, loggerName, currentDay, 0),
		log:             log.Logger{},
	}
	// init logf
	l.nextDay(now)
	return l
}

func (l *defaultLogger) Msg(msg string) {
	l.println(msg)
}

func (l *defaultLogger) Info(format string, a ...interface{}) {
	l.printf("I", format, a...)
}

func (l *defaultLogger) Warn(format string, a ...interface{}) {
	l.printf("W", format, a...)
}

func (l *defaultLogger) Error(format string, a ...interface{}) {
	l.printf("E", format, a...)
}

func (l *defaultLogger) Fatal(format string, a ...interface{}) {
	l.printf("F", format, a...)
}

func (l *defaultLogger) println(msg string) {
	// 刷新日志文件 logf
	l.Lock()
	defer l.Unlock()

	l.initLogger()
	l.log.Println(msg)
	l.currentN++
}

func (l *defaultLogger) printf(level string, format string, a ...interface{}) {
	// 刷新日志文件 logf
	l.Lock()
	defer l.Unlock()

	l.initLogger()
	l.log.Printf(fmt.Sprintf("[%s][%s]", level, format), a...)
	l.currentN++
}

func (l *defaultLogger) initLogger() {
	// if currentN >= limitN 则需要切换文件，id+1，currentN=0
	// if now != currentDay 则需要切换目录，id=0，currentN=0

	// 1 currentN < limitN and currentDay == now: return
	// 2 currentN < limitN and currentDay != now: eof and switch dir
	// 3 currentN >= limitN and currentDay == now: switch file
	// 4 currentN >= limitN and currentDay != now: switch file, eof and switch dir

	now := time.Now()
	currentDay := CurrentDay(now)
	if l.currentN < l.limitN && l.currentDay == currentDay {
	} else if l.currentN < l.limitN && l.currentDay != currentDay {
		l.logEOF()
		l.nextDay(now)
	} else if l.currentN >= l.limitN && l.currentDay == currentDay {
		l.nextFile()
	} else {
		l.nextFile()
		l.logEOF()
		l.nextDay(now)
	}
}

func (l *defaultLogger) logEOF() {
	l.log.Print(constants.LOG_EOF_DAY)
	if l.logf != nil {
		l.logf.Close()
	}
}

// nextDay id=0，currentN=0
func (l *defaultLogger) nextDay(now time.Time) {
	if l.logf != nil {
		l.logf.Close()
	}
	l.currentDay = CurrentDay(now)
	l.currentFileID = 0
	l.currentN = 0

	parentPath, err := filepath.Abs(CurrentIDLogFileParentPath(LogFileBasePath, l.loggerName, l.currentDay))
	if err != nil {
		panic(fmt.Sprintf("[initLogger]%v", err))
	}
	_, err = os.Stat(parentPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(parentPath, os.ModePerm)
		if err != nil {
			panic(fmt.Sprintf("[initLogger]%v", err))
		}
	}

	l.currentFilepath = CurrentIDLogFilepath(LogFileBasePath, l.loggerName, l.currentDay, l.currentFileID)

	// set log writer
	f, err := os.OpenFile(l.currentFilepath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("[initLogger]%v", err))
	}
	l.logf = f
	l.log.SetOutput(l.logf)
}

// nextFile id+1，currentN=0
func (l *defaultLogger) nextFile() {
	if l.logf != nil {
		l.logf.Close()
	}
	l.currentFileID += 1
	l.currentN = 0

	parentPath, err := filepath.Abs(CurrentIDLogFileParentPath(LogFileBasePath, l.loggerName, l.currentDay))
	if err != nil {
		panic(fmt.Sprintf("[initLogger]%v", err))
	}
	_, err = os.Stat(parentPath)
	if os.IsNotExist(err) {
		err := os.MkdirAll(parentPath, os.ModePerm)
		if err != nil {
			panic(fmt.Sprintf("[initLogger]%v", err))
		}
	}

	l.currentFilepath = CurrentIDLogFilepath(LogFileBasePath, l.loggerName, l.currentDay, l.currentFileID)

	// set log writer
	f, err := os.OpenFile(l.currentFilepath, os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	if err != nil {
		panic(fmt.Sprintf("[initLogger]%v", err))
	}
	l.logf = f
	l.log.SetOutput(l.logf)
}
