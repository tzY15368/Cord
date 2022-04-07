package logging

import (
	"io"
	"log"
	"os"
	"sync"

	"github.com/shiena/ansicolor"
	"github.com/sirupsen/logrus"
)

var loggerMap = make(map[string]*logrus.Logger)
var mu sync.Mutex

func DropLogFile(name string) {
	var logFilename = name + "-out.log"
	os.Remove(logFilename)
}

func PrepareLogger(name string, level string) {
	lv := logrus.DebugLevel
	switch level {
	case "debug":
		break
	case "info":
		lv = logrus.InfoLevel
	case "warn":
		lv = logrus.WarnLevel
	default:
		panic("logger: invalid log level:" + level)
	}
	GetLogger(name, lv)
}

func GetLogger(name string, level logrus.Level) *logrus.Logger {
	mu.Lock()
	defer mu.Unlock()
	_logger, ok := loggerMap[name]
	if ok {
		return _logger
	}
	log.Println("starting logger", name)
	var logFilename = name + "-out.log"
	os.Remove(logFilename)
	logFile, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
	if err != nil {
		panic(err)
	}

	logger := logrus.New()
	if level < logrus.InfoLevel {

		fileAndStdoutWriter := io.MultiWriter(logFile, os.Stdout)
		writer := ansicolor.NewAnsiColorWriter(fileAndStdoutWriter)
		logger.Out = writer
	} else {
		logger.Out = logFile
	}

	logger.Level = level
	loggerMap[name] = logger
	// logLevel := logrus.DebugLevel
	// logrus.SetLevel(logLevel)
	// logrus.SetReportCaller(false)
	return logger
}
