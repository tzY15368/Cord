package raft

// func init() {

// 	var logFilename = "raft-out.log"
// 	os.Remove(logFilename)
// 	logFile, err := os.OpenFile(logFilename, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0666)
// 	if err != nil {
// 		panic(err)
// 	}

// 	fileAndStdoutWriter := io.MultiWriter(logFile, os.Stdout)
// 	logrus.SetOutput(ansicolor.NewAnsiColorWriter(fileAndStdoutWriter))

// 	logLevel := logrus.DebugLevel
// 	logrus.SetLevel(logLevel)
// 	logrus.SetReportCaller(false)
// }
