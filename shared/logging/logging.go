package logging

import (
	"github.com/sirupsen/logrus"
)

func NewLogger(loggerName string) *logrus.Logger {
	logger := logrus.New()
	//logger.SetOutput(io.MultiWriter(os.Stdout))
	return logger
}
