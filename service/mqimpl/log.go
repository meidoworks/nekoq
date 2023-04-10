package mqimpl

import (
	"github.com/meidoworks/nekoq/shared/logging"
)

var _mqLogger = logging.NewLogger("MQCommon")

func LogInfo(v ...interface{}) {
	_mqLogger.Infoln(v...)
}

func LogError(v ...interface{}) {
	_mqLogger.Errorln(v...)
}
