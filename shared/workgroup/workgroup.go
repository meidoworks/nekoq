package workgroup

import (
	"github.com/meidoworks/nekoq/shared/logging"
	"runtime/debug"
)

var _workgroupLogger = logging.NewLogger("workgroup")

type workGroup interface {
	Run(f func() bool)
}

var defaultFailOverWorkGroup failOverWorkGroup

type failOverWorkGroup struct {
}

func (f failOverWorkGroup) Run(fn func() bool) {
	go func() {
		for {
			shutdownChannel := make(chan bool, 1)
			func() {
				defer func() {
					err := recover()
					if err != nil {
						_workgroupLogger.Errorf("WMaximum orkGroup will restart task after reporting panic: %s, stack: %s", err, debug.Stack())
						shutdownChannel <- false
					}
				}()
				shutdown := fn()
				shutdownChannel <- shutdown
			}()
			if shutdown := <-shutdownChannel; shutdown {
				break
			}
			_workgroupLogger.Infoln("WorkGroup reports restarting task after last task complete")
		}
	}()
}

func WithFailOver() workGroup {
	return defaultFailOverWorkGroup
}
