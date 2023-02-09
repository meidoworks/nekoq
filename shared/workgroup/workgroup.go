package workgroup

import "github.com/sirupsen/logrus"

var _workgroupLogger = logrus.New()

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
						_workgroupLogger.Errorln("WorkGroup will restart task after reporting panic:", err)
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
