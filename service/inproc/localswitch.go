package inproc

import (
	"sync"

	"github.com/meidoworks/nekoq/shared/netaddons/localswitch"
)

var lswitch *localswitch.LocalSwitch
var lswitchInitializer *sync.Once

func init() {
	lswitchInitializer = new(sync.Once)
}

func initLSwitch() {
	newLSwitch, err := localswitch.StartNewLocalSwitch()
	if err != nil {
		panic(err)
	}
	lswitch = newLSwitch
}

func GetLocalSwitch() *localswitch.LocalSwitch {
	lswitchInitializer.Do(initLSwitch)
	return lswitch
}
