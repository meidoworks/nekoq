package inproc

import "github.com/meidoworks/nekoq/shared/hooks"

const (
	LocalSwitchNumGen       = 1
	LocalSwitchMessageQueue = 2
	LocalSwitchDiscovery    = 11
	LocalSwitchWarehouse    = 21
)

var _globalShutdownHook = new(hooks.ShutdownHook)

func GetGlobalShutdownHook() *hooks.ShutdownHook {
	return _globalShutdownHook
}
