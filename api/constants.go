package api

import "github.com/meidoworks/nekoq/shared/hooks"

const (
	LocalSwitchNumGen             = 1
	LocalSwitchMessageQueue       = 2
	LocalSwitchDiscoveryAndCellar = 31
	LocalSwitchWarehouse          = 36
)

const (
	DefaultConfigLocalSwitchNamingAddress = "inproc"
)

var _globalShutdownHook = new(hooks.ShutdownHook)

func GetGlobalShutdownHook() *hooks.ShutdownHook {
	return _globalShutdownHook
}
