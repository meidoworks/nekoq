package api

import "github.com/meidoworks/nekoq/shared/hooks"

var _globalShutdownHook = new(hooks.ShutdownHook)

func GetGlobalShutdownHook() *hooks.ShutdownHook {
	return _globalShutdownHook
}
