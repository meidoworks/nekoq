package naming

import (
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/naming/discovery"
)

func SyncStartNaming(cfg *config.NekoConfig) error {
	d, err := discovery.NewHttpService(cfg, discovery.NewDataStore())
	if err != nil {
		return err
	}
	return d.StartService()
}
