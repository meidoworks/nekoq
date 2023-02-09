package naming

import "github.com/meidoworks/nekoq/service/naming/discovery"

func SyncStartNaming(discoveryAddr string) error {
	d := discovery.NewHttpService(discoveryAddr, discovery.NewDataStore())
	return d.StartService()
}
