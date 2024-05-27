package naming

import (
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/naming/discovery"
	"github.com/meidoworks/nekoq/service/naming/warehouse"
	"github.com/meidoworks/nekoq/shared/logging"
)

var (
	_namingLogger = logging.NewLogger("NamingLogger")
)

func StartNaming() error {
	// start warehouse
	wh := warehouse.NewWarehouse()
	if err := wh.Startup(); err != nil {
		return err
	}

	datastore := discovery.NewDataStore()

	var peers []*discovery.Peer
	for _, v := range config.Instance.Services.Naming.Discovery.Peers {
		if v.NodeId == config.Instance.NekoQ.NodeId {
			continue
		}
		peerService := discovery.NewHttpServerPeerService(v.Address, v.NodeId)
		peer, err := discovery.StartPeer(peerService, v.NodeId, datastore)
		if err != nil {
			return err
		}
		_namingLogger.Infof("start peer service:[%d]", v.NodeId)
		peers = append(peers, peer)
	}

	d, err := discovery.NewHttpService(datastore)
	if err != nil {
		return err
	}
	_namingLogger.Infof("start discover service.")
	return d.StartService()
}
