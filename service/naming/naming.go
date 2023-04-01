package naming

import (
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/naming/discovery"
	"github.com/meidoworks/nekoq/shared/logging"
)

var (
	_namingLogger = logging.NewLogger("NamingLogger")
)

func StartNaming(cfg *config.NekoConfig) error {
	datastore := discovery.NewDataStore()

	var peers []*discovery.Peer
	for _, v := range cfg.Naming.Discovery.Peers {
		if v.NodeId == *cfg.Shared.NodeId {
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

	d, err := discovery.NewHttpService(cfg, datastore)
	if err != nil {
		return err
	}
	_namingLogger.Infof("start discover service.")
	return d.StartService()
}
