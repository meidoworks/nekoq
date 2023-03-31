package discovery

import (
	"github.com/meidoworks/nekoq/service/naming/warehouseapi"
	"github.com/meidoworks/nekoq/shared/logging"
)

var (
	_nodeServiceLogger = logging.NewLogger("NodeService")
)

type LocalNodeService struct {
	DataStore    *DataStore
	DiscoveryUse warehouseapi.DiscoveryUse
}

func (l *LocalNodeService) OfflineN(keys []*RecordKey) error {
	l.DataStore.OfflineNRecords(keys)
	return nil
}

func (l *LocalNodeService) SelfKeepAlive(recordKey *RecordKey, record *Record) error {
	l.DataStore.PersistRecord(recordKey, record)
	return nil
}

func (l *LocalNodeService) SlimKeepAlive(key *RecordKey) error {
	// currently no need to keep alive inside LocalNodeService
	return nil
}

func (l *LocalNodeService) Offline(key *RecordKey) error {
	l.DataStore.OfflineRecord(key)
	return nil
}

func (l *LocalNodeService) ControlData(data *ControlData) error {
	//TODO implement me
	panic("implement me")
}

func (l *LocalNodeService) CustomInformation(info *CustomInfo) error {
	//TODO implement me
	panic("implement me")
}

func (l *LocalNodeService) Fetch(service, area string) ([]*Record, error) {
	areaList, err := l.DiscoveryUse.AreaLevels(area)
	if err != nil {
		if err == warehouseapi.ErrAreaNotFound {
			_nodeServiceLogger.Infof("area:[%s] not found", area)
			return nil, nil
		}
		return nil, err
	}
	for _, v := range areaList {
		records, err := l.DataStore.Fetch(service, v.Area())
		if err != nil {
			return nil, err
		}
		if len(records) != 0 {
			return records, nil
		}
	}
	return nil, nil
}

func NewLocalNodeService(dataStore *DataStore, discoveryUse warehouseapi.DiscoveryUse) NodeService {
	return &LocalNodeService{
		DataStore:    dataStore,
		DiscoveryUse: discoveryUse,
	}
}
