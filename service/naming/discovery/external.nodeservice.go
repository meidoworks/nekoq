package discovery

type LocalNodeService struct {
	DataStore *DataStore
}

func (l *LocalNodeService) OfflineN(keys []*RecordKey) error {
	l.DataStore.OfflineNRecords(keys)
	return nil
}

func (l *LocalNodeService) SelfKeepAlive(record *Record) error {
	l.DataStore.PersistRecord(record)
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
	return l.DataStore.Fetch(service, area)
}

func NewLocalNodeService(dataStore *DataStore) NodeService {
	return &LocalNodeService{
		DataStore: dataStore,
	}
}
