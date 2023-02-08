package discovery

import "strconv"

type LocalPeerService struct {
	DataStore *DataStore
}

func (l *LocalPeerService) FullFetch() (*FullSet, error) {
	return l.DataStore.FetchLocalFull()
}

func (l *LocalPeerService) IncrementalFetch(lastVersion string) (*IncrementalSet, error) {
	versionId, err := strconv.ParseInt(lastVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	return l.DataStore.FetchLocalIncremental(versionId)
}

func NewLocalPeerService(dataStore *DataStore) PeerService {
	lps := new(LocalPeerService)
	lps.DataStore = dataStore

	return lps
}

type RemoteClientPeerService struct {
}

type HttpServerPeerService struct {
}
