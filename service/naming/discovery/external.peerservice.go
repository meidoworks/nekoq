package discovery

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"

	"github.com/meidoworks/nekoq/shared/logging"

	"github.com/go-resty/resty/v2"
)

var (
	_peerServiceLog = logging.NewLogger("PeerService")
)

type LocalPeerService struct {
	DataStore *DataStore
}

func (l *LocalPeerService) FullFetch() (*FullSet, error) {
	return l.DataStore.PeerFetchLocalFull()
}

func (l *LocalPeerService) IncrementalFetch(lastVersion string) (*IncrementalSet, error) {
	versionId, err := strconv.ParseInt(lastVersion, 10, 64)
	if err != nil {
		return nil, err
	}
	return l.DataStore.PeerFetchLocalIncremental(versionId)
}

func NewLocalPeerService(dataStore *DataStore) PeerService {
	lps := new(LocalPeerService)
	lps.DataStore = dataStore

	return lps
}

type HttpServerPeerService struct {
	address string
	peerId  int16

	client *resty.Client
}

func (h *HttpServerPeerService) FullFetch() (*FullSet, error) {
	f := new(FullSet)

	resp, err := h.client.R().EnableTrace().
		Get(fmt.Sprint(h.address, "/peer/full"))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		_peerServiceLog.Errorf("unexpected status code:[%d]", resp.StatusCode())
		return nil, errors.New("unexpected status code")
	}
	_peerServiceLog.Infof("fetch peer full data from:[%d]", h.peerId)
	data := resp.Body()
	if err := f.UnmarshalCbor(data); err != nil {
		return nil, err
	}
	return f, nil
}

func (h *HttpServerPeerService) IncrementalFetch(lastVersion string) (*IncrementalSet, error) {
	incSet := new(IncrementalSet)

	resp, err := h.client.R().EnableTrace().
		Get(fmt.Sprint(h.address, "/peer/incremental/", lastVersion))
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != 200 {
		_peerServiceLog.Errorf("unexpected status code:[%d]", resp.StatusCode())
		return nil, errors.New("unexpected status code")
	}
	_peerServiceLog.Infof("fetch peer incremental data from:[%d] starting at version:[%s]", h.peerId, lastVersion)
	data := resp.Body()
	if err := json.Unmarshal(data, incSet); err != nil {
		return nil, err
	}
	return incSet, nil
}

func NewHttpServerPeerService(address string, peerId int16) PeerService {
	h := new(HttpServerPeerService)
	h.peerId = peerId
	h.address = address

	h.client = resty.New()

	return h
}

type RemoteClientPeerService struct {
}
