package discovery_test

import (
	"testing"
	"time"

	"github.com/meidoworks/nekoq/service/naming/discovery"
)

func TestInternalApiSlimUsage(t *testing.T) {
	ds1 := discovery.NewDataStore()
	ps1 := discovery.NewLocalPeerService(ds1)
	ns1 := discovery.NewLocalNodeService(ds1)
	ds2 := discovery.NewDataStore()
	ps2 := discovery.NewLocalPeerService(ds2)
	ns2 := discovery.NewLocalNodeService(ds2)
	ds3 := discovery.NewDataStore()
	ps3 := discovery.NewLocalPeerService(ds3)
	ns3 := discovery.NewLocalNodeService(ds3)
	s1 := setupService(t, 1, []*discovery.DataStore{ds1, ds2, ds3}, []discovery.PeerService{ps1, ps2, ps3})
	s2 := setupService(t, 2, []*discovery.DataStore{ds1, ds2, ds3}, []discovery.PeerService{ps1, ps2, ps3})
	s3 := setupService(t, 3, []*discovery.DataStore{ds1, ds2, ds3}, []discovery.PeerService{ps1, ps2, ps3})

	_ = ns1
	_ = ns2
	_ = ns3
	_ = s1
	_ = s2
	_ = s3

	if err := ns1.SelfKeepAlive(&discovery.Record{
		Service:       "demo.service01",
		Area:          "cn",
		NodeId:        "AAA0001",
		RecordVersion: 0,
		Tags:          nil,
		ServiceData:   nil,
		MetaData:      nil,
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	if r, err := ns2.Fetch("demo.service01", "cn"); err != nil {
		t.Fatal(err)
	} else if len(r) == 0 {
		t.Fatal("expect 1 result")
	}
	if r, err := ns3.Fetch("demo.service01", "cn"); err != nil {
		t.Fatal(err)
	} else if len(r) == 0 {
		t.Fatal("expect 1 result")
	}

	if err := ns1.Offline(&discovery.RecordKey{
		Service: "demo.service01",
		Area:    "cn",
		NodeId:  "AAA0001",
	}); err != nil {
		t.Fatal(err)
	}

	time.Sleep(3 * time.Second)

	if r, err := ns2.Fetch("demo.service01", "cn"); err != nil {
		t.Fatal(err)
	} else if len(r) != 0 {
		t.Fatal("expect 0 result")
	}
	if r, err := ns3.Fetch("demo.service01", "cn"); err != nil {
		t.Fatal(err)
	} else if len(r) != 0 {
		t.Fatal("expect 0 result")
	}
}

type testService struct {
	DataStore *discovery.DataStore
	Peers     []*discovery.Peer
}

func setupService(t *testing.T, peerId int16, stores []*discovery.DataStore, services []discovery.PeerService) *testService {
	service := new(testService)

	dataStore := stores[peerId-1]
	service.DataStore = dataStore

	if peerId != 1 {
		// assemble one peer
		peer1, err := discovery.StartPeer(services[0], 1, dataStore)
		if err != nil {
			t.Fatal(err)
		}
		service.Peers = append(service.Peers, peer1)
	}
	if peerId != 2 {
		// assemble one peer
		peer2, err := discovery.StartPeer(services[1], 2, dataStore)
		if err != nil {
			t.Fatal(err)
		}
		service.Peers = append(service.Peers, peer2)
	}
	if peerId != 3 {
		// assemble one peer
		peer3, err := discovery.StartPeer(services[2], 3, dataStore)
		if err != nil {
			t.Fatal(err)
		}
		service.Peers = append(service.Peers, peer3)
	}

	return service
}
