package discovery

import (
	"errors"
	"fmt"
	"sync"
)

type DataStore struct {
	LocalData  localData
	PeerData   peerData
	MergedData mergedData

	sync.RWMutex
}

func NewDataStore() *DataStore {
	return &DataStore{
		LocalData: localData{Data: map[string]map[string][]*Record{}},
		PeerData: peerData{
			peerDetailMap:  map[int16]map[string]*Record{},
			peerVersionMap: map[int16]string{},
		},
		MergedData: mergedData{Data: map[string]map[string][]*Record{}},
	}
}

type localData struct {
	Data map[string]map[string][]*Record // service -> { area -> service_list }
}

type peerData struct {
	peerDetailMap  map[int16]map[string]*Record
	peerVersionMap map[int16]string
}

func (p *peerData) buildPeerFromFullSet(peerId int16, fullSet *FullSet) error {
	detailMap := make(map[string]*Record)
	for _, v := range fullSet.Records {
		detailMap[makeServiceKey(v)] = v
	}

	p.peerDetailMap[peerId] = detailMap
	p.peerVersionMap[peerId] = fullSet.CurrentVersion

	return nil
}

type mergedData struct {
	Data map[string]map[string][]*Record // service -> { area -> service_list }
}

func (d *DataStore) InitPeer(peerId int16, fullSet *FullSet) error {
	d.Lock()
	defer d.Unlock()

	// 1. Create or replace peer data
	if err := d.PeerData.buildPeerFromFullSet(peerId, fullSet); err != nil {
		return err
	}
	// 2. regenerate merged data
	d.regenerateMerged()

	return nil
}

func (d *DataStore) UpdatePeer(peerId int16, incSet *IncrementalSet) error {
	d.Lock()
	defer d.Unlock()

	// 1. update peer data
	detailMap, ok := d.PeerData.peerDetailMap[peerId]
	if !ok {
		return errors.New("no such peerId")
	}
	for _, v := range incSet.Records {
		switch v.Operation {
		case "change":
			detailMap[makeServiceKey(&v.Record)] = &v.Record
		case "remove":
			delete(detailMap, makeServiceKey(&v.Record))
		}
	}
	d.PeerData.peerVersionMap[peerId] = incSet.CurrentVersion
	// 2. regenerate merged data
	d.regenerateMerged()

	return nil
}

func (d *DataStore) CleanupPeer(peerId int16) error {
	d.Lock()
	defer d.Unlock()

	// 1. remove peer
	delete(d.PeerData.peerVersionMap, peerId)
	delete(d.PeerData.peerDetailMap, peerId)
	// 2. regenerate merged data
	d.regenerateMerged()

	return nil
}

func (d *DataStore) regenerateMerged() {
	newMap := map[string]map[string][]*Record{}
	// merge peer data
	for _, peer := range d.PeerData.peerDetailMap {
		for _, record := range peer {
			areaMap, ok := newMap[record.Service]
			if !ok {
				areaMap = map[string][]*Record{}
				newMap[record.Service] = areaMap
			}
			areaMap[record.Area] = append(areaMap[record.Area], record)
		}
	}
	// merge local data
	for service, areaMap := range d.LocalData.Data {
		for area, records := range areaMap {
			for _, record := range records {
				areaMap, ok := newMap[service]
				if !ok {
					areaMap = map[string][]*Record{}
					newMap[service] = areaMap
				}
				areaMap[area] = append(areaMap[area], record)
			}
		}
	}
	// update merge data
	d.MergedData.Data = newMap
}

func makeServiceKey(record *Record) string {
	return fmt.Sprint(record.Service, "||", record.Area, "||", record.NodeId)
}
