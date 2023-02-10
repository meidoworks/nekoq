package discovery

import (
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
)

const (
	IncrementalOperationChange = "change"
	IncrementalOperationRemove = "remove"
)

const (
	_sizeOfHistory = 256
)

type DataStore struct {
	LocalData  localData
	PeerData   peerData
	MergedData mergedData

	sync.RWMutex
}

func NewDataStore() *DataStore {
	sizeOfHistory := _sizeOfHistory
	return &DataStore{
		LocalData: localData{
			Data:          map[string]map[string][]*Record{},
			ChangeLog:     make([][]*IncrementalRecord, sizeOfHistory),
			SizeOfHistory: int64(sizeOfHistory),
		},
		PeerData: peerData{
			peerDetailMap:  map[int16]map[string]*Record{},
			peerVersionMap: map[int16]string{},
		},
		MergedData: mergedData{Data: map[string]map[string][]*Record{}},
	}
}

type localData struct {
	Data map[string]map[string][]*Record // service -> { area -> service_list }

	ChangeLog [][]*IncrementalRecord

	SizeOfHistory int64
	Version       int64
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
		case IncrementalOperationChange:
			detailMap[makeServiceKey(&v.Record)] = &v.Record
		case IncrementalOperationRemove:
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

func (d *DataStore) OfflineRecord(key *RecordKey) {
	d.Lock()
	defer d.Unlock()

	changed := false
	// remove from data
	if areaMap, ok := d.LocalData.Data[key.Service]; ok {
		if srvList, ok := areaMap[key.Area]; ok {
			found := false
			var idx int
			for i, v := range srvList {
				if v.NodeId == key.NodeId {
					found = true
					idx = i
				}
			}
			if found {
				changed = true
				var newList = make([]*Record, len(srvList)-1)
				copy(newList[0:idx], srvList[0:idx])
				copy(newList[idx:], srvList[idx+1:])
				areaMap[key.Area] = newList
				// inc version
				atomic.AddInt64(&d.LocalData.Version, 1)
				// update history only after service existing
				r := &Record{
					Service: key.Service,
					Area:    key.Area,
					NodeId:  key.NodeId,
				}
				var slot = []*IncrementalRecord{{
					Record:    *r,
					Operation: IncrementalOperationRemove,
				}}
				offset := d.LocalData.Version % d.LocalData.SizeOfHistory
				d.LocalData.ChangeLog[offset] = slot
			}
		}
	}
	if changed {
		// regenerate merged data
		d.regenerateMerged()
	}
}

func (d *DataStore) KeepAliveRecord(record *Record) {
	d.Lock()
	defer d.Unlock()

	// inc version
	atomic.AddInt64(&d.LocalData.Version, 1)
	// update history
	var slot = []*IncrementalRecord{{
		Record:    *record,
		Operation: IncrementalOperationChange,
	}}
	offset := d.LocalData.Version % d.LocalData.SizeOfHistory
	d.LocalData.ChangeLog[offset] = slot
	// update data
	updateLocalDataMap(record, d.LocalData.Data)
	// regenerate merged data
	d.regenerateMerged()
}

func updateLocalDataMap(record *Record, m map[string]map[string][]*Record) {
	areaMap, ok := m[record.Service]
	if !ok {
		areaMap = map[string][]*Record{}
		m[record.Service] = areaMap
	}
	srvList, ok := areaMap[record.Area]
	if ok {
		found := false
		for i, v := range srvList {
			if v.NodeId == record.NodeId {
				srvList[i] = record
				found = true
				break
			}
		}
		if !found {
			areaMap[record.Area] = append(srvList, record)
		}
	} else {
		areaMap[record.Area] = append(srvList, record)
	}
}

func makeServiceKey(record *Record) string {
	return fmt.Sprint(record.Service, "||", record.Area, "||", record.NodeId)
}

func (d *DataStore) FetchLocalFull() (*FullSet, error) {
	d.RLock()
	defer d.RUnlock()

	var records []*Record
	for _, v := range d.LocalData.Data {
		for _, vv := range v {
			records = append(records, vv...)
		}
	}

	fullSet := &FullSet{
		CurrentVersion: fmt.Sprint(d.LocalData.Version),
		Records:        records,
	}

	return fullSet, nil
}

func (d *DataStore) FetchLocalIncremental(lastVersion int64) (*IncrementalSet, error) {
	d.RLock()
	defer d.RUnlock()

	// history rewind
	if lastVersion+d.LocalData.SizeOfHistory <= d.LocalData.Version {
		return &IncrementalSet{
			CurrentVersion: "",
			ReSync:         true,
			Records:        nil,
		}, nil
	}

	var records []*IncrementalRecord
	for i := lastVersion + 1; i <= d.LocalData.Version; i++ {
		for _, v := range d.LocalData.ChangeLog[i%d.LocalData.SizeOfHistory] {
			records = append(records, v)
		}
	}

	return &IncrementalSet{
		CurrentVersion: fmt.Sprint(d.LocalData.Version),
		ReSync:         false,
		Records:        records,
	}, nil
}

func (d *DataStore) Fetch(service, area string) ([]*Record, error) {
	d.RLock()
	defer d.RUnlock()

	areaMap, ok := d.MergedData.Data[service]
	if !ok {
		return nil, nil
	}
	rs, ok := areaMap[area]
	if !ok {
		return nil, nil
	}
	return rs, nil
}
