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
			Data:          map[string]map[string]map[string]*Record{},
			ChangeLog:     make([][]*IncrementalRecord, sizeOfHistory),
			SizeOfHistory: int64(sizeOfHistory),
		},
		PeerData: peerData{
			peerDetailMap:  map[int16]map[string]*IncrementalRecord{},
			peerVersionMap: map[int16]string{},
		},
		MergedData: mergedData{Data: map[string]map[string][]*Record{}},
	}
}

type localData struct {
	Data map[string]map[string]map[string]*Record // service -> { area -> service_list }

	ChangeLog [][]*IncrementalRecord

	SizeOfHistory int64
	Version       int64
}

type peerData struct {
	peerDetailMap  map[int16]map[string]*IncrementalRecord
	peerVersionMap map[int16]string
}

func (p *peerData) buildPeerFromFullSet(peerId int16, fullSet *FullSet) error {
	detailMap := make(map[string]*IncrementalRecord)

	for _, serviceItem := range fullSet.RecordSet {
		service := serviceItem.Service
		for _, areaItem := range serviceItem.Areas {
			area := areaItem.Area
			for _, srvRecord := range areaItem.Records {
				detailMap[makeServiceKeyFromParam(serviceItem.Service, areaItem.Area, srvRecord.NodeId)] = &IncrementalRecord{
					Record: *srvRecord,
					RecordKey: RecordKey{
						Service: service,
						Area:    area,
						NodeId:  srvRecord.NodeId,
					},
					Operation: "",
				}
			}
		}
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
			detailMap[makeServiceKeyFromParam(v.RecordKey.Service, v.RecordKey.Area, v.RecordKey.NodeId)] = v
		case IncrementalOperationRemove:
			delete(detailMap, makeServiceKeyFromParam(v.RecordKey.Service, v.RecordKey.Area, v.RecordKey.NodeId))
		}
	}
	d.PeerData.peerVersionMap[peerId] = incSet.CurrentVersion
	// 2. incremental merged data
	d.incrementalMerged(incSet.Records)

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

func (d *DataStore) incrementalMerged(entries []*IncrementalRecord) {
	var data = d.MergedData.Data
	for _, record := range entries {
		areaMap, ok := data[record.RecordKey.Service]
		if !ok {
			areaMap = map[string][]*Record{}
		}
		records := areaMap[record.RecordKey.Area]
		offset := -1
		for idx, v := range records {
			//FIXME avoid liner search
			if v.NodeId == record.RecordKey.NodeId {
				offset = idx
				break
			}
		}
		switch record.Operation {
		case IncrementalOperationChange:
			if offset != -1 {
				var record = record.Record
				records[offset] = &record
			} else {
				var r = record.Record
				areaMap[record.RecordKey.Area] = append(records, &r)
			}
		case IncrementalOperationRemove:
			if offset != -1 {
				//FIXME avoid the operation both freq and copy
				// Update1: looks like copy has good performance
				var newList = make([]*Record, len(records)-1)
				copy(newList[0:offset], records[0:offset])
				copy(newList[offset:], records[offset+1:])
				areaMap[record.RecordKey.Area] = newList
			}
		}
	}
}

func (d *DataStore) regenerateMerged() {
	newMap := map[string]map[string][]*Record{}
	// merge peer data
	for _, peer := range d.PeerData.peerDetailMap {
		for _, record := range peer {
			areaMap, ok := newMap[record.RecordKey.Service]
			if !ok {
				areaMap = map[string][]*Record{}
				newMap[record.RecordKey.Service] = areaMap
			}
			var r = record.Record
			areaMap[record.RecordKey.Area] = append(areaMap[record.RecordKey.Area], &r)
		}
	}
	// merge local data
	for service, areaMap := range d.LocalData.Data {
		for area, srvMap := range areaMap {
			for _, record := range srvMap {
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

func (d *DataStore) OfflineNRecords(keys []*RecordKey) {
	d.Lock()
	defer d.Unlock()

	var changeLog = make([]*IncrementalRecord, 0, 32)
	changed := false
	for _, key := range keys {
		// remove from data
		if areaMap, ok := d.LocalData.Data[key.Service]; ok {
			if srvList, ok := areaMap[key.Area]; ok {
				_, ok := srvList[key.NodeId]
				if ok {
					changed = true
					delete(srvList, key.NodeId)
					// add to history only after service existing
					r := &RecordKey{
						Service: key.Service,
						Area:    key.Area,
						NodeId:  key.NodeId,
					}
					changeLog = append(changeLog, &IncrementalRecord{
						RecordKey: *r,
						Operation: IncrementalOperationRemove,
					})
				}
			}
		}
	}
	if changed {
		// inc version
		atomic.AddInt64(&d.LocalData.Version, 1)
		// update history
		offset := d.LocalData.Version % d.LocalData.SizeOfHistory
		d.LocalData.ChangeLog[offset] = changeLog
		// incremental merged data
		d.incrementalMerged(changeLog)
	}
}

func (d *DataStore) OfflineRecord(key *RecordKey) {
	records := []*RecordKey{key}
	d.OfflineNRecords(records)
}

func (d *DataStore) PersistRecord(recordKey *RecordKey, record *Record) {
	d.Lock()
	defer d.Unlock()

	// inc version
	atomic.AddInt64(&d.LocalData.Version, 1)
	// update history
	var slot = []*IncrementalRecord{{
		Record:    *record,
		RecordKey: *recordKey,
		Operation: IncrementalOperationChange,
	}}
	offset := d.LocalData.Version % d.LocalData.SizeOfHistory
	d.LocalData.ChangeLog[offset] = slot
	// update data
	updateLocalDataMap(recordKey, record, d.LocalData.Data)
	// incremental merged data
	d.incrementalMerged(slot)
}

func updateLocalDataMap(recordKey *RecordKey, record *Record, m map[string]map[string]map[string]*Record) {
	areaMap, ok := m[recordKey.Service]
	if !ok {
		areaMap = map[string]map[string]*Record{}
		m[recordKey.Service] = areaMap
	}
	srvList, ok := areaMap[recordKey.Area]
	if ok {
		srvList[record.NodeId] = record
	} else {
		srvList = map[string]*Record{}
		srvList[record.NodeId] = record
		areaMap[recordKey.Area] = srvList
	}
}

func makeServiceKeyFromParam(service, area, nodeId string) string {
	return fmt.Sprint(service, "||", area, "||", nodeId)
}

func (d *DataStore) PeerFetchLocalFull() (*FullSet, error) {
	d.RLock()
	defer d.RUnlock()

	var source = d.LocalData.Data

	count := 0
	recordSet := make([]*ServiceSetItem, 0, len(source))
	for service, areaMap := range source {
		if len(areaMap) == 0 {
			continue
		}
		areaSet := make([]*AreaSetItem, 0, len(areaMap))
		for area, srvMap := range areaMap {
			if len(srvMap) == 0 {
				continue
			}
			count += len(srvMap)
			records := make([]*Record, 0, len(srvMap))
			for _, record := range srvMap {
				records = append(records, record)
			}
			areaSet = append(areaSet, &AreaSetItem{
				Area:    area,
				Records: records,
			})
		}
		recordSet = append(recordSet, &ServiceSetItem{
			Service: service,
			Areas:   areaSet,
		})
	}

	fullSet := &FullSet{
		CurrentVersion:   fmt.Sprint(d.LocalData.Version),
		totalRecordCount: count,
		RecordSet:        recordSet,
	}

	return fullSet, nil
}

func (d *DataStore) PeerFetchLocalIncremental(lastVersion int64) (*IncrementalSet, error) {
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
