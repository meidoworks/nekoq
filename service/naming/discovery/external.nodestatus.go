package discovery

import (
	"sync"
	"time"

	"github.com/meidoworks/nekoq/shared/workgroup"

	"github.com/sirupsen/logrus"
)

const (
	_keepAliveLoopInterview = 5
	_serviceTTL             = 20
)

var _nodeStatusLogger = logrus.New()

type TimeoutEntry struct {
	*RecordKey
	LatestTime time.Time

	next *TimeoutEntry
	prev *TimeoutEntry
}

type RecordFinalizer func(recordKey *RecordKey) error

type NodeStatusManager struct {
	recordFinalizer RecordFinalizer

	timeoutMap       map[string]*TimeoutEntry
	timeoutEntryHead *TimeoutEntry
	timeoutLock      sync.Mutex
}

func (n *NodeStatusManager) SetFinalizer(f RecordFinalizer) {
	n.recordFinalizer = f
}

func (n *NodeStatusManager) StartNode(recordKey *RecordKey) error {
	n.timeoutLock.Lock()
	defer n.timeoutLock.Unlock()

	en, ok := n.timeoutMap[recordKey.GetKey()]
	if ok {
		en.LatestTime = time.Now()
		return nil
	}

	newEntry := &TimeoutEntry{
		RecordKey:  recordKey,
		LatestTime: time.Now(),
		next:       n.timeoutEntryHead.next,
		prev:       n.timeoutEntryHead,
	}
	if newEntry.next != nil {
		newEntry.next.prev = newEntry
	}
	n.timeoutEntryHead.next = newEntry
	n.timeoutMap[recordKey.GetKey()] = newEntry

	return nil
}

func (n *NodeStatusManager) KeepAlive(recordKey *RecordKey) error {
	n.timeoutLock.Lock()
	defer n.timeoutLock.Unlock()

	en, ok := n.timeoutMap[recordKey.GetKey()]
	if ok {
		en.LatestTime = time.Now()
		return nil
	} else {
		return nil
	}
}

func (n *NodeStatusManager) Offline(recordKey *RecordKey) error {
	n.timeoutLock.Lock()
	defer n.timeoutLock.Unlock()

	en, ok := n.timeoutMap[recordKey.GetKey()]
	if ok {
		if err := n.recordFinalizer(recordKey); err != nil {
			return err
		}
		delete(n.timeoutMap, recordKey.GetKey())
		en.prev.next = en.next
		if en.next != nil {
			en.next.prev = en.prev
		}
	}

	return nil
}

func (n *NodeStatusManager) KeepAliveLoop() bool {
	ticker := time.NewTicker(_keepAliveLoopInterview * time.Second) // walk through all node interval
	threshold := _serviceTTL * time.Second                          // timeout to cleanup

	for {
		_ = <-ticker.C // do not use the time from ticker
		now := time.Now()
		n.foreachEntries(now, threshold)
	}
}

func (n *NodeStatusManager) foreachEntries(now time.Time, threshold time.Duration) {
	n.timeoutLock.Lock()
	defer n.timeoutLock.Unlock()

	for entry := n.timeoutEntryHead.next; entry != nil; entry = entry.next {
		if now.Sub(entry.LatestTime) > threshold {
			if err := n.recordFinalizer(entry.RecordKey); err != nil {
				_nodeStatusLogger.Errorf("service finalize error: %s", err)
				continue
			}
			delete(n.timeoutMap, entry.RecordKey.GetKey())
			entry.prev.next = entry.next
			if entry.next != nil {
				entry.next.prev = entry.prev
			}
		}
	}
}

func NewNodeStatusManager() *NodeStatusManager {
	mgr := new(NodeStatusManager)
	mgr.timeoutMap = map[string]*TimeoutEntry{}
	mgr.timeoutEntryHead = new(TimeoutEntry)

	workgroup.WithFailOver().Run(mgr.KeepAliveLoop)

	return mgr
}
