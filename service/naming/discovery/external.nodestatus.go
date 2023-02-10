package discovery

import (
	"math/rand"
	"sync"
	"time"

	"github.com/meidoworks/nekoq/shared/hash"
	"github.com/meidoworks/nekoq/shared/priorityqueue"
	"github.com/meidoworks/nekoq/shared/workgroup"

	"github.com/sirupsen/logrus"
)

const (
	_keepAliveLoopInterview = 5
	_serviceTTL             = 20
	_serviceTTLRandomInc    = 10
)

var _nodeStatusLogger = logrus.New()

type TimeoutEntry struct {
	*RecordKey
	ExpireTime time.Time
	Deleted    bool
}

type BatchRecordFinalizer func(recordKeys []*RecordKey) error

type NodeStatusManager struct {
	batchRecordFinalizer BatchRecordFinalizer

	nWorker       uint32
	nRandGen      []*rand.Rand
	nTimeoutMap   []map[string]*TimeoutEntry
	nTimeoutQueue []*priorityqueue.PriorityQueue[*TimeoutEntry]
	nTimeoutLock  []*sync.Mutex
}

func (n *NodeStatusManager) SetBatchFinalizer(f BatchRecordFinalizer) {
	n.batchRecordFinalizer = f
}

func (n *NodeStatusManager) newExpireTime(idx int) time.Time {
	return time.Now().Add(time.Duration(n.nRandGen[idx].Intn(_serviceTTLRandomInc)) * time.Second)
}

func (n *NodeStatusManager) shardKey(key string) int {
	return int(hash.DoMurmur3([]byte(key)) % n.nWorker)
}

func (n *NodeStatusManager) StartNode(recordKey *RecordKey) error {
	key := recordKey.GetKey()
	idx := n.shardKey(key)
	n.nTimeoutLock[idx].Lock()
	defer n.nTimeoutLock[idx].Unlock()

	en, ok := n.nTimeoutMap[idx][key]
	if ok {
		en.ExpireTime = n.newExpireTime(idx)
		return nil
	}

	newEntry := &TimeoutEntry{
		RecordKey:  recordKey,
		ExpireTime: n.newExpireTime(idx),
	}
	n.nTimeoutQueue[idx].Push(newEntry, int(newEntry.ExpireTime.UnixMilli()))
	n.nTimeoutMap[idx][key] = newEntry

	return nil
}

func (n *NodeStatusManager) KeepAlive(recordKey *RecordKey) error {
	key := recordKey.GetKey()
	idx := n.shardKey(key)
	n.nTimeoutLock[idx].Lock()
	defer n.nTimeoutLock[idx].Unlock()

	en, ok := n.nTimeoutMap[idx][key]
	if ok {
		en.ExpireTime = n.newExpireTime(idx)
		return nil
	} else {
		return nil
	}
}

func (n *NodeStatusManager) Offline(recordKey *RecordKey) error {
	key := recordKey.GetKey()
	idx := n.shardKey(key)
	n.nTimeoutLock[idx].Lock()
	defer n.nTimeoutLock[idx].Unlock()

	en, ok := n.nTimeoutMap[idx][key]
	if ok {
		if err := n.batchRecordFinalizer([]*RecordKey{recordKey}); err != nil {
			return err
		}
		delete(n.nTimeoutMap[idx], key)
		en.Deleted = true
	}

	return nil
}

func (n *NodeStatusManager) nLoop(idx int) func() bool {
	return func() bool {
		ticker := time.NewTicker(_keepAliveLoopInterview * time.Second) // walk through all node interval
		threshold := _serviceTTL * time.Second                          // timeout to cleanup

		for {
			_ = <-ticker.C // do not use the time from ticker
			now := time.Now()
			n.foreachEntries(now, threshold, idx)
		}
	}
}

func (n *NodeStatusManager) foreachEntries(now time.Time, threshold time.Duration, idx int) {
	n.nTimeoutLock[idx].Lock()
	defer n.nTimeoutLock[idx].Unlock()

	startTime := time.Now()
	if n.nTimeoutQueue[idx].IsEmpty() {
		_nodeStatusLogger.Infof("node status checker[%d] - empty", idx)
		return
	}
	var pendingCleanRecordKeys = make([]*RecordKey, 0, 32)
	for !n.nTimeoutQueue[idx].IsEmpty() {
		entry := n.nTimeoutQueue[idx].Peak()
		if startTime.Sub(entry.ExpireTime) > threshold {
			// timeout
			pendingCleanRecordKeys = append(pendingCleanRecordKeys, entry.RecordKey)
			delete(n.nTimeoutMap[idx], entry.RecordKey.GetKey())
			n.nTimeoutQueue[idx].Pop()
		} else if entry.Deleted {
			// delete
			pendingCleanRecordKeys = append(pendingCleanRecordKeys, entry.RecordKey)
			delete(n.nTimeoutMap[idx], entry.RecordKey.GetKey())
			n.nTimeoutQueue[idx].Pop()
		} else {
			break
		}
	}

	if len(pendingCleanRecordKeys) > 0 {
		if err := n.batchRecordFinalizer(pendingCleanRecordKeys); err != nil {
			_nodeStatusLogger.Errorf("batch finalier in node status manager shoudl not error but occurs: %s", err)
		} else {
			_nodeStatusLogger.Infof("batch clean success[%d]", idx)
		}
	}

	_nodeStatusLogger.Infof("node status checker[%d] - removed records:[%d], rest records:[%d], time: [%d]ms", idx, len(pendingCleanRecordKeys), n.nTimeoutQueue[idx].Size(), time.Now().UnixMilli()-startTime.UnixMilli())
}

func NewNodeStatusManager() *NodeStatusManager {
	mgr := new(NodeStatusManager)
	mgr.nWorker = 4
	var i uint32 = 0
	for ; i < mgr.nWorker; i++ {
		mgr.nRandGen = append(mgr.nRandGen, rand.New(rand.NewSource(int64(time.Now().Nanosecond()))))
		mgr.nTimeoutMap = append(mgr.nTimeoutMap, map[string]*TimeoutEntry{})
		mgr.nTimeoutQueue = append(mgr.nTimeoutQueue, priorityqueue.NewMinPriorityQueue[*TimeoutEntry](
			priorityqueue.WithPreallocateSize[*TimeoutEntry](8*65536),
		))
		mgr.nTimeoutLock = append(mgr.nTimeoutLock, new(sync.Mutex))
		workgroup.WithFailOver().Run(mgr.nLoop(int(i)))
	}

	return mgr
}
