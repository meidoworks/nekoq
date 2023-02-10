package discovery

import (
	"math/rand"
	"sync"
	"time"

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

	next *TimeoutEntry
	prev *TimeoutEntry
}

type RecordFinalizer func(recordKey *RecordKey) error

type NodeStatusManager struct {
	recordFinalizer RecordFinalizer

	randGen *rand.Rand

	timeoutMap       map[string]*TimeoutEntry
	timeoutEntryHead *TimeoutEntry
	timeoutQueue     *priorityqueue.PriorityQueue[*TimeoutEntry]
	timeoutLock      sync.Mutex
}

func (n *NodeStatusManager) SetFinalizer(f RecordFinalizer) {
	n.recordFinalizer = f
}

func (n *NodeStatusManager) newExpireTime() time.Time {
	return time.Now().Add(time.Duration(n.randGen.Intn(_serviceTTLRandomInc)) * time.Second)
}

func (n *NodeStatusManager) StartNode(recordKey *RecordKey) error {
	n.timeoutLock.Lock()
	defer n.timeoutLock.Unlock()

	en, ok := n.timeoutMap[recordKey.GetKey()]
	if ok {
		en.ExpireTime = n.newExpireTime()
		return nil
	}

	newEntry := &TimeoutEntry{
		RecordKey:  recordKey,
		ExpireTime: n.newExpireTime(),
		next:       n.timeoutEntryHead.next,
		prev:       n.timeoutEntryHead,
	}
	n.timeoutQueue.Push(newEntry, int(newEntry.ExpireTime.UnixMilli()))
	n.timeoutMap[recordKey.GetKey()] = newEntry

	return nil
}

func (n *NodeStatusManager) KeepAlive(recordKey *RecordKey) error {
	n.timeoutLock.Lock()
	defer n.timeoutLock.Unlock()

	en, ok := n.timeoutMap[recordKey.GetKey()]
	if ok {
		en.ExpireTime = n.newExpireTime()
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
		en.Deleted = true
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

	startTime := time.Now()
	if n.timeoutQueue.IsEmpty() {
		_nodeStatusLogger.Infof("node status checker - empty")
		return
	}
	count := 0
	for !n.timeoutQueue.IsEmpty() {
		entry := n.timeoutQueue.Peak()
		if startTime.Sub(entry.ExpireTime) > threshold {
			// timeout
			if err := n.recordFinalizer(entry.RecordKey); err != nil {
				_nodeStatusLogger.Errorf("service finalize error: %s", err)
				continue
			}
			delete(n.timeoutMap, entry.RecordKey.GetKey())
			n.timeoutQueue.Pop()
			count++
		} else if entry.Deleted {
			// delete
			if err := n.recordFinalizer(entry.RecordKey); err != nil {
				_nodeStatusLogger.Errorf("service finalize error: %s", err)
				continue
			}
			delete(n.timeoutMap, entry.RecordKey.GetKey())
			n.timeoutQueue.Pop()
			count++
		} else {
			break
		}
	}
	_nodeStatusLogger.Infof("node status checker - removed records:[%d], rest records:[%d], time: [%d]ms", count, n.timeoutQueue.Size(), time.Now().UnixMilli()-startTime.UnixMilli())
}

func NewNodeStatusManager() *NodeStatusManager {
	mgr := new(NodeStatusManager)
	mgr.timeoutMap = map[string]*TimeoutEntry{}
	mgr.timeoutEntryHead = new(TimeoutEntry)
	mgr.randGen = rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	mgr.timeoutQueue = priorityqueue.NewMinPriorityQueue[*TimeoutEntry](
		priorityqueue.WithPreallocateSize[*TimeoutEntry](8 * 65536),
	)

	workgroup.WithFailOver().Run(mgr.KeepAliveLoop)

	return mgr
}
