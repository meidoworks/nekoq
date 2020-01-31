package mq

import (
	"sync"
	"time"
)

type SgQMap struct {
	q  *Queue
	qr *QueueRecord
}

type SubChanElem struct {
	Request     *Request
	Queue       *Queue
	QueueRecord *QueueRecord
}

type ReleaseChanElem struct {
	Request     *Request
	Queue       *Queue
	QueueRecord *QueueRecord
}

type SubscribeGroup struct {
	subscribeGroupID IdType

	queueMap  map[IdType]SgQMap
	basicLock sync.Mutex

	obtainFailRetryInterval int // milliseconds, default 100ms

	SubCh     chan SubChanElem
	ReleaseCh chan ReleaseChanElem
}

type SubscribeGroupOption struct {
	SubscribeChannelSize    int
	ObtainFailRetryInterval int // milliseconds, default 100ms
}

func (this *Broker) NewSubscribeGroup(subscribeGroupId IdType, option *SubscribeGroupOption) (*SubscribeGroup, error) {
	sg := new(SubscribeGroup)
	sg.subscribeGroupID = subscribeGroupId
	sg.queueMap = make(map[IdType]SgQMap)
	sg.SubCh = make(chan SubChanElem, option.SubscribeChannelSize)
	sg.ReleaseCh = make(chan ReleaseChanElem, option.SubscribeChannelSize)
	if option.ObtainFailRetryInterval <= 0 {
		sg.obtainFailRetryInterval = 100
	} else {
		sg.obtainFailRetryInterval = option.ObtainFailRetryInterval
	}
	err := this.addSubscribeGroup(sg)
	if err != nil {
		return nil, err
	}

	return sg, nil
}

func (this *Broker) addSubscribeGroup(subscribeGroup *SubscribeGroup) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.subscribeGroup[subscribeGroup.subscribeGroupID]; ok {
		return ErrSubscribeGroupAlreadyExist
	}

	c := make(chan map[IdType]*SubscribeGroup)
	go func() {
		newMap := make(map[IdType]*SubscribeGroup)
		// copy old kv
		for k, v := range this.subscribeGroup {
			newMap[k] = v
		}
		// add new
		newMap[subscribeGroup.subscribeGroupID] = subscribeGroup

		c <- newMap
	}()
	this.subscribeGroup = <-c
	return nil
}

func (this *Broker) deleteSubscribeGroup(subscribeGroupId IdType) error {
	//TODO
	return nil
}

func (this *SubscribeGroup) Loop(record *QueueRecord, queue *Queue) {
	out := this.SubCh
	errCnt := 0
	obtainFailRetryInterval := this.obtainFailRetryInterval
	for {
		result, err := queue.BatchObtain(record, 16, nil)
		if err != nil {
			LogError("subscribeGroup loop error while batchObtain:", err)
			if result.Requests != nil && len(result.Requests) > 0 {
				// fall through and process messages
			} else {
				// handle error: retry and limit retry
				errCnt++
				if errCnt > 3 {
					time.Sleep(time.Duration(obtainFailRetryInterval) * time.Millisecond)
					errCnt = 0
				}
				continue
			}
		}
		for _, v := range result.Requests {
			out <- SubChanElem{
				Request:     v,
				Queue:       queue,
				QueueRecord: record,
			}
		}
	}
}

func (this *SubscribeGroup) ReleaseLoop(record *QueueRecord, queue *Queue) {
	out := this.ReleaseCh
	errCnt := 0
	obtainFailRetryInterval := this.obtainFailRetryInterval
	for {
		result, err := queue.BatchObtainReleased(record, 16, nil)
		if err != nil {
			LogError("subscribeGroup loop error while batchObtain:", err)
			if result.Requests != nil && len(result.Requests) > 0 {
				// fall through and process messages
			} else {
				// handle error: retry and limit retry
				errCnt++
				if errCnt > 3 {
					time.Sleep(time.Duration(obtainFailRetryInterval) * time.Millisecond)
					errCnt = 0
				}
				continue
			}
		}
		for _, v := range result.Requests {
			out <- ReleaseChanElem{
				Request:     v,
				Queue:       queue,
				QueueRecord: record,
			}
		}
	}
}

// qos - at least once - ack
// qos - exactly once - commit
func (this *SubscribeGroup) Commit(queueId IdType, record *QueueRecord, ack *Ack) error {
	queue, ok := this.queueMap[queueId]
	if !ok {
		return ErrQueueNotExist
	}
	return queue.q.ConfirmConsumed(record, ack)
}

// qos - exactly once - release
func (this *SubscribeGroup) Release(queueId IdType, record *QueueRecord, ack *Ack) error {
	queue, ok := this.queueMap[queueId]
	if !ok {
		return ErrQueueNotExist
	}
	return queue.q.ReleaseConsumed(record, ack)
}

func (this *SubscribeGroup) Join(node *Node) error {
	node.InitFunc(this)
	return nil
}

func (this *SubscribeGroup) Leave(node *Node) {
	//TODO
}
