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

type SubscribeGroup struct {
	subscribeGroupID IdType

	queueMap  map[IdType]SgQMap
	basicLock sync.Mutex

	SubCh chan SubChanElem
}

type SubscribeGroupOption struct {
	SubscribeChannelSize int
}

func (this *Broker) NewSubscribeGroup(subscribeGroupId IdType, option *SubscribeGroupOption) (*SubscribeGroup, error) {
	sg := new(SubscribeGroup)
	sg.subscribeGroupID = subscribeGroupId
	sg.queueMap = make(map[IdType]SgQMap)
	sg.SubCh = make(chan SubChanElem, option.SubscribeChannelSize)
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
	//TODO support qos
	out := this.SubCh
	errCnt := 0
	for {
		result, err := queue.BatchObtain(record, 16, nil)
		if err != nil {
			logError("subscribeGroup loop error while batchObtain:", err)
			if result.Requests == nil || len(result.Requests) == 0 {
				// fall through and process messages
			} else {
				// handle error: retry and limit retry
				errCnt++
				if errCnt > 10 {
					time.Sleep(100 * time.Millisecond)
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

// qos - at least once - ack
// qos - exactly once - commit
func (this *SubscribeGroup) Commit(queue *Queue, record *QueueRecord, ack *Ack) error {
	return queue.ConfirmConsumed(record, ack)
}

func (this *SubscribeGroup) Join(node *Node) error {
	node.InitFunc(this)
	return nil
}

func (this *SubscribeGroup) Leave(node *Node) {
	//TODO
}
