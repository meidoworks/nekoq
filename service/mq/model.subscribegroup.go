package mq

import "sync"

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
		for k, v := range this.subscribeGroup {
			newMap[k] = v
		}
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
	for {
		result, err := queue.BatchObtain(record, 1024, nil)
		if err != nil {
			//TODO handle error
			return
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
