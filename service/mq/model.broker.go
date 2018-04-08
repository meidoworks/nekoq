package mq

import (
	"sync"
	"sync/atomic"
)

type Broker struct {
	topicMap        map[IdType]*Topic
	queueMap        map[IdType]*Queue
	publishGroupMap map[IdType]*PublishGroup
	subscribeGroup  map[IdType]*SubscribeGroup
	basicLock       sync.Mutex

	clientNodeMap     map[IdType]*Node
	clientNodeMapLock sync.RWMutex

	nodeId int16

	//ephemeral
	topicInternalId int32
	queueInternalId int32
}

type BrokerOption struct {
	NodeId int16
}

func NewBroker(option *BrokerOption) *Broker {
	broker := new(Broker)
	broker.topicMap = make(map[IdType]*Topic)
	broker.queueMap = make(map[IdType]*Queue)
	broker.publishGroupMap = make(map[IdType]*PublishGroup)
	broker.subscribeGroup = make(map[IdType]*SubscribeGroup)
	broker.nodeId = option.NodeId
	broker.topicInternalId = 0
	broker.queueInternalId = 0
	broker.clientNodeMap = make(map[IdType]*Node)
	return broker
}

func (this *Broker) GenNewInternalTopicId() (int32, error) {
	old := this.topicInternalId
	for !atomic.CompareAndSwapInt32(&this.topicInternalId, old, old+1) {
		old = this.topicInternalId
	}
	result := old + 1
	if result < 0x7FFFFFFF {
		return result, nil
	}
	return 0, ErrTopicInternalIdExceeded
}

func (this *Broker) GenNewInternalQueueId() (int32, error) {
	old := this.queueInternalId
	for !atomic.CompareAndSwapInt32(&this.queueInternalId, old, old+1) {
		old = this.queueInternalId
	}
	result := old + 1
	if result < 0x7FFFFFFF {
		return result, nil
	}
	return 0, ErrTopicInternalIdExceeded
}

func (this *Broker) GetNode(nodeId IdType) (*Node, error) {
	this.clientNodeMapLock.RLock()
	node, ok := this.clientNodeMap[nodeId]
	this.clientNodeMapLock.RUnlock()
	if !ok {
		return nil, ErrNodeNotExist
	}
	return node, nil
}
