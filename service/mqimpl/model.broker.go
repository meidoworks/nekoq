package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"sync"
	"sync/atomic"
)

var _ mqapi.Broker = new(Broker)

type Broker struct {
	topicMap        map[mqapi.TopicId]*Topic
	queueMap        map[mqapi.QueueId]*Queue
	publishGroupMap map[mqapi.PublishGroupId]*PublishGroup
	subscribeGroup  map[mqapi.SubscribeGroupId]*SubscribeGroup
	basicLock       sync.Mutex

	clientNodeMap     map[mqapi.NodeId]*Node
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
	broker.topicMap = make(map[mqapi.TopicId]*Topic)
	broker.queueMap = make(map[mqapi.QueueId]*Queue)
	broker.publishGroupMap = make(map[mqapi.PublishGroupId]*PublishGroup)
	broker.subscribeGroup = make(map[mqapi.SubscribeGroupId]*SubscribeGroup)
	broker.nodeId = option.NodeId
	broker.topicInternalId = 0
	broker.queueInternalId = 0
	broker.clientNodeMap = make(map[mqapi.NodeId]*Node)
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
	return 0, mqapi.ErrTopicInternalIdExceeded
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
	return 0, mqapi.ErrTopicInternalIdExceeded
}

func (this *Broker) GetNode(nodeId mqapi.NodeId) (*Node, error) {
	this.clientNodeMapLock.RLock()
	node, ok := this.clientNodeMap[nodeId]
	this.clientNodeMapLock.RUnlock()
	if !ok {
		return nil, mqapi.ErrNodeNotExist
	}
	return node, nil
}
