package mqimpl

import (
	"sync"
	"sync/atomic"

	"github.com/meidoworks/nekoq/service/mqapi"
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

func (b *Broker) GenNewInternalTopicId() (int32, error) {
	old := b.topicInternalId
	for !atomic.CompareAndSwapInt32(&b.topicInternalId, old, old+1) {
		old = b.topicInternalId
	}
	result := old + 1
	if result < 0x7FFFFFFF {
		return result, nil
	}
	return 0, mqapi.ErrTopicInternalIdExceeded
}

func (b *Broker) GenNewInternalQueueId() (int32, error) {
	old := b.queueInternalId
	for !atomic.CompareAndSwapInt32(&b.queueInternalId, old, old+1) {
		old = b.queueInternalId
	}
	result := old + 1
	if result < 0x7FFFFFFF {
		return result, nil
	}
	return 0, mqapi.ErrTopicInternalIdExceeded
}

func (b *Broker) GetNode(nodeId mqapi.NodeId) (*Node, error) {
	b.clientNodeMapLock.RLock()
	node, ok := b.clientNodeMap[nodeId]
	b.clientNodeMapLock.RUnlock()
	if !ok {
		return nil, mqapi.ErrNodeNotExist
	}
	return node, nil
}
