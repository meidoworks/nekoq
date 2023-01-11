package mqapi

import "context"

type PublishGroup interface {
	Join(node Node) error

	PublishMessage(req *Request, ctx *Ctx) (Ack, error)
	PublishGuaranteeMessage(req *Request, ctx *Ctx) (Ack, error)
	PrePublishMessage(req *Request, ctx *Ctx) (MessageReceived, error)
	CommitMessage(req *MessageCommit, ctx *Ctx) (MessageFinish, error)

	Reply(reply *Reply, ctx *Ctx) error
}

type Topic interface {
	DeliveryLevel() DeliveryLevelType
	TopicId() TopicId
}

type Queue interface {
	DeliveryLevel() DeliveryLevelType
	QueueId() QueueId
}

type SubscribeGroup interface {
	SubscribeChannel() <-chan SubChanElem
	ReleaseChannel() <-chan ReleaseChanElem

	Join(node Node) error

	Commit(queueId QueueId, record *QueueRecord, ack *Ack) error
	Release(queueId QueueId, record *QueueRecord, ack *Ack) error
}

type Node interface {
	SubscribeGroupInitialize(sg SubscribeGroup) error
	PublishGroupInitialize(pg PublishGroup) error

	DirectReply(reply *Reply, ctx *Ctx) error
	GetReplyChannel() <-chan *Reply

	GetNodeId() NodeId

	// Leave means the end of Node lifecycle
	Leave() error
}

type brokerLifecycle interface {
	Start() error
	Shutdown(ctx context.Context) error
}

type brokerManagement interface {
	DefineNewTopic(topicId TopicId, option *TopicOption) (Topic, error)
	DeleteTopic(topicId TopicId) error
	DefineNewQueue(queueId QueueId, option *QueueOption) (Queue, error)
	DeleteQueue(queueId QueueId) error
	BindTopicAndQueue(topicId TopicId, queueId QueueId, tags []TagId) error
	UnbindTopicAndQueue(topicId TopicId, queueId QueueId) error
	DefineNewPublishGroup(publishGroupId PublishGroupId) (PublishGroup, error)
	DeletePublishGroup(publishGroupId PublishGroupId) error
	BindPublishGroupToTopic(publishGroupId PublishGroupId, topicId TopicId) error
	UnbindPublishGroupFromTopic(publishGroupId PublishGroupId, topic TopicId) error
	DefineNewSubscribeGroup(subscribeGroupId SubscribeGroupId, option *SubscribeGroupOption) (SubscribeGroup, error)
	DeleteSubscribeGroup(subscribeGroupId SubscribeGroupId) error
	BindSubscribeGroupToQueue(subscribeGroupId SubscribeGroupId, queueId QueueId) error
	UnbindSubscribeGroupFromQueue(subscribeGroupId SubscribeGroupId, queueId QueueId) error

	AddNode() (Node, error)

	GetSubscribeGroup(subscribeGroupId SubscribeGroupId) SubscribeGroup
	GetPublishGroup(publishGroupId PublishGroupId) PublishGroup
	GetNode(nodeId NodeId) Node
}

type Broker interface {
	brokerLifecycle
	brokerManagement
}
