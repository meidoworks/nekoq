package mqapi

type TopicId IdType
type QueueId IdType
type TagId IdType
type PublishGroupId IdType
type SubscribeGroupId IdType
type NodeId IdType

type PartitionId IdType

type MsgId IdType
type OutId IdType

type DeliveryLevelType byte

const (
	AtMostOnce  DeliveryLevelType = 0
	AtLeastOnce DeliveryLevelType = 1
	ExactlyOnce DeliveryLevelType = 2
)

type TopicOption struct {
	DeliveryLevel DeliveryLevelType
}

type QueueStoreType byte

const (
	CUSTOM_STORE QueueStoreType = 0
	MEM_STORE    QueueStoreType = 1
	FILE_STORE   QueueStoreType = 2
)

type QueueOption struct {
	QueueChannelSize        int
	QueueInboundChannelSize int

	DeliveryLevel  DeliveryLevelType
	QueueStoreType QueueStoreType

	UncommittedMessageRetainTime int // in seconds, default 7 * 24 * 3600
	RedeliverIntervalTime        int // in seconds, default 5 seconds

	// custom queue type
	CustomQueueTypeInst QueueType
}

type SubscribeGroupOption struct {
	SubscribeChannelSize    int
	ObtainFailRetryInterval int // milliseconds, default 100ms
}

type SubChanElem struct {
	Request     *Request
	Queue       Queue
	QueueRecord *QueueRecord
}

type ReleaseChanElem struct {
	Request     *Request
	Queue       Queue
	QueueRecord *QueueRecord
}
