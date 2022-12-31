package mqapi

import "github.com/meidoworks/nekoq/shared/idgen"

type TopicId idgen.IdType
type QueueId idgen.IdType
type TagId idgen.IdType
type PublishGroupId idgen.IdType
type SubscribeGroupId idgen.IdType
type NodeId idgen.IdType

type PartitionId idgen.IdType

type MsgId idgen.IdType
type OutId idgen.IdType

type DeliveryLevelType byte

const (
	AtMostOnce  DeliveryLevelType = 0
	AtLeastOnce DeliveryLevelType = 1
	ExactlyOnce DeliveryLevelType = 2
)

type TopicOption struct {
	DeliveryLevel DeliveryLevelType
}

type QueueOption struct {
	QueueChannelSize        int
	QueueInboundChannelSize int

	DeliveryLevel DeliveryLevelType
	QueueType     string

	UncommittedMessageRetainTime int // in seconds, default 7 * 24 * 3600
	RedeliverIntervalTime        int // in seconds, default 5 seconds
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
