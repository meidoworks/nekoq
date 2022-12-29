package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
)

var _ mqapi.Queue = new(Queue)

type Queue struct {
	queueID       mqapi.QueueId
	deliveryLevel mqapi.DeliveryLevelType

	UncommittedMessageRetainTime int // in seconds, default 7 * 24 * 3600
	InitBatchObtainCount         int // 16
	MaxBatchObtainCount          int // 1024

	QueueChannel chan *mqapi.Request

	QueueInternalId int32

	// internal
	mqapi.QueueType
}

func (this *Queue) QueueId() mqapi.QueueId {
	return this.queueID
}

func (this *Queue) DeliveryLevel() mqapi.DeliveryLevelType {
	return this.deliveryLevel
}

type Partition struct {
}

// special queue
// 1. non-durable
// 2. always fan out to all subscribers
// 3. can have responses to publisher
type Subject struct {
}
