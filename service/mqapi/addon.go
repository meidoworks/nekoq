package mqapi

import (
	"context"

	"github.com/meidoworks/nekoq/shared/idgen"
)

type RecordOffset idgen.IdType

type QueueRecord struct {
	FromId RecordOffset
}

type QueueType interface {
	PublishMessage(req *Request, ctx *Ctx) error
	PrePublishMessage(req *Request, ctx *Ctx) error
	CommitMessages(commit *MessageCommit, ctx *Ctx) error

	CreateRecord(subscribeGroupId SubscribeGroupId, ctx *Ctx) (*QueueRecord, error)
	BatchObtain(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error)
	BatchObtainReleased(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error)
	ConfirmConsumed(record *QueueRecord, ack *Ack) error
	ReleaseConsumed(record *QueueRecord, ack *Ack) error

	Init(queue Queue, option *QueueOption) error
	Close(ctx context.Context) error
}
