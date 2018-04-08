package mq

import "errors"

var (
	ErrTopicAlreadyExist          = errors.New("topic already exists")
	ErrQueueAlreadyExist          = errors.New("queue already exists")
	ErrPublishGroupAlreadyExist   = errors.New("publishGroup already exists")
	ErrSubscribeGroupAlreadyExist = errors.New("subscribeGroup already exists")

	ErrTopicNotExist          = errors.New("topic not exists")
	ErrQueueNotExist          = errors.New("queue not exists")
	ErrPublishGroupNotExist   = errors.New("publishGroup not exist")
	ErrSubscribeGroupNotExist = errors.New("subscribeGroup not exist")
	ErrNodeNotExist           = errors.New("node not exist")

	ErrTopicInternalIdExceeded = errors.New("topic internal id exceeded")

	ErrDeliveryLevelNotMatch         = errors.New("delivery level not match")
	ErrDeliveryLevelUnknown          = errors.New("delivery level unknown")
	ErrDeliveryLevelIllegalOperation = errors.New("illegal delivery level operation")

	ErrQueueStoreUnknown      = errors.New("queue store type unknown")
	ErrQueueStoreNotSupported = errors.New("queue store type not supported")

	ErrReplyTypeUnknown         = errors.New("reply type unknown")
	ErrReplyDestinationNotExist = errors.New("reply destination not exist")
)
