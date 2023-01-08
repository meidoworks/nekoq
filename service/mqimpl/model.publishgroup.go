package mqimpl

import (
	"sync"

	"github.com/meidoworks/nekoq/service/mqapi"
)

var _ mqapi.PublishGroup = new(PublishGroup)

type PublishGroup struct {
	publishGroupID mqapi.PublishGroupId

	topicMap  map[mqapi.TopicId]*Topic
	basicLock sync.Mutex
}

// at most once
func (pg *PublishGroup) PublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	topic, ok := pg.topicMap[req.Header.TopicId]
	if !ok {
		return EMPTY_MESSAGE_ID_LIST, mqapi.ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != mqapi.AtMostOnce || topic.deliveryLevel != deliveryLevel {
		return EMPTY_MESSAGE_ID_LIST, mqapi.ErrDeliveryLevelNotMatch
	}
	ack, err := topic.PublishMessage(req, ctx)
	return ack, err
}

// at least once
func (pg *PublishGroup) PublishGuaranteeMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	topic, ok := pg.topicMap[req.Header.TopicId]
	if !ok {
		return EMPTY_MESSAGE_ID_LIST, mqapi.ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != mqapi.AtLeastOnce || topic.deliveryLevel != deliveryLevel {
		return EMPTY_MESSAGE_ID_LIST, mqapi.ErrDeliveryLevelNotMatch
	}
	return topic.PublishMessageWithResponse(req, ctx)
}

// exactly once
func (pg *PublishGroup) PrePublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.MessageReceived, error) {
	topic, ok := pg.topicMap[req.Header.TopicId]
	if !ok {
		return mqapi.MessageReceived{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, mqapi.ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != mqapi.ExactlyOnce || topic.deliveryLevel != deliveryLevel {
		return mqapi.MessageReceived{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, mqapi.ErrDeliveryLevelNotMatch
	}
	ack, err := topic.PrePublishMessage(req, ctx)
	return mqapi.MessageReceived{
		Ack: ack,
	}, err
}

// exactly once
func (pg *PublishGroup) CommitMessage(req *mqapi.MessageCommit, ctx *mqapi.Ctx) (mqapi.MessageFinish, error) {
	topic, ok := pg.topicMap[req.Header.TopicId]
	if !ok {
		return mqapi.MessageFinish{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, mqapi.ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != mqapi.ExactlyOnce || topic.deliveryLevel != deliveryLevel {
		return mqapi.MessageFinish{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, mqapi.ErrDeliveryLevelNotMatch
	}
	ack, err := topic.CommitMessages(req, ctx)
	return mqapi.MessageFinish{
		Ack: ack,
	}, err
}

func (pg *PublishGroup) Reply(reply *mqapi.Reply, ctx *mqapi.Ctx) error {
	//TODO
	return nil
}

func (pg *PublishGroup) Join(node mqapi.Node) error {
	return node.PublishGroupInitialize(pg)
}

func (pg *PublishGroup) Leave(node *Node) {
	//TODO
}
