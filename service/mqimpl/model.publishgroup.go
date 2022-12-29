package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"sync"
)

var _ mqapi.PublishGroup = new(PublishGroup)

type PublishGroup struct {
	publishGroupID mqapi.PublishGroupId

	topicMap  map[mqapi.TopicId]*Topic
	basicLock sync.Mutex
}

// at most once
func (this *PublishGroup) PublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) error {
	topic, ok := this.topicMap[req.Header.TopicId]
	if !ok {
		return mqapi.ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != mqapi.AtMostOnce || topic.deliveryLevel != deliveryLevel {
		return mqapi.ErrDeliveryLevelNotMatch
	}
	return topic.PublishMessage(req, ctx)
}

// at least once
func (this *PublishGroup) PublishGuaranteeMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	topic, ok := this.topicMap[req.Header.TopicId]
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
func (this *PublishGroup) PrePublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.MessageReceived, error) {
	topic, ok := this.topicMap[req.Header.TopicId]
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
	ack, err := topic.PublishMessageWithResponse(req, ctx)
	return mqapi.MessageReceived{
		Ack: ack,
	}, err
}

// exactly once
func (this *PublishGroup) CommitMessage(req *mqapi.MessageCommit, ctx *mqapi.Ctx) (mqapi.MessageFinish, error) {
	topic, ok := this.topicMap[req.Header.TopicId]
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

func (this *PublishGroup) Reply(reply *mqapi.Reply, ctx *mqapi.Ctx) error {
	//TODO
	return nil
}

func (this *PublishGroup) Join(node mqapi.Node) error {
	return node.PublishGroupInitialize(this)
}

func (this *PublishGroup) Leave(node *Node) {
	//TODO
}
