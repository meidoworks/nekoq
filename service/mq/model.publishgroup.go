package mq

import "sync"

type PublishGroup struct {
	publishGroupID IdType

	topicMap  map[IdType]*Topic
	basicLock sync.Mutex
}

func (this *Broker) NewPublishGroup(publishGroupId IdType) (*PublishGroup, error) {
	pg := new(PublishGroup)
	pg.publishGroupID = publishGroupId
	pg.topicMap = make(map[IdType]*Topic)
	err := this.addPublishGroup(pg)
	if err != nil {
		return nil, err
	}

	return pg, nil
}

func (this *Broker) addPublishGroup(publishGroup *PublishGroup) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.publishGroupMap[publishGroup.publishGroupID]; ok {
		return ErrPublishGroupAlreadyExist
	}

	c := make(chan map[IdType]*PublishGroup)
	go func() {
		newMap := make(map[IdType]*PublishGroup)
		for k, v := range this.publishGroupMap {
			newMap[k] = v
		}
		newMap[publishGroup.publishGroupID] = publishGroup

		c <- newMap
	}()
	this.publishGroupMap = <-c
	return nil
}

func (this *Broker) deletePublishGroup(publishGroupId IdType) error {
	//TODO
	return nil
}

// at most once
func (this *PublishGroup) PublishMessage(req *Request, ctx *Ctx) error {
	topic, ok := this.topicMap[req.Header.TopicId]
	if !ok {
		return ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != AtMostOnce || topic.deliveryLevel != deliveryLevel {
		return ErrDeliveryLevelNotMatch
	}
	return topic.PublishMessage(req, ctx)
}

// at least once
func (this *PublishGroup) PublishGuaranteeMessage(req *Request, ctx *Ctx) (Ack, error) {
	topic, ok := this.topicMap[req.Header.TopicId]
	if !ok {
		return EMPTY_MESSAGE_ID_LIST, ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != AtLeastOnce || topic.deliveryLevel != deliveryLevel {
		return EMPTY_MESSAGE_ID_LIST, ErrDeliveryLevelNotMatch
	}
	return topic.PublishMessageWithResponse(req, ctx)
}

// exactly once
func (this *PublishGroup) PrePublishMessage(req *Request, ctx *Ctx) (MessageReceived, error) {
	topic, ok := this.topicMap[req.Header.TopicId]
	if !ok {
		return MessageReceived{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != ExactlyOnce || topic.deliveryLevel != deliveryLevel {
		return MessageReceived{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, ErrDeliveryLevelNotMatch
	}
	ack, err := topic.PublishMessageWithResponse(req, ctx)
	return MessageReceived{
		Ack: ack,
	}, err
}

// exactly once
func (this *PublishGroup) CommitMessage(req *MessageCommit, ctx *Ctx) (MessageFinish, error) {
	topic, ok := this.topicMap[req.Header.TopicId]
	if !ok {
		return MessageFinish{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, ErrTopicNotExist
	}
	deliveryLevel := req.Header.DeliveryLevel
	if deliveryLevel != ExactlyOnce || topic.deliveryLevel != deliveryLevel {
		return MessageFinish{
			Ack: EMPTY_MESSAGE_ID_LIST,
		}, ErrDeliveryLevelNotMatch
	}
	ack, err := topic.CommitMessages(req, ctx)
	return MessageFinish{
		Ack: ack,
	}, err
}

func (this *PublishGroup) Reply(reply *Reply, ctx *Ctx) error {
	//TODO
	return nil
}

func (this *PublishGroup) Join(node *Node) error {
	//TODO
	return nil
}

func (this *PublishGroup) Leave(node *Node) {
	//TODO
}
