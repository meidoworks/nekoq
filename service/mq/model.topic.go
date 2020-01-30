package mq

import (
	"sync"
)

type Topic struct {
	topicID       IdType
	deliveryLevel DeliveryLevelType

	queueList []*Queue
	queueMap  map[IdType][]*Queue
	basicLock sync.Mutex

	topicInternalId   int32
	topicMessageIdGen *IdGen

	broker *Broker
}

type TopicOption struct {
	DeliveryLevel DeliveryLevelType
}

func (this *Broker) NewTopic(topicId IdType, option *TopicOption) (*Topic, error) {
	t := new(Topic)
	t.topicID = topicId
	t.queueList = []*Queue{}
	t.queueMap = make(map[IdType][]*Queue)
	switch option.DeliveryLevel {
	case AtMostOnce:
		t.deliveryLevel = AtMostOnce
	case AtLeastOnce:
		t.deliveryLevel = AtLeastOnce
	case ExactlyOnce:
		t.deliveryLevel = ExactlyOnce
	default:
		t.deliveryLevel = AtMostOnce
	}
	topicInternalId, err := this.GenNewInternalTopicId()
	if err != nil {
		return nil, err
	}
	t.topicMessageIdGen = NewIdGen(this.nodeId, topicInternalId)
	t.topicInternalId = topicInternalId
	t.broker = this

	err = this.addTopic(t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (this *Broker) addTopic(topic *Topic) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.topicMap[topic.topicID]; ok {
		return ErrTopicAlreadyExist
	}

	c := make(chan map[IdType]*Topic)
	go func() {
		newMap := make(map[IdType]*Topic)
		for k, v := range this.topicMap {
			newMap[k] = v
		}
		newMap[topic.topicID] = topic

		c <- newMap
	}()
	this.topicMap = <-c
	return nil
}

func (this *Broker) deleteTopic(topicId IdType) error {
	//TODO
	return nil
}

func (this *Topic) PreQueue(req *Request) {
	//TODO
	//TODO features: pre-persistent, status check callback
}

func (this *Topic) PublishMessage(req *Request, ctx *Ctx) error {
	tags := req.Header.Tags
	queueList := this.queueList

	// generate message id
	idgen := this.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	for i := 0; i < msgCnt; i++ {
		var err error
		messages[i].MsgId, err = idgen.Next()
		if err != nil {
			return err
		}
	}

	this.PreQueue(req)

	if len(tags) == 0 {
		// all queues
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.PublishMessage(req, ctx)
			if err != nil {
				return err
			}
		}
	} else {
		// specified queues
		m := make(map[int32]*Queue)
		queueMap := this.queueMap
		for _, tag := range tags {
			queues, ok := queueMap[tag]
			if ok {
				for _, q := range queues {
					id := q.QueueInternalId
					_, ok := m[id]
					if !ok {
						m[q.QueueInternalId] = q
						err := q.PublishMessage(req, ctx)
						if err != nil {
							return err
						}
					}
				}
			}
		}
	}

	return nil
}

// at least once and exactly once
// omit dup flag
func (this *Topic) PublishMessageWithResponse(req *Request, ctx *Ctx) (Ack, error) {
	tags := req.Header.Tags
	queueList := this.queueList

	// generate message id
	idgen := this.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	msgIds := make([]MessageId, msgCnt)
	for i := 0; i < msgCnt; i++ {
		var err error
		var id IdType
		id, err = idgen.Next()
		if err != nil {
			return EMPTY_MESSAGE_ID_LIST, err
		}
		messages[i].MsgId = id
		msgId := MessageId{
			MsgId: id,
			OutId: messages[i].OutId,
		}
		msgIds[i] = msgId
	}

	this.PreQueue(req)

	m := make(map[int32]*Queue)
	for _, q := range queueList {
		//m[q.QueueInternalId] = q
		err := q.PublishMessage(req, ctx)
		if err != nil {
			return EMPTY_MESSAGE_ID_LIST, err
		}
	}
	if len(tags) != 0 {
		queueMap := this.queueMap
		for _, tag := range tags {
			queue, ok := queueMap[tag]
			if ok {
				for _, q := range queue {
					id := q.QueueInternalId
					_, ok := m[id]
					if !ok {
						m[q.QueueInternalId] = q
						err := q.PublishMessage(req, ctx)
						if err != nil {
							return EMPTY_MESSAGE_ID_LIST, err
						}
					}
				}
			}
		}
	}

	return Ack{
		AckIdList: msgIds,
	}, nil
}

// exactly once
// omit dup flag
func (this *Topic) CommitMessages(req *MessageCommit, ctx *Ctx) (Ack, error) {
	tags := req.Header.Tags
	queueList := this.queueList

	m := make(map[int32]*Queue)
	for _, q := range queueList {
		//m[q.QueueInternalId] = q
		err := q.CommitMessages(req, ctx)
		if err != nil {
			return EMPTY_MESSAGE_ID_LIST, err
		}
	}
	if len(tags) != 0 {
		queueMap := this.queueMap
		for _, tag := range tags {
			queue, ok := queueMap[tag]
			if ok {
				for _, q := range queue {
					id := q.QueueInternalId
					_, ok := m[id]
					if !ok {
						m[q.QueueInternalId] = q
						err := q.CommitMessages(req, ctx)
						if err != nil {
							return EMPTY_MESSAGE_ID_LIST, err
						}
					}
				}
			}
		}
	}

	return req.Ack, nil
}
