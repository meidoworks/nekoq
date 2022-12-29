package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"sync"
)

var _ mqapi.Topic = new(Topic)

type Topic struct {
	topicID       mqapi.TopicId
	deliveryLevel mqapi.DeliveryLevelType

	queueList []*Queue
	queueMap  map[mqapi.TagId][]*Queue
	basicLock sync.Mutex

	topicInternalId   int32
	topicMessageIdGen *mqapi.IdGen

	broker *Broker
}

func (this *Topic) TopicId() mqapi.TopicId {
	return this.topicID
}

func (this *Topic) DeliveryLevel() mqapi.DeliveryLevelType {
	return this.deliveryLevel
}

func (this *Topic) PreQueue(req *mqapi.Request) {
	//TODO
	//TODO features: pre-persistent, status check callback
}

func (this *Topic) PublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) error {
	tags := req.Header.Tags
	queueList := this.queueList

	// generate message id
	idgen := this.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	for i := 0; i < msgCnt; i++ {
		msgId, err := idgen.Next()
		messages[i].MsgId = mqapi.MsgId(msgId)
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

// at least once
func (this *Topic) PublishMessageWithResponse(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags
	queueList := this.queueList

	// generate message id
	idgen := this.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	msgIds := make([]mqapi.MessageId, msgCnt)
	for i := 0; i < msgCnt; i++ {
		id, err := idgen.Next()
		if err != nil {
			return EMPTY_MESSAGE_ID_LIST, err
		}
		messages[i].MsgId = mqapi.MsgId(id)
		msgId := mqapi.MessageId{
			MsgId: mqapi.MsgId(id),
			OutId: messages[i].OutId,
		}
		msgIds[i] = msgId
	}

	this.PreQueue(req)

	if len(tags) == 0 {
		// all queues
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.PublishMessage(req, ctx)
			if err != nil {
				return EMPTY_MESSAGE_ID_LIST, err
			}
		}
	} else {
		// specified queues
		m := make(map[int32]*Queue)
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

	return mqapi.Ack{
		AckIdList: msgIds,
	}, nil
}

// exactly once
// omit dup flag
func (this *Topic) PrePublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags
	queueList := this.queueList

	// generate message id
	idgen := this.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	msgIds := make([]mqapi.MessageId, msgCnt)
	for i := 0; i < msgCnt; i++ {
		id, err := idgen.Next()
		if err != nil {
			return EMPTY_MESSAGE_ID_LIST, err
		}
		messages[i].MsgId = mqapi.MsgId(id)
		msgId := mqapi.MessageId{
			MsgId: mqapi.MsgId(id),
			OutId: messages[i].OutId,
		}
		msgIds[i] = msgId
	}

	this.PreQueue(req)

	if len(tags) == 0 {
		// all queues
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.PublishMessage(req, ctx)
			if err != nil {
				return EMPTY_MESSAGE_ID_LIST, err
			}
		}
	} else {
		// specified queues
		m := make(map[int32]*Queue)
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

	return mqapi.Ack{
		AckIdList: msgIds,
	}, nil
}

// exactly once
// omit dup flag
func (this *Topic) CommitMessages(req *mqapi.MessageCommit, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags
	queueList := this.queueList

	if len(tags) == 0 {
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.CommitMessages(req, ctx)
			if err != nil {
				return EMPTY_MESSAGE_ID_LIST, err
			}
		}
	} else {
		m := make(map[int32]*Queue)
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
