package mqimpl

import (
	"sync"

	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/shared/idgen"
)

var _ mqapi.Topic = new(Topic)

type Topic struct {
	topicID       mqapi.TopicId
	deliveryLevel mqapi.DeliveryLevelType

	queueList []*Queue
	queueMap  map[mqapi.TagId][]*Queue
	basicLock sync.Mutex

	topicMessageIdGen *idgen.IdGen

	broker *Broker
}

func (topic *Topic) TopicId() mqapi.TopicId {
	return topic.topicID
}

func (topic *Topic) DeliveryLevel() mqapi.DeliveryLevelType {
	return topic.deliveryLevel
}

func (topic *Topic) PreQueue(req *mqapi.Request) {
	//TODO features: pre-persistent, status check callback
}

func (topic *Topic) PublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags

	// generate message id
	ig := topic.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	msgIds := make([]mqapi.MessageId, msgCnt)
	for i := 0; i < msgCnt; i++ {
		msgId, err := ig.Next()
		if err != nil {
			return EmptyMessageIdList, err
		}
		messages[i].MsgId = mqapi.MsgId(msgId)
		msgIds[i] = mqapi.MessageId{
			MsgId: mqapi.MsgId(msgId),
			OutId: messages[i].OutId,
		}
	}

	topic.PreQueue(req)

	if len(tags) == 0 {
		queueList := topic.queueList
		// all queues
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.PublishMessage(req, ctx)
			if err != nil {
				return EmptyMessageIdList, err
			}
		}
	} else {
		// specified queues
		m := make(map[mqapi.QueueId]*Queue)
		queueMap := topic.queueMap
		for _, tag := range tags {
			queues, ok := queueMap[tag]
			if ok {
				for _, q := range queues {
					id := q.QueueId()
					_, ok := m[id]
					if !ok {
						m[id] = q
						err := q.PublishMessage(req, ctx)
						if err != nil {
							return EmptyMessageIdList, err
						}
					}
				}
			}
		}
	}

	return mqapi.Ack{AckIdList: msgIds}, nil
}

// PublishMessageWithResponse apply to at least once
func (topic *Topic) PublishMessageWithResponse(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags

	// generate message id
	ig := topic.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	msgIds := make([]mqapi.MessageId, msgCnt)
	for i := 0; i < msgCnt; i++ {
		id, err := ig.Next()
		if err != nil {
			return EmptyMessageIdList, err
		}
		messages[i].MsgId = mqapi.MsgId(id)
		msgId := mqapi.MessageId{
			MsgId: mqapi.MsgId(id),
			OutId: messages[i].OutId,
		}
		msgIds[i] = msgId
	}

	topic.PreQueue(req)

	if len(tags) == 0 {
		queueList := topic.queueList
		// all queues
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.PublishMessage(req, ctx)
			if err != nil {
				return EmptyMessageIdList, err
			}
		}
	} else {
		// specified queues
		m := make(map[mqapi.QueueId]*Queue)
		queueMap := topic.queueMap
		for _, tag := range tags {
			queue, ok := queueMap[tag]
			if ok {
				for _, q := range queue {
					id := q.QueueId()
					_, ok := m[id]
					if !ok {
						m[id] = q
						err := q.PublishMessage(req, ctx)
						if err != nil {
							return EmptyMessageIdList, err
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

// PrePublishMessage apply to exactly once
// omit dup flag
func (topic *Topic) PrePublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags

	// generate message id
	ig := topic.topicMessageIdGen
	messages := req.BatchMessage
	msgCnt := len(messages)
	msgIds := make([]mqapi.MessageId, msgCnt)
	for i := 0; i < msgCnt; i++ {
		id, err := ig.Next()
		if err != nil {
			return EmptyMessageIdList, err
		}
		messages[i].MsgId = mqapi.MsgId(id)
		msgId := mqapi.MessageId{
			MsgId: mqapi.MsgId(id),
			OutId: messages[i].OutId,
		}
		msgIds[i] = msgId
	}

	topic.PreQueue(req)

	if len(tags) == 0 {
		queueList := topic.queueList
		// all queues
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.PrePublishMessage(req, ctx)
			if err != nil {
				return EmptyMessageIdList, err
			}
		}
	} else {
		// specified queues
		m := make(map[mqapi.QueueId]*Queue)
		queueMap := topic.queueMap
		for _, tag := range tags {
			queue, ok := queueMap[tag]
			if ok {
				for _, q := range queue {
					id := q.QueueId()
					_, ok := m[id]
					if !ok {
						m[id] = q
						err := q.PrePublishMessage(req, ctx)
						if err != nil {
							return EmptyMessageIdList, err
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

// CommitMessages apply to exactly once
// omit dup flag
func (topic *Topic) CommitMessages(req *mqapi.MessageCommit, ctx *mqapi.Ctx) (mqapi.Ack, error) {
	tags := req.Header.Tags

	if len(tags) == 0 {
		queueList := topic.queueList
		for _, q := range queueList {
			//m[q.QueueInternalId] = q
			err := q.CommitMessages(req, ctx)
			if err != nil {
				return EmptyMessageIdList, err
			}
		}
	} else {
		m := make(map[mqapi.QueueId]*Queue)
		queueMap := topic.queueMap
		for _, tag := range tags {
			queue, ok := queueMap[tag]
			if ok {
				for _, q := range queue {
					id := q.QueueId()
					_, ok := m[id]
					if !ok {
						m[id] = q
						err := q.CommitMessages(req, ctx)
						if err != nil {
							return EmptyMessageIdList, err
						}
					}
				}
			}
		}
	}

	return req.Ack, nil
}
