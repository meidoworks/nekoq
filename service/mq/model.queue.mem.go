package mq

import "sync"

var _ QueueType = new(MemQueue)

type MemQueue struct {
	DeliveryLevel DeliveryLevelType

	PrePubMapWithOutId  map[IdType]Message
	PrePubMapLock       sync.Mutex
	InflightMessageMap  map[IdType]Message
	InflightMessageLock sync.Mutex

	MessageChannel chan Message

	Queue *Queue
}

func (this *MemQueue) PublishMessage(req *Request, ctx *Ctx) error {
	switch this.DeliveryLevel {
	case AtMostOnce:
		fallthrough
	case AtLeastOnce:
		ch := this.MessageChannel
		for _, msg := range req.BatchMessage {
			ch <- msg
		}
		return nil
	case ExactlyOnce:
		//TODO need ttl
		this.PrePubMapLock.Lock()
		for _, msg := range req.BatchMessage {
			this.PrePubMapWithOutId[msg.OutId] = msg
		}
		//TODO clear timeout message
		this.PrePubMapLock.Unlock()
		return nil
	default:
		return ErrDeliveryLevelUnknown
	}
}

func (this *MemQueue) CommitMessages(commit *MessageCommit, ctx *Ctx) error {
	if this.DeliveryLevel != ExactlyOnce {
		return ErrDeliveryLevelIllegalOperation
	}
	prepubMap := this.PrePubMapWithOutId
	ch := this.MessageChannel
	this.PrePubMapLock.Lock()
	for _, msgId := range commit.Ack.AckIdList {
		msg, ok := prepubMap[msgId.OutId]
		if ok {
			ch <- msg
		}
	}
	//TODO clear timeout message
	this.PrePubMapLock.Unlock()
	return nil
}

func (this *MemQueue) CreateRecord(subscribeGroupId IdType, ctx *Ctx) (*QueueRecord, error) {
	//TODO need support
	return nil, nil
}

func (this *MemQueue) BatchObtain(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error) {
	//TODO need support record
	switch this.DeliveryLevel {
	case AtMostOnce:
		ch := this.MessageChannel
		result := BatchObtainResult{
			Requests: []*Request{
				{},
			},
		}
		req := result.Requests[0]
	AT_MOST_ONCE_LOOP:
		for i := 0; i < maxCnt; i++ {
			select {
			case msg := <-ch:
				req.BatchMessage = append(req.BatchMessage, msg)
			default:
				if len(req.BatchMessage) == 0 {
					req.BatchMessage = append(req.BatchMessage, <-ch)
				} else {
					break AT_MOST_ONCE_LOOP
				}
			}
		}
		return result, nil
	case AtLeastOnce:
		//TODO inflight retry interval
		//TODO 1. pump in-flight
		//TODO 2. pump messageChannel and put into in-flight map
		return BatchObtainResult{}, ErrUnsupportedOperation
	case ExactlyOnce:
		//TODO inflight retry interval
		//TODO 1. pump in-flight
		//TODO 2. pump messageChannel and put into in-flight map
		return BatchObtainResult{}, ErrUnsupportedOperation
	default:
		return BatchObtainResult{}, ErrUnsupportedOperation
	}
}

func (this *MemQueue) ConfirmConsumed(record *QueueRecord, ack *Ack) error {
	if this.DeliveryLevel == AtMostOnce {
		return ErrDeliveryLevelIllegalOperation
	}
	//TODO need support record
	// delete in-flight map
	inflightMap := this.InflightMessageMap
	this.InflightMessageLock.Lock()
	for _, v := range ack.AckIdList {
		delete(inflightMap, v.MsgId)
	}
	this.InflightMessageLock.Unlock()
	return nil
}

func (this *MemQueue) Init(queue *Queue, option *QueueOption) error {
	this.DeliveryLevel = option.DeliveryLevel
	this.Queue = queue
	switch this.DeliveryLevel {
	case AtMostOnce:
	case AtLeastOnce:
		this.InflightMessageMap = make(map[IdType]Message)
	case ExactlyOnce:
		this.PrePubMapWithOutId = make(map[IdType]Message)
		this.InflightMessageMap = make(map[IdType]Message)
	}
	this.MessageChannel = make(chan Message, option.QueueChannelSize)
	return nil
}
