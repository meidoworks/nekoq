package mq

import (
	"sync"
	"time"
)

var _ QueueType = new(MemQueue)

type MemQueue struct {
	DeliveryLevel DeliveryLevelType

	PrePubMapWithOutId  map[IdType]Message
	PrePubMapLock       sync.Mutex
	InflightMessageMap  map[IdType]Message
	InflightMessageLock sync.Mutex

	RedeliverIntervalTime int
	lastRedeliveryTime    time.Time

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
		ch := this.MessageChannel
		result := BatchObtainResult{
			Requests: []*Request{
				{},
			},
		}
		req := result.Requests[0]

		cnt := 0

		// check inflight messages
		now := time.Now()
		if now.Sub(this.lastRedeliveryTime) >= time.Duration(this.RedeliverIntervalTime)*time.Second {
			// pump messages from inflight map
			this.InflightMessageLock.Lock()
			for k, v := range this.InflightMessageMap {
				delete(this.InflightMessageMap, k)
				req.BatchMessage = append(req.BatchMessage, v)
				cnt++
				if cnt >= maxCnt {
					break
				}
			}
			this.InflightMessageLock.Unlock()
		}
		this.lastRedeliveryTime = now

		// pump messages from messageChannel
	AT_LEAST_ONCE_LOOP:
		for ; cnt < maxCnt; cnt++ {
			select {
			case msg := <-ch:
				this.InflightMessageLock.Lock()
				this.InflightMessageMap[msg.MsgId] = msg
				this.InflightMessageLock.Unlock()
				req.BatchMessage = append(req.BatchMessage, msg)
			default:
				if len(req.BatchMessage) == 0 {
					msg := <-ch
					this.InflightMessageLock.Lock()
					this.InflightMessageMap[msg.MsgId] = msg
					this.InflightMessageLock.Unlock()
					req.BatchMessage = append(req.BatchMessage, msg)
				} else {
					break AT_LEAST_ONCE_LOOP
				}
			}
		}
		return result, nil
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

	// in-flight map
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
		if option.RedeliverIntervalTime <= 0 {
			this.RedeliverIntervalTime = 5
		} else {
			this.RedeliverIntervalTime = option.RedeliverIntervalTime
		}
		this.lastRedeliveryTime = time.Now()
	case ExactlyOnce:
		this.PrePubMapWithOutId = make(map[IdType]Message)
		this.InflightMessageMap = make(map[IdType]Message)
	}
	this.MessageChannel = make(chan Message, option.QueueChannelSize)
	return nil
}
