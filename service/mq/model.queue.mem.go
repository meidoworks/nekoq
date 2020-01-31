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
	ReleaseMessageMap   map[IdType]Message
	ReleaseMessageLock  sync.Mutex
	ReleaseChannel      chan Message

	RedeliverIntervalTime int
	lastRedeliveryTime    time.Time

	MessageChannel chan Message

	Queue *Queue
}

func (this *MemQueue) PrePublishMessage(req *Request, ctx *Ctx) error {
	return this.PublishMessage(req, ctx)
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
			_, ok := this.PrePubMapWithOutId[msg.OutId]
			// check dup flag
			if ok && req.Header.Dup {
				continue
			}
			// store pre-pub message
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
	msgs := make([]Message, 0, len(commit.AckIdList))

	this.PrePubMapLock.Lock()
	for _, msgId := range commit.AckIdList {
		msg, ok := prepubMap[msgId.OutId]
		if ok {
			delete(prepubMap, msgId.OutId)
			msgs = append(msgs, msg)
		}
	}
	//TODO clear timeout message
	this.PrePubMapLock.Unlock()

	for _, msg := range msgs {
		ch <- msg
	}
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
	AtLeastOnceMainLoop:
		for {
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
						t := time.NewTimer(time.Duration(this.RedeliverIntervalTime) * time.Second)
						select {
						case msg := <-ch:
							this.InflightMessageLock.Lock()
							this.InflightMessageMap[msg.MsgId] = msg
							this.InflightMessageLock.Unlock()
							req.BatchMessage = append(req.BatchMessage, msg)
							t.Stop()
						case <-t.C:
							t.Stop()
							continue AtLeastOnceMainLoop
						}
					} else {
						break AT_LEAST_ONCE_LOOP
					}
				}
			}
			return result, nil
		}
	case ExactlyOnce:
	ExactlyOnceMainLoop:
		for {
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
				for _, v := range this.InflightMessageMap {
					req.BatchMessage = append(req.BatchMessage, v)
					cnt++
					if cnt >= maxCnt {
						break
					}
				}
				this.InflightMessageLock.Unlock()
			}
			this.lastRedeliveryTime = now

			// set dup flag
			if len(req.BatchMessage) > 0 {
				req.Header.Dup = true

				result.Requests = append(result.Requests, new(Request))
				req = result.Requests[1]
			}

			// pump messages from messageChannel
		EXACTLY_ONCE_LOOP:
			for ; cnt < maxCnt; cnt++ {
				select {
				case msg := <-ch:
					this.InflightMessageLock.Lock()
					this.InflightMessageMap[msg.MsgId] = msg
					this.InflightMessageLock.Unlock()
					req.BatchMessage = append(req.BatchMessage, msg)
				default:
					if len(req.BatchMessage) == 0 {
						t := time.NewTimer(time.Duration(this.RedeliverIntervalTime) * time.Second)
						select {
						case msg := <-ch:
							this.InflightMessageLock.Lock()
							this.InflightMessageMap[msg.MsgId] = msg
							this.InflightMessageLock.Unlock()
							req.BatchMessage = append(req.BatchMessage, msg)
							t.Stop()
						case <-t.C:
							t.Stop()
							continue ExactlyOnceMainLoop
						}
					} else {
						break EXACTLY_ONCE_LOOP
					}
				}
			}
			return result, nil
		}
	default:
		return BatchObtainResult{}, ErrUnsupportedOperation
	}
}

func (this *MemQueue) BatchObtainReleased(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error) {
	switch this.DeliveryLevel {
	case ExactlyOnce:
	ExactlyOnceMainLoop:
		for {
			ch := this.ReleaseChannel
			result := BatchObtainResult{
				Requests: []*Request{
					{},
				},
			}
			req := result.Requests[0]

			cnt := 0

			// pump messages from messageChannel
		EXACTLY_ONCE_LOOP:
			for ; cnt < maxCnt; cnt++ {
				select {
				case msg := <-ch:
					this.ReleaseMessageLock.Lock()
					this.ReleaseMessageMap[msg.MsgId] = msg
					this.ReleaseMessageLock.Unlock()
					req.BatchMessage = append(req.BatchMessage, msg)
				default:
					if len(req.BatchMessage) == 0 {
						t := time.NewTimer(time.Duration(this.RedeliverIntervalTime) * time.Second)
						select {
						case msg := <-ch:
							this.ReleaseMessageLock.Lock()
							this.ReleaseMessageMap[msg.MsgId] = msg
							this.ReleaseMessageLock.Unlock()
							req.BatchMessage = append(req.BatchMessage, msg)
							t.Stop()
						case <-t.C:
							t.Stop()
							continue ExactlyOnceMainLoop
						}
					} else {
						break EXACTLY_ONCE_LOOP
					}
				}
			}

			return result, nil
		}
	default:
		return BatchObtainResult{}, ErrUnsupportedOperation
	}
}

func (this *MemQueue) ConfirmConsumed(record *QueueRecord, ack *Ack) error {
	if this.DeliveryLevel == AtMostOnce {
		return ErrDeliveryLevelIllegalOperation
	}

	//TODO need support record

	msgs := make([]Message, 0, len(ack.AckIdList))
	// in-flight map
	inflightMap := this.InflightMessageMap
	this.InflightMessageLock.Lock()
	for _, v := range ack.AckIdList {
		msg, ok := inflightMap[v.MsgId]
		if ok {
			delete(inflightMap, v.MsgId)
			msgs = append(msgs, msg)
		}
	}
	this.InflightMessageLock.Unlock()

	// release map
	if this.Queue.DeliveryLevel == ExactlyOnce {
		releases := make([]Message, 0, len(msgs))
		this.ReleaseMessageLock.Lock()
		for _, msg := range msgs {
			releases = append(releases, msg)
		}
		this.ReleaseMessageLock.Unlock()

		for _, v := range releases {
			this.ReleaseChannel <- v
		}
	}

	return nil
}

func (this *MemQueue) ReleaseConsumed(record *QueueRecord, ack *Ack) error {
	if this.DeliveryLevel != ExactlyOnce {
		return ErrDeliveryLevelIllegalOperation
	}

	// release map
	releaseMap := this.ReleaseMessageMap
	this.ReleaseMessageLock.Lock()
	for _, msg := range ack.AckIdList {
		delete(releaseMap, msg.MsgId)
	}
	this.ReleaseMessageLock.Unlock()

	return nil
}

func (this *MemQueue) Init(queue *Queue, option *QueueOption) error {
	this.DeliveryLevel = option.DeliveryLevel
	this.Queue = queue
	switch this.DeliveryLevel {
	case AtMostOnce:
	case ExactlyOnce:
		this.PrePubMapWithOutId = make(map[IdType]Message)
		this.ReleaseMessageMap = make(map[IdType]Message)
		this.ReleaseChannel = make(chan Message, option.QueueChannelSize)
		fallthrough
	case AtLeastOnce:
		this.InflightMessageMap = make(map[IdType]Message)
		if option.RedeliverIntervalTime <= 0 {
			this.RedeliverIntervalTime = 5
		} else {
			this.RedeliverIntervalTime = option.RedeliverIntervalTime
		}
		this.lastRedeliveryTime = time.Now()
	}
	this.MessageChannel = make(chan Message, option.QueueChannelSize)
	return nil
}
