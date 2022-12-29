package mqext

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"sync"
	"time"
)

var _ mqapi.QueueType = new(MemQueue)

type MemQueue struct {
	DeliveryLevel mqapi.DeliveryLevelType

	PrePubMapWithOutId  map[mqapi.OutId]mqapi.Message
	PrePubMapLock       sync.Mutex
	InflightMessageMap  map[mqapi.MsgId]mqapi.Message
	InflightMessageLock sync.Mutex
	ReleaseMessageMap   map[mqapi.MsgId]mqapi.Message
	ReleaseMessageLock  sync.Mutex
	ReleaseChannel      chan mqapi.Message

	RedeliverIntervalTime int
	lastRedeliveryTime    time.Time

	MessageChannel chan mqapi.Message

	Queue mqapi.Queue
}

func (this *MemQueue) PrePublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) error {
	return this.PublishMessage(req, ctx)
}

func (this *MemQueue) PublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) error {
	switch this.DeliveryLevel {
	case mqapi.AtMostOnce:
		fallthrough
	case mqapi.AtLeastOnce:
		ch := this.MessageChannel
		for _, msg := range req.BatchMessage {
			ch <- msg
		}
		return nil
	case mqapi.ExactlyOnce:
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
		return mqapi.ErrDeliveryLevelUnknown
	}
}

func (this *MemQueue) CommitMessages(commit *mqapi.MessageCommit, ctx *mqapi.Ctx) error {
	if this.DeliveryLevel != mqapi.ExactlyOnce {
		return mqapi.ErrDeliveryLevelIllegalOperation
	}
	prepubMap := this.PrePubMapWithOutId
	ch := this.MessageChannel
	msgs := make([]mqapi.Message, 0, len(commit.AckIdList))

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

func (this *MemQueue) CreateRecord(subscribeGroupId mqapi.SubscribeGroupId, ctx *mqapi.Ctx) (*mqapi.QueueRecord, error) {
	//TODO need support
	return nil, nil
}

func (this *MemQueue) BatchObtain(record *mqapi.QueueRecord, maxCnt int, ctx *mqapi.Ctx) (mqapi.BatchObtainResult, error) {
	//TODO need support record
	switch this.DeliveryLevel {
	case mqapi.AtMostOnce:
		ch := this.MessageChannel
		result := mqapi.BatchObtainResult{
			Requests: []*mqapi.Request{
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
	case mqapi.AtLeastOnce:
	AtLeastOnceMainLoop:
		for {
			ch := this.MessageChannel
			result := mqapi.BatchObtainResult{
				Requests: []*mqapi.Request{
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
	case mqapi.ExactlyOnce:
	ExactlyOnceMainLoop:
		for {
			ch := this.MessageChannel
			result := mqapi.BatchObtainResult{
				Requests: []*mqapi.Request{
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

				result.Requests = append(result.Requests, new(mqapi.Request))
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
		return mqapi.BatchObtainResult{}, mqapi.ErrUnsupportedOperation
	}
}

func (this *MemQueue) BatchObtainReleased(record *mqapi.QueueRecord, maxCnt int, ctx *mqapi.Ctx) (mqapi.BatchObtainResult, error) {
	switch this.DeliveryLevel {
	case mqapi.ExactlyOnce:
	ExactlyOnceMainLoop:
		for {
			ch := this.ReleaseChannel
			result := mqapi.BatchObtainResult{
				Requests: []*mqapi.Request{
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
		return mqapi.BatchObtainResult{}, mqapi.ErrUnsupportedOperation
	}
}

func (this *MemQueue) ConfirmConsumed(record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	if this.DeliveryLevel == mqapi.AtMostOnce {
		return mqapi.ErrDeliveryLevelIllegalOperation
	}

	//TODO need support record

	msgs := make([]mqapi.Message, 0, len(ack.AckIdList))
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
	if this.Queue.DeliveryLevel() == mqapi.ExactlyOnce {
		releases := make([]mqapi.Message, 0, len(msgs))
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

func (this *MemQueue) ReleaseConsumed(record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	if this.DeliveryLevel != mqapi.ExactlyOnce {
		return mqapi.ErrDeliveryLevelIllegalOperation
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

func (this *MemQueue) Init(queue mqapi.Queue, option *mqapi.QueueOption) error {
	this.DeliveryLevel = option.DeliveryLevel
	this.Queue = queue
	switch this.DeliveryLevel {
	case mqapi.AtMostOnce:
	case mqapi.ExactlyOnce:
		this.PrePubMapWithOutId = make(map[mqapi.OutId]mqapi.Message)
		this.ReleaseMessageMap = make(map[mqapi.MsgId]mqapi.Message)
		this.ReleaseChannel = make(chan mqapi.Message, option.QueueChannelSize)
		fallthrough
	case mqapi.AtLeastOnce:
		this.InflightMessageMap = make(map[mqapi.MsgId]mqapi.Message)
		if option.RedeliverIntervalTime <= 0 {
			this.RedeliverIntervalTime = 5
		} else {
			this.RedeliverIntervalTime = option.RedeliverIntervalTime
		}
		this.lastRedeliveryTime = time.Now()
	}
	this.MessageChannel = make(chan mqapi.Message, option.QueueChannelSize)
	return nil
}
