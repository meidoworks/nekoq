package mqext

import (
	"context"
	"sync"
	"time"

	"github.com/meidoworks/nekoq/service/mqapi"
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

func (m *MemQueue) Issues() {
	//FIXME 1. not support cluster replication
	//FIXME 2. block publish if MessageChannel is full especially when no subscriber
}

func (m *MemQueue) Close(ctx context.Context) error {
	//TODO
	return nil
}

func (m *MemQueue) PrePublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) error {
	return m.PublishMessage(req, ctx)
}

func (m *MemQueue) PublishMessage(req *mqapi.Request, ctx *mqapi.Ctx) error {
	switch m.DeliveryLevel {
	case mqapi.AtMostOnce:
		fallthrough
	case mqapi.AtLeastOnce:
		ch := m.MessageChannel
		for _, msg := range req.BatchMessage {
			ch <- msg
		}
		return nil
	case mqapi.ExactlyOnce:
		//TODO need ttl
		m.PrePubMapLock.Lock()
		for _, msg := range req.BatchMessage {
			_, ok := m.PrePubMapWithOutId[msg.OutId]
			// check dup flag
			if ok && req.Header.Dup {
				continue
			}
			// store pre-pub message
			m.PrePubMapWithOutId[msg.OutId] = msg
		}
		//TODO clear timeout message
		m.PrePubMapLock.Unlock()
		return nil
	default:
		return mqapi.ErrDeliveryLevelUnknown
	}
}

func (m *MemQueue) CommitMessages(commit *mqapi.MessageCommit, ctx *mqapi.Ctx) error {
	if m.DeliveryLevel != mqapi.ExactlyOnce {
		return mqapi.ErrDeliveryLevelIllegalOperation
	}
	prepubMap := m.PrePubMapWithOutId
	ch := m.MessageChannel
	msgs := make([]mqapi.Message, 0, len(commit.AckIdList))

	m.PrePubMapLock.Lock()
	for _, msgId := range commit.AckIdList {
		msg, ok := prepubMap[msgId.OutId]
		if ok {
			delete(prepubMap, msgId.OutId)
			msgs = append(msgs, msg)
		}
	}
	//TODO clear timeout message
	m.PrePubMapLock.Unlock()

	for _, msg := range msgs {
		ch <- msg
	}
	return nil
}

func (m *MemQueue) CreateRecord(subscribeGroupId mqapi.SubscribeGroupId, ctx *mqapi.Ctx) (*mqapi.QueueRecord, error) {
	//TODO need support
	return nil, nil
}

func (m *MemQueue) BatchObtain(record *mqapi.QueueRecord, maxCnt int, ctx *mqapi.Ctx) (mqapi.BatchObtainResult, error) {
	//TODO need support record
	switch m.DeliveryLevel {
	case mqapi.AtMostOnce:
		ch := m.MessageChannel
		result := mqapi.BatchObtainResult{
			Requests: []*mqapi.Request{
				{},
			},
		}
		req := result.Requests[0]
	AtMostOnceLoop:
		for i := 0; i < maxCnt; i++ {
			select {
			case msg := <-ch:
				req.BatchMessage = append(req.BatchMessage, msg)
			default:
				if len(req.BatchMessage) == 0 {
					req.BatchMessage = append(req.BatchMessage, <-ch)
				} else {
					break AtMostOnceLoop
				}
			}
		}
		return result, nil
	case mqapi.AtLeastOnce:
	AtLeastOnceMainLoop:
		for {
			ch := m.MessageChannel
			result := mqapi.BatchObtainResult{
				Requests: []*mqapi.Request{
					{},
				},
			}
			req := result.Requests[0]

			cnt := 0

			// check inflight messages
			now := time.Now()
			if now.Sub(m.lastRedeliveryTime) >= time.Duration(m.RedeliverIntervalTime)*time.Second {
				// pump messages from inflight map
				m.InflightMessageLock.Lock()
				for k, v := range m.InflightMessageMap {
					delete(m.InflightMessageMap, k)
					req.BatchMessage = append(req.BatchMessage, v)
					cnt++
					if cnt >= maxCnt {
						break
					}
				}
				m.InflightMessageLock.Unlock()
			}
			m.lastRedeliveryTime = now

			// pump messages from messageChannel
		AtLeastOnceLoop:
			for ; cnt < maxCnt; cnt++ {
				select {
				case msg := <-ch:
					m.InflightMessageLock.Lock()
					m.InflightMessageMap[msg.MsgId] = msg
					m.InflightMessageLock.Unlock()
					req.BatchMessage = append(req.BatchMessage, msg)
				default:
					if len(req.BatchMessage) == 0 {
						t := time.NewTimer(time.Duration(m.RedeliverIntervalTime) * time.Second)
						select {
						case msg := <-ch:
							m.InflightMessageLock.Lock()
							m.InflightMessageMap[msg.MsgId] = msg
							m.InflightMessageLock.Unlock()
							req.BatchMessage = append(req.BatchMessage, msg)
							t.Stop()
						case <-t.C:
							t.Stop()
							continue AtLeastOnceMainLoop
						}
					} else {
						break AtLeastOnceLoop
					}
				}
			}
			return result, nil
		}
	case mqapi.ExactlyOnce:
	ExactlyOnceMainLoop:
		for {
			ch := m.MessageChannel
			result := mqapi.BatchObtainResult{
				Requests: []*mqapi.Request{
					{},
				},
			}
			req := result.Requests[0]

			cnt := 0

			// check inflight messages
			now := time.Now()
			if now.Sub(m.lastRedeliveryTime) >= time.Duration(m.RedeliverIntervalTime)*time.Second {
				// pump messages from inflight map
				m.InflightMessageLock.Lock()
				for _, v := range m.InflightMessageMap {
					req.BatchMessage = append(req.BatchMessage, v)
					cnt++
					if cnt >= maxCnt {
						break
					}
				}
				m.InflightMessageLock.Unlock()
			}
			m.lastRedeliveryTime = now

			// set dup flag
			if len(req.BatchMessage) > 0 {
				req.Header.Dup = true

				result.Requests = append(result.Requests, new(mqapi.Request))
				req = result.Requests[1]
			}

			// pump messages from messageChannel
		ExactlyOnceLoop:
			for ; cnt < maxCnt; cnt++ {
				select {
				case msg := <-ch:
					m.InflightMessageLock.Lock()
					m.InflightMessageMap[msg.MsgId] = msg
					m.InflightMessageLock.Unlock()
					req.BatchMessage = append(req.BatchMessage, msg)
				default:
					if len(req.BatchMessage) == 0 {
						t := time.NewTimer(time.Duration(m.RedeliverIntervalTime) * time.Second)
						select {
						case msg := <-ch:
							m.InflightMessageLock.Lock()
							m.InflightMessageMap[msg.MsgId] = msg
							m.InflightMessageLock.Unlock()
							req.BatchMessage = append(req.BatchMessage, msg)
							t.Stop()
						case <-t.C:
							t.Stop()
							continue ExactlyOnceMainLoop
						}
					} else {
						break ExactlyOnceLoop
					}
				}
			}
			return result, nil
		}
	default:
		return mqapi.BatchObtainResult{}, mqapi.ErrUnsupportedOperation
	}
}

func (m *MemQueue) BatchObtainReleasing(record *mqapi.QueueRecord, maxCnt int, ctx *mqapi.Ctx) (mqapi.BatchObtainResult, error) {
	switch m.DeliveryLevel {
	case mqapi.ExactlyOnce:
	ExactlyOnceMainLoop:
		for {
			ch := m.ReleaseChannel
			result := mqapi.BatchObtainResult{
				Requests: []*mqapi.Request{
					{},
				},
			}
			req := result.Requests[0]

			cnt := 0

			// pump messages from messageChannel
		ExactlyOnceLoop:
			for ; cnt < maxCnt; cnt++ {
				select {
				case msg := <-ch:
					m.ReleaseMessageLock.Lock()
					m.ReleaseMessageMap[msg.MsgId] = msg
					m.ReleaseMessageLock.Unlock()
					req.BatchMessage = append(req.BatchMessage, msg)
				default:
					if len(req.BatchMessage) == 0 {
						t := time.NewTimer(time.Duration(m.RedeliverIntervalTime) * time.Second)
						select {
						case msg := <-ch:
							//FIXME if release command from client side isn't replied forever, the message will not be released any more
							//FIXME need pump release message from the release message map
							m.ReleaseMessageLock.Lock()
							m.ReleaseMessageMap[msg.MsgId] = msg
							m.ReleaseMessageLock.Unlock()
							req.BatchMessage = append(req.BatchMessage, msg)
							t.Stop()
						case <-t.C:
							t.Stop()
							continue ExactlyOnceMainLoop
						}
					} else {
						break ExactlyOnceLoop
					}
				}
			}

			return result, nil
		}
	default:
		return mqapi.BatchObtainResult{}, mqapi.ErrUnsupportedOperation
	}
}

func (m *MemQueue) ConfirmConsumed(record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	if m.DeliveryLevel == mqapi.AtMostOnce {
		return mqapi.ErrDeliveryLevelIllegalOperation
	}

	//TODO need support record

	msgs := make([]mqapi.Message, 0, len(ack.AckIdList))
	// in-flight map
	inflightMap := m.InflightMessageMap
	m.InflightMessageLock.Lock()
	for _, v := range ack.AckIdList {
		msg, ok := inflightMap[v.MsgId]
		if ok {
			delete(inflightMap, v.MsgId)
			msgs = append(msgs, msg)
		}
	}
	m.InflightMessageLock.Unlock()

	// release map
	if m.Queue.DeliveryLevel() == mqapi.ExactlyOnce {
		releases := make([]mqapi.Message, 0, len(msgs))
		m.ReleaseMessageLock.Lock()
		for _, msg := range msgs {
			releases = append(releases, msg)
		}
		m.ReleaseMessageLock.Unlock()

		for _, v := range releases {
			m.ReleaseChannel <- v
		}
	}

	return nil
}

func (m *MemQueue) ReleaseConsumed(record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	if m.DeliveryLevel != mqapi.ExactlyOnce {
		return mqapi.ErrDeliveryLevelIllegalOperation
	}

	// release map
	releaseMap := m.ReleaseMessageMap
	m.ReleaseMessageLock.Lock()
	for _, msg := range ack.AckIdList {
		delete(releaseMap, msg.MsgId)
	}
	m.ReleaseMessageLock.Unlock()

	return nil
}

func (m *MemQueue) Init(queue mqapi.Queue, option *mqapi.QueueOption) error {
	m.DeliveryLevel = option.DeliveryLevel
	m.Queue = queue
	switch m.DeliveryLevel {
	case mqapi.AtMostOnce:
	case mqapi.ExactlyOnce:
		m.PrePubMapWithOutId = make(map[mqapi.OutId]mqapi.Message)
		m.ReleaseMessageMap = make(map[mqapi.MsgId]mqapi.Message)
		m.ReleaseChannel = make(chan mqapi.Message, option.QueueChannelSize)
		fallthrough
	case mqapi.AtLeastOnce:
		m.InflightMessageMap = make(map[mqapi.MsgId]mqapi.Message)
		if option.RedeliverIntervalTime <= 0 {
			m.RedeliverIntervalTime = 5
		} else {
			m.RedeliverIntervalTime = option.RedeliverIntervalTime
		}
		m.lastRedeliveryTime = time.Now()
	}
	m.MessageChannel = make(chan mqapi.Message, option.QueueChannelSize)
	return nil
}
