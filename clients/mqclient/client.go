package mqclient

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/meidoworks/nekoq/shared/idgen"
)

const (
	maxRpcPayloadSize = 64 * 1024 //64KB
)

type DeliveryType int

const (
	AtMostOnce  DeliveryType = 1
	AtLeastOnce DeliveryType = 2
	ExactlyOnce DeliveryType = 3
)

type TopicOption struct {
	DeliveryLevelType DeliveryType
}

type QueueOption struct {
	DeliveryLevelType DeliveryType
}

type Client struct {
	addrList []string
	idgen    *idgen.IdGen

	debugFlag bool
}

func NewClient(addrs ...string) (*Client, error) {
	if len(addrs) <= 0 {
		return nil, errors.New("no address")
	}

	return &Client{
		addrList: addrs,
		idgen:    idgen.NewIdGen(1, 1),
	}, nil
}

func (c *Client) SetDebug(debug bool) {
	c.debugFlag = debug
}

func (c *Client) Connect(ctx context.Context) (*Session, error) {
	//TODO need implement address failover
	s := new(Session)
	s.addressList = c.addrList
	if ch, err := newChannel(s.addressList[0]); err != nil {
		return nil, err
	} else {
		s.channel = ch
	}
	s.idgen = c.idgen
	s.debugFlag = c.debugFlag
	return s, nil
}

type Session struct {
	addressList []string
	channel     *webChannel
	idgen       *idgen.IdGen

	debugFlag bool
}

func (c *Session) CreateTopic(topic string, topicOption TopicOption) error {
	tp := new(NewTopicRequest)
	tp.Topic = topic
	switch topicOption.DeliveryLevelType {
	case AtMostOnce:
		tp.DeliveryLevelType = DeliveryTypeAtMostOnce
	case AtLeastOnce:
		tp.DeliveryLevelType = DeliveryTypeAtLeastOnce
	case ExactlyOnce:
		tp.DeliveryLevelType = DeliveryTypeExactlyOnce
	default:
		return errors.New("Unknown delivery level type:" + fmt.Sprint(topicOption.DeliveryLevelType))
	}

	req := new(ToServerSidePacket)
	req.NewTopic = tp
	req.Operation = OperationNewTopic
	id, err := c.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := c.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if c.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("create topic failed from server:" + fmt.Sprint(r.Status))
		}
	}
	return nil
}

func (c *Session) CreateQueue(queue string, queueOption QueueOption) error {
	qp := new(NewQueueRequest)
	qp.Queue = queue
	switch queueOption.DeliveryLevelType {
	case AtMostOnce:
		qp.DeliveryLevelType = DeliveryTypeAtMostOnce
	case AtLeastOnce:
		qp.DeliveryLevelType = DeliveryTypeAtLeastOnce
	case ExactlyOnce:
		qp.DeliveryLevelType = DeliveryTypeExactlyOnce
	default:
		return errors.New("Unknown delivery level type:" + fmt.Sprint(queueOption.DeliveryLevelType))
	}

	req := new(ToServerSidePacket)
	req.NewQueue = qp
	req.Operation = OperationNewQueue
	id, err := c.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := c.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if c.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("create queue failed from server:" + fmt.Sprint(r.Status))
		}
	}
	return nil
}

func (c *Session) BindTopicAndQueue(topic, queue, bindingKey string) error {
	bp := new(BindRequest)
	bp.Topic = topic
	bp.Queue = queue
	bp.BindingKey = bindingKey

	req := new(ToServerSidePacket)
	req.NewBinding = bp
	req.Operation = OperationBind
	id, err := c.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := c.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if c.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("bind failed from server:" + fmt.Sprint(r.Status))
		}
	}
	return nil
}

func (c *Session) CreatePublishGroup(publishGroupName, topic string) (PublishGroup, error) {
	bp := new(NewPublishGroupRequest)
	bp.Topic = topic
	bp.PublishGroup = publishGroupName

	req := new(ToServerSidePacket)
	req.NewPublishGroupRequest = bp
	req.Operation = OperationNewPublishGroup
	id, err := c.idgen.Next()
	if err != nil {
		return nil, err
	}
	req.RequestId = id.HexString()

	if ch, err := c.channel.writeObj(req); err != nil {
		return nil, err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if c.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return nil, errors.New("bind failed from server:" + fmt.Sprint(r.Status))
		}
		if r.PublishGroupResponse.PublishGroup != publishGroupName {
			return nil, errors.New("publish group names don't match")
		}
		pg := new(publishGroupImpl)
		pg.Session = c
		pg.Topic = topic
		pg.PublishGroup = publishGroupName
		return pg, nil
	}
}

// CreateSubscribeGroup bind the subscribeGroup to the queue and create/reuse subscribe callback
// Note: for the same subscribeGroup, only the earliest subscribe will be registered
func (c *Session) CreateSubscribeGroup(subscribeGroup, queue string, newSub Subscribe) error {
	sp := new(NewSubscribeGroupRequest)
	sp.SubscribeGroup = subscribeGroup
	sp.Queue = queue

	req := new(ToServerSidePacket)
	req.NewSubscribeGroupRequest = sp
	req.Operation = OperationNewSubscribeGroup
	id, err := c.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := c.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if c.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("create subscribe group failed from server:" + fmt.Sprint(r.Status))
		}
		if r.SubscribeGroupResponse.SubscribeGroup != subscribeGroup {
			return errors.New("subscribe group names don't match")
		}
	}
	c.channel.ChannelMapLock.Lock()
	sgCh, ok := c.channel.SgServerMessageChannels[subscribeGroup]
	if !ok {
		sgCh = make(chan *ServerSideIncoming, 1024)
		c.channel.SgServerMessageChannels[subscribeGroup] = sgCh
	}
	sgRCh, ok := c.channel.SgServerMessageReleasingChannels[subscribeGroup]
	if !ok {
		sgRCh = make(chan *ServerSideIncoming, 1024)
		c.channel.SgServerMessageReleasingChannels[subscribeGroup] = sgRCh
	}
	c.channel.ChannelMapLock.Unlock()

	// pump message from remote source for the NEW sgCh
	//FIXME this will cause only one subscribe could be effective on the same subscribeGroup
	if !ok {
		go func() {
			sg := new(subscribeGroupImpl)
			sg.subscribeGroup = subscribeGroup
			sg.session = c

		PumpMessageFromServerLoop:
			for {
				select {
				// close this subscribe when session closed or actively close the subscription
				case <-c.channel.closeCh:
					break PumpMessageFromServerLoop
				case incoming := <-sgCh:
					// copy immutable values
					incoming.Message.messageId = incoming.Message.MessageId
					incoming.Message.queue = incoming.Message.Queue

					if err := newSub.OnMessage(incoming.Message, sg); err != nil {
						log.Println("handle OnMessage callback failed:" + fmt.Sprint(err))
					}
				case incomingReleasing := <-sgRCh:
					// copy immutable values
					incomingReleasing.MessageReleasing.messageId = incomingReleasing.MessageReleasing.MessageId
					incomingReleasing.MessageReleasing.queue = incomingReleasing.MessageReleasing.Queue

					if err := newSub.OnReleasing(incomingReleasing.MessageReleasing, sg); err != nil {
						log.Println("handle OnReleasing callback failed:" + fmt.Sprint(err))
					}
				}
			}
		}()
	}

	return nil
}

func (c *Session) CreateRpcStub(methodTopic, bindingKey, publishGroup string, encoder Codec) RpcStub {
	//TODO should make sure the topic is using at-least-once or exactly-once
	return &rpcStubImpl{
		Session:      c,
		Topic:        methodTopic,
		BindingKey:   bindingKey,
		PublishGroup: publishGroup,
		Codec:        encoder,
	}
}

func (c *Session) RpcHandle(serviceQueue, subscribeGroup string, encoder Codec, h RpcHandler) error {
	//TODO add max payload check in response

	sp := new(NewSubscribeGroupRequest)
	sp.SubscribeGroup = subscribeGroup
	sp.Queue = serviceQueue

	req := new(ToServerSidePacket)
	req.NewSubscribeGroupRequest = sp
	req.Operation = OperationNewSubscribeGroup
	id, err := c.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := c.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if c.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("create subscribe group failed from server:" + fmt.Sprint(r.Status))
		}
		if r.SubscribeGroupResponse.SubscribeGroup != subscribeGroup {
			return errors.New("subscribe group names don't match")
		}
	}
	c.channel.ChannelMapLock.Lock()
	sgCh, ok := c.channel.SgServerMessageChannels[subscribeGroup]
	if !ok {
		sgCh = make(chan *ServerSideIncoming, 1024)
		c.channel.SgServerMessageChannels[subscribeGroup] = sgCh
	}
	sgRCh, ok := c.channel.SgServerMessageReleasingChannels[subscribeGroup]
	if !ok {
		sgRCh = make(chan *ServerSideIncoming, 1024)
		c.channel.SgServerMessageReleasingChannels[subscribeGroup] = sgRCh
	}
	c.channel.ChannelMapLock.Unlock()

	// pump message from remote source for the NEW sgCh
	//FIXME this will cause only one subscribe could be effective on the same subscribeGroup
	if !ok {
		go func() {
			sg := new(subscribeGroupImpl)
			sg.subscribeGroup = subscribeGroup
			sg.session = c

		PumpMessageFromServerLoop:
			for {
				select {
				// close this subscribe when session closed or actively close the subscription
				case <-c.channel.closeCh:
					break PumpMessageFromServerLoop
				case incoming := <-sgCh:
					// copy immutable values
					incoming.Message.messageId = incoming.Message.MessageId
					incoming.Message.queue = incoming.Message.Queue

					obj, err := encoder.ReqUnmarshal(incoming.Message.Payload)
					if err != nil {
						//TODO need respond failure
						log.Println("unmarshal request failed:" + fmt.Sprint(err))
						continue
					}
					//TODO need parallel run handler
					res, err := h.Handle(obj)
					if err != nil {
						//TODO need respond failure
						log.Println("handle request failed:" + fmt.Sprint(err))
						continue
					}
					data, err := encoder.ResMarshal(res)
					if err != nil {
						//TODO need respond failure
						log.Println("marshal response failed:" + fmt.Sprint(err))
						continue
					}

					message := incoming.Message

					ap := new(AckMessage)
					ap.Queue = message.queue
					ap.SubscribeGroup = subscribeGroup
					ap.MessageId = message.messageId
					ap.ReplyId = message.ReplyId
					ap.ReplyIdentifier = message.ReplyIdentifier
					ap.Payload = data

					req := new(ToServerSidePacket)
					req.NewAckMessage = ap
					req.Operation = OperationAckMessage
					id, err := sg.session.idgen.Next()
					if err != nil {
						//TODO need respond failure
						log.Println("get next id failed:" + fmt.Sprint(err))
						continue
					}
					req.RequestId = id.HexString()

					if _, err := sg.session.channel.writeObj(req); err != nil {
						//TODO need respond failure
						log.Println("writeObj failed:" + fmt.Sprint(err))
						continue
					} else {
						// don't wait for respond
					}

				case _ = <-sgRCh:
					//FIXME currently not support exactly-once queue
					log.Println("currently not support exactly-once queue")
				}
			}
		}()
	}

	return nil
}

func (c *Session) Close(ctx context.Context) error {
	//FIXME unsubscribe all before connection closed
	return c.channel.close()
}

type PublishGroup interface {
	// Publish for publishing message in at most once & at least once modes
	Publish(payload []byte, bindingKey string) (*MessageDesc, error)
	// CommitPublish for committing message at delivery level type of exactly-once
	CommitPublish(desc *MessageDesc) error

	//TODO AddReplyHandler(h func() error) error
}

type publishGroupImpl struct {
	Topic        string
	PublishGroup string

	*Session
}

func (p *publishGroupImpl) CommitPublish(desc *MessageDesc) error {
	req := new(ToServerSidePacket)
	req.Operation = OperationNewMessageCommit
	req.NewMessageCommitRequest = desc
	id, err := p.Session.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := p.Session.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if p.Session.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("new message from server:" + fmt.Sprint(r.Status))
		}
	}

	return nil
}

func (p *publishGroupImpl) Publish(payload []byte, bindingKey string) (*MessageDesc, error) {
	mp := new(NewMessageRequest)
	mp.BindingKey = bindingKey
	mp.PublishGroup = p.PublishGroup
	mp.Topic = p.Topic
	mp.Payload = payload

	req := new(ToServerSidePacket)
	req.NewMessageRequest = mp
	req.Operation = OperationNewMessage
	id, err := p.Session.idgen.Next()
	if err != nil {
		return nil, err
	}
	req.RequestId = id.HexString()

	var msgDesc = new(MessageDesc)
	if ch, err := p.Session.channel.writeObj(req); err != nil {
		return nil, err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if p.Session.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return nil, errors.New("new message from server:" + fmt.Sprint(r.Status))
		}
		if p.Session.debugFlag {
			log.Println("publish message id:" + fmt.Sprint(r.NewMessageResponse.MessageIdList))
		}
		msgDesc.MessageIdList = r.NewMessageResponse.MessageIdList
		msgDesc.BindingKey = r.NewMessageResponse.BindingKey
		msgDesc.Topic = r.NewMessageResponse.Topic
		msgDesc.PublishGroup = p.PublishGroup
	}

	return msgDesc, nil
}

type SubscribeGroup interface {
	Commit(message *Message) error
	Release(messageMeta *MessageReleasing) error
}

type subscribeGroupImpl struct {
	subscribeGroup string
	session        *Session
}

func (s *subscribeGroupImpl) Commit(message *Message) error {
	ap := new(AckMessage)
	ap.Queue = message.queue
	ap.SubscribeGroup = s.subscribeGroup
	ap.MessageId = message.messageId
	ap.ReplyId = message.ReplyId
	ap.ReplyIdentifier = message.ReplyIdentifier

	req := new(ToServerSidePacket)
	req.NewAckMessage = ap
	req.Operation = OperationAckMessage
	id, err := s.session.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := s.session.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if s.session.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("commit message to server:" + fmt.Sprint(r.Status))
		}
	}

	return nil
}

func (s *subscribeGroupImpl) Release(messageMeta *MessageReleasing) error {
	ap := new(ReleaseMessage)
	ap.Queue = messageMeta.queue
	ap.SubscribeGroup = s.subscribeGroup
	ap.MessageId = messageMeta.messageId
	ap.ReplyIdentifier = messageMeta.ReplyIdentifier
	ap.ReplyId = messageMeta.ReplyId

	req := new(ToServerSidePacket)
	req.NewReleaseMessage = ap
	req.Operation = OperationReleaseMessage
	id, err := s.session.idgen.Next()
	if err != nil {
		return err
	}
	req.RequestId = id.HexString()

	if ch, err := s.session.channel.writeObj(req); err != nil {
		return err
	} else {
		//TODO max wait time on client side
		r := <-ch
		if s.session.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if r.Status != "200" {
			return errors.New("release message to server:" + fmt.Sprint(r.Status))
		}
	}

	return nil
}

type Subscribe interface {
	OnMessage(message *Message, sg SubscribeGroup) error
	OnReleasing(messageMeta *MessageReleasing, sg SubscribeGroup) error
}

type RpcStub interface {
	Call(req interface{}) (interface{}, error)
}

type rpcStubImpl struct {
	*Session
	Topic        string
	BindingKey   string
	PublishGroup string
	Codec
}

type RpcHandler interface {
	Handle(req interface{}) (interface{}, error)
}

type SimpleRpcHandler struct {
	H func(req interface{}) (interface{}, error)
}

func (s *SimpleRpcHandler) Handle(req interface{}) (interface{}, error) {
	return s.H(req)
}

func (r *rpcStubImpl) Call(reqObj interface{}) (interface{}, error) {
	data, err := r.ReqMarshal(reqObj)
	if err != nil {
		return nil, err
	}
	if len(data) > maxRpcPayloadSize {
		return nil, errors.New("request payload exceeded")
	}

	mp := new(NewMessageRequest)
	mp.BindingKey = r.BindingKey
	mp.PublishGroup = r.PublishGroup
	mp.Topic = r.Topic
	mp.Payload = data
	mp.RpcMeta = new(struct {
		ReplyIdentifier string `json:"reply_identifier"`
	})

	req := new(ToServerSidePacket)
	req.NewMessageRequest = mp
	req.Operation = OperationNewMessage
	id, err := r.Session.idgen.Next()
	if err != nil {
		return nil, err
	}
	req.RequestId = id.HexString()
	mp.RpcMeta.ReplyIdentifier = req.RequestId
	// insert into rpc list
	rpcReq := new(_rpcRequest)
	rpcReq.ch = make(chan *ServerSideIncoming, 1)
	r.channel.rpcRequestMapLock.Lock()
	r.channel.rpcRequestMap[req.RequestId] = rpcReq
	r.channel.rpcRequestMapLock.Unlock()

	var msgDesc = new(MessageDesc)
	if ch, err := r.Session.channel.writeObj(req); err != nil {
		return nil, err
	} else {
		//TODO max wait time on client side
		rr := <-ch
		if r.Session.debugFlag {
			log.Println("receive response from server:" + fmt.Sprint(r))
		}
		if rr.Status != "200" {
			return nil, errors.New("new message from server:" + fmt.Sprint(rr.Status))
		}
		if r.Session.debugFlag {
			log.Println("publish message id:" + fmt.Sprint(rr.NewMessageResponse.MessageIdList))
		}
		msgDesc.MessageIdList = rr.NewMessageResponse.MessageIdList
		msgDesc.BindingKey = rr.NewMessageResponse.BindingKey
		msgDesc.Topic = rr.NewMessageResponse.Topic
		msgDesc.PublishGroup = r.PublishGroup
	}

	select {
	// waiting for rpc response
	case reply := <-rpcReq.ch:
		return r.ResUnmarshal(reply.Reply.Payload)
	case <-time.NewTimer(10 * time.Second).C: //TODO need customize client side timeout
		return nil, errors.New("rpc timeout")
	}

}

type Codec interface {
	ReqMarshal(req interface{}) ([]byte, error)
	ReqUnmarshal(data []byte) (interface{}, error)
	ResMarshal(req interface{}) ([]byte, error)
	ResUnmarshal(data []byte) (interface{}, error)
}
