package mqclient

import (
	"context"
	"errors"
	"fmt"
	"log"

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
	DeliveryType DeliveryType
}

type QueueOption struct {
	DeliveryType DeliveryType
}

type Client struct {
	addrList []string
	idgen    *idgen.IdGen
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
	return s, nil
}

type Session struct {
	addressList []string
	channel     *webChannel
	idgen       *idgen.IdGen
}

func (c *Session) CreateTopic(topic string, topicOption TopicOption) error {
	tp := new(NewTopicRequest)
	tp.Topic = topic
	switch topicOption.DeliveryType {
	case AtMostOnce:
		tp.DeliveryLevelType = DeliveryTypeAtMostOnce
	case AtLeastOnce:
		tp.DeliveryLevelType = DeliveryTypeAtLeastOnce
	case ExactlyOnce:
		tp.DeliveryLevelType = DeliveryTypeExactlyOnce
	default:
		return errors.New("Unknown delivery type:" + fmt.Sprint(topicOption.DeliveryType))
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
		r := <-ch
		log.Println("receive response from server:" + fmt.Sprint(r))
		if r.Status != "200" {
			return errors.New("create topic failed from server:" + fmt.Sprint(r.Status))
		}
	}
	return nil
}

func (c *Session) CreateQueue(queue string, queueOption QueueOption) error {
	//TODO implement me
	panic("implement me")
}

func (c *Session) BindTopicAndQueue(topic, queue, bindingKey string) error {
	//TODO implement me
	panic("implement me")
}

func (c *Session) CreatePublishGroup(publishGroupName, topic string) (PublishGroup, error) {
	//TODO implement me
	panic("implement me")
}

func (c *Session) CreateSubscribeGroup(subscribeGroup, queue string, sg SubscribeGroup) error {
	//TODO implement me
	panic("implement me")
}

func (c *Session) CreateRpcStub(methodTopic, bindingKey string, encoder Codec) RpcStub {
	return &rpcStubImpl{
		Session:    c,
		Topic:      methodTopic,
		BindingKey: bindingKey,
		Codec:      encoder,
	}
}

func (c *Session) RpcHandle(serviceQueue string, encoder Codec, h RpcHandler) error {
	//TODO implement me
	//TODO add max payload check in response
	panic("implement me")
}

func (c *Session) Close(ctx context.Context) error {
	//TODO unsubscribe all before connection closed
	return c.channel.close()
}

type PublishGroup interface {
	//TODO publish methods
	Publish()

	AddReplyHandler(h func() error) error //TODO
}

type SubscribeGroup interface {
	Handle() error //TODO
}

type RpcStub interface {
	Call(req interface{}) (interface{}, error)
}

type rpcStubImpl struct {
	*Session
	Topic      string
	BindingKey string
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

func (r *rpcStubImpl) Call(req interface{}) (interface{}, error) {
	data, err := r.ReqMarshal(req)
	if err != nil {
		return nil, err
	}
	if len(data) > maxRpcPayloadSize {
		return nil, errors.New("request payload exceeded")
	}
	//TODO implement me
	panic("implement me")
}

type Codec interface {
	ReqMarshal(req interface{}) ([]byte, error)
	ReqUnmarshal(data []byte) (interface{}, error)
	ResMarshal(req interface{}) ([]byte, error)
	ResUnmarshal(data []byte) (interface{}, error)
}
