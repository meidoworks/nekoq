package mqclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"testing"

	"github.com/meidoworks/nekoq/clients/mqclient"
)

type sayHelloRpcCodec struct {
}

func (s sayHelloRpcCodec) ReqMarshal(req interface{}) ([]byte, error) {
	return json.Marshal(req)
}

func (s sayHelloRpcCodec) ReqUnmarshal(data []byte) (interface{}, error) {
	var r string
	err := json.Unmarshal(data, &r)
	return r, err
}

func (s sayHelloRpcCodec) ResMarshal(req interface{}) ([]byte, error) {
	return json.Marshal(req)
}

func (s sayHelloRpcCodec) ResUnmarshal(data []byte) (interface{}, error) {
	var r string
	err := json.Unmarshal(data, &r)
	return r, err
}

func TestClientRpc(t *testing.T) {
	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		t.Fatal(err)
	}

	s, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	sc, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer sc.Close(context.Background())

	if err := s.RpcHandle("backend.sayHello", sayHelloRpcCodec{}, &mqclient.SimpleRpcHandler{H: func(req interface{}) (interface{}, error) {
		in := req.(string)
		return "Hello, " + in + "!", nil
	}}); err != nil {
		t.Fatal(s)
	}

	rpcStub := sc.CreateRpcStub("service.sayHello", "*", sayHelloRpcCodec{})
	result, err := rpcStub.Call("world")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result.(string))
}

func TestSession_CreateAtMostOnce(t *testing.T) {

	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		t.Fatal(err)
	}

	s, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	err = s.CreateTopic("demo.001", mqclient.TopicOption{
		DeliveryLevelType: mqclient.AtMostOnce,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.CreateQueue("demo.queue.001", mqclient.QueueOption{
		DeliveryLevelType: mqclient.AtMostOnce,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.BindTopicAndQueue("demo.001", "demo.queue.001", "demo.routing.*"); err != nil {
		t.Fatal(err)
	}

	// new publish group
	// bind publish group
	pg, err := s.CreatePublishGroup("demo.pg.001", "demo.001")
	if err != nil {
		t.Fatal(err)
	}

	// new subscribe group
	// bind subscribe group
	// subscribe
	if err := s.CreateSubscribeGroup("demo.sg.001", "demo.queue.001", func(message *mqclient.Message, sg mqclient.SubscribeGroup) error {
		log.Println("receive message:" + fmt.Sprint(message))
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	// publish & consume
	if err := pg.Publish([]byte("hello world~"), "demo.routing.demo001"); err != nil {
		t.Fatal(err)
	}
}
