package mqclient_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"sync"
	"testing"
	"time"

	"github.com/meidoworks/nekoq/clients/mqclient"
)

type stubSubscribe struct {
	HandleMessage   func(message *mqclient.Message, sg mqclient.SubscribeGroup) error
	HandleReleasing func(messageMeta *mqclient.MessageReleasing, sg mqclient.SubscribeGroup) error
}

func (s *stubSubscribe) OnMessage(message *mqclient.Message, sg mqclient.SubscribeGroup) error {
	return s.HandleMessage(message, sg)
}

func (s *stubSubscribe) OnReleasing(messageMeta *mqclient.MessageReleasing, sg mqclient.SubscribeGroup) error {
	return s.HandleReleasing(messageMeta, sg)
}

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
	createSampleComponents(t)

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

	if err := s.RpcHandle("backend.sayHello", "sg.sg001", sayHelloRpcCodec{}, &mqclient.SimpleRpcHandler{H: func(req interface{}) (interface{}, error) {
		in := req.(string)
		return "Hello, " + in + "!", nil
	}}); err != nil {
		t.Fatal(err)
	}

	rpcStub := sc.CreateRpcStub("service.sayHello", "all_dc", "pg.pg001", sayHelloRpcCodec{})
	result, err := rpcStub.Call("world")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result.(string))

	rpcStub = sc.CreateRpcStub("service.sayHello", "all_dc", "pg.pg001", sayHelloRpcCodec{})
	result, err = rpcStub.Call("golang")
	if err != nil {
		t.Fatal(err)
	}
	t.Log(result.(string))
}

func createSampleComponents(t *testing.T) {

	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		t.Fatal(err)
	}
	c.SetDebug(true)

	s, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	err = s.CreateTopic("service.sayHello", mqclient.TopicOption{
		DeliveryLevelType: mqclient.AtLeastOnce,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.CreateQueue("backend.sayHello", mqclient.QueueOption{
		DeliveryLevelType: mqclient.AtLeastOnce,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.BindTopicAndQueue("service.sayHello", "backend.sayHello", "*"); err != nil {
		t.Fatal(err)
	}

	// new publish group
	// bind publish group
	_, err = s.CreatePublishGroup("pg.pg001", "service.sayHello")
	if err != nil {
		t.Fatal(err)
	}

}

func TestSession_CreateAtMostOnce(t *testing.T) {

	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		t.Fatal(err)
	}
	c.SetDebug(true)

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

	sub := new(stubSubscribe)
	sub.HandleMessage = func(message *mqclient.Message, sg mqclient.SubscribeGroup) error {
		log.Println("receive message:" + fmt.Sprint(message))
		log.Println(string(message.Payload))
		return nil
	}
	// new subscribe group
	// bind subscribe group
	// subscribe
	if err := s.CreateSubscribeGroup("demo.sg.001", "demo.queue.001", sub); err != nil {
		t.Fatal(err)
	}

	// publish & consume
	if desc, err := pg.Publish([]byte("hello world~"), "demo.routing.demo001"); err != nil {
		t.Fatal(err)
	} else {
		t.Log("publish desc:" + fmt.Sprint(desc))
	}

	time.Sleep(2 * time.Second)
}

func TestSession_CreateAtLeastOnce(t *testing.T) {

	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		t.Fatal(err)
	}
	c.SetDebug(true)

	s, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	err = s.CreateTopic("demo.002", mqclient.TopicOption{
		DeliveryLevelType: mqclient.AtLeastOnce,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.CreateQueue("demo.queue.002", mqclient.QueueOption{
		DeliveryLevelType: mqclient.AtLeastOnce,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.BindTopicAndQueue("demo.002", "demo.queue.002", "demo.routing.*"); err != nil {
		t.Fatal(err)
	}

	// new publish group
	// bind publish group
	pg, err := s.CreatePublishGroup("demo.pg.002", "demo.002")
	if err != nil {
		t.Fatal(err)
	}

	sub := new(stubSubscribe)
	sub.HandleMessage = func(message *mqclient.Message, sg mqclient.SubscribeGroup) error {
		log.Println("receive message:" + fmt.Sprint(message))
		log.Println(string(message.Payload))
		if err := sg.Commit(message); err != nil {
			log.Println("commit message error:" + fmt.Sprint(err))
		}
		return nil
	}
	// new subscribe group
	// bind subscribe group
	// subscribe
	if err := s.CreateSubscribeGroup("demo.sg.002", "demo.queue.002", sub); err != nil {
		t.Fatal(err)
	}

	// publish & consume
	if desc, err := pg.Publish([]byte("hello world~"), "demo.routing.demo001"); err != nil {
		t.Fatal(err)
	} else {
		t.Log("publish desc:" + fmt.Sprint(desc))
	}

	time.Sleep(2 * time.Second)
}

func TestSession_CreateExactlyOnce(t *testing.T) {

	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		t.Fatal(err)
	}
	c.SetDebug(true)

	s, err := c.Connect(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close(context.Background())

	err = s.CreateTopic("demo.003", mqclient.TopicOption{
		DeliveryLevelType: mqclient.ExactlyOnce,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err := s.CreateQueue("demo.queue.003", mqclient.QueueOption{
		DeliveryLevelType: mqclient.ExactlyOnce,
	}); err != nil {
		t.Fatal(err)
	}

	if err := s.BindTopicAndQueue("demo.003", "demo.queue.003", "demo.routing.*"); err != nil {
		t.Fatal(err)
	}

	// new publish group
	// bind publish group
	pg, err := s.CreatePublishGroup("demo.pg.003", "demo.003")
	if err != nil {
		t.Fatal(err)
	}

	sub := new(stubSubscribe)
	sub.HandleMessage = func(message *mqclient.Message, sg mqclient.SubscribeGroup) error {
		log.Println("receive message:" + fmt.Sprint(message))
		log.Println(string(message.Payload))
		if err := sg.Commit(message); err != nil {
			log.Println("commit message error:" + fmt.Sprint(err))
		}
		return nil
	}
	sub.HandleReleasing = func(messageMeta *mqclient.MessageReleasing, sg mqclient.SubscribeGroup) error {
		log.Println("receive message metadata:" + fmt.Sprint(messageMeta))
		if err := sg.Release(messageMeta); err != nil {
			log.Println("release message error:" + fmt.Sprint(err))
		}
		return nil
	}
	// new subscribe group
	// bind subscribe group
	// subscribe
	// support commit/release responses
	if err := s.CreateSubscribeGroup("demo.sg.003", "demo.queue.003", sub); err != nil {
		t.Fatal(err)
	}

	// publish & consume
	var desc *mqclient.MessageDesc
	if d, err := pg.Publish([]byte("hello world~"), "demo.routing.demo001"); err != nil {
		t.Fatal(err)
	} else {
		desc = d
	}
	time.Sleep(2 * time.Second)
	// commit message
	if err := pg.CommitPublish(desc); err != nil {
		t.Fatal(err)
	}

	time.Sleep(2 * time.Second)
}

func BenchmarkSession_CreateAtMostOnce(b *testing.B) {

	c, err := mqclient.NewClient("ws://127.0.0.1:9301")
	if err != nil {
		b.Fatal(err)
	}

	s, err := c.Connect(context.Background())
	if err != nil {
		b.Fatal(err)
	}
	defer s.Close(context.Background())

	// new publish group
	// bind publish group
	pg, err := s.CreatePublishGroup("demo.pg.001", "demo.001")
	if err != nil {
		b.Fatal(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(b.N)

	sub := new(stubSubscribe)
	sub.HandleMessage = func(message *mqclient.Message, sg mqclient.SubscribeGroup) error {
		//log.Println("receive message:" + fmt.Sprint(message))
		//log.Println(string(message.Payload))
		if message != nil && len(message.Payload) > 0 {
			wg.Done()
		} else {
			log.Println("message is not valid")
		}
		return nil
	}
	// new subscribe group
	// bind subscribe group
	// subscribe
	if err := s.CreateSubscribeGroup("demo.sg.001", "demo.queue.001", sub); err != nil {
		b.Fatal(err)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// publish & consume
		if _, err := pg.Publish([]byte("hello world~"), "demo.routing.demo001"); err != nil {
			b.Fatal(err)
		}
	}
	wg.Wait()
	b.StopTimer()
}
