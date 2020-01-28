package mq_test

import (
	"runtime"
	"sync"
	"testing"
	"time"

	"goimport.moetang.info/nekoq/service/mq"
)

var TOPIC = "topic.demo"
var TOPIC_ID = [2]int64{1, 0}
var PUBLISH_GROUP = "group.publish.demo"
var PUBLISH_GROUP_ID = [2]int64{2, 0}
var SUBSCRIBE_GROUP = "group.subscribe.demo"
var SUBSCRIBE_GROUP_ID = [2]int64{3, 0}
var QUEUE = "queue.demo"
var QUEUE_ID = [2]int64{4, 0}
var QUEUE2 = "queue.demo2"
var QUEUE2_ID = [2]int64{5, 0}
var QUEUE2_TAG_ID = [2]int64{6, 0}

func TestBuildBroker(t *testing.T) {
	brokerOption := &mq.BrokerOption{
		NodeId: 1,
	}

	broker := mq.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mq.TopicOption{}
	queueOption := &mq.QueueOption{
		QueueChannelSize: 1024,
		QueueStoreType:   mq.MEM_STORE,
	}
	subOption := &mq.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.NewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := topic.NewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.NewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.NewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := topic.NewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicQueue(TOPIC_ID, QUEUE2_ID, []mq.IdType{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicQueue(TOPIC_ID, QUEUE2_ID, []mq.IdType{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindPublishGroupToTopic(PUBLISH_GROUP_ID, TOPIC_ID)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindSubscribeGroupToQueue(SUBSCRIBE_GROUP_ID, QUEUE_ID)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	node := &mq.Node{
		InitFunc: func(sub *mq.SubscribeGroup) {
			go func() {
				ch := sub.SubCh
				for {
					msg := <-ch
					t.Log("receive:", msg)
					t.Log("receive message id:", msg.Request.BatchMessage[0].MsgId)
					wg.Done()
				}
			}()
		},
	}

	subscribeGroup.Join(node)

	msg := mq.Request{
		Header: mq.Header{
			TopicId: TOPIC_ID,
		},
		BatchMessage: []mq.Message{
			{},
		},
	}
	err = publishGroup.PublishMessage(&msg, &mq.Ctx{})
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	t.Log("topic:", topic)
	t.Log("publishGroup:", publishGroup)
	t.Log("subscribeGroup:", subscribeGroup)
	t.Log("queue:", queue)
	t.Log("queue2", queue2)
	t.Log("broker:", broker)
}

func TestPrintBrokerTime(t *testing.T) {
	preMaxProcs := runtime.GOMAXPROCS(1)
	t.Log(preMaxProcs)
	t.Log(runtime.GOMAXPROCS(1))
	defer runtime.GOMAXPROCS(preMaxProcs)

	brokerOption := &mq.BrokerOption{
		NodeId: 1,
	}

	broker := mq.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mq.TopicOption{}
	queueOption := &mq.QueueOption{
		QueueChannelSize: 1024,
		QueueStoreType:   mq.MEM_STORE,
	}
	subOption := &mq.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.NewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := topic.NewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.NewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.NewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := topic.NewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicQueue(TOPIC_ID, QUEUE2_ID, []mq.IdType{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicQueue(TOPIC_ID, QUEUE2_ID, []mq.IdType{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindPublishGroupToTopic(PUBLISH_GROUP_ID, TOPIC_ID)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindSubscribeGroupToQueue(SUBSCRIBE_GROUP_ID, QUEUE_ID)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	node := &mq.Node{
		InitFunc: func(sub *mq.SubscribeGroup) {
			go func() {
				ch := sub.SubCh
				for {
					elem := <-ch
					for _ = range elem.Request.BatchMessage {
						wg.Done()
					}
				}
			}()
		},
	}

	subscribeGroup.Join(node)

	msg := mq.Request{
		Header: mq.Header{
			TopicId: TOPIC_ID,
		},
		BatchMessage: []mq.Message{
			mq.Message{},
		},
	}
	err = publishGroup.PublishMessage(&msg, &mq.Ctx{})
	if err != nil {
		t.Fatal(err)
	}

	wg.Wait()

	t.Log("topic:", topic)
	t.Log("publishGroup:", publishGroup)
	t.Log("subscribeGroup:", subscribeGroup)
	t.Log("queue:", queue)
	t.Log("queue2", queue2)
	t.Log("broker:", broker)

	var CNT = 4000000

	wg.Add(CNT)

	var start = time.Now()

	for i := 0; i < CNT; i++ {
		err = publishGroup.PublishMessage(&msg, &mq.Ctx{})
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	var end = time.Now()

	t.Log(end.Sub(start))
}

func TestPrintBrokerWithResponseTime(t *testing.T) {
	preMaxProcs := runtime.GOMAXPROCS(1)
	t.Log(preMaxProcs)
	t.Log(runtime.GOMAXPROCS(1))
	defer runtime.GOMAXPROCS(preMaxProcs)

	brokerOption := &mq.BrokerOption{
		NodeId: 1,
	}

	broker := mq.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mq.TopicOption{
		DeliveryLevel: mq.AtLeastOnce,
	}
	queueOption := &mq.QueueOption{
		QueueChannelSize: 1024,
		DeliveryLevel:    mq.AtLeastOnce,
		QueueStoreType:   mq.MEM_STORE,
	}
	subOption := &mq.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.NewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := topic.NewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.NewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.NewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := topic.NewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicQueue(TOPIC_ID, QUEUE2_ID, []mq.IdType{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicQueue(TOPIC_ID, QUEUE2_ID, []mq.IdType{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindPublishGroupToTopic(PUBLISH_GROUP_ID, TOPIC_ID)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindSubscribeGroupToQueue(SUBSCRIBE_GROUP_ID, QUEUE_ID)
	if err != nil {
		t.Fatal(err)
	}

	wg := &sync.WaitGroup{}
	wg.Add(1)

	node := &mq.Node{
		InitFunc: func(sub *mq.SubscribeGroup) {
			go func() {
				ch := sub.SubCh
				for {
					elem := <-ch
					for _ = range elem.Request.BatchMessage {
						wg.Done()
					}
				}
			}()
		},
	}

	subscribeGroup.Join(node)

	msg := mq.Request{
		Header: mq.Header{
			TopicId:       TOPIC_ID,
			DeliveryLevel: mq.AtLeastOnce,
		},
		BatchMessage: []mq.Message{
			{
				MessageId: mq.MessageId{
					MsgId: mq.IdType{0, 0},
					OutId: mq.IdType{1, 1},
				},
			},
		},
	}
	ack, err := publishGroup.PublishGuaranteeMessage(&msg, &mq.Ctx{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Ack:", ack.AckIdList)

	wg.Wait()

	t.Log("topic:", topic)
	t.Log("publishGroup:", publishGroup)
	t.Log("subscribeGroup:", subscribeGroup)
	t.Log("queue:", queue)
	t.Log("queue2", queue2)
	t.Log("broker:", broker)

	var CNT = 50000000

	wg.Add(CNT)

	var start = time.Now()

	for i := 0; i < CNT; i++ {
		_, err = publishGroup.PublishGuaranteeMessage(&msg, &mq.Ctx{})
		if err != nil {
			t.Fatal(err)
		}
	}

	wg.Wait()

	var end = time.Now()

	t.Log(end.Sub(start))
}
