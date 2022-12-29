package mqimpl_test

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/service/mqimpl"
	"runtime"
	"sync"
	"testing"
	"time"
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
	brokerOption := &mqimpl.BrokerOption{
		NodeId: 1,
	}

	broker := mqimpl.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mqapi.TopicOption{}
	queueOption := &mqapi.QueueOption{
		QueueChannelSize: 1024,
		QueueStoreType:   mqapi.MEM_STORE,
	}
	subOption := &mqapi.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.DefineNewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := broker.DefineNewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.DefineNewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.DefineNewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := broker.DefineNewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
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

	node := &mqimpl.Node{
		InitFunc: func(sub mqapi.SubscribeGroup) {
			go func() {
				ch := sub.SubscribeChannel()
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

	msg := mqapi.Request{
		Header: mqapi.Header{
			TopicId: TOPIC_ID,
		},
		BatchMessage: []mqapi.Message{
			{},
		},
	}
	err = publishGroup.PublishMessage(&msg, &mqapi.Ctx{})
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

	brokerOption := &mqimpl.BrokerOption{
		NodeId: 1,
	}

	broker := mqimpl.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mqapi.TopicOption{}
	queueOption := &mqapi.QueueOption{
		QueueChannelSize: 1024,
		QueueStoreType:   mqapi.MEM_STORE,
	}
	subOption := &mqapi.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.DefineNewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := broker.DefineNewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.DefineNewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.DefineNewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := broker.DefineNewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
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

	node := &mqimpl.Node{
		InitFunc: func(sub mqapi.SubscribeGroup) {
			go func() {
				ch := sub.SubscribeChannel()
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

	msg := mqapi.Request{
		Header: mqapi.Header{
			TopicId: TOPIC_ID,
		},
		BatchMessage: []mqapi.Message{
			mqapi.Message{},
		},
	}
	err = publishGroup.PublishMessage(&msg, &mqapi.Ctx{})
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
		err = publishGroup.PublishMessage(&msg, &mqapi.Ctx{})
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

	brokerOption := &mqimpl.BrokerOption{
		NodeId: 1,
	}

	broker := mqimpl.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mqapi.TopicOption{
		DeliveryLevel: mqapi.AtLeastOnce,
	}
	queueOption := &mqapi.QueueOption{
		QueueChannelSize: 1024,
		DeliveryLevel:    mqapi.AtLeastOnce,
		QueueStoreType:   mqapi.MEM_STORE,
	}
	subOption := &mqapi.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.DefineNewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := broker.DefineNewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.DefineNewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.DefineNewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := broker.DefineNewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
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

	node := &mqimpl.Node{
		InitFunc: func(sub mqapi.SubscribeGroup) {
			go func() {
				ch := sub.SubscribeChannel()
				for {
					elem := <-ch
					ack := &mqapi.Ack{AckIdList: []mqapi.MessageId{}}
					for _, msg := range elem.Request.BatchMessage {
						ack.AckIdList = append(ack.AckIdList, mqapi.MessageId{MsgId: msg.MsgId})
						wg.Done()
					}
					err := sub.Commit(elem.Queue.QueueId(), nil, ack)
					if err != nil {
						panic(err)
					}
				}
			}()
		},
	}

	subscribeGroup.Join(node)

	msg := mqapi.Request{
		Header: mqapi.Header{
			TopicId:       TOPIC_ID,
			DeliveryLevel: mqapi.AtLeastOnce,
		},
		BatchMessage: []mqapi.Message{
			{
				MessageId: mqapi.MessageId{
					MsgId: mqapi.MsgId{0, 0},
					OutId: mqapi.OutId{1, 1},
				},
			},
		},
	}
	ack, err := publishGroup.PublishGuaranteeMessage(&msg, &mqapi.Ctx{})
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

	var CNT = 5000000

	wg.Add(CNT)

	var start = time.Now()

	for i := 0; i < CNT; i++ {
		_, err = publishGroup.PublishGuaranteeMessage(&msg, &mqapi.Ctx{})
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	wg.Wait()

	var end = time.Now()

	t.Log(end.Sub(start))
}

func TestPrintBrokerExactlyOnceWithResponseTime(t *testing.T) {
	preMaxProcs := runtime.GOMAXPROCS(1)
	t.Log(preMaxProcs)
	t.Log(runtime.GOMAXPROCS(1))
	defer runtime.GOMAXPROCS(preMaxProcs)

	brokerOption := &mqimpl.BrokerOption{
		NodeId: 1,
	}

	broker := mqimpl.NewBroker(brokerOption)
	broker.Start()

	topicOption := &mqapi.TopicOption{
		DeliveryLevel: mqapi.ExactlyOnce,
	}
	queueOption := &mqapi.QueueOption{
		QueueChannelSize: 1024,
		DeliveryLevel:    mqapi.ExactlyOnce,
		QueueStoreType:   mqapi.MEM_STORE,
	}
	subOption := &mqapi.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	}

	topic, err := broker.DefineNewTopic(TOPIC_ID, topicOption)
	if err != nil {
		t.Fatal(err)
	}
	queue, err := broker.DefineNewQueue(QUEUE_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}
	publishGroup, err := broker.DefineNewPublishGroup(PUBLISH_GROUP_ID)
	if err != nil {
		t.Fatal(err)
	}
	subscribeGroup, err := broker.DefineNewSubscribeGroup(SUBSCRIBE_GROUP_ID, subOption)
	if err != nil {
		t.Fatal(err)
	}
	queue2, err := broker.DefineNewQueue(QUEUE2_ID, queueOption)
	if err != nil {
		t.Fatal(err)
	}

	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE_ID, nil)
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
	if err != nil {
		t.Fatal(err)
	}
	err = broker.BindTopicAndQueue(TOPIC_ID, QUEUE2_ID, []mqapi.TagId{QUEUE2_TAG_ID})
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

	node := &mqimpl.Node{
		InitFunc: func(sub mqapi.SubscribeGroup) {
			go func() {
				ch := sub.SubscribeChannel()
				for {
					elem := <-ch
					ack := &mqapi.Ack{AckIdList: []mqapi.MessageId{}}
					for _, msg := range elem.Request.BatchMessage {
						ack.AckIdList = append(ack.AckIdList, mqapi.MessageId{MsgId: msg.MsgId})
					}
					err := sub.Commit(elem.Queue.QueueId(), nil, ack)
					if err != nil {
						panic(err)
					}
				}
			}()
			go func() {
				// release chan
				ch := sub.ReleaseChannel()
				for {
					elem := <-ch
					ack := &mqapi.Ack{AckIdList: []mqapi.MessageId{}}
					for _, msg := range elem.Request.BatchMessage {
						ack.AckIdList = append(ack.AckIdList, mqapi.MessageId{MsgId: msg.MsgId})
					}
					err := sub.Release(elem.Queue.QueueId(), nil, ack)
					if err != nil {
						panic(err)
					}
					for range elem.Request.BatchMessage {
						wg.Done()
					}
				}
			}()
		},
	}

	subscribeGroup.Join(node)

	msg := mqapi.Request{
		Header: mqapi.Header{
			TopicId:       TOPIC_ID,
			DeliveryLevel: mqapi.ExactlyOnce,
		},
		BatchMessage: []mqapi.Message{
			{
				MessageId: mqapi.MessageId{
					MsgId: mqapi.MsgId{0, 0},
					OutId: mqapi.OutId{1, 1},
				},
			},
		},
	}
	received, err := publishGroup.PrePublishMessage(&msg, &mqapi.Ctx{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Ack:", received.AckIdList)
	commitReq := &mqapi.MessageCommit{
		Header: mqapi.Header{
			TopicId:       TOPIC_ID,
			DeliveryLevel: mqapi.ExactlyOnce,
		},
		Ack: mqapi.Ack{AckIdList: []mqapi.MessageId{
			msg.BatchMessage[0].MessageId,
		}},
	}
	finished, err := publishGroup.CommitMessage(commitReq, &mqapi.Ctx{})
	if err != nil {
		t.Fatal(err)
	}
	t.Log("Finished:", finished.AckIdList)

	time.Sleep(100 * time.Millisecond)
	wg.Wait()

	t.Log("topic:", topic)
	t.Log("publishGroup:", publishGroup)
	t.Log("subscribeGroup:", subscribeGroup)
	t.Log("queue:", queue)
	t.Log("queue2", queue2)
	t.Log("broker:", broker)

	var CNT = 5000000

	wg.Add(CNT)

	var start = time.Now()

	idgen := mqapi.NewIdGen(1, 1)
	for i := 0; i < CNT; i++ {
		id, err := idgen.Next()
		AssertError(t, err)
		msg := mqapi.Request{
			Header: mqapi.Header{
				TopicId:       TOPIC_ID,
				DeliveryLevel: mqapi.ExactlyOnce,
			},
			BatchMessage: []mqapi.Message{
				{
					MessageId: mqapi.MessageId{
						MsgId: mqapi.MsgId{0, 0},
						OutId: mqapi.OutId(id),
					},
				},
			},
		}
		_, err = publishGroup.PrePublishMessage(&msg, &mqapi.Ctx{})
		if err != nil {
			t.Fatal(err)
		}
		commitReq := &mqapi.MessageCommit{
			Header: mqapi.Header{
				TopicId:       TOPIC_ID,
				DeliveryLevel: mqapi.ExactlyOnce,
			},
			Ack: mqapi.Ack{AckIdList: []mqapi.MessageId{
				msg.BatchMessage[0].MessageId,
			}},
		}
		_, err = publishGroup.CommitMessage(commitReq, &mqapi.Ctx{})
		if err != nil {
			t.Fatal(err)
		}
	}

	time.Sleep(100 * time.Millisecond)

	wg.Wait()

	var end = time.Now()

	t.Log(end.Sub(start))
}
