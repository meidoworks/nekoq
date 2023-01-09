package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/shared/idgen"
)

func (b *Broker) DefineNewTopic(topicId mqapi.TopicId, option *mqapi.TopicOption) (mqapi.Topic, error) {
	t := new(Topic)
	t.topicID = topicId
	t.queueList = []*Queue{}
	t.queueMap = make(map[mqapi.TagId][]*Queue)
	switch option.DeliveryLevel {
	case mqapi.AtMostOnce:
		t.deliveryLevel = mqapi.AtMostOnce
	case mqapi.AtLeastOnce:
		t.deliveryLevel = mqapi.AtLeastOnce
	case mqapi.ExactlyOnce:
		t.deliveryLevel = mqapi.ExactlyOnce
	default:
		t.deliveryLevel = mqapi.AtMostOnce
	}
	topicInternalId, err := b.GenNewInternalTopicId()
	if err != nil {
		return nil, err
	}
	t.topicMessageIdGen = idgen.NewIdGen(b.nodeId, topicInternalId)
	t.topicInternalId = topicInternalId
	t.broker = b

	err = b.addTopic(t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (b *Broker) addTopic(topic *Topic) error {
	b.basicLock.Lock()
	defer b.basicLock.Unlock()

	if _, ok := b.topicMap[topic.topicID]; ok {
		return mqapi.ErrTopicAlreadyExist
	}

	b.topicMap = CopyAddMap(b.topicMap, topic.topicID, topic)
	return nil
}

func (b *Broker) DeleteTopic(topicId mqapi.TopicId) error {
	//TODO
	return nil
}

func (b *Broker) DefineNewQueue(queueId mqapi.QueueId, option *mqapi.QueueOption) (mqapi.Queue, error) {
	q := new(Queue)
	switch option.DeliveryLevel {
	case mqapi.AtMostOnce:
		q.deliveryLevel = mqapi.AtMostOnce
	case mqapi.AtLeastOnce:
		q.deliveryLevel = mqapi.AtLeastOnce
	case mqapi.ExactlyOnce:
		q.deliveryLevel = mqapi.ExactlyOnce
	default:
		return nil, mqapi.ErrDeliveryLevelUnknown
	}
	if option.UncommittedMessageRetainTime == 0 {
		q.UncommittedMessageRetainTime = 7 * 24 * 3600
	} else {
		q.UncommittedMessageRetainTime = option.UncommittedMessageRetainTime
	}

	if qtgen, err := mqapi.GetQueueTypeContainer(option.QueueType); err != nil {
		return nil, err
	} else {
		q.QueueType = qtgen()
	}

	q.queueID = queueId
	q.QueueChannel = make(chan *mqapi.Request, option.QueueChannelSize)
	q.InitBatchObtainCount = 16
	q.MaxBatchObtainCount = 1024
	queueInternalId, err := b.GenNewInternalQueueId()
	if err != nil {
		return nil, err
	}
	q.QueueInternalId = queueInternalId
	err = q.Init(q, option)
	if err != nil {
		return nil, err
	}
	err = b.addQueue(q)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (b *Broker) addQueue(queue *Queue) error {
	b.basicLock.Lock()
	defer b.basicLock.Unlock()

	if _, ok := b.queueMap[queue.queueID]; ok {
		return mqapi.ErrQueueAlreadyExist
	}

	b.queueMap = CopyAddMap(b.queueMap, queue.queueID, queue)
	return nil
}

func (b *Broker) DeleteQueue(queueId mqapi.QueueId) error {
	//TODO
	return nil
}

func (b *Broker) BindTopicAndQueue(topicId mqapi.TopicId, queueId mqapi.QueueId, tags []mqapi.TagId) error {
	topicMap := b.topicMap
	queueMap := b.queueMap

	topic, ok := topicMap[topicId]
	if !ok {
		return mqapi.ErrTopicNotExist
	}
	queue, ok := queueMap[queueId]
	if !ok {
		return mqapi.ErrQueueNotExist
	}

	if topic.deliveryLevel != queue.deliveryLevel {
		return mqapi.ErrDeliveryLevelNotMatch
	}

	topic.basicLock.Lock()
	defer topic.basicLock.Unlock()

	if len(tags) > 0 {
		// use tag match
		c := make(chan map[mqapi.TagId][]*Queue)
		go func() {
			newMap := make(map[mqapi.TagId][]*Queue)
			for k, v := range topic.queueMap {
				newMap[k] = v
			}
			for _, v := range tags {
				list, ok := newMap[v]
				if ok {
					// copy
					dup := false
					for _, q := range list {
						if q.queueID == queue.queueID {
							dup = true
							break
						}
					}
					if !dup {
						newList := make([]*Queue, len(list)+1)
						copy(newList, list)
						newList[len(newList)-1] = queue
						newMap[v] = newList
					}
				} else {
					newMap[v] = []*Queue{queue}
				}
			}
			c <- newMap
		}()
		topic.queueMap = <-c
	} else {
		// all match
		c := make(chan []*Queue)
		go func() {
			list := topic.queueList
			dup := false
			for _, q := range list {
				if q.queueID == queue.queueID {
					dup = true
					break
				}
			}
			if !dup {
				newList := make([]*Queue, len(list)+1)
				copy(newList, list)
				newList[len(newList)-1] = queue
				c <- newList
			} else {
				c <- list
			}
		}()
		topic.queueList = <-c
	}
	return nil
}

func (b *Broker) UnbindTopicAndQueue(topicId mqapi.TopicId, queueId mqapi.QueueId) error {
	//TODO
	return nil
}

func (b *Broker) DefineNewPublishGroup(publishGroupId mqapi.PublishGroupId) (mqapi.PublishGroup, error) {
	pg := new(PublishGroup)
	pg.publishGroupID = publishGroupId
	pg.topicMap = make(map[mqapi.TopicId]*Topic)
	err := b.addPublishGroup(pg)
	if err != nil {
		return nil, err
	}

	return pg, nil
}

func (b *Broker) addPublishGroup(publishGroup *PublishGroup) error {
	b.basicLock.Lock()
	defer b.basicLock.Unlock()

	if _, ok := b.publishGroupMap[publishGroup.publishGroupID]; ok {
		return mqapi.ErrPublishGroupAlreadyExist
	}

	b.publishGroupMap = CopyAddMap(b.publishGroupMap, publishGroup.publishGroupID, publishGroup)
	return nil
}

func (b *Broker) DeletePublishGroup(publishGroupId mqapi.PublishGroupId) error {
	//TODO
	return nil
}

func (b *Broker) BindPublishGroupToTopic(publishGroupId mqapi.PublishGroupId, topicId mqapi.TopicId) error {
	publishGroupMap := b.publishGroupMap
	topicMap := b.topicMap

	publishGroup, ok := publishGroupMap[publishGroupId]
	if !ok {
		return mqapi.ErrPublishGroupNotExist
	}
	topic, ok := topicMap[topicId]
	if !ok {
		return mqapi.ErrTopicNotExist
	}

	publishGroup.basicLock.Lock()
	defer publishGroup.basicLock.Unlock()

	pgTopicMap := publishGroup.topicMap
	_, ok = pgTopicMap[topicId]
	if !ok {
		publishGroup.topicMap = CopyAddMap(pgTopicMap, topicId, topic)
	}

	return nil
}

func (b *Broker) UnbindPublishGroupFromTopic(publishGroupId mqapi.PublishGroupId, topic mqapi.TopicId) error {
	//TODO
	return nil
}

func (b *Broker) DefineNewSubscribeGroup(subscribeGroupId mqapi.SubscribeGroupId, option *mqapi.SubscribeGroupOption) (mqapi.SubscribeGroup, error) {
	sg := new(SubscribeGroup)
	sg.subscribeGroupID = subscribeGroupId
	sg.queueMap = make(map[mqapi.QueueId]SgQMap)
	sg.SubCh = make(chan mqapi.SubChanElem, option.SubscribeChannelSize)
	sg.ReleaseCh = make(chan mqapi.ReleaseChanElem, option.SubscribeChannelSize)
	sg.broker = b
	if option.ObtainFailRetryInterval <= 0 {
		sg.obtainFailRetryInterval = 100
	} else {
		sg.obtainFailRetryInterval = option.ObtainFailRetryInterval
	}
	err := b.addSubscribeGroup(sg)
	if err != nil {
		return nil, err
	}

	return sg, nil
}

func (b *Broker) addSubscribeGroup(subscribeGroup *SubscribeGroup) error {
	b.basicLock.Lock()
	defer b.basicLock.Unlock()

	if _, ok := b.subscribeGroup[subscribeGroup.subscribeGroupID]; ok {
		return mqapi.ErrSubscribeGroupAlreadyExist
	}

	b.subscribeGroup = CopyAddMap(b.subscribeGroup, subscribeGroup.subscribeGroupID, subscribeGroup)
	return nil
}

func (b *Broker) DeleteSubscribeGroup(subscribeGroupId mqapi.SubscribeGroupId) error {
	//TODO
	return nil
}

func (b *Broker) BindSubscribeGroupToQueue(subscribeGroupId mqapi.SubscribeGroupId, queueId mqapi.QueueId) error {
	sgMap := b.subscribeGroup
	queueMap := b.queueMap

	sg, ok := sgMap[subscribeGroupId]
	if !ok {
		return mqapi.ErrSubscribeGroupNotExist
	}
	queue, ok := queueMap[queueId]
	if !ok {
		return mqapi.ErrQueueNotExist
	}

	sg.basicLock.Lock()
	defer sg.basicLock.Unlock()

	_, ok = sg.queueMap[queueId]
	if !ok {
		record, err := queue.CreateRecord(sg.subscribeGroupID, nil)
		if err != nil {
			return err
		}
		c := make(chan map[mqapi.QueueId]SgQMap)
		go func() {
			newMap := make(map[mqapi.QueueId]SgQMap)
			for k, v := range sg.queueMap {
				newMap[k] = v
			}
			newMap[queueId] = SgQMap{
				q:  queue,
				qr: record,
			}
			c <- newMap
		}()
		sg.queueMap = <-c
		run("subscribe_loop", func() {
			sg.PumpLoop(record, queue)
		})
		run("subscribe_release_loop", func() {
			sg.PumpReleasingLoop(record, queue)
		})
	}
	return nil
}

func (b *Broker) UnbindSubscribeGroupFromQueue(subscribeGroupId mqapi.SubscribeGroupId, queueId mqapi.QueueId) error {
	//TODO
	return nil
}

func (b *Broker) GetSubscribeGroup(subscribeGroupId mqapi.SubscribeGroupId) mqapi.SubscribeGroup {
	g := b.subscribeGroup
	sg, ok := g[subscribeGroupId]
	if ok {
		return sg
	} else {
		return nil
	}
}
func (b *Broker) GetPublishGroup(publishGroupId mqapi.PublishGroupId) mqapi.PublishGroup {
	g := b.publishGroupMap
	pg, ok := g[publishGroupId]
	if ok {
		return pg
	} else {
		return nil
	}
}

func (b *Broker) GetNode(nodeId mqapi.NodeId) mqapi.Node {
	b.clientNodeMapLock.RLock()
	node, ok := b.clientNodeMap[nodeId]
	b.clientNodeMapLock.RUnlock()
	if !ok {
		return nil
	}
	return node
}

func (b *Broker) AddNode() (mqapi.Node, error) {
	//TODO
	return nil, nil
}
