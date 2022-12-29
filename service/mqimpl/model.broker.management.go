package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
)

func (this *Broker) DefineNewTopic(topicId mqapi.TopicId, option *mqapi.TopicOption) (mqapi.Topic, error) {
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
	topicInternalId, err := this.GenNewInternalTopicId()
	if err != nil {
		return nil, err
	}
	t.topicMessageIdGen = mqapi.NewIdGen(this.nodeId, topicInternalId)
	t.topicInternalId = topicInternalId
	t.broker = this

	err = this.addTopic(t)
	if err != nil {
		return nil, err
	}

	return t, nil
}

func (this *Broker) addTopic(topic *Topic) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.topicMap[topic.topicID]; ok {
		return mqapi.ErrTopicAlreadyExist
	}

	c := make(chan map[mqapi.TopicId]*Topic)
	go func() {
		newMap := make(map[mqapi.TopicId]*Topic)
		for k, v := range this.topicMap {
			newMap[k] = v
		}
		newMap[topic.topicID] = topic

		c <- newMap
	}()
	this.topicMap = <-c
	return nil
}

func (this *Broker) DeleteTopic(topicId mqapi.TopicId) error {
	//TODO
	return nil
}

func (this *Broker) DefineNewQueue(queueId mqapi.QueueId, option *mqapi.QueueOption) (mqapi.Queue, error) {
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

	switch option.QueueStoreType {
	case mqapi.MEM_STORE:
		q.QueueStoreType = mqapi.MEM_STORE
		q.QueueType = new(MemQueue)
	case mqapi.FILE_STORE:
		q.QueueStoreType = mqapi.FILE_STORE
		return nil, mqapi.ErrQueueStoreNotSupported
	default:
		queueTypeInst := option.CustomQueueTypeInst
		if queueTypeInst != nil {
			q.QueueType = queueTypeInst
			q.QueueStoreType = mqapi.CUSTOM_STORE
		} else {
			return nil, mqapi.ErrQueueStoreUnknown
		}
	}

	q.queueID = queueId
	q.QueueChannel = make(chan *mqapi.Request, option.QueueChannelSize)
	q.InitBatchObtainCount = 16
	q.MaxBatchObtainCount = 1024
	queueInternalId, err := this.GenNewInternalQueueId()
	if err != nil {
		return nil, err
	}
	q.QueueInternalId = queueInternalId
	err = q.Init(q, option)
	if err != nil {
		return nil, err
	}
	err = this.addQueue(q)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (this *Broker) addQueue(queue *Queue) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.queueMap[queue.queueID]; ok {
		return mqapi.ErrQueueAlreadyExist
	}

	c := make(chan map[mqapi.QueueId]*Queue)
	go func() {
		newMap := make(map[mqapi.QueueId]*Queue)
		// copy old kv
		for k, v := range this.queueMap {
			newMap[k] = v
		}
		// add new
		newMap[queue.queueID] = queue

		c <- newMap
	}()
	this.queueMap = <-c
	return nil
}

func (this *Broker) DeleteQueue(queueId mqapi.QueueId) error {
	//TODO
	return nil
}

func (this *Broker) BindTopicAndQueue(topicId mqapi.TopicId, queueId mqapi.QueueId, tags []mqapi.TagId) error {
	topicMap := this.topicMap
	queueMap := this.queueMap

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

func (this *Broker) UnbindTopicAndQueue(topicId mqapi.TopicId, queueId mqapi.QueueId) error {
	//TODO
	return nil
}

func (this *Broker) DefineNewPublishGroup(publishGroupId mqapi.PublishGroupId) (mqapi.PublishGroup, error) {
	pg := new(PublishGroup)
	pg.publishGroupID = publishGroupId
	pg.topicMap = make(map[mqapi.TopicId]*Topic)
	err := this.addPublishGroup(pg)
	if err != nil {
		return nil, err
	}

	return pg, nil
}

func (this *Broker) addPublishGroup(publishGroup *PublishGroup) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.publishGroupMap[publishGroup.publishGroupID]; ok {
		return mqapi.ErrPublishGroupAlreadyExist
	}

	c := make(chan map[mqapi.PublishGroupId]*PublishGroup)
	go func() {
		newMap := make(map[mqapi.PublishGroupId]*PublishGroup)
		// copy old kv
		for k, v := range this.publishGroupMap {
			newMap[k] = v
		}
		// add new
		newMap[publishGroup.publishGroupID] = publishGroup

		c <- newMap
	}()
	this.publishGroupMap = <-c
	return nil
}

func (this *Broker) DeletePublishGroup(publishGroupId mqapi.PublishGroupId) error {
	//TODO
	return nil
}

func (this *Broker) BindPublishGroupToTopic(publishGroupId mqapi.PublishGroupId, topicId mqapi.TopicId) error {
	publishGroupMap := this.publishGroupMap
	topicMap := this.topicMap

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
		c := make(chan map[mqapi.TopicId]*Topic)
		go func() {
			newMap := make(map[mqapi.TopicId]*Topic)
			for k, v := range pgTopicMap {
				newMap[k] = v
			}
			newMap[topicId] = topic
			c <- newMap
		}()
		publishGroup.topicMap = <-c
	}

	return nil
}

func (this *Broker) UnbindPublishGroupFromTopic(publishGroupId mqapi.PublishGroupId, topic mqapi.TopicId) error {
	//TODO
	return nil
}

func (this *Broker) DefineNewSubscribeGroup(subscribeGroupId mqapi.SubscribeGroupId, option *mqapi.SubscribeGroupOption) (mqapi.SubscribeGroup, error) {
	sg := new(SubscribeGroup)
	sg.subscribeGroupID = subscribeGroupId
	sg.queueMap = make(map[mqapi.QueueId]SgQMap)
	sg.SubCh = make(chan mqapi.SubChanElem, option.SubscribeChannelSize)
	sg.ReleaseCh = make(chan mqapi.ReleaseChanElem, option.SubscribeChannelSize)
	if option.ObtainFailRetryInterval <= 0 {
		sg.obtainFailRetryInterval = 100
	} else {
		sg.obtainFailRetryInterval = option.ObtainFailRetryInterval
	}
	err := this.addSubscribeGroup(sg)
	if err != nil {
		return nil, err
	}

	return sg, nil
}

func (this *Broker) addSubscribeGroup(subscribeGroup *SubscribeGroup) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.subscribeGroup[subscribeGroup.subscribeGroupID]; ok {
		return mqapi.ErrSubscribeGroupAlreadyExist
	}

	c := make(chan map[mqapi.SubscribeGroupId]*SubscribeGroup)
	go func() {
		newMap := make(map[mqapi.SubscribeGroupId]*SubscribeGroup)
		// copy old kv
		for k, v := range this.subscribeGroup {
			newMap[k] = v
		}
		// add new
		newMap[subscribeGroup.subscribeGroupID] = subscribeGroup

		c <- newMap
	}()
	this.subscribeGroup = <-c
	return nil
}

func (this *Broker) DeleteSubscribeGroup(subscribeGroupId mqapi.SubscribeGroupId) error {
	//TODO
	return nil
}

func (this *Broker) BindSubscribeGroupToQueue(subscribeGroupId mqapi.SubscribeGroupId, queueId mqapi.QueueId) error {
	sgMap := this.subscribeGroup
	queueMap := this.queueMap

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

func (this *Broker) UnbindSubscribeGroupFromQueue(subscribeGroupId mqapi.SubscribeGroupId, queueId mqapi.QueueId) error {
	//TODO
	return nil
}
