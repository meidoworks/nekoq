package mq

func (this *Broker) BindTopicQueue(topicId, queueId IdType, tags []IdType) error {
	topicMap := this.topicMap
	queueMap := this.queueMap

	topic, ok := topicMap[topicId]
	if !ok {
		return ErrTopicNotExist
	}
	queue, ok := queueMap[queueId]
	if !ok {
		return ErrQueueNotExist
	}

	if topic.deliveryLevel != queue.DeliveryLevel {
		return ErrDeliveryLevelNotMatch
	}

	topic.basicLock.Lock()
	defer topic.basicLock.Unlock()

	if len(tags) > 0 {
		// use tag match
		c := make(chan map[IdType][]*Queue)
		go func() {
			newMap := make(map[IdType][]*Queue)
			for k, v := range topic.queueMap {
				newMap[k] = v
			}
			for _, v := range tags {
				list, ok := newMap[v]
				if ok {
					// copy
					dup := false
					for _, q := range list {
						if q.QueueID == queue.QueueID {
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
				if q.QueueID == queue.QueueID {
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

func (this *Broker) UnbindTopicQueue(topicId, queueId IdType) error {
	//TODO
	return nil
}

func (this *Broker) BindPublishGroupToTopic(publishGroupId, topicId IdType) error {
	publishGroupMap := this.publishGroupMap
	topicMap := this.topicMap

	publishGroup, ok := publishGroupMap[publishGroupId]
	if !ok {
		return ErrPublishGroupNotExist
	}
	topic, ok := topicMap[topicId]
	if !ok {
		return ErrTopicNotExist
	}

	publishGroup.basicLock.Lock()
	defer publishGroup.basicLock.Unlock()

	pgTopicMap := publishGroup.topicMap
	_, ok = pgTopicMap[topicId]
	if !ok {
		c := make(chan map[IdType]*Topic)
		go func() {
			newMap := make(map[IdType]*Topic)
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

func (this *Broker) UnbindPublishGroupToTopic(publishGroupId, topic IdType) error {
	//TODO
	return nil
}

func (this *Broker) BindSubscribeGroupToQueue(subscribeGroupId, queueId IdType) error {
	sgMap := this.subscribeGroup
	queueMap := this.queueMap

	sg, ok := sgMap[subscribeGroupId]
	if !ok {
		return ErrSubscribeGroupNotExist
	}
	queue, ok := queueMap[queueId]
	if !ok {
		return ErrQueueNotExist
	}

	sg.basicLock.Lock()
	defer sg.basicLock.Unlock()

	_, ok = sg.queueMap[queueId]
	if !ok {
		record, err := queue.CreateRecord(sg.subscribeGroupID, nil)
		if err != nil {
			return err
		}
		c := make(chan map[IdType]SgQMap)
		go func() {
			newMap := make(map[IdType]SgQMap)
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
			sg.Loop(record, queue)
		})
		run("subscribe_release_loop", func() {
			sg.ReleaseLoop(record, queue)
		})
	}
	return nil
}

func (this *Broker) UnbindSubscribeGroupToQueue(subscribeGroupId, queueId IdType) error {
	//TODO
	return nil
}
