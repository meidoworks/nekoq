package mqimpl

import (
	"context"
	"sync"
	"time"

	"github.com/meidoworks/nekoq/service/mqapi"
)

var _ mqapi.SubscribeGroup = new(SubscribeGroup)

type SgQMap struct {
	q  *Queue
	qr *mqapi.QueueRecord
}

type SubscribeGroup struct {
	subscribeGroupID mqapi.SubscribeGroupId

	queueMap  map[mqapi.QueueId]SgQMap
	basicLock sync.Mutex

	obtainFailRetryInterval int // milliseconds, default 100ms

	SubCh     chan mqapi.SubChanElem
	ReleaseCh chan mqapi.ReleaseChanElem

	broker *Broker
}

func (sg *SubscribeGroup) ReleaseChannel() <-chan mqapi.ReleaseChanElem {
	return sg.ReleaseCh
}

func (sg *SubscribeGroup) SubscribeChannel() <-chan mqapi.SubChanElem {
	return sg.SubCh
}

func (sg *SubscribeGroup) PumpLoop(record *mqapi.QueueRecord, queue *Queue) {
	out := sg.SubCh
	errCnt := 0
	obtainFailRetryInterval := sg.obtainFailRetryInterval
	for {
		result, err := queue.BatchObtain(record, 16, nil)
		if err != nil {
			LogError("subscribeGroup loop error while batchObtain:", err)
			if result.Requests != nil && len(result.Requests) > 0 {
				// fall through and process messages
			} else {
				// handle error: retry and limit retry
				errCnt++
				if errCnt > 3 {
					time.Sleep(time.Duration(obtainFailRetryInterval) * time.Millisecond)
					errCnt = 0
				}
				continue
			}
		}
		for _, v := range result.Requests {
			out <- mqapi.SubChanElem{
				Request:     v,
				Queue:       queue,
				QueueRecord: record, //FIXME this may not be appropriate since record will represent one of the batch offset but all messages will share the same one
			}
		}
	}
}

func (sg *SubscribeGroup) PumpReleasingLoop(record *mqapi.QueueRecord, queue *Queue) {
	if queue.DeliveryLevel() != mqapi.ExactlyOnce {
		LogInfo("Queue is not in ExactlyOnce mode. PumpReleasingLoop exit.")
		return
	}
	out := sg.ReleaseCh
	errCnt := 0
	obtainFailRetryInterval := sg.obtainFailRetryInterval
	for {
		result, err := queue.BatchObtainReleasing(record, 16, nil)
		if err != nil {
			LogError("subscribeGroup loop error while BatchObtainReleasing:", err)
			if result.Requests != nil && len(result.Requests) > 0 {
				// fall through and process messages
			} else {
				// handle error: retry and limit retry
				errCnt++
				if errCnt > 3 {
					time.Sleep(time.Duration(obtainFailRetryInterval) * time.Millisecond)
					errCnt = 0
				}
				continue
			}
		}
		for _, v := range result.Requests {
			out <- mqapi.ReleaseChanElem{
				Request:     v,
				Queue:       queue,
				QueueRecord: record, //FIXME this may not be appropriate since record will represent one of the batch offset but all messages will share the same one
			}
		}
	}
}

// Commit support the following QoS
// qos - at least once - ack
// qos - exactly once - commit
func (sg *SubscribeGroup) Commit(queueId mqapi.QueueId, record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	queue, ok := sg.queueMap[queueId]
	if !ok {
		return mqapi.ErrQueueNotExist
	}

	// handle reply
	if queue.q.DeliveryLevel() == mqapi.AtLeastOnce && ack.Reply != nil {
		// call broker to reply
		if err := sg.reply(ack); err != nil {
			return err
		}
	}
	return queue.q.ConfirmConsumed(record, ack)
}

// Release support the following QoS
// qos - exactly once - release
func (sg *SubscribeGroup) Release(queueId mqapi.QueueId, record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	queue, ok := sg.queueMap[queueId]
	if !ok {
		return mqapi.ErrQueueNotExist
	}

	// handle reply
	if queue.q.DeliveryLevel() == mqapi.ExactlyOnce && ack.Reply != nil {
		// call broker to reply
		if err := sg.reply(ack); err != nil {
			return err
		}
	}
	return queue.q.ReleaseConsumed(record, ack)
}

func (sg *SubscribeGroup) reply(ack *mqapi.Ack) error {
	switch ack.Reply.ReplyType {
	case 1:
		//to node
		node := sg.broker.GetNode(ack.Reply.ReplyToNode)
		if node != nil {
			return mqapi.ErrNodeNotExist
		}
		return node.DirectReply(ack.Reply, &mqapi.Ctx{Context: context.Background()})
	case 2:
		//to publishGroup
		pg := sg.broker.GetPublishGroup(ack.Reply.ReplyToPublishGroup)
		if pg == nil {
			return mqapi.ErrReplyDestinationNotExist
		}
		return pg.Reply(ack.Reply, &mqapi.Ctx{Context: context.Background()})
	default:
		return mqapi.ErrReplyTypeUnknown
	}
}

func (sg *SubscribeGroup) Join(node mqapi.Node) error {
	return node.SubscribeGroupInitialize(sg)
}

func (sg *SubscribeGroup) Leave(node *Node) {
	//TODO
}
