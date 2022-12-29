package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"sync"
	"time"
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
}

func (this *SubscribeGroup) ReleaseChannel() <-chan mqapi.ReleaseChanElem {
	return this.ReleaseCh
}

func (this *SubscribeGroup) SubscribeChannel() <-chan mqapi.SubChanElem {
	return this.SubCh
}

func (this *SubscribeGroup) PumpLoop(record *mqapi.QueueRecord, queue *Queue) {
	out := this.SubCh
	errCnt := 0
	obtainFailRetryInterval := this.obtainFailRetryInterval
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
				QueueRecord: record,
			}
		}
	}
}

func (this *SubscribeGroup) PumpReleasingLoop(record *mqapi.QueueRecord, queue *Queue) {
	if queue.DeliveryLevel() != mqapi.ExactlyOnce {
		LogInfo("Queue is not in ExactlyOnce mode. PumpReleasingLoop exit.")
		return
	}
	out := this.ReleaseCh
	errCnt := 0
	obtainFailRetryInterval := this.obtainFailRetryInterval
	for {
		result, err := queue.BatchObtainReleased(record, 16, nil)
		if err != nil {
			LogError("subscribeGroup loop error while BatchObtainReleased:", err)
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
				QueueRecord: record,
			}
		}
	}
}

// qos - at least once - ack
// qos - exactly once - commit
func (this *SubscribeGroup) Commit(queueId mqapi.QueueId, record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	queue, ok := this.queueMap[queueId]
	if !ok {
		return mqapi.ErrQueueNotExist
	}
	return queue.q.ConfirmConsumed(record, ack)
}

// qos - exactly once - release
func (this *SubscribeGroup) Release(queueId mqapi.QueueId, record *mqapi.QueueRecord, ack *mqapi.Ack) error {
	queue, ok := this.queueMap[queueId]
	if !ok {
		return mqapi.ErrQueueNotExist
	}
	return queue.q.ReleaseConsumed(record, ack)
}

func (this *SubscribeGroup) Join(node mqapi.Node) error {
	return node.SubscribeGroupInitialize(this)
}

func (this *SubscribeGroup) Leave(node *Node) {
	//TODO
}
