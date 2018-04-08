package mq

type QueueStoreType byte

const (
	CUSTOM_STORE QueueStoreType = 0
	MEM_STORE    QueueStoreType = 1
	FILE_STORE   QueueStoreType = 2
)

type Queue struct {
	QueueID        IdType
	DeliveryLevel  DeliveryLevelType
	QueueStoreType QueueStoreType

	UncommittedMessageRetainTime int // in seconds, default 7 * 24 * 3600
	InitBatchObtainCount         int // 16
	MaxBatchObtainCount          int // 1024

	QueueChannel chan *Request

	QueueInternalId int32

	// internal
	QueueType
}

type QueueOption struct {
	QueueChannelSize        int
	QueueInboundChannelSize int

	DeliveryLevel  DeliveryLevelType
	QueueStoreType QueueStoreType

	UncommittedMessageRetainTime int // in seconds, default 7 * 24 * 3600

	// custom queue type
	CustomQueueTypeInst QueueType
}

type QueueRecord struct {
	FromId IdType
}

func (this *Topic) NewQueue(queueId IdType, option *QueueOption) (*Queue, error) {
	if option.DeliveryLevel != this.deliveryLevel {
		return nil, ErrDeliveryLevelNotMatch
	}
	q := new(Queue)
	switch option.DeliveryLevel {
	case AtMostOnce:
		q.DeliveryLevel = AtMostOnce
	case AtLeastOnce:
		q.DeliveryLevel = AtLeastOnce
	case ExactlyOnce:
		q.DeliveryLevel = ExactlyOnce
	default:
		return nil, ErrDeliveryLevelUnknown
	}
	if option.UncommittedMessageRetainTime == 0 {
		q.UncommittedMessageRetainTime = 7 * 24 * 3600
	} else {
		q.UncommittedMessageRetainTime = option.UncommittedMessageRetainTime
	}

	switch option.QueueStoreType {
	case MEM_STORE:
		q.QueueStoreType = MEM_STORE
		q.QueueType = new(MemQueue)
	case FILE_STORE:
		q.QueueStoreType = FILE_STORE
		return nil, ErrQueueStoreNotSupported
	default:
		queueTypeInst := option.CustomQueueTypeInst
		if queueTypeInst != nil {
			q.QueueType = queueTypeInst
			q.QueueStoreType = CUSTOM_STORE
		} else {
			return nil, ErrQueueStoreUnknown
		}
	}

	q.QueueID = queueId
	q.QueueChannel = make(chan *Request, option.QueueChannelSize)
	q.InitBatchObtainCount = 16
	q.MaxBatchObtainCount = 1024
	queueInternalId, err := this.broker.GenNewInternalQueueId()
	if err != nil {
		return nil, err
	}
	q.QueueInternalId = queueInternalId
	err = q.Init(q, option)
	if err != nil {
		return nil, err
	}
	err = this.broker.addQueue(q)
	if err != nil {
		return nil, err
	}

	return q, nil
}

func (this *Broker) addQueue(queue *Queue) error {
	this.basicLock.Lock()
	defer this.basicLock.Unlock()

	if _, ok := this.queueMap[queue.QueueID]; ok {
		return ErrQueueAlreadyExist
	}

	c := make(chan map[IdType]*Queue)
	go func() {
		newMap := make(map[IdType]*Queue)
		for k, v := range this.queueMap {
			newMap[k] = v
		}
		newMap[queue.QueueID] = queue

		c <- newMap
	}()
	this.queueMap = <-c
	return nil
}

func (this *Broker) deleteQueue(queueId IdType) error {
	//TODO
	return nil
}

type Partition struct {
}

// special queue
// 1. non-durable
// 2. always fan out to all subscribers
// 3. can have responses to publisher
type Subject struct {
}

type QueueType interface {
	PublishMessage(req *Request, ctx *Ctx) error
	CommitMessages(commit *MessageCommit, ctx *Ctx) error

	CreateRecord(subscribeGroupId IdType, ctx *Ctx) (*QueueRecord, error)
	BatchObtain(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error)
	ConfirmConsumed(record *QueueRecord, ack *Ack) error

	Init(queue *Queue, option *QueueOption) error
}

type BatchObtainResult struct {
	Requests []*Request
}

type SimpleQueue struct {
	queue                *Queue
	InitBatchObtainCount int
	MaxBatchObtainCount  int

	QueueInboundChannel chan *Request
}

func (this *SimpleQueue) PublishMessage(reg *Request, ctx *Ctx) error {
	this.QueueInboundChannel <- reg
	return nil
}

func (this *SimpleQueue) CommitMessages(reg *MessageCommit, ctx *Ctx) error {
	//TODO
	return nil
}

func (this *SimpleQueue) BatchObtain(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error) {
	capacity := this.InitBatchObtainCount
	reqs := make([]*Request, capacity)
	idx := 0
	ch := this.QueueInboundChannel
LOOP:
	for idx < capacity {
		select {
		case r := <-ch:
			reqs[idx] = r
			idx++
		default:
			if idx == 0 {
				r := <-ch
				reqs[idx] = r
				idx++
				continue LOOP
			} else {
				break LOOP
			}
		}
	}
	return BatchObtainResult{
		Requests: reqs[:idx],
	}, nil
}

func (this *SimpleQueue) Init(queue *Queue, option *QueueOption) error {
	this.InitBatchObtainCount = queue.InitBatchObtainCount
	this.MaxBatchObtainCount = queue.MaxBatchObtainCount
	this.QueueInboundChannel = make(chan *Request, option.QueueInboundChannelSize)
	this.queue = queue
	//run("queue_loop", this.Loop)
	return nil
}

func (this *SimpleQueue) CreateRecord(ctx *Ctx) (*QueueRecord, error) {
	return nil, nil
}

func (this *SimpleQueue) ConfirmConsumed(record *QueueRecord, ack *Ack) error {
	return nil
}
