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
	RedeliverIntervalTime        int // in seconds, default 5 seconds

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
		// copy old kv
		for k, v := range this.queueMap {
			newMap[k] = v
		}
		// add new
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
	PrePublishMessage(req *Request, ctx *Ctx) error
	CommitMessages(commit *MessageCommit, ctx *Ctx) error

	CreateRecord(subscribeGroupId IdType, ctx *Ctx) (*QueueRecord, error)
	BatchObtain(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error)
	BatchObtainReleased(record *QueueRecord, maxCnt int, ctx *Ctx) (BatchObtainResult, error)
	ConfirmConsumed(record *QueueRecord, ack *Ack) error
	ReleaseConsumed(record *QueueRecord, ack *Ack) error

	Init(queue *Queue, option *QueueOption) error
}

type BatchObtainResult struct {
	Requests []*Request
}
