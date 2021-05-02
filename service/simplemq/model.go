package simplemq

type TopicQueueMapping struct {
	MappingId int64
	Topic     string
	QueueName string
}

const (
	MessageTypePublish            = 1
	MessageTypePublishAck         = 2
	MessageTypeSubQueueMsg        = 3
	MessageTypeSubQueueMsgResp    = 4
	MessageTypeSubQueueMsgAck     = 5
	MessageTypeSubQueueMsgAckResp = 6
	MessageTypeErrorMsg           = 0
	MessageTypeBye                = 99

	MessageResultSuccess        = 0
	MessageResultNoMsg          = 1
	MessageResultGeneralFailure = 99

	MessageStatusReady       = 0
	MessageStatusInFlight    = 1
	MessageStatusDelivered   = 2
	MessageStatusUndelivered = 3
	MessageDeletedActive     = 0
	MessageDeletedDeleted    = 1
)

type MessageRequest struct {
	QueueName string
	RespCh    chan *Message
}
type MessageAckRequest struct {
	ReqId  string
	RespCh chan struct{}
}

type Message struct {
	MsgId         int64
	MsgStatus     int16
	MsgDeleted    int16
	Topic         string
	QueueName     string
	Header        []byte
	Payload       []byte
	TimeCreated   int64
	TimeDelivered *int64
	ScheduleTime  int64
}

type MessageMessage struct {
	Result int8   `json:"result"`
	Type   int8   `json:"type"`
	ReqId  string `json:"req_id"`

	Topic   string   `json:"topic"`
	Queues  []string `json:"queues"`
	Header  []byte   `json:"header"`
	Payload []byte   `json:"payload"`
}
