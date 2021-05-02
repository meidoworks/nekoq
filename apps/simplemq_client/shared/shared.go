package shared

type MessageMessage struct {
	Result int8   `json:"result"`
	Type   int8   `json:"type"`
	ReqId  string `json:"req_id"`

	Topic   string   `json:"topic"`
	Queues  []string `json:"queues"`
	Header  []byte   `json:"header"`
	Payload []byte   `json:"payload"`
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
