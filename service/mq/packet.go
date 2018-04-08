package mq

type DeliveryLevelType byte

const (
	AtMostOnce  DeliveryLevelType = 0
	AtLeastOnce DeliveryLevelType = 1
	ExactlyOnce DeliveryLevelType = 2
)

type Body []byte

type Request struct {
	Header       Header
	BatchMessage []Message
}

type MessageId struct {
	MsgId IdType
	OutId IdType
}

type Message struct {
	MessageId
	Body Body
}

type Header struct {
	Dup           bool
	DeliveryLevel DeliveryLevelType

	TopicId   IdType
	Partition IdType
	Tags      []IdType
	ReplyTo   *ReplyTo
}

type ReplyTo struct {
	ReplyType byte //0 - non, 1 - nodeId, 2 - publishGroupId
	ReplyTo   IdType
}

// DeliveryLevelType = 1
type Ack struct {
	AckIdList []MessageId
}

// DeliveryLevelType = 2
type MessageReceived struct {
	Ack
}

type MessageCommit struct {
	Header
	Ack
}

type MessageFinish struct {
	Ack
}

type Reply struct {
	ReplyTo
	TopicId IdType
	Message
}
