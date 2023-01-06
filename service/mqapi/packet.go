package mqapi

type Body []byte

type Request struct {
	Header       Header
	BatchMessage []Message
}

type MessageId struct {
	MsgId MsgId
	OutId OutId
}

type Message struct {
	MessageId

	Attributes map[string][]string
	Body       Body
}

type Header struct {
	Dup           bool
	DeliveryLevel DeliveryLevelType

	TopicId   TopicId
	Partition PartitionId
	Tags      []TagId
	ReplyTo   *ReplyTo
}

type ReplyTo struct {
	ReplyType           byte //0 - non, 1 - nodeId, 2 - publishGroupId
	ReplyToNode         NodeId
	ReplyToPublishGroup PublishGroupId
}

// Ack is used for when DeliveryLevelType = 1
type Ack struct {
	AckIdList []MessageId
}

// MessageReceived is used for when DeliveryLevelType = 2
type MessageReceived struct {
	Ack
}

// MessageCommit is used for when DeliveryLevelType = 2
type MessageCommit struct {
	Header
	Ack
}

// MessageFinish is used for when DeliveryLevelType = 2
type MessageFinish struct {
	Ack
}

type Reply struct {
	ReplyTo
	TopicId TopicId
	Message
}

type BatchObtainResult struct {
	Requests []*Request
}
