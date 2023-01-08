package mqclient

import (
	"github.com/meidoworks/nekoq/shared/idgen"
)

const (
	DeliveryTypeAtMostOnce  = "at_most_once"
	DeliveryTypeAtLeastOnce = "at_least_once"
	DeliveryTypeExactlyOnce = "exactly_once"
)

const (
	OperationNewTopic          = "new_topic"
	OperationNewQueue          = "new_queue"
	OperationBind              = "bind"
	OperationNewPublishGroup   = "new_publish_group"
	OperationNewSubscribeGroup = "new_subscribe_group"
	OperationNewMessage        = "new_message"
	OperationAckMessage        = "ack_message"
	OperationNewMessageCommit  = "new_message_commit"
)

const (
	IncomingOperationMessage = "message"
)

type ServerSideIncoming struct {
	Status    string `json:"status"`
	Info      string `json:"info"`
	RequestId string `json:"request_id"`

	PublishGroupResponse   *PublishGroupRes   `json:"publish_group_res,omitempty"`
	SubscribeGroupResponse *SubscribeGroupRes `json:"subscribe_group_res,omitempty"`

	NewMessageResponse *NewMessageRes `json:"new_message,omitempty"`

	IncomingOperation *string  `json:"operation,omitempty"`
	Message           *Message `json:"message,omitempty"`
}

type PublishGroupRes struct {
	PublishGroup string `json:"publish_group"`
}

type SubscribeGroupRes struct {
	SubscribeGroup string `json:"subscribe_group"`
}

type NewMessageRes struct {
	MessageIdList []MessageId `json:"message_id_list"`
	Topic         string      `json:"topic"`
	BindingKey    string      `json:"binding_key"`
}

type MessageId struct {
	MsgId idgen.IdType `json:"msg_id"`
	OutId idgen.IdType `json:"out_id"`
}

type ToServerSidePacket struct {
	Operation string `json:"operation"`
	RequestId string `json:"request_id"`

	NewTopic                 *NewTopicRequest          `json:"new_topic,omitempty"`
	NewQueue                 *NewQueueRequest          `json:"new_queue,omitempty"`
	NewBinding               *BindRequest              `json:"new_binding,omitempty"`
	NewPublishGroupRequest   *NewPublishGroupRequest   `json:"new_publish_group,omitempty"`
	NewSubscribeGroupRequest *NewSubscribeGroupRequest `json:"new_subscribe_group,omitempty"`
	NewMessageRequest        *NewMessageRequest        `json:"new_message,omitempty"`
	NewAckMessage            *AckMessage               `json:"ack_message,omitempty"`
	NewMessageCommitRequest  *MessageDesc              `json:"new_message_commit,omitempty"`
}

type NewTopicRequest struct {
	Topic             string `json:"topic"`
	DeliveryLevelType string `json:"delivery_level_type"`
}

type NewQueueRequest struct {
	Queue             string `json:"queue"`
	DeliveryLevelType string `json:"delivery_level_type"`
}

type BindRequest struct {
	Topic      string `json:"topic"`
	Queue      string `json:"queue"`
	BindingKey string `json:"binding_key"`
}

type NewPublishGroupRequest struct {
	Topic        string `json:"topic"`
	PublishGroup string `json:"publish_group"`
}

type NewSubscribeGroupRequest struct {
	Queue          string `json:"queue"`
	SubscribeGroup string `json:"subscribe_group"`
}

type NewMessageRequest struct {
	Topic        string `json:"topic"`
	PublishGroup string `json:"publish_group"`
	BindingKey   string `json:"binding_key"`

	Payload []byte `json:"payload"`
}

type AckMessage struct {
	SubscribeGroup string       `json:"subscribe_group"`
	Queue          string       `json:"queue"`
	MessageId      idgen.IdType `json:"message_id"`
}

type MessageDesc struct {
	MessageIdList []MessageId `json:"message_id_list"`
	Topic         string      `json:"topic"`
	BindingKey    string      `json:"binding_key"`
	PublishGroup  string      `json:"publish_group"`
}

type Message struct {
	Topic          string       `json:"topic"`
	Queue          string       `json:"queue"`
	BindingKey     string       `json:"binding_key"`
	SubscribeGroup string       `json:"subscribe_group"`
	MessageId      idgen.IdType `json:"message_id"`
	Payload        []byte       `json:"payload"`

	// immutable values
	queue     string
	messageId idgen.IdType
}

type PublishRequest struct {
	Topic      string
	BindingKey string
	Payload    []byte
}
