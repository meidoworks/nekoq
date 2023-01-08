package mq

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/shared/idgen"
)

const (
	DeliveryTypeAtMostOnce  = "at_most_once"
	DeliveryTypeAtLeastOnce = "at_least_once"
	DeliveryTypeExactlyOnce = "exactly_once"
)

const (
	ResponseOperationMessage = "message"
)

type GeneralReq struct {
	Operation string `json:"operation"`
	RequestId string `json:"request_id"`

	NewTopic          *TopicDef          `json:"new_topic,omitempty"`
	NewQueue          *QueueDef          `json:"new_queue,omitempty"`
	NewBinding        *BindDef           `json:"new_binding,omitempty"`
	NewPublishGroup   *PublishGroupDef   `json:"new_publish_group,omitempty"`
	NewSubscribeGroup *SubscribeGroupDef `json:"new_subscribe_group,omitempty"`
	NewMessage        *MessageDef        `json:"new_message,omitempty"`
	NewMessageCommit  *MessageDescDef    `json:"new_message_commit,omitempty"`
	AckMessage        *AckDef            `json:"ack_message,omitempty"`
}

type TopicDef struct {
	Topic             string `json:"topic"`
	DeliveryLevelType string `json:"delivery_level_type"`
}

type QueueDef struct {
	Queue             string `json:"queue"`
	DeliveryLevelType string `json:"delivery_level_type"`
}

type BindDef struct {
	Topic      string `json:"topic"`
	Queue      string `json:"queue"`
	BindingKey string `json:"binding_key"`
}

type PublishGroupDef struct {
	Topic        string `json:"topic"`
	PublishGroup string `json:"publish_group"`
}

type SubscribeGroupDef struct {
	Queue          string `json:"queue"`
	SubscribeGroup string `json:"subscribe_group"`
}

type MessageDef struct {
	Topic        string `json:"topic"`
	PublishGroup string `json:"publish_group"`
	BindingKey   string `json:"binding_key"`

	Payload []byte `json:"payload"`
}

type MessageDescDef struct {
	MessageIdList []struct {
		MsgId idgen.IdType `json:"msg_id"`
		OutId idgen.IdType `json:"out_id"`
	} `json:"message_id_list"`
	Topic        string `json:"topic"`
	BindingKey   string `json:"binding_key"`
	PublishGroup string `json:"publish_group"`
}

type AckDef struct {
	SubscribeGroup string      `json:"subscribe_group"`
	Queue          string      `json:"queue"`
	MessageId      mqapi.MsgId `json:"message_id"`
}

type GeneralRes struct {
	Status    string `json:"status"`
	Info      string `json:"info"`
	RequestId string `json:"request_id"`

	PublishGroupResponse   *PublishGroupRes   `json:"publish_group_res,omitempty"`
	SubscribeGroupResponse *SubscribeGroupRes `json:"subscribe_group_res,omitempty"`

	NewMessageResponse *NewMessageRes `json:"new_message,omitempty"`

	Operation      *string         `json:"operation,omitempty"`
	WrittenMessage *WrittenMessage `json:"message,omitempty"`
}

type PublishGroupRes struct {
	PublishGroup string `json:"publish_group"`
}

type SubscribeGroupRes struct {
	SubscribeGroup string `json:"subscribe_group"`
}

type NewMessageRes struct {
	MessageIdList []struct {
		MsgId idgen.IdType `json:"msg_id"`
		OutId idgen.IdType `json:"out_id"`
	} `json:"message_id_list"`
	Topic      string `json:"topic"`
	BindingKey string `json:"binding_key"`
}

type WrittenMessage struct {
	Topic          string      `json:"topic"`
	Queue          string      `json:"queue"`
	BindingKey     string      `json:"binding_key"`
	SubscribeGroup string      `json:"subscribe_group"`
	MessageId      mqapi.MsgId `json:"message_id"`
	Payload        []byte      `json:"payload"`
}
