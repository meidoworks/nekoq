package mqclient

import "github.com/meidoworks/nekoq/shared/idgen"

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
)

type ServerSideIncoming struct {
	Status    string `json:"status"`
	Info      string `json:"info"`
	RequestId string `json:"request_id"`

	PublishGroupResponse   *PublishGroupRes   `json:"publish_group_res,omitempty"`
	SubscribeGroupResponse *SubscribeGroupRes `json:"subscribe_group_res,omitempty"`
	//TODO implement me
	Message *interface{} `json:"message"`
}

type ToServerSidePacket struct {
	Operation string `json:"operation"`
	RequestId string `json:"request_id"`

	NewTopic                 *NewTopicRequest          `json:"new_topic,omitempty"`
	NewQueue                 *NewQueueRequest          `json:"new_queue,omitempty"`
	NewBinding               *BindRequest              `json:"new_binding,omitempty"`
	NewPublishGroupRequest   *NewPublishGroupRequest   `json:"new_publish_group,omitempty"`
	NewSubscribeGroupRequest *NewSubscribeGroupRequest `json:"new_subscribe_group,omitempty"`
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

type PublishGroupRes struct {
	PublishGroup string `json:"publish_group"`
}

type SubscribeGroupRes struct {
	SubscribeGroup string `json:"subscribe_group"`
}

type NewSubscribeGroupRequest struct {
	Queue          string `json:"queue"`
	SubscribeGroup string `json:"subscribe_group"`
}

type Message struct {
	Topic     string
	Queue     string
	MessageId idgen.IdType
	Payload   []byte
}

type PublishRequest struct {
	Topic      string
	BindingKey string
	Payload    []byte
}
