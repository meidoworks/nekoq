package mq

const (
	DeliveryTypeAtMostOnce  = "at_most_once"
	DeliveryTypeAtLeastOnce = "at_least_once"
	DeliveryTypeExactlyOnce = "exactly_once"
)

type GeneralReq struct {
	Operation string `json:"operation"`
	RequestId string `json:"request_id"`

	NewTopic   *TopicDef `json:"new_topic,omitempty"`
	NewQueue   *QueueDef `json:"new_queue,omitempty"`
	NewBinding *BindDef  `json:"new_binding,omitempty"`
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

type GeneralRes struct {
	Status    string `json:"status"`
	Info      string `json:"info"`
	RequestId string `json:"request_id"`
}
