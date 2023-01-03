package mqclient

const (
	DeliveryTypeAtMostOnce  = "at_most_once"
	DeliveryTypeAtLeastOnce = "at_least_once"
	DeliveryTypeExactlyOnce = "exactly_once"
)

const (
	OperationNewTopic = "new_topic"
	OperationNewQueue = "new_queue"
	OperationBind     = "bind"
)

type ServerSideIncoming struct {
	Status    string `json:"status"`
	Info      string `json:"info"`
	RequestId string `json:"request_id"`

	//TODO implement me
	Message *interface{} `json:"message"`
}

type ToServerSidePacket struct {
	Operation string `json:"operation"`
	RequestId string `json:"request_id"`

	NewTopic   *NewTopicRequest `json:"new_topic,omitempty"`
	NewQueue   *NewQueueRequest `json:"new_queue,omitempty"`
	NewBinding *BindRequest     `json:"new_binding,omitempty"`
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

type PublishRequest struct {
	Topic      string
	BindingKey string
	Payload    []byte
}
