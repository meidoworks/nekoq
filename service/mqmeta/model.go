package mqmeta

import "github.com/meidoworks/nekoq/service/mqapi"

type TopicReq struct {
	Topic             string                  `json:"topic"`
	DeliveryLevelType mqapi.DeliveryLevelType `json:"delivery_level_type"`

	TopicId string `json:"topic_id"`
}

type QueueReq struct {
	Queue             string                  `json:"queue"`
	DeliveryLevelType mqapi.DeliveryLevelType `json:"delivery_level_type"`
	QueueType         string                  `json:"queue_type"`

	QueueId string `json:"queue_id"`
}

type PublishGroupReq struct {
	PublicGroup string `json:"public_group"`

	PublicGroupId string `json:"public_group_id"`
}

type SubscribeGroupReq struct {
	SubscribeGroup string `json:"subscribe_group"`

	SubscribeGroupId string `json:"subscribe_group_id"`
}

type BindTopicAndPublishGroupReq struct {
	Topic        string `json:"topic"`
	PublishGroup string `json:"publish_group"`
}

type BindQueueAndSubscribeGroupReq struct {
	Queue          string `json:"queue"`
	SubscribeGroup string `json:"subscribe_group"`
}

type BindTopicQueueReq struct {
	Topic               string `json:"topic"`
	Queue               string `json:"queue"`
	BindKeyMatchingRule string `json:"binding_rule"`

	TagId string `json:"tag_id"`
}

type PublishMeta struct {
	TopicId mqapi.TopicId
	Tags    []mqapi.TagId
}
