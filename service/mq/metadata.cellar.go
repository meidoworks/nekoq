package mq

import "github.com/meidoworks/nekoq/service/mqapi"

type CellarMetadataStorage struct {
}

func (c *CellarMetadataStorage) RegisterMqInstance() error {
	return nil
}

func (c *CellarMetadataStorage) NewTopic(topic string) (mqapi.TopicId, error) {
	return mqapi.TopicId{}, nil
}

func NewCellarMetadataStorage() (*CellarMetadataStorage, error) {
	return nil, nil
}
