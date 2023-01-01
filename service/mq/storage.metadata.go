package mq

import (
	"errors"
	"fmt"
	"sync"

	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/pelletier/go-toml"

	"github.com/spf13/afero"
)

var (
	ErrDefinitionMismatch    = errors.New("definition mismatch")
	ErrInvalidInputParameter = errors.New("invalid input parameter")
)

const (
	metadataFolder = "data"
	metadataFile   = "messagequeue.metadata"
)

var _container *metadataContainer

func GetMetadataContainer() *metadataContainer {
	c := _container
	if c == nil {
		panic(errors.New("metadata container is not initialized"))
	}
	return c
}

type metadataContainer struct {
	Topics   []*metaTopic                `toml:"topics"`
	Queues   []*metaQueue                `toml:"queues"`
	Bindings []*metaTopicAndQueueBinding `toml:"bindings"`

	topicMap map[string]*metaTopic
	queueMap map[string]*metaQueue

	lock sync.Mutex
}

func (m *metadataContainer) initMem() {
	m.topicMap = make(map[string]*metaTopic)
	m.queueMap = make(map[string]*metaQueue)
}

func (m *metadataContainer) PrepareBroker() {
	// load topic
	for _, v := range m.Topics {
		m.Topics = append(m.Topics, v)
		m.topicMap[v.Topic] = v
		to := &mqapi.TopicOption{
			DeliveryLevel: convertDeliveryLevelType(v.DeliveryLevelType),
		}
		_, err := GetBroker().DefineNewTopic(v.TopicId, to)
		if err != nil {
			panic(err)
		}
	}
	//TODO load queue
	//TODO load binding
}

func (m *metadataContainer) NewTopic(t *TopicDef) (mqapi.TopicId, bool, error) {
	id, newlyAdded, err := m.newTopic0(t)
	if err != nil {
		return id, newlyAdded, err
	}
	if newlyAdded {
		//FIXME perhaps here we should persist metadata first?
		err = _persistMetadata()
	}
	return id, newlyAdded, err
}

func (m *metadataContainer) newTopic0(t *TopicDef) (mqapi.TopicId, bool, error) {
	// validation
	if !ValidateNameForBrokerMechanisms(t.Topic) {
		return mqapi.TopicId{}, false, ErrInvalidInputParameter
	}
	if !validateDeliveryType(t.DeliveryLevelType) {
		return mqapi.TopicId{}, false, ErrInvalidInputParameter
	}

	id, err := idgenerator.Next()
	if err != nil {
		return mqapi.TopicId{}, false, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	ov, ok := m.topicMap[t.Topic]
	// same definition
	if ok {
		// check options equivalent
		if ov.DeliveryLevelType != t.DeliveryLevelType {
			return mqapi.TopicId{}, false, ErrDefinitionMismatch
		} else {
			return ov.TopicId, false, nil
		}
	}

	mt := &metaTopic{
		Topic:             t.Topic,
		DeliveryLevelType: t.DeliveryLevelType,
		TopicId:           mqapi.TopicId(id),
	}
	m.Topics = append(m.Topics, mt)
	m.topicMap[t.Topic] = mt

	return mt.TopicId, true, nil
}

type metaTopicAndQueueBinding struct {
}

type metaQueue struct {
}

type metaTopic struct {
	Topic             string        `toml:"topic"`
	DeliveryLevelType string        `toml:"delivery_level_type"`
	TopicId           mqapi.TopicId `toml:"topic_id"`
}

func LoadMetadata() error {
	fs := afero.NewOsFs()
	if ok, err := afero.Exists(fs, fmt.Sprint(metadataFolder, afero.FilePathSeparator, metadataFile)); err != nil {
		return err
	} else if ok {
		data, err := afero.ReadFile(fs, fmt.Sprint(metadataFolder, afero.FilePathSeparator, metadataFile))
		if err != nil {
			return err
		}

		m := new(metadataContainer)
		err = toml.Unmarshal(data, m)
		if err != nil {
			return err
		}
		_container = m
	} else {
		if ok, _ = afero.Exists(fs, metadataFolder); !ok {
			_ = fs.MkdirAll(metadataFolder, 0755)
		}
		if _, err := fs.Create(fmt.Sprint(metadataFolder, afero.FilePathSeparator, metadataFile)); err != nil {
			return err
		}
		_container = new(metadataContainer)
	}

	_container.initMem()

	return nil
}

func _persistMetadata() error {
	//TODO need optimize callee
	fs := afero.NewOsFs()

	GetMetadataContainer().lock.Lock()
	defer GetMetadataContainer().lock.Unlock()

	data, err := toml.Marshal(_container)
	if err != nil {
		return err
	}
	return afero.WriteFile(fs, fmt.Sprint(metadataFolder, afero.FilePathSeparator, metadataFile), data, 0644)
}

func validateDeliveryType(t string) bool {
	switch t {
	case DeliveryTypeExactlyOnce:
		fallthrough
	case DeliveryTypeAtMostOnce:
		fallthrough
	case DeliveryTypeAtLeastOnce:
		return true
	default:
		return false
	}
}
