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
	ErrComponentNotExist     = errors.New("component not exist")
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

	topicMap   map[string]*metaTopic
	queueMap   map[string]*metaQueue
	bindingMap map[string][]*metaTopicAndQueueBinding

	lock sync.RWMutex

	publishGroupMap   map[string]mqapi.PublishGroupId
	subscribeGroupMap map[string]mqapi.SubscribeGroupId
	pgMap             map[string]mqapi.PublishGroup
	sgMap             map[string]mqapi.SubscribeGroup
	groupLock         sync.RWMutex
}

func (m *metadataContainer) initMem() {
	m.topicMap = make(map[string]*metaTopic)
	m.queueMap = make(map[string]*metaQueue)
	m.bindingMap = make(map[string][]*metaTopicAndQueueBinding)
	m.publishGroupMap = make(map[string]mqapi.PublishGroupId)
	m.subscribeGroupMap = make(map[string]mqapi.SubscribeGroupId)
	m.pgMap = make(map[string]mqapi.PublishGroup)
	m.sgMap = make(map[string]mqapi.SubscribeGroup)
}

func (m *metadataContainer) PrepareBroker() {
	// load topic
	for _, v := range m.Topics {
		m.topicMap[v.Topic] = v
		to := &mqapi.TopicOption{
			DeliveryLevel: convertDeliveryLevelType(v.DeliveryLevelType),
		}
		_, err := GetBroker().DefineNewTopic(v.TopicId, to)
		if err != nil {
			panic(err)
		}
	}
	// load queue
	for _, v := range m.Queues {
		m.queueMap[v.Queue] = v
		to := &mqapi.QueueOption{
			DeliveryLevel: convertDeliveryLevelType(v.DeliveryLevelType),
			QueueType:     "memory", //FIXME should be configured on demand
		}
		_, err := GetBroker().DefineNewQueue(v.QueueId, to)
		if err != nil {
			panic(err)
		}
	}
	// load binding
	for _, v := range m.Bindings {
		bds, ok := m.bindingMap[v.Topic]
		if !ok {
			bds = make([]*metaTopicAndQueueBinding, 0, 16)
		}
		bds = append(bds, v)
		m.bindingMap[v.Topic] = bds
		err := GetBroker().BindTopicAndQueue(v.TopicId, v.QueueId, []mqapi.TagId{v.Tag})
		if err != nil {
			panic(err)
		}
	}
}

func (m *metadataContainer) FilterOutBindingTag(topic, messageBindingKey string) []mqapi.TagId {
	bds := m.getTopicBindings(topic)
	if len(bds) == 0 {
		return nil
	}
	var tags []mqapi.TagId
	for _, v := range bds {
		if matchBindingKey(v.BindingKey, messageBindingKey) {
			tags = append(tags, v.Tag)
		}
	}
	return tags
}

func (m *metadataContainer) getTopicBindings(t string) []*metaTopicAndQueueBinding {
	m.lock.RLock()
	defer m.lock.RUnlock()

	bds, ok := m.bindingMap[t]
	if ok {
		return bds
	} else {
		return nil
	}
}

func (m *metadataContainer) GetTopic(t string) *metaTopic {
	m.lock.RLock()
	defer m.lock.RUnlock()

	tp, ok := m.topicMap[t]
	if ok {
		return tp
	} else {
		return nil
	}
}

func (m *metadataContainer) GetQueue(q string) *metaQueue {
	m.lock.RLock()
	defer m.lock.RUnlock()

	qp, ok := m.queueMap[q]
	if ok {
		return qp
	} else {
		return nil
	}
}

func (m *metadataContainer) InsertSg(sgName string, subscribeGroup mqapi.SubscribeGroup) bool {
	m.groupLock.Lock()
	defer m.groupLock.Unlock()

	_, ok := m.sgMap[sgName]
	if ok {
		return false
	} else {
		m.sgMap[sgName] = subscribeGroup
		return true
	}
}

func (m *metadataContainer) GetSg(sgName string) mqapi.SubscribeGroup {
	m.groupLock.RLock()
	defer m.groupLock.RUnlock()

	sg, ok := m.sgMap[sgName]
	if ok {
		return sg
	} else {
		return nil
	}
}

func (m *metadataContainer) InsertPg(pgName string, publishGroup mqapi.PublishGroup) bool {
	m.groupLock.Lock()
	defer m.groupLock.Unlock()

	_, ok := m.pgMap[pgName]
	if ok {
		return false
	} else {
		m.pgMap[pgName] = publishGroup
		return true
	}
}

func (m *metadataContainer) GetPg(pgName string) mqapi.PublishGroup {
	m.groupLock.RLock()
	defer m.groupLock.RUnlock()

	pg, ok := m.pgMap[pgName]
	if ok {
		return pg
	} else {
		return nil
	}
}

func (m *metadataContainer) NewSubscribeGroup(g string) (mqapi.SubscribeGroupId, error) {
	i, err := idgenerator.Next()
	if err != nil {
		return mqapi.SubscribeGroupId{}, err
	}

	m.groupLock.Lock()
	defer m.groupLock.Unlock()

	r, ok := m.subscribeGroupMap[g]
	if ok {
		return r, nil
	} else {
		m.publishGroupMap[g] = mqapi.PublishGroupId(i)
		return r, nil
	}
}

func (m *metadataContainer) NewPublishGroup(g string) (mqapi.PublishGroupId, error) {
	i, err := idgenerator.Next()
	if err != nil {
		return mqapi.PublishGroupId{}, err
	}

	m.groupLock.Lock()
	defer m.groupLock.Unlock()

	r, ok := m.publishGroupMap[g]
	if ok {
		return r, nil
	} else {
		m.publishGroupMap[g] = mqapi.PublishGroupId(i)
		return r, nil
	}
}

func (m *metadataContainer) NewBinding(b *BindDef) (StoredBinding, bool, error) {
	bds, newlyAdded, err := m.newBinding0(b)
	if err != nil {
		return bds, newlyAdded, err
	}
	if newlyAdded {
		//FIXME perhaps here we should persist metadata first?
		err = _persistMetadata()
	}
	return bds, newlyAdded, err
}

func (m *metadataContainer) newBinding0(b *BindDef) (StoredBinding, bool, error) {
	// validation
	if !ValidateNameForBrokerMechanisms(b.Topic) {
		return StoredBinding{}, false, ErrInvalidInputParameter
	}
	if !ValidateNameForBrokerMechanisms(b.Queue) {
		return StoredBinding{}, false, ErrInvalidInputParameter
	}
	if !validateBindingKey(b.BindingKey) {
		return StoredBinding{}, false, ErrInvalidInputParameter
	}

	tag, err := idgenerator.Next()
	if err != nil {
		return StoredBinding{}, false, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	t, ok := m.topicMap[b.Topic]
	if !ok {
		return StoredBinding{}, false, ErrComponentNotExist
	}
	q, ok := m.queueMap[b.Queue]
	if !ok {
		return StoredBinding{}, false, ErrComponentNotExist
	}
	bds, ok := m.bindingMap[b.Topic]
	if !ok {
		bds = make([]*metaTopicAndQueueBinding, 0, 16)
	}
	for _, v := range bds {
		if v.Queue == b.Queue && v.BindingKey == b.BindingKey {
			return StoredBinding{
				TopicId: v.TopicId,
				QueueId: v.QueueId,
				Tag:     v.Tag,
			}, false, nil
		}
	}
	metaBinding := new(metaTopicAndQueueBinding)
	metaBinding.Tag = mqapi.TagId(tag)
	metaBinding.BindingKey = b.BindingKey
	metaBinding.QueueId = q.QueueId
	metaBinding.Queue = b.Queue
	metaBinding.Topic = b.Topic
	metaBinding.TopicId = t.TopicId
	bds = append(bds, metaBinding)
	m.bindingMap[b.Topic] = bds
	m.Bindings = append(m.Bindings, metaBinding)

	return StoredBinding{
		TopicId: metaBinding.TopicId,
		QueueId: metaBinding.QueueId,
		Tag:     metaBinding.Tag,
	}, true, nil
}

func (m *metadataContainer) NewQueue(t *QueueDef) (mqapi.QueueId, bool, error) {
	id, newlyAdded, err := m.newQueue0(t)
	if err != nil {
		return id, newlyAdded, err
	}
	if newlyAdded {
		//FIXME perhaps here we should persist metadata first?
		err = _persistMetadata()
	}
	return id, newlyAdded, err
}

func (m *metadataContainer) newQueue0(t *QueueDef) (mqapi.QueueId, bool, error) {
	// validation
	if !ValidateNameForBrokerMechanisms(t.Queue) {
		return mqapi.QueueId{}, false, ErrInvalidInputParameter
	}
	if !validateDeliveryType(t.DeliveryLevelType) {
		return mqapi.QueueId{}, false, ErrInvalidInputParameter
	}

	id, err := idgenerator.Next()
	if err != nil {
		return mqapi.QueueId{}, false, err
	}

	m.lock.Lock()
	defer m.lock.Unlock()

	ov, ok := m.queueMap[t.Queue]
	// same definition
	if ok {
		// check options equivalent
		if ov.DeliveryLevelType != t.DeliveryLevelType {
			return mqapi.QueueId{}, false, ErrDefinitionMismatch
		} else {
			return ov.QueueId, false, nil
		}
	}

	mqi := &metaQueue{
		Queue:             t.Queue,
		DeliveryLevelType: t.DeliveryLevelType,
		QueueId:           mqapi.QueueId(id),
	}
	m.Queues = append(m.Queues, mqi)
	m.queueMap[t.Queue] = mqi

	return mqi.QueueId, true, nil
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

type StoredBinding struct {
	TopicId mqapi.TopicId
	QueueId mqapi.QueueId
	Tag     mqapi.TagId
}

type metaTopicAndQueueBinding struct {
	Topic      string        `toml:"topic"`
	TopicId    mqapi.TopicId `toml:"topic_id"`
	Queue      string        `toml:"queue"`
	QueueId    mqapi.QueueId `toml:"queue_id"`
	BindingKey string        `toml:"binding_key"`
	Tag        mqapi.TagId   `toml:"tag_id"`
}

type metaQueue struct {
	Queue             string        `toml:"queue"`
	DeliveryLevelType string        `toml:"delivery_level_type"`
	QueueId           mqapi.QueueId `toml:"queue_id"`
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
	//FIXME need optimize callee
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
