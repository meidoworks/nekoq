package mqmeta

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/shared/idgen"
)

type MetaManager interface {
	RegisterTopic(req *TopicReq) error
	RegisterQueue(req *QueueReq) error
	RegisterPublishGroup(req *PublishGroupReq) error
	RegisterSubscribeGroup(req *SubscribeGroupReq) error

	BindTopicAndPublishGroup(req *BindTopicAndPublishGroupReq) error
	BindQueueAndSubscribeGroup(req *BindQueueAndSubscribeGroupReq) error
	BindTopicQueue(req *BindTopicQueueReq) error

	PublishMessageMeta(topic string, bindingKey string) (PublishMeta, error)
	PublishGroupMeta(publishGroup string) (mqapi.PublishGroup, error)
	SubscribeGroupMeta(subscribeGroup string) (mqapi.SubscribeGroup, error)
}

const (
	cfgPathPrefix              = "/nekoq/mq/%s/" // MQ cluster base path. Add prefix(10/20/etc) to sub folders to make meta loaded in order. Avoid out-of-order init issue during startup.
	cfgTopicPrefix             = cfgPathPrefix + "10topic/"
	cfgTopicPath               = cfgTopicPrefix + "%s"
	cfgQueuePrefix             = cfgPathPrefix + "10queue/"
	cfgQueuePath               = cfgQueuePrefix + "%s"
	cfgPublicGroupPrefix       = cfgPathPrefix + "10publicgroup/"
	cfgPublicGroupPath         = cfgPublicGroupPrefix + "%s"
	cfgSubscribeGroupPrefix    = cfgPathPrefix + "10subscribegroupu/"
	cfgSubscribeGroupPath      = cfgSubscribeGroupPrefix + "%s"
	cfgTopicPgBindingPrefix    = cfgPathPrefix + "20topic_pg_binding/"
	cfgTopicPgBindingPath      = cfgTopicPgBindingPrefix + "%s/%s"
	cfgQueueSgBindingPrefix    = cfgPathPrefix + "20queue_sg_binding/"
	cfgQueueSgBindingPath      = cfgQueueSgBindingPrefix + "%s/%s"
	cfgTopicQueueBindingPrefix = cfgPathPrefix + "20topic_queue_binding/"
	cfgTopicQueueBindingPath   = cfgTopicQueueBindingPrefix + "%s/%s/%s"
)

type MetaManagerImpl struct {
	broker mqapi.Broker

	clusterName string

	sync.RWMutex                                               // only for concurrency between write operations and event loop, not for read operations
	topicIdMap               map[string]mqapi.TopicId          // key:topic
	queueIdMap               map[string]mqapi.QueueId          // key:queue
	publicGroupIdMap         map[string]mqapi.PublishGroupId   // key:public group
	subscribeGroupIdMap      map[string]mqapi.SubscribeGroupId // key: subscribe group
	topicQueueBindingMap     map[string]mqapi.TagId            // key: topic-queue-bindingKey full path
	topicQueueBindingRuleMap map[string][]*RoutingMatching     // key: topic

	prefixes struct {
		topicPrefix             string
		queuePrefix             string
		publishGroupPrefix      string
		subscribeGroupPrefix    string
		topicPgBindingPrefix    string
		queueSgBindingPrefix    string
		topicQueueBindingPrefix string
	}
}

func NewMetaManager(broker mqapi.Broker) (*MetaManagerImpl, error) {
	if len(config.Instance.MQ.ClusterName) == 0 {
		return nil, fmt.Errorf("cluster name is empty")
	}
	mgr := &MetaManagerImpl{
		broker:      broker,
		clusterName: config.Instance.MQ.ClusterName,

		// initialize maps
		topicIdMap:               make(map[string]mqapi.TopicId),
		queueIdMap:               make(map[string]mqapi.QueueId),
		publicGroupIdMap:         make(map[string]mqapi.PublishGroupId),
		subscribeGroupIdMap:      make(map[string]mqapi.SubscribeGroupId),
		topicQueueBindingMap:     make(map[string]mqapi.TagId),
		topicQueueBindingRuleMap: make(map[string][]*RoutingMatching),

		prefixes: struct {
			topicPrefix             string
			queuePrefix             string
			publishGroupPrefix      string
			subscribeGroupPrefix    string
			topicPgBindingPrefix    string
			queueSgBindingPrefix    string
			topicQueueBindingPrefix string
		}{
			topicPrefix:             fmt.Sprintf(cfgTopicPrefix, config.Instance.MQ.ClusterName),
			queuePrefix:             fmt.Sprintf(cfgQueuePrefix, config.Instance.MQ.ClusterName),
			publishGroupPrefix:      fmt.Sprintf(cfgPublicGroupPrefix, config.Instance.MQ.ClusterName),
			subscribeGroupPrefix:    fmt.Sprintf(cfgSubscribeGroupPrefix, config.Instance.MQ.ClusterName),
			topicPgBindingPrefix:    fmt.Sprintf(cfgTopicPgBindingPrefix, config.Instance.MQ.ClusterName),
			queueSgBindingPrefix:    fmt.Sprintf(cfgQueueSgBindingPrefix, config.Instance.MQ.ClusterName),
			topicQueueBindingPrefix: fmt.Sprintf(cfgTopicQueueBindingPrefix, config.Instance.MQ.ClusterName),
		},
	}
	return mgr, nil
}

func (mgr *MetaManagerImpl) Startup() error {
	ch, cfn, err := inproc.WarehouseInst.WatchFolder(fmt.Sprintf(cfgPathPrefix, mgr.clusterName))
	if err != nil {
		return err
	}

	go mgr.metaEventLoop(ch, cfn)
	return nil
}

func (mgr *MetaManagerImpl) metaEventLoop(ch <-chan api.WatchEvent, cfn func()) {
	for {
		ev, ok := <-ch
		if !ok {
			break
		}
		for i := 0; i < math.MaxInt; i++ { // infinite retry loop to avoid issue
			f := func() error {
				reqs := make([]struct {
					Path string
					Data []byte
				}, 0, len(ev.Ev))
				for _, v := range ev.Ev {
					switch v.EventType {
					case api.WatchEventUnknown:
						//FIXME log unknown event information
						continue
					case api.WatchEventDelete:
						//FIXME support delete
						continue
					default:
						if data, err := inproc.WarehouseInst.Get(v.Key); err != nil {
							return err
						} else {
							reqs = append(reqs, struct {
								Path string
								Data []byte
							}{Path: v.Key, Data: data})
						}
					}
				}
				mgr.Lock()
				defer mgr.Unlock()
				for _, req := range reqs {
					if err := mgr.processEventRequest(req); err != nil {
						return err
					}
				}
				return nil
			}
			if err := f(); err != nil {
				//FIXME log error information
				fmt.Println("errors:", err)
				time.Sleep(100 * time.Millisecond)
				continue
			} else {
				break
			}
		}
	}
}

type createOperation struct {
	prepareDataFn           func() (string, []byte, error)
	fastFailCheckFn         func() error
	slowOperationPreCheckFn func() error
	slowOperationFn         func(key string, data []byte) error
}

func (mgr *MetaManagerImpl) metaCreateOperation(op createOperation) error {
	key, data, err := op.prepareDataFn()
	if err != nil {
		return err
	}

	// fast fail check
	f := func() error {
		mgr.RLock()
		defer mgr.RUnlock()
		return op.fastFailCheckFn()
	}
	if err := f(); err != nil {
		return err
	}

	// slow remote operation
	// Here using lock is to block event loop to make sure event loop writes happens after this operation
	// It guarantees event loop will always have the latest data.
	// The drawback is the write operations may have large latency due to remote warehouse call.
	s := func() error {
		mgr.Lock()
		defer mgr.Unlock()
		if err := op.slowOperationPreCheckFn(); err != nil {
			return err
		}

		// try to write data
		if err := inproc.WarehouseInst.SetIfNotExists(key, data); err != nil {
			return err
		}
		// retrieve latest data
		if newData, err := inproc.WarehouseInst.Get(key); err != nil {
			return err
		} else {
			data = newData
		}

		return op.slowOperationFn(key, data)
	}
	return s()
}

func (mgr *MetaManagerImpl) topicPath(topic string) string {
	return fmt.Sprintf(cfgTopicPath, mgr.clusterName, topic)
}

func (mgr *MetaManagerImpl) RegisterTopic(req *TopicReq) error {
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			// generate id
			ids, err := inproc.NumGenInst.GenerateFor("topic", 1)
			if err != nil {
				return "", nil, err
			}
			req.TopicId = ids[0].HexString()
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := mgr.topicPath(req.Topic)
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.topicIdMap[req.Topic]; ok {
				return errors.New("topic already exists")
			} else {
				return nil
			}
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.topicIdMap[req.Topic]; ok {
				return errors.New("topic already exists")
			} else {
				return nil
			}
		},
		slowOperationFn: func(key string, data []byte) error {
			t := new(TopicReq)
			if err := json.Unmarshal(data, t); err != nil {
				return err
			}
			topicId, err := idgen.FromHexString(t.TopicId)
			if err != nil {
				return err
			}

			if _, err := mgr.broker.DefineNewTopic(mqapi.TopicId(topicId), &mqapi.TopicOption{
				DeliveryLevel: t.DeliveryLevelType,
			}); err != nil {
				return err
			}
			mgr.topicIdMap[req.Topic] = mqapi.TopicId(topicId)

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) queuePath(queue string) string {
	return fmt.Sprintf(cfgQueuePath, mgr.clusterName, queue)
}

func (mgr *MetaManagerImpl) RegisterQueue(req *QueueReq) error {
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			// generate id
			ids, err := inproc.NumGenInst.GenerateFor("queue", 1)
			if err != nil {
				return "", nil, err
			}
			req.QueueId = ids[0].HexString()
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := mgr.queuePath(req.Queue)
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.queueIdMap[req.Queue]; ok {
				return errors.New("queue already exists")
			} else {
				return nil
			}
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.queueIdMap[req.Queue]; ok {
				return errors.New("queue already exists")
			} else {
				return nil
			}
		},
		slowOperationFn: func(key string, data []byte) error {
			q := new(QueueReq)
			if err := json.Unmarshal(data, q); err != nil {
				return err
			}
			queueId, err := idgen.FromHexString(q.QueueId)
			if err != nil {
				return err
			}

			if _, err := mgr.broker.DefineNewQueue(mqapi.QueueId(queueId), &mqapi.QueueOption{
				DeliveryLevel:    q.DeliveryLevelType,
				QueueChannelSize: 1024,
				QueueType:        q.QueueType,
			}); err != nil {
				return err
			}
			mgr.queueIdMap[req.Queue] = mqapi.QueueId(queueId)

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) publicGroupPath(pg string) string {
	return fmt.Sprintf(cfgPublicGroupPath, mgr.clusterName, pg)
}

func (mgr *MetaManagerImpl) RegisterPublishGroup(req *PublishGroupReq) error {
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			// generate id
			ids, err := inproc.NumGenInst.GenerateFor("publicgroup", 1)
			if err != nil {
				return "", nil, err
			}
			req.PublicGroupId = ids[0].HexString()
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := mgr.publicGroupPath(req.PublicGroup)
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.publicGroupIdMap[req.PublicGroup]; ok {
				return errors.New("public group already exists")
			} else {
				return nil
			}
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.publicGroupIdMap[req.PublicGroup]; ok {
				return errors.New("public group already exists")
			} else {
				return nil
			}
		},
		slowOperationFn: func(key string, data []byte) error {
			q := new(PublishGroupReq)
			if err := json.Unmarshal(data, q); err != nil {
				return err
			}
			publicGroupId, err := idgen.FromHexString(q.PublicGroupId)
			if err != nil {
				return err
			}

			if _, err := mgr.broker.DefineNewPublishGroup(mqapi.PublishGroupId(publicGroupId)); err != nil {
				return err
			}
			mgr.publicGroupIdMap[req.PublicGroup] = mqapi.PublishGroupId(publicGroupId)

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) subscribeGroupPath(sg string) string {
	return fmt.Sprintf(cfgSubscribeGroupPath, mgr.clusterName, sg)
}

func (mgr *MetaManagerImpl) RegisterSubscribeGroup(req *SubscribeGroupReq) error {
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			// generate id
			ids, err := inproc.NumGenInst.GenerateFor("subscribegroup", 1)
			if err != nil {
				return "", nil, err
			}
			req.SubscribeGroupId = ids[0].HexString()
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := mgr.subscribeGroupPath(req.SubscribeGroup)
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.subscribeGroupIdMap[req.SubscribeGroup]; ok {
				return errors.New("subscribe group already exists")
			} else {
				return nil
			}
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.subscribeGroupIdMap[req.SubscribeGroup]; ok {
				return errors.New("subscribe group already exists")
			} else {
				return nil
			}
		},
		slowOperationFn: func(key string, data []byte) error {
			q := new(SubscribeGroupReq)
			if err := json.Unmarshal(data, q); err != nil {
				return err
			}
			subscribeGroupId, err := idgen.FromHexString(q.SubscribeGroupId)
			if err != nil {
				return err
			}

			if _, err := mgr.broker.DefineNewSubscribeGroup(mqapi.SubscribeGroupId(subscribeGroupId), &mqapi.SubscribeGroupOption{
				SubscribeChannelSize: 1024,
			}); err != nil {
				return err
			}
			mgr.subscribeGroupIdMap[req.SubscribeGroup] = mqapi.SubscribeGroupId(subscribeGroupId)

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) topicPgPath(topic, pg string) string {
	return fmt.Sprintf(cfgTopicPgBindingPath, mgr.clusterName, topic, pg)
}

func (mgr *MetaManagerImpl) BindTopicAndPublishGroup(req *BindTopicAndPublishGroupReq) error {
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := mgr.topicPgPath(req.Topic, req.PublishGroup)
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.topicIdMap[req.Topic]; !ok {
				return errors.New("topic does not exist")
			}
			if _, ok := mgr.publicGroupIdMap[req.PublishGroup]; !ok {
				return errors.New("public group does not exist")
			}
			return nil
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.topicIdMap[req.Topic]; !ok {
				return errors.New("topic does not exist")
			}
			if _, ok := mgr.publicGroupIdMap[req.PublishGroup]; !ok {
				return errors.New("public group does not exist")
			}
			return nil
		},
		slowOperationFn: func(key string, data []byte) error {
			q := new(BindTopicAndPublishGroupReq)
			if err := json.Unmarshal(data, q); err != nil {
				return err
			}
			pgId := mgr.publicGroupIdMap[req.PublishGroup]
			tId := mgr.topicIdMap[req.Topic]

			if err := mgr.broker.BindPublishGroupToTopic(pgId, tId); err != nil {
				return err
			}

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) queueSgPath(queue, sg string) string {
	return fmt.Sprintf(cfgQueueSgBindingPath, mgr.clusterName, queue, sg)
}

func (mgr *MetaManagerImpl) BindQueueAndSubscribeGroup(req *BindQueueAndSubscribeGroupReq) error {
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := mgr.queueSgPath(req.Queue, req.SubscribeGroup)
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.queueIdMap[req.Queue]; !ok {
				return errors.New("queue does not exist")
			}
			if _, ok := mgr.subscribeGroupIdMap[req.SubscribeGroup]; !ok {
				return errors.New("subscribe group does not exist")
			}
			return nil
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.queueIdMap[req.Queue]; !ok {
				return errors.New("queue does not exist")
			}
			if _, ok := mgr.subscribeGroupIdMap[req.SubscribeGroup]; !ok {
				return errors.New("subscribe group does not exist")
			}
			return nil
		},
		slowOperationFn: func(key string, data []byte) error {
			q := new(BindQueueAndSubscribeGroupReq)
			if err := json.Unmarshal(data, q); err != nil {
				return err
			}
			sgId := mgr.subscribeGroupIdMap[req.SubscribeGroup]
			qId := mgr.queueIdMap[req.Queue]

			if err := mgr.broker.BindSubscribeGroupToQueue(sgId, qId); err != nil {
				return err
			}

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) topicQueueBindingPath(topic, queue, bindingRule string) string {
	return fmt.Sprintf(cfgTopicQueueBindingPath, mgr.clusterName, topic, queue, bindingRule)
}

func (mgr *MetaManagerImpl) BindTopicQueue(req *BindTopicQueueReq) error {
	p := mgr.topicQueueBindingPath(req.Topic, req.Queue, req.BindKeyMatchingRule)
	return mgr.metaCreateOperation(createOperation{
		prepareDataFn: func() (string, []byte, error) {
			// generate id
			ids, err := inproc.NumGenInst.GenerateFor("tag", 1)
			if err != nil {
				return "", nil, err
			}
			req.TagId = ids[0].HexString()
			data, err := json.Marshal(req)
			if err != nil {
				return "", nil, err
			}
			key := p
			return key, data, nil
		},
		fastFailCheckFn: func() error {
			if _, ok := mgr.topicIdMap[req.Topic]; !ok {
				return errors.New("topic does not exist")
			}
			if _, ok := mgr.queueIdMap[req.Queue]; !ok {
				return errors.New("queue does not exist")
			}
			if _, ok := mgr.topicQueueBindingMap[p]; ok {
				return errors.New("topic-queue binding already exists")
			}
			return nil
		},
		slowOperationPreCheckFn: func() error {
			if _, ok := mgr.topicIdMap[req.Topic]; !ok {
				return errors.New("topic does not exist")
			}
			if _, ok := mgr.queueIdMap[req.Queue]; !ok {
				return errors.New("queue does not exist")
			}
			if _, ok := mgr.topicQueueBindingMap[p]; ok {
				return errors.New("topic-queue binding already exists")
			}
			return nil
		},
		slowOperationFn: func(key string, data []byte) error {
			q := new(BindTopicQueueReq)
			if err := json.Unmarshal(data, q); err != nil {
				return err
			}
			tagId, err := idgen.FromHexString(q.TagId)
			if err != nil {
				return err
			}

			if err := mgr.broker.BindTopicAndQueue(mgr.topicIdMap[q.Topic], mgr.queueIdMap[q.Queue], []mqapi.TagId{mqapi.TagId(tagId)}); err != nil {
				return err
			}
			mgr.topicQueueBindingMap[p] = mqapi.TagId(tagId)
			mgr.topicQueueBindingRuleMap[q.Topic] = append(mgr.topicQueueBindingRuleMap[q.Topic], &RoutingMatching{
				Rule:  q.BindKeyMatchingRule,
				TagId: mqapi.TagId(tagId),
			})

			return nil
		},
	})
}

func (mgr *MetaManagerImpl) PublishMessageMeta(topic string, bindingKey string) (meta PublishMeta, err error) {
	if topicId, ok := mgr.topicIdMap[topic]; ok {
		meta.TopicId = topicId
	}
	arr, ok := mgr.topicQueueBindingRuleMap[topic]
	if !ok {
		return PublishMeta{}, errors.New("binding does not exist")
	}
	matched := false
	for _, r := range arr {
		if r.Match(bindingKey) {
			meta.Tags = append(meta.Tags, r.TagId)
			matched = true
		}
	}
	if !matched {
		err = errors.New("no matched routing key found")
	}
	return
}

func (mgr *MetaManagerImpl) PublishGroupMeta(publishGroup string) (mqapi.PublishGroup, error) {
	m := mgr.publicGroupIdMap
	if v, ok := m[publishGroup]; ok {
		return mgr.broker.GetPublishGroup(v), nil
	} else {
		return nil, errors.New("publish group does not exist")
	}
}

func (mgr *MetaManagerImpl) SubscribeGroupMeta(subscribeGroup string) (mqapi.SubscribeGroup, error) {
	m := mgr.subscribeGroupIdMap
	if v, ok := m[subscribeGroup]; ok {
		return mgr.broker.GetSubscribeGroup(v), nil
	} else {
		return nil, errors.New("subscribe group does not exist")
	}
}

func (mgr *MetaManagerImpl) processEventRequest(req struct {
	Path string
	Data []byte
}) error {
	if strings.HasPrefix(req.Path, mgr.prefixes.topicPrefix) {
		r := new(TopicReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		if _, ok := mgr.topicIdMap[r.Topic]; ok {
			return nil
		}
		id, err := idgen.FromHexString(r.TopicId)
		if err != nil {
			return err
		}

		if _, err := mgr.broker.DefineNewTopic(mqapi.TopicId(id), &mqapi.TopicOption{
			DeliveryLevel: r.DeliveryLevelType,
		}); err != nil {
			return err
		}
		mgr.topicIdMap[r.Topic] = mqapi.TopicId(id)
		return nil
	}
	if strings.HasPrefix(req.Path, mgr.prefixes.queuePrefix) {
		r := new(QueueReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		if _, ok := mgr.queueIdMap[r.Queue]; ok {
			return nil
		}
		id, err := idgen.FromHexString(r.QueueId)
		if err != nil {
			return err
		}

		if _, err := mgr.broker.DefineNewQueue(mqapi.QueueId(id), &mqapi.QueueOption{
			DeliveryLevel:    r.DeliveryLevelType,
			QueueChannelSize: 1024,
			QueueType:        r.QueueType,
		}); err != nil {
			return err
		}
		mgr.queueIdMap[r.Queue] = mqapi.QueueId(id)
		return nil
	}
	if strings.HasPrefix(req.Path, mgr.prefixes.publishGroupPrefix) {
		r := new(PublishGroupReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		if _, ok := mgr.publicGroupIdMap[r.PublicGroup]; ok {
			return nil
		}
		id, err := idgen.FromHexString(r.PublicGroupId)
		if err != nil {
			return err
		}

		if _, err := mgr.broker.DefineNewPublishGroup(mqapi.PublishGroupId(id)); err != nil {
			return err
		}
		mgr.publicGroupIdMap[r.PublicGroup] = mqapi.PublishGroupId(id)
		return nil
	}
	if strings.HasPrefix(req.Path, mgr.prefixes.subscribeGroupPrefix) {
		r := new(SubscribeGroupReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		if _, ok := mgr.subscribeGroupIdMap[r.SubscribeGroup]; ok {
			return nil
		}
		id, err := idgen.FromHexString(r.SubscribeGroupId)
		if err != nil {
			return err
		}

		if _, err := mgr.broker.DefineNewSubscribeGroup(mqapi.SubscribeGroupId(id), &mqapi.SubscribeGroupOption{
			SubscribeChannelSize: 1024,
		}); err != nil {
			return err
		}
		mgr.subscribeGroupIdMap[r.SubscribeGroup] = mqapi.SubscribeGroupId(id)
		return nil
	}
	if strings.HasPrefix(req.Path, mgr.prefixes.topicPgBindingPrefix) {
		r := new(BindTopicAndPublishGroupReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		topicId, ok := mgr.topicIdMap[r.Topic]
		if !ok {
			return errors.New("topic does not exist")
		}
		pgId, ok := mgr.publicGroupIdMap[r.PublishGroup]
		if !ok {
			return errors.New("publish group does not exist")
		}
		if err := mgr.broker.BindPublishGroupToTopic(pgId, topicId); err != nil {
			return err
		}
		return nil
	}
	if strings.HasPrefix(req.Path, mgr.prefixes.queueSgBindingPrefix) {
		r := new(BindQueueAndSubscribeGroupReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		queueId, ok := mgr.queueIdMap[r.Queue]
		if !ok {
			return errors.New("queue does not exist")
		}
		sgId, ok := mgr.subscribeGroupIdMap[r.SubscribeGroup]
		if !ok {
			return errors.New("subscribe group does not exist")
		}
		if err := mgr.broker.BindSubscribeGroupToQueue(sgId, queueId); err != nil {
			return err
		}
		return nil
	}
	if strings.HasPrefix(req.Path, mgr.prefixes.topicQueueBindingPrefix) {
		r := new(BindTopicQueueReq)
		if err := json.Unmarshal(req.Data, r); err != nil {
			return err
		}
		p := mgr.topicQueueBindingPath(r.Topic, r.Queue, r.BindKeyMatchingRule)
		if _, ok := mgr.topicIdMap[r.Topic]; !ok {
			return errors.New("topic does not exist")
		}
		if _, ok := mgr.queueIdMap[r.Queue]; !ok {
			return errors.New("queue does not exist")
		}
		if _, ok := mgr.topicQueueBindingRuleMap[p]; ok {
			return nil
		}
		id, err := idgen.FromHexString(r.TagId)
		if err != nil {
			return err
		}

		if err := mgr.broker.BindTopicAndQueue(mgr.topicIdMap[r.Topic], mgr.queueIdMap[r.Queue], []mqapi.TagId{mqapi.TagId(id)}); err != nil {
			return err
		}
		mgr.topicQueueBindingMap[p] = mqapi.TagId(id)
		mgr.topicQueueBindingRuleMap[r.Topic] = append(mgr.topicQueueBindingRuleMap[r.Topic], &RoutingMatching{
			Rule:  r.BindKeyMatchingRule,
			TagId: mqapi.TagId(id),
		})
		return nil
	}
	return errors.New("unknown path type: " + req.Path)
}
