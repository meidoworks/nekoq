package mq

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"sync"

	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/shared/idgen"

	"nhooyr.io/websocket"
)

type wsch struct {
	Cid  idgen.IdType
	Conn *websocket.Conn

	sgMap map[string]mqapi.SubscribeGroup
	pgMap map[string]mqapi.PublishGroup
	lock  sync.Mutex

	closeCh   chan struct{}
	closeOnce sync.Once
}

func (w *wsch) sgWorker(sgName string, queueName string, sg mqapi.SubscribeGroup) {
	// newly subscribed - start working
	w.lock.Lock()
	existing, ok := w.sgMap[sgName]
	if ok {
		return // exit
	}
	w.sgMap[sgName] = sg
	existing = sg
	w.lock.Unlock()

	log.Println("start sgWorker for ch:" + fmt.Sprint(w.Cid))
SgWorkerLoop:
	for {
		select {
		case <-w.closeCh:
			break SgWorkerLoop
		case m := <-existing.SubscribeChannel():
			w.writeMessage(m)
		case m := <-existing.ReleaseChannel():
			w.writeReleasingMessage(m)
		}
	}
}

func newWsch(cid idgen.IdType, conn *websocket.Conn) *wsch {
	return &wsch{
		Cid:     cid,
		Conn:    conn,
		sgMap:   make(map[string]mqapi.SubscribeGroup),
		pgMap:   make(map[string]mqapi.PublishGroup),
		closeCh: make(chan struct{}),
	}
}

func dispatch(p *GeneralReq, c *wsch) error {
	log.Println("dispatch request:")
	DebugJsonPrint(p)

	f := func() (io.WriteCloser, error) {
		return c.Conn.Writer(context.Background(), websocket.MessageBinary)
	}

	switch p.Operation {
	case "new_topic":
		res, err := handleNewTopic(p)
		log.Println("process new topic completed")
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_queue":
		res, err := handleNewQueue(p)
		log.Println("process new queue completed")
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "bind":
		res, err := handleBind(p)
		log.Println("process bind completed")
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_publish_group":
		res, err := handleNewPublishGroup(p)
		log.Println("process new publish group completed")
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_subscribe_group":
		res, err := handleNewSubscribeGroup(p, c)
		log.Println("process new subscribe group completed")
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	default:
		return errors.New("unknown operation:" + p.Operation)
	}
}

func (ch *wsch) cleanupWsChannel() {
	ch.closeOnce.Do(func() {
		log.Println("cleanupWsChannel closeCh closed")
		close(ch.closeCh)
	})
}

func (w *wsch) writeMessage(m mqapi.SubChanElem) {
	//TODO implement me!!!!!!!!!!!!!
}

func (w *wsch) writeReleasingMessage(m mqapi.ReleaseChanElem) {
	//TODO implement me!!!!!!!!!!!!!
}

func handleNewSubscribeGroup(p *GeneralReq, c *wsch) (*GeneralRes, error) {
	// validation
	if p.NewSubscribeGroup == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	sgId, err := GetMetadataContainer().NewSubscribeGroup(p.NewSubscribeGroup.SubscribeGroup)
	if err != nil {
		log.Println("new subscribe group failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	q := GetMetadataContainer().GetQueue(p.NewSubscribeGroup.Queue)
	if q == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	sg, err := GetBroker().DefineNewSubscribeGroup(sgId, &mqapi.SubscribeGroupOption{
		SubscribeChannelSize: 1024,
	})
	if err != nil && err != mqapi.ErrSubscribeGroupAlreadyExist {
		log.Println("DefineNewSubscribeGroup failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	if err := GetBroker().BindSubscribeGroupToQueue(sgId, q.QueueId); err != nil {
		log.Println("BindSubscribeGroupToQueue failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	if sg == nil { // sg will be nil if already exists
		sg = GetBroker().GetSubscribeGroup(sgId)
		if sg == nil {
			log.Println("unexpected nil of subscribeGroup")
			res := newFailedResponse("500", "internal error", p.RequestId)
			return res, nil
		}
	}
	GetMetadataContainer().InsertSg(p.NewSubscribeGroup.SubscribeGroup, sg)
	// node subscribe part
	go c.sgWorker(p.NewSubscribeGroup.SubscribeGroup, p.NewSubscribeGroup.Queue, sg)

	// prepare output
	res := newSuccessResponse(p.RequestId)
	res.SubscribeGroupResponse = &SubscribeGroupRes{SubscribeGroup: p.NewSubscribeGroup.SubscribeGroup}

	return res, nil
}

func handleNewPublishGroup(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.NewPublishGroup == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	pgId, err := GetMetadataContainer().NewPublishGroup(p.NewPublishGroup.PublishGroup)
	if err != nil {
		log.Println("new publish group failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	t := GetMetadataContainer().GetTopic(p.NewPublishGroup.Topic)
	if t == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	tId := t.TopicId
	pg, err := GetBroker().DefineNewPublishGroup(pgId)
	if err != nil && err != mqapi.ErrPublishGroupAlreadyExist {
		log.Println("DefineNewPublishGroup failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	if err := GetBroker().BindPublishGroupToTopic(pgId, tId); err != nil {
		log.Println("BindPublishGroupToTopic failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	if pg == nil { // pg will be nil if already exists
		pg = GetBroker().GetPublishGroup(pgId)
		if pg == nil {
			log.Println("unexpected nil of publishGroup")
			res := newFailedResponse("500", "internal error", p.RequestId)
			return res, nil
		}
	}
	GetMetadataContainer().InsertPg(p.NewPublishGroup.PublishGroup, pg)

	// prepare output
	res := newSuccessResponse(p.RequestId)
	res.PublishGroupResponse = &PublishGroupRes{PublishGroup: p.NewPublishGroup.PublishGroup}

	return res, nil
}

func handleBind(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.NewBinding == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	if bds, _, err := GetMetadataContainer().NewBinding(p.NewBinding); err == ErrInvalidInputParameter {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	} else if err != nil {
		log.Println("unknown occurs:" + fmt.Sprint(err))
		res := newFailedResponse("500", "unknown error", p.RequestId)
		return res, nil
	} else {
		// broker operation
		err := GetBroker().BindTopicAndQueue(bds.TopicId, bds.QueueId, []mqapi.TagId{bds.Tag})
		if err != nil {
			log.Println("bind topic and queue failed:" + fmt.Sprint(err))
			return newFailedResponse("500", "bind topic and queue failed", p.RequestId), nil
		}
	}

	// prepare output
	return newSuccessResponse(p.RequestId), nil
}

func handleNewQueue(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.NewQueue == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	// step1: check metadata db
	if id, _, err := GetMetadataContainer().NewQueue(p.NewQueue); err == ErrInvalidInputParameter {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	} else if err != nil {
		log.Println("unknown occurs:" + fmt.Sprint(err))
		res := newFailedResponse("500", "unknown error", p.RequestId)
		return res, nil
	} else {
		// step2: define it in the broker
		to := &mqapi.QueueOption{
			DeliveryLevel: convertDeliveryLevelType(p.NewQueue.DeliveryLevelType),
			QueueType:     "memory", //FIXME should be configured on demand
		}
		_, err := GetBroker().DefineNewQueue(id, to)
		if err != nil && err == mqapi.ErrQueueAlreadyExist {
			// skip on topic existing to make the define operation idempotent
			return newSuccessResponse(p.RequestId), nil
		} else if err != nil {
			log.Println("broker returns error:" + fmt.Sprint(err))
			return newFailedResponse("500", "internal error", p.RequestId), nil
		}
	}

	// prepare output
	return newSuccessResponse(p.RequestId), nil
}

func handleNewTopic(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.NewTopic == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	// step1: check metadata db
	if id, _, err := GetMetadataContainer().NewTopic(p.NewTopic); err == ErrInvalidInputParameter {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	} else if err != nil {
		log.Println("unknown occurs:" + fmt.Sprint(err))
		res := newFailedResponse("500", "unknown error", p.RequestId)
		return res, nil
	} else {
		// step2: define it in the broker
		to := &mqapi.TopicOption{
			DeliveryLevel: convertDeliveryLevelType(p.NewTopic.DeliveryLevelType),
		}
		_, err := GetBroker().DefineNewTopic(id, to)
		if err != nil && err == mqapi.ErrTopicAlreadyExist {
			// skip on topic existing to make the define operation idempotent
			return newSuccessResponse(p.RequestId), nil
		} else if err != nil {
			log.Println("broker returns error:" + fmt.Sprint(err))
			return newFailedResponse("500", "internal error", p.RequestId), nil
		}
	}

	// prepare output
	return newSuccessResponse(p.RequestId), nil
}

func handleResponse(res *GeneralRes, err error, f func() (io.WriteCloser, error)) error {
	if err != nil {
		return err
	}
	wc, err := f()
	if err != nil {
		// prepare to write failed
		// just exit
		return err
	}
	defer wc.Close()
	if err := respondPacket(res, wc); err != nil {
		// prepare to write failed
		// just exit
		return err
	}
	return nil
}

func convertDeliveryLevelType(t string) mqapi.DeliveryLevelType {
	switch t {
	case DeliveryTypeAtLeastOnce:
		return mqapi.AtLeastOnce
	case DeliveryTypeAtMostOnce:
		return mqapi.AtMostOnce
	case DeliveryTypeExactlyOnce:
		return mqapi.ExactlyOnce
	default:
		panic("unknown type is not allowed")
	}
}

func newFailedResponse(code, info, requestId string) *GeneralRes {
	res := new(GeneralRes)
	res.Status = code
	res.Info = info
	res.RequestId = requestId
	return res
}

func newSuccessResponse(requestId string) *GeneralRes {
	res := new(GeneralRes)
	res.Status = "200"
	res.Info = "operation success"
	res.RequestId = requestId
	return res
}
