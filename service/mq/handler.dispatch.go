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

const (
	MessageAttrTopic      = "nekoq.topic"
	MessageAttrBindingKey = "nekoq.binding_key"
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
			w.writeMessage(m, sgName)
		case m := <-existing.ReleaseChannel():
			w.writeReleasingMessage(m, sgName)
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
	const debugFlag = false
	if debugFlag {
		log.Println("dispatch request:")
		DebugJsonPrint(p)
	}

	f := func() (io.WriteCloser, error) {
		return c.Conn.Writer(context.Background(), websocket.MessageBinary)
	}

	switch p.Operation {
	case "new_topic":
		res, err := handleNewTopic(p)
		if debugFlag {
			log.Println("process new topic completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_queue":
		res, err := handleNewQueue(p)
		if debugFlag {
			log.Println("process new queue completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "bind":
		res, err := handleBind(p)
		if debugFlag {
			log.Println("process bind completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_publish_group":
		res, err := handleNewPublishGroup(p)
		if debugFlag {
			log.Println("process new publish group completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_subscribe_group":
		res, err := handleNewSubscribeGroup(p, c)
		if debugFlag {
			log.Println("process new subscribe group completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_message":
		res, err := handleNewMessage(p)
		if debugFlag {
			log.Println("process new message completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "new_message_commit":
		res, err := handleNewMessageCommit(p)
		if debugFlag {
			log.Println("process new message commit completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "ack_message":
		res, err := handleAckMessage(p)
		if debugFlag {
			log.Println("process ack message completed")
		}
		if err := handleResponse(res, err, f); err != nil {
			return err
		}
		return nil
	case "release_message":
		res, err := handleReleaseMessage(p)
		if debugFlag {
			log.Println("process ack message completed")
		}
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

func (w *wsch) writeMessage(m mqapi.SubChanElem, sgName string) {
	if len(m.Request.BatchMessage) <= 0 {
		log.Println("empty batch message list")
		return
	}

	for _, v := range m.Request.BatchMessage {
		res := new(GeneralRes)
		res.Status = "200"
		res.Info = "message"
		wm := new(WrittenMessage)
		res.WrittenMessage = wm
		op := ResponseOperationMessage
		res.Operation = &op

		wm.Payload = v.Body
		wm.MessageId = v.MsgId

		//FIXME all of the three should not be nil
		t := v.Attributes[MessageAttrTopic][0]
		b := v.Attributes[MessageAttrBindingKey][0]
		q := GetMetadataContainer().GetQueueById(m.Queue.QueueId()).Queue
		wm.Topic = t
		wm.BindingKey = b
		wm.Queue = q
		wm.SubscribeGroup = sgName

		if err := handleResponse(res, nil, func() (io.WriteCloser, error) {
			return w.Conn.Writer(context.Background(), websocket.MessageBinary)
		}); err != nil {
			log.Println("writeMessage failed:" + fmt.Sprint(err))
		}
	}

}

func (w *wsch) writeReleasingMessage(m mqapi.ReleaseChanElem, sgName string) {
	if len(m.Request.BatchMessage) <= 0 {
		log.Println("empty batch message list")
		return
	}

	for _, v := range m.Request.BatchMessage {
		res := new(GeneralRes)
		res.Status = "200"
		res.Info = "message"
		wm := new(WrittenMessageReleasing)
		res.WrittenMessageReleasing = wm
		op := ResponseOperationMessageReleasing
		res.Operation = &op

		wm.MessageId = v.MsgId

		//FIXME all of the three should not be nil
		t := v.Attributes[MessageAttrTopic][0]
		b := v.Attributes[MessageAttrBindingKey][0]
		q := GetMetadataContainer().GetQueueById(m.Queue.QueueId()).Queue
		wm.Topic = t
		wm.BindingKey = b
		wm.Queue = q
		wm.SubscribeGroup = sgName

		if err := handleResponse(res, nil, func() (io.WriteCloser, error) {
			return w.Conn.Writer(context.Background(), websocket.MessageBinary)
		}); err != nil {
			log.Println("writeReleasingMessage failed:" + fmt.Sprint(err))
		}
	}

}

func newDefaultCtx() *mqapi.Ctx {
	return &mqapi.Ctx{Context: context.Background()}
}

func handleReleaseMessage(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.ReleaseMessage == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	sg := GetMetadataContainer().GetSg(p.ReleaseMessage.SubscribeGroup)
	if sg == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	q := GetMetadataContainer().GetQueue(p.ReleaseMessage.Queue)
	if q == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	err := sg.Release(q.QueueId, nil, &mqapi.Ack{AckIdList: []mqapi.MessageId{{p.ReleaseMessage.MessageId, mqapi.OutId{}}}})
	if err != nil {
		log.Println("commit message failed:" + fmt.Sprint(p.ReleaseMessage.MessageId))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}

	// prepare output
	return newSuccessResponse(p.RequestId, "release_message"), nil
}

func handleAckMessage(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.AckMessage == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	sg := GetMetadataContainer().GetSg(p.AckMessage.SubscribeGroup)
	if sg == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	q := GetMetadataContainer().GetQueue(p.AckMessage.Queue)
	if q == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	err := sg.Commit(q.QueueId, nil, &mqapi.Ack{AckIdList: []mqapi.MessageId{{p.AckMessage.MessageId, mqapi.OutId{}}}})
	if err != nil {
		log.Println("commit message failed:" + fmt.Sprint(p.AckMessage.MessageId))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}

	// prepare output
	return newSuccessResponse(p.RequestId, "ack_message"), nil
}

func handleNewMessageCommit(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.NewMessageCommit == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	pg := GetMetadataContainer().GetPg(p.NewMessageCommit.PublishGroup)
	if pg == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	t := GetMetadataContainer().GetTopic(p.NewMessageCommit.Topic)
	if t == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	var tags = GetMetadataContainer().FilterOutBindingTag(p.NewMessageCommit.Topic, p.NewMessageCommit.BindingKey)
	var idList = make([]mqapi.MessageId, len(p.NewMessageCommit.MessageIdList))
	for i, v := range p.NewMessageCommit.MessageIdList {
		idList[i] = mqapi.MessageId{
			MsgId: mqapi.MsgId(v.MsgId),
			OutId: mqapi.OutId(v.OutId),
		}
	}
	commitReq := &mqapi.MessageCommit{
		Header: mqapi.Header{
			TopicId:       t.TopicId,
			DeliveryLevel: mqapi.ExactlyOnce,
			Tags:          tags,
		},
		Ack: mqapi.Ack{AckIdList: idList},
	}
	if mf, err := pg.CommitMessage(commitReq, newDefaultCtx()); err != nil {
		log.Println("CommitMessage failed: " + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	} else {
		// no need to respond anything
		_ = mf
	}

	// prepare output
	return newSuccessResponse(p.RequestId, "new_message_commit"), nil
}

func handleNewMessage(p *GeneralReq) (*GeneralRes, error) {
	// validation
	if p.NewMessage == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}

	// process
	pg := GetMetadataContainer().GetPg(p.NewMessage.PublishGroup)
	if pg == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	t := GetMetadataContainer().GetTopic(p.NewMessage.Topic)
	if t == nil {
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	var tags = GetMetadataContainer().FilterOutBindingTag(p.NewMessage.Topic, p.NewMessage.BindingKey)
	outId, err := idgenerator.Next()
	if err != nil {
		log.Println("generate outId failed:" + fmt.Sprint(err))
		res := newFailedResponse("500", "internal error", p.RequestId)
		return res, nil
	}
	var dlt mqapi.DeliveryLevelType
	switch t.DeliveryLevelType {
	case DeliveryTypeAtMostOnce:
		dlt = mqapi.AtMostOnce
	case DeliveryTypeAtLeastOnce:
		dlt = mqapi.AtLeastOnce
	case DeliveryTypeExactlyOnce:
		dlt = mqapi.ExactlyOnce
	default:
		res := newFailedResponse("400", "parameter invalid", p.RequestId)
		return res, nil
	}
	msg := &mqapi.Request{
		Header: mqapi.Header{
			TopicId:       t.TopicId,
			DeliveryLevel: dlt,
			Tags:          tags,
		},
		BatchMessage: []mqapi.Message{
			{
				MessageId: mqapi.MessageId{
					OutId: mqapi.OutId(outId),
				},
				Attributes: map[string][]string{
					MessageAttrTopic:      {p.NewMessage.Topic},
					MessageAttrBindingKey: {p.NewMessage.BindingKey},
				},
				Body: p.NewMessage.Payload,
			},
		},
	}
	var acks mqapi.Ack
	switch dlt {
	case mqapi.AtMostOnce:
		if pres, err := pg.PublishMessage(msg, newDefaultCtx()); err != nil {
			log.Println("PublishMessage failed: " + fmt.Sprint(err))
			res := newFailedResponse("500", "internal error", p.RequestId)
			return res, nil
		} else {
			acks = pres
		}
	case mqapi.AtLeastOnce:
		if pres, err := pg.PublishGuaranteeMessage(msg, newDefaultCtx()); err != nil {
			log.Println("PublishGuaranteeMessage failed: " + fmt.Sprint(err))
			res := newFailedResponse("500", "internal error", p.RequestId)
			return res, nil
		} else {
			acks = pres
		}
	case mqapi.ExactlyOnce:
		// support exactly once
		if pres, err := pg.PrePublishMessage(msg, newDefaultCtx()); err != nil {
			log.Println("PrePublishMessage failed: " + fmt.Sprint(err))
			res := newFailedResponse("500", "internal error", p.RequestId)
			return res, nil
		} else {
			acks = pres.Ack
		}
	default:
		panic(errors.New("should not reach here based on delivery type"))
	}

	// prepare output
	res := newSuccessResponse(p.RequestId, "new_message")
	if len(acks.AckIdList) > 0 {
		res.NewMessageResponse = new(NewMessageRes)
		for _, v := range acks.AckIdList {
			res.NewMessageResponse.MessageIdList = append(res.NewMessageResponse.MessageIdList, struct {
				MsgId idgen.IdType `json:"msg_id"`
				OutId idgen.IdType `json:"out_id"`
			}{MsgId: idgen.IdType(v.MsgId), OutId: idgen.IdType(v.OutId)})
			res.NewMessageResponse.Topic = p.NewMessage.Topic
			res.NewMessageResponse.BindingKey = p.NewMessage.BindingKey
		}
	}

	return res, nil
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
	res := newSuccessResponse(p.RequestId, "new_subscribe_group")
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
	res := newSuccessResponse(p.RequestId, "new_publish_group")
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
	return newSuccessResponse(p.RequestId, "new_binding"), nil
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
			return newSuccessResponse(p.RequestId, "new_queue"), nil
		} else if err != nil {
			log.Println("broker returns error:" + fmt.Sprint(err))
			return newFailedResponse("500", "internal error", p.RequestId), nil
		}
	}

	// prepare output
	return newSuccessResponse(p.RequestId, "new_queue"), nil
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
			return newSuccessResponse(p.RequestId, "new_topic"), nil
		} else if err != nil {
			log.Println("broker returns error:" + fmt.Sprint(err))
			return newFailedResponse("500", "internal error", p.RequestId), nil
		}
	}

	// prepare output
	return newSuccessResponse(p.RequestId, "new_topic"), nil
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

func newSuccessResponse(requestId string, opType string) *GeneralRes {
	res := new(GeneralRes)
	res.Status = "200"
	res.Info = "operation success:" + fmt.Sprint(opType)
	res.RequestId = requestId
	return res
}
