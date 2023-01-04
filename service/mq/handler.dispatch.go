package mq

import (
	"errors"
	"fmt"
	"io"
	"log"

	"github.com/meidoworks/nekoq/service/mqapi"
)

func dispatch(p *GeneralReq, f func() (io.WriteCloser, error)) error {
	log.Println("dispatch request:")
	DebugJsonPrint(p)

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
		//TODO
		return nil
	default:
		return errors.New("unknown operation:" + p.Operation)
	}
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
	_, err = GetBroker().DefineNewPublishGroup(pgId)
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
