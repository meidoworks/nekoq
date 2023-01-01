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
		res, err := handleNewTopic(p, f)
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
		log.Println("process new topic completed")
		return nil
	default:
		return errors.New("unknown operation:" + p.Operation)
	}
}

func handleNewTopic(p *GeneralReq, f func() (io.WriteCloser, error)) (*GeneralRes, error) {
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
		if err != nil && err != mqapi.ErrTopicAlreadyExist {
			// skip on topic existing to make the define operation idempotent
			return newSuccessResponse(p.RequestId), nil
		}
	}

	// prepare output
	return newSuccessResponse(p.RequestId), nil
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
