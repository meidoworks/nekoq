package lib

import (
	"bytes"
	"errors"
	"io"
	"log"
	"strconv"
	"strings"

	"goimport.moetang.info/nekoq/service/mq"
)

func cmdInitBroker(cmds string, broker *mq.Broker) error {
	buf := bytes.NewBufferString(cmds)
LOOP:
	for {
		line, err := buf.ReadString('\n')
		line = strings.TrimSpace(line)
		if len(line) > 0 {
			err = cmdProcessLine(line, broker)
			if err != nil {
				return err
			}
		}
		if err != nil {
			switch err {
			case io.EOF:
				break LOOP
			default:
				return err
			}
		}
	}
	return nil
}

func cmdProcessLine(s string, broker *mq.Broker) error {
	items := strings.Split(s, " ")
	var result []string
	for _, v := range items {
		v = strings.TrimSpace(v)
		if len(v) > 0 {
			result = append(result, v)
		}
	}

	switch strings.ToLower(result[0]) {

	case "topic":
		_, ok := topicIdMapping[result[1]]
		if ok {
			return errors.New("topic exists")
		}
		id, err := idgen.Next()
		if err != nil {
			return err
		}
		i, err := strconv.Atoi(result[2])
		if err != nil {
			return err
		}
		topicOption := new(mq.TopicOption)
		switch mq.DeliveryLevelType(i) {
		case mq.AtMostOnce:
			topicOption.DeliveryLevel = mq.AtMostOnce
		case mq.AtLeastOnce:
			topicOption.DeliveryLevel = mq.AtLeastOnce
		case mq.ExactlyOnce:
			topicOption.DeliveryLevel = mq.ExactlyOnce
		default:
			return errors.New("unknown delivery type")
		}
		topicIdMapping[result[1]] = id

		_, err = broker.NewTopic(id, topicOption)
		if err != nil {
			return err
		}

		log.Println("topic [", result[1], "] added.")

	case "queue":
		_, ok := queueIdMapping[result[2]]
		if ok {
			return errors.New("queue exists")
		}
		id, err := idgen.Next()
		if err != nil {
			return err
		}
		i, err := strconv.Atoi(result[3])
		if err != nil {
			return err
		}

		//TODO get topic and new queue

	case "publishgroup":
	case "subscribegroup":
	case "bind":
	default:
		return errors.New("unknown command")
	}
	return nil
}
