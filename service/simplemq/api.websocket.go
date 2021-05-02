package simplemq

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"
)

type httpHandler struct {
	*AppCtx
}

type ConnWrapper struct {
	Id   int64
	conn *websocket.Conn
	sync.Mutex
}

func (c *ConnWrapper) ReadMessage() (messageType int, p []byte, err error) {
	c.Lock()
	defer c.Unlock()
	return c.conn.ReadMessage()
}

func (c *ConnWrapper) WriteMessage(messageType int, data []byte) error {
	c.Lock()
	defer c.Unlock()
	return c.conn.WriteMessage(messageType, data)
}

var upgrader = websocket.Upgrader{}
var idCounter int64 = 0

func init() {
	upgrader.WriteBufferSize = 16 * 1024
	upgrader.ReadBufferSize = 16 * 1024
	upgrader.HandshakeTimeout = 5 * time.Second
}

func (this *httpHandler) handleWebsocket(w http.ResponseWriter, r *http.Request) {
	conn, err := upgrader.Upgrade(w, r, nil)
	if err != nil {
		panic(err)
	}
	defer conn.Close()

	// message limit - 128K+buffer
	conn.SetReadLimit(129 * 1024)

	id := atomic.AddInt64(&idCounter, 1)
	connWrapper := &ConnWrapper{
		Id:   id,
		conn: conn,
	}

LOOP:
	for {
		_, message, err := connWrapper.ReadMessage()
		if err == io.EOF {
			break LOOP
		}
		if err != nil {
			log.Println("[ERROR] read:", err)
			break LOOP
		}
		log.Printf("recv: %s", message)

		msg := new(MessageMessage)
		err = json.Unmarshal(message, msg)
		if err != nil {
			log.Println("[ERROR] unmarshal message:", err)
			break LOOP
		}
		switch msg.Type {
		case MessageTypePublish:
			err := this.handlePublish(msg, connWrapper)
			if err != nil {
				break LOOP
			}
		case MessageTypeSubQueueMsg:
			err := this.handleSubQueueMsg(msg, connWrapper)
			if err != nil {
				break LOOP
			}
		case MessageTypeSubQueueMsgAck:
			err := this.handleSubQueueMsgAck(msg, connWrapper)
			if err != nil {
				break LOOP
			}
		default:
			log.Println("[ERROR] unknown msg type:", msg.Type)
			break
		}
	}
}

func (this *httpHandler) handleSubQueueMsgAck(msg *MessageMessage, conn *ConnWrapper) error {
	if len(msg.ReqId) == 0 {
		failureMsg := new(MessageMessage)
		failureMsg.Type = MessageTypeErrorMsg
		failureMsg.Result = MessageResultGeneralFailure
		failureMsg.ReqId = msg.ReqId
		failureMsg.Topic = msg.Topic
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(failureMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	}

	ch := make(chan struct{}, 1)
	this.SubAckMessageRequestQueue <- &MessageAckRequest{
		ReqId:  msg.ReqId,
		RespCh: ch,
	}
	_, ok := <-ch
	if ok {
		respMsg := new(MessageMessage)
		respMsg.ReqId = msg.ReqId
		respMsg.Type = MessageTypeSubQueueMsgAckResp
		respMsg.Result = MessageResultSuccess
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(respMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	} else {
		respMsg := new(MessageMessage)
		respMsg.ReqId = msg.ReqId
		respMsg.Type = MessageTypeSubQueueMsgAckResp
		respMsg.Result = MessageResultNoMsg
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(respMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	}
}

func (this *httpHandler) handleSubQueueMsg(msg *MessageMessage, conn *ConnWrapper) error {
	if len(msg.Queues) != 1 {
		failureMsg := new(MessageMessage)
		failureMsg.Type = MessageTypeErrorMsg
		failureMsg.Result = MessageResultGeneralFailure
		failureMsg.ReqId = msg.ReqId
		failureMsg.Topic = msg.Topic
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(failureMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	}

	ch := make(chan *Message, 1)
	this.SubMessageRequestQueue <- &MessageRequest{
		QueueName: msg.Queues[0],
		RespCh:    ch,
	}
	i, ok := <-ch
	if ok {
		respMsg := new(MessageMessage)
		respMsg.Queues = msg.Queues
		respMsg.Payload = i.Payload
		respMsg.Header = i.Header
		respMsg.ReqId = fmt.Sprint(i.MsgId)
		respMsg.Topic = i.Topic
		respMsg.Type = MessageTypeSubQueueMsgResp
		respMsg.Result = MessageResultSuccess
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(respMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	} else {
		respMsg := new(MessageMessage)
		respMsg.Queues = msg.Queues
		respMsg.ReqId = msg.ReqId
		respMsg.Type = MessageTypeSubQueueMsgResp
		respMsg.Result = MessageResultNoMsg
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(respMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	}
}

func (this *httpHandler) handlePublish(msg *MessageMessage, conn *ConnWrapper) error {
	if len(msg.ReqId) == 0 || len(msg.Topic) == 0 || len(msg.Payload) == 0 {
		failureMsg := new(MessageMessage)
		failureMsg.Type = MessageTypeErrorMsg
		failureMsg.Result = MessageResultGeneralFailure
		failureMsg.ReqId = msg.ReqId
		failureMsg.Topic = msg.Topic
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(failureMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	}

	mapping := this.mapping
	if _, ok := mapping[msg.Topic]; !ok {
		failureMsg := new(MessageMessage)
		failureMsg.Type = MessageTypeErrorMsg
		failureMsg.Result = MessageResultGeneralFailure
		failureMsg.ReqId = msg.ReqId
		failureMsg.Topic = msg.Topic
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(failureMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return errors.New("topic is not found")
	}

	var queues map[string]TopicQueueMapping
	if len(msg.Queues) > 0 {
		allMappings := mapping[msg.Topic]
		queues = make(map[string]TopicQueueMapping)
		for _, v := range msg.Queues {
			if _, ok := allMappings[v]; !ok {
				failureMsg := new(MessageMessage)
				failureMsg.Type = MessageTypeErrorMsg
				failureMsg.Result = MessageResultGeneralFailure
				failureMsg.ReqId = msg.ReqId
				failureMsg.Topic = msg.Topic
				err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(failureMsg)))
				if err != nil {
					log.Println("write:", err)
					return err
				}
				return errors.New("queue is not found")
			}
			queues[v] = TopicQueueMapping{
				Topic:     msg.Topic,
				QueueName: v,
			}
		}
	} else {
		queues = mapping[msg.Topic]
	}

	// persist data
	var messages []*Message
	for k, _ := range queues {
		messages = append(messages, &Message{
			MsgStatus:  MessageStatusReady,
			MsgDeleted: MessageDeletedActive,
			Topic:      msg.Topic,
			QueueName:  k,
			Header:     msg.Header,
			Payload:    msg.Payload,
		})
	}
	err := SaveMessages(this.AppCtx, messages)
	if err != nil {
		log.Println("[ERROR] save messages error:", err)

		failureMsg := new(MessageMessage)
		failureMsg.Type = MessageTypeErrorMsg
		failureMsg.Result = MessageResultGeneralFailure
		failureMsg.ReqId = msg.ReqId
		failureMsg.Topic = msg.Topic
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(failureMsg)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	} else {
		// success
		successAck := new(MessageMessage)
		successAck.Type = MessageTypePublishAck
		successAck.Result = MessageResultSuccess
		successAck.ReqId = msg.ReqId
		successAck.Topic = msg.Topic
		err := conn.WriteMessage(websocket.BinaryMessage, []byte(PrintJson(successAck)))
		if err != nil {
			log.Println("write:", err)
			return err
		}
		return nil
	}
}
