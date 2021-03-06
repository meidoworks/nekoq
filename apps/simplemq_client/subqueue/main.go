package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"goimport.moetang.info/nekoq/apps/simplemq_client/shared"
)

func main() {
	u := url.URL{Scheme: "ws", Host: "127.0.0.1:7012", Path: "/simplemq"}
	log.Printf("connecting to %s", u.String())

	dialer := new(websocket.Dialer)
	dialer.Proxy = http.ProxyFromEnvironment
	dialer.HandshakeTimeout = 5 * time.Second
	dialer.ReadBufferSize = 16000
	dialer.WriteBufferSize = 16000

	c, _, err := dialer.Dial(u.String(), nil)
	if err != nil {
		log.Fatal("dial:", err)
	}
	defer c.Close()

	ticker := time.NewTicker(500 * time.Millisecond)
	for range ticker.C {
		subMsg := new(shared.MessageMessage)

		subMsg.Type = shared.MessageTypeSubQueueMsg
		subMsg.Queues = []string{"demoqueue"}
		data, _ := json.Marshal(subMsg)

		err = c.WriteMessage(websocket.BinaryMessage, data)
		if err != nil {
			log.Println("write:", err)
			return
		}
		_, message, err := c.ReadMessage()
		if err != nil {
			log.Println("read:", err)
			return
		}
		log.Printf("recv: %s", message)

		msgP := new(shared.MessageMessage)
		_ = json.Unmarshal(message, msgP)

		if msgP.Type == shared.MessageTypeSubQueueMsgResp && msgP.Result == shared.MessageResultSuccess {
			subMsg.Type = shared.MessageTypeSubQueueMsgAck
			subMsg.ReqId = msgP.ReqId
			data, _ := json.Marshal(subMsg)

			err = c.WriteMessage(websocket.BinaryMessage, data)
			if err != nil {
				log.Println("write:", err)
				return
			}
			_, message, err := c.ReadMessage()
			if err != nil {
				log.Println("read:", err)
				return
			}
			log.Printf("recv: %s", message)

		}
	}
}
