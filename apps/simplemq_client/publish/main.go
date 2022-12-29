package main

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"

	"github.com/meidoworks/nekoq/apps/simplemq_client/shared"
)

func main() {
	pubMsg := new(shared.MessageMessage)

	pubMsg.Type = shared.MessageTypePublish
	pubMsg.ReqId = "1234"
	pubMsg.Topic = "demo"
	//pubMsg.Queues = []string{}
	pubMsg.Header = []byte("{}")
	pubMsg.Payload = []byte("{\"key\":\"value\"}")
	data, _ := json.Marshal(pubMsg)

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
