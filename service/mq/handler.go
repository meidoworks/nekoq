package mq

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"

	"github.com/meidoworks/nekoq/service/mqapi"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type messagingHandler struct {
}

func (s messagingHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c, err := websocket.Accept(w, r, &websocket.AcceptOptions{
		Subprotocols: []string{"NekoQ"},
	})
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer c.Close(websocket.StatusInternalError, "the sky is falling")

	if c.Subprotocol() != "NekoQ" {
		c.Close(websocket.StatusPolicyViolation, "client must speak the NekoQ subprotocol")
		return
	}

	cid, err := idgenerator.Next()
	if err != nil {
		log.Println("generate id failed: " + fmt.Sprint(err))
		c.Close(websocket.StatusInternalError, "internal error")
		return
	}
	log.Println("new connection income:" + fmt.Sprint(r.RemoteAddr))

	//TODO process auth methods

	// create new node from the broker
	node, err := GetBroker().AddNode()
	if err != nil {
		log.Println("Add new node to the broker failed:" + fmt.Sprint(err))
	}
	ch := newWsch(cid, c, node)

	// start node reply worker
	go func() {
		for {
			r := <-node.GetReplyChannel()
			if r == nil {
				log.Println("receive nil reply from channel. exit processing loop.")
				return
			}
			if err := processReply(ch, r); err != nil {
				log.Println("process reply failed:" + fmt.Sprint(err))
			}
		}
	}()

	defer func() {
		if err := node.Leave(); err != nil {
			log.Println("Node leave failed:" + fmt.Sprint(err))
		}
		ch.cleanupWsChannel()
	}()

	for {
		err = requestHandler(r.Context(), ch, node)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return
		}
		if err != nil {
			log.Printf("failed to ServeHTTP with %v: %v\n", r.RemoteAddr, err)
			return
		}
	}
}

func requestHandler(ctx context.Context, c *wsch, node mqapi.Node) error {
	p := new(GeneralReq)
	//FIXME may need check protocol type = MessageBinary
	if err := wsjson.Read(context.Background(), c.Conn, p); err != nil {
		return err
	}

	return dispatch(p, c, node)
}

func respondPacket(p *GeneralRes, wc io.WriteCloser) error {
	data, err := json.Marshal(p)
	if err != nil {
		return err
	}
	_, err = wc.Write(data)
	if err != nil {
		return err
	} else {
		return nil
	}
}
