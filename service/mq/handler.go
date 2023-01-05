package mq

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"

	"nhooyr.io/websocket"
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
	ch := newWsch(cid, c)

	defer func() {
		ch.cleanupWsChannel()
	}()

	for {
		err = requestHandler(r.Context(), ch)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return
		}
		if err != nil {
			log.Printf("failed to ServeHTTP with %v: %v\n", r.RemoteAddr, err)
			return
		}
	}
}

func requestHandler(ctx context.Context, c *wsch) error {
	typ, r, err := c.Conn.Reader(ctx)
	if err != nil {
		return err
	}
	if typ != websocket.MessageBinary {
		return errors.New("message type is not binary")
	}

	fullData, err := io.ReadAll(r)
	if err != nil {
		return err
	}

	p := new(GeneralReq)
	if err := json.Unmarshal(fullData, p); err != nil {
		return err
	}

	return dispatch(p, c)
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
