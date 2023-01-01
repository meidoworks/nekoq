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
		Subprotocols: []string{"nekoq"},
	})
	if err != nil {
		log.Printf("%v\n", err)
		return
	}
	defer c.Close(websocket.StatusInternalError, "the sky is falling")

	if c.Subprotocol() != "nekoq" {
		c.Close(websocket.StatusPolicyViolation, "client must speak the nekoq subprotocol")
		return
	}

	log.Println("new connection income:" + fmt.Sprint(r.RemoteAddr))

	for {
		err = requestHandler(r.Context(), c)
		if websocket.CloseStatus(err) == websocket.StatusNormalClosure {
			return
		}
		if err != nil {
			log.Printf("failed to ServeHTTP with %v: %v\n", r.RemoteAddr, err)
			return
		}
	}
}

func requestHandler(ctx context.Context, c *websocket.Conn) error {
	typ, r, err := c.Reader(ctx)
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

	return dispatch(p, func() (io.WriteCloser, error) {
		return c.Writer(context.Background(), websocket.MessageBinary)
	})
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
