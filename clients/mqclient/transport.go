package mqclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"nhooyr.io/websocket"
	"nhooyr.io/websocket/wsjson"
)

type _request struct {
	ch chan *ServerSideIncoming
}

type _rpcRequest struct {
	ch chan *ServerSideIncoming
}

type webChannel struct {
	Conn        *websocket.Conn
	closeCh     chan struct{}
	closeStatus int32

	requestLock  sync.Mutex
	requestIdMap map[string]*_request

	SgServerMessageChannels          map[string]chan *ServerSideIncoming
	SgServerMessageReleasingChannels map[string]chan *ServerSideIncoming
	ChannelMapLock                   sync.RWMutex

	rpcRequestMapLock sync.Mutex
	rpcRequestMap     map[string]*_rpcRequest
}

func newChannel(address string) (*webChannel, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	c, _, err := websocket.Dial(ctx, address, &websocket.DialOptions{
		Subprotocols: []string{"NekoQ"},
	})
	if err != nil {
		return nil, err
	}

	ch := new(webChannel)
	ch.Conn = c
	ch.closeCh = make(chan struct{})
	ch.requestIdMap = make(map[string]*_request)
	ch.SgServerMessageChannels = make(map[string]chan *ServerSideIncoming)
	ch.SgServerMessageReleasingChannels = make(map[string]chan *ServerSideIncoming)
	ch.rpcRequestMap = make(map[string]*_rpcRequest)
	go ch.dispatchLoop() // start dispatch loop
	return ch, nil
}

func (w *webChannel) clearRequestId(id string) {
	w.requestLock.Lock()
	delete(w.requestIdMap, id)
	w.requestLock.Unlock()
}

func (w *webChannel) writeObj(o *ToServerSidePacket) (<-chan *ServerSideIncoming, error) {
	// store request
	r := &_request{
		ch: make(chan *ServerSideIncoming, 1),
	}
	w.requestLock.Lock()
	w.requestIdMap[o.RequestId] = r
	w.requestLock.Unlock()

	// send request & cleanup if request failed
	if b, err := json.Marshal(o); err != nil {
		w.clearRequestId(o.RequestId)
		return nil, err
	} else {
		err := w._write(b)
		if err != nil {
			w.clearRequestId(o.RequestId)
		}
		return r.ch, err
	}
}

func (w *webChannel) _write(data []byte) error {
	if len(data) > maxRpcPayloadSize {
		return errors.New("payload size exceeded")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	return w.Conn.Write(ctx, websocket.MessageBinary, data)
}

func (w *webChannel) dispatchLoop() {
DispatchLoop:
	for {
		select {
		case <-w.closeCh:
			//FIXME should read until EOF?
			break DispatchLoop
		default:
			// keep read looping
			s := new(ServerSideIncoming)
			err := w.readObj(s)
			if err != nil {
				log.Println("read object error:" + fmt.Sprint(err))
				//FIXME should directly close the channel?
				w.close()
				continue DispatchLoop
			}

			if s.IncomingOperation != nil {
				w.processIncomingOperation(s)
				continue
			}

			w.requestLock.Lock()
			r, ok := w.requestIdMap[s.RequestId]
			delete(w.requestIdMap, s.RequestId)
			w.requestLock.Unlock()
			if !ok {
				log.Println(fmt.Sprintf("request id: %s not found", s.RequestId))
			} else {
				r.ch <- s
			}
		}
	}
}

func (w *webChannel) readObj(o *ServerSideIncoming) error {
	return wsjson.Read(context.Background(), w.Conn, o)
}

func (w *webChannel) close() error {
	if atomic.CompareAndSwapInt32(&w.closeStatus, 0, 1) {
		close(w.closeCh)
	}
	return w.Conn.Close(websocket.StatusNormalClosure, "close operation")
}

func (w *webChannel) processIncomingOperation(s *ServerSideIncoming) {
	switch *s.IncomingOperation {
	case IncomingOperationMessage:
		sgName := s.Message.SubscribeGroup
		w.ChannelMapLock.RLock()
		ch, ok := w.SgServerMessageChannels[sgName]
		w.ChannelMapLock.RUnlock()
		if !ok {
			log.Println("cannot find subscribeGroup message channel:" + fmt.Sprint(sgName))
			return
		}
		ch <- s
	case IncomingOperationMessageReleasing:
		sgName := s.MessageReleasing.SubscribeGroup
		w.ChannelMapLock.RLock()
		ch, ok := w.SgServerMessageReleasingChannels[sgName]
		w.ChannelMapLock.RUnlock()
		if !ok {
			log.Println("cannot find subscribeGroup releasing channel:" + fmt.Sprint(sgName))
			return
		}
		ch <- s
	case IncomingOperationReply:
		// receive reply
		rid := s.Reply.ReplyIdentifier
		w.rpcRequestMapLock.Lock()
		r, ok := w.rpcRequestMap[rid]
		delete(w.rpcRequestMap, rid)
		w.rpcRequestMapLock.Unlock()
		if !ok {
			log.Println("rpc identifier not exist:" + fmt.Sprint(rid))
			return
		}
		r.ch <- s
	default:
		log.Println("unknown incoming operation:" + (*s.IncomingOperation))
	}
}
