package mqimpl

import (
	"sync/atomic"

	"github.com/meidoworks/nekoq/service/mqapi"
)

var _ mqapi.Node = new(Node)

type Node struct {
	broker *Broker
	nodeId mqapi.NodeId

	replyChannel chan *mqapi.Reply
	closeFlag    int32

	InitSubscribeGroupFn func(sub mqapi.SubscribeGroup)
	InitPublishGroupFn   func(pub mqapi.PublishGroup)
}

func (n *Node) PublishGroupInitialize(pg mqapi.PublishGroup) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node) SubscribeGroupInitialize(sg mqapi.SubscribeGroup) error {
	n.InitSubscribeGroupFn(sg)
	return nil
}

func (n *Node) DirectReply(reply *mqapi.Reply, ctx *mqapi.Ctx) error {
	select {
	case n.replyChannel <- reply:
	case <-ctx.Context.Done():
		return mqapi.ErrReplyTimeout
	}
	return nil
}

func (n *Node) Leave() error {
	n.broker.clientNodeMapLock.Lock()
	delete(n.broker.clientNodeMap, n.nodeId)
	n.broker.clientNodeMapLock.Unlock()
	if atomic.CompareAndSwapInt32(&n.closeFlag, 0, 1) {
		close(n.replyChannel)
	}
	return nil
}

func (n *Node) GetReplyChannel() <-chan *mqapi.Reply {
	return n.replyChannel
}

func (n *Node) GetNodeId() mqapi.NodeId {
	return n.nodeId
}
