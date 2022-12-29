package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
)

var _ mqapi.Node = new(Node)

type Node struct {
	broker *Broker

	InitFunc func(sub mqapi.SubscribeGroup)
}

func (this *Node) PublishGroupInitialize(pg mqapi.PublishGroup) error {
	//TODO implement me
	panic("implement me")
}

func (this *Node) SubscribeGroupInitialize(sg mqapi.SubscribeGroup) error {
	this.InitFunc(sg)
	return nil
}

func (this *Broker) AddNode() *Node {
	//TODO
	return nil
}

func (this *Node) ReplyTo(reply *mqapi.Reply, ctx *mqapi.Ctx) error {
	switch reply.ReplyType {
	case 1:
		//to node
		node, err := this.broker.GetNode(reply.ReplyTo.ReplyToNode)
		if err != nil {
			return err
		}
		return node.DirectReply(reply, ctx)
	case 2:
		//to publishGroup
		pg, ok := this.broker.publishGroupMap[reply.ReplyTo.ReplyToPublishGroup]
		if !ok {
			return mqapi.ErrReplyDestinationNotExist
		}
		return pg.Reply(reply, ctx)
	default:
		return mqapi.ErrReplyTypeUnknown
	}
}

func (this *Node) DirectReply(reply *mqapi.Reply, ctx *mqapi.Ctx) error {
	//TODO
	return nil
}
