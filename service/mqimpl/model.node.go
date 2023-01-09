package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
)

var _ mqapi.Node = new(Node)

type Node struct {
	broker *Broker

	InitFunc func(sub mqapi.SubscribeGroup)
}

func (n *Node) PublishGroupInitialize(pg mqapi.PublishGroup) error {
	//TODO implement me
	panic("implement me")
}

func (n *Node) SubscribeGroupInitialize(sg mqapi.SubscribeGroup) error {
	n.InitFunc(sg)
	return nil
}

func (n *Node) DirectReply(reply *mqapi.Reply, ctx *mqapi.Ctx) error {
	//TODO
	return nil
}
