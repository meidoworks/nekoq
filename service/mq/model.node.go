package mq

type Node struct {
	broker *Broker

	InitFunc func(sub *SubscribeGroup)
}

func (this *Broker) AddNode() *Node {
	//TODO
	return nil
}

func (this *Node) ReplyTo(reply *Reply, ctx *Ctx) error {
	switch reply.ReplyType {
	case 1:
		//to node
		node, err := this.broker.GetNode(reply.ReplyTo.ReplyTo)
		if err != nil {
			return err
		}
		return node.DirectReply(reply, ctx)
	case 2:
		//to publishGroup
		pg, ok := this.broker.publishGroupMap[reply.ReplyTo.ReplyTo]
		if !ok {
			return ErrReplyDestinationNotExist
		}
		return pg.Reply(reply, ctx)
	default:
		return ErrReplyTypeUnknown
	}
}

func (this *Node) DirectReply(reply *Reply, ctx *Ctx) error {
	//TODO
	return nil
}
