package mq

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/service/mqext"
)

func init() {
	if err := mqapi.Register("memory", func() mqapi.QueueType {
		return new(mqext.MemQueue)
	}); err != nil {
		panic(err)
	}
}
