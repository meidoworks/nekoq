package lib

import (
	"net/http"

	"goimport.moetang.info/nekoq/service/mq"
)

var mux = http.NewServeMux()
var manager = new(Manager)

var idgen *mq.IdGen
var topicIdMapping = make(map[string]mq.IdType)
var queueIdMapping = make(map[string]mq.IdType)

var (
	_MQ_LISTEN_ADDRESS      = "0.0.0.0:16000"
	_MQ_MANAGE_HTTP_ADDRESS = "0.0.0.0:16001"
	_MQ_NODEID              = 1
)

func init() {
	//TODO
	mux.HandleFunc("/manage/topic", manager.manageTopic)
}

func Run() error {
	demo := `
topic chat.room1 0
queue mem chat.queue1 0
bind  chat.room1 chat.queue2
`

	idgen = mq.NewIdGen(int16(_MQ_NODEID), 1)

	option := &mq.BrokerOption{
		NodeId: int16(_MQ_NODEID),
	}
	manager.broker = mq.NewBroker(option)

	err := cmdInitBroker(demo, manager.broker)
	if err != nil {
		return err
	}

	http.ListenAndServe(_MQ_MANAGE_HTTP_ADDRESS, mux)
	return nil
}
