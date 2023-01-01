package mq

import (
	"errors"

	"github.com/meidoworks/nekoq/service/mqapi"
	"github.com/meidoworks/nekoq/service/mqimpl"
	"github.com/meidoworks/nekoq/shared/idgen"
)

var _broker mqapi.Broker
var idgenerator *idgen.IdGen

func GetBroker() mqapi.Broker {
	b := _broker
	if b == nil {
		panic(errors.New("broker has not been initialized"))
	}
	return b
}

func InitBroker() error {
	idgenerator = idgen.NewIdGen(0, 0) // default hardcoded value
	// load persisted metadata
	if err := LoadMetadata(); err != nil {
		return err
	}

	// Start broker
	brokerOption := &mqimpl.BrokerOption{
		NodeId: 1,
	}
	broker := mqimpl.NewBroker(brokerOption)
	err := broker.Start()
	if err != nil {
		return err
	}
	_broker = broker

	// Prepare broker
	GetMetadataContainer().PrepareBroker()

	//TODO load persisted data

	return nil
}
