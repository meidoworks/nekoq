package mqimpl

import (
	"github.com/meidoworks/nekoq/service/mqapi"
	"testing"
)

func TestBroker_GenNewInternalTopicId(t *testing.T) {
	option := &BrokerOption{
		NodeId: 1,
	}
	broker := NewBroker(option)

	t.Log(broker.GenNewInternalTopicId())

	r1, err := broker.GenNewInternalTopicId()
	if err != nil {
		t.Fatal(err)
	}
	r2, err := broker.GenNewInternalTopicId()
	if err != nil {
		t.Fatal(err)
	}
	r3, err := broker.GenNewInternalTopicId()
	if err != nil {
		t.Fatal(err)
	}

	if r2 <= r1 || r3 <= r1 || r3 <= r2 {
		t.Error("id not increment")
	} else {
		t.Log(r1, r2, r3)
	}

	for i := 0; i < 10000000; i++ {
		_, err = broker.GenNewInternalTopicId()
		if err == mqapi.ErrTopicInternalIdExceeded {
			break
		}
	}
	t.Log("finish allocate id test")
}
