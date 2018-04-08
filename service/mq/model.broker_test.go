package mq

import (
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

	for {
		_, err = broker.GenNewInternalTopicId()
		if err == ErrTopicInternalIdExceeded {
			break
		}
	}
	t.Log("finish allocate id test")
}
