package rpcproxy_test

import (
	"testing"
	"time"
)

import (
	"goimport.moetang.info/nekoq/service/rpcproxy"
)

func TestBasicUsage(t *testing.T) {
	option := &rpcproxy.MultiplexingOption{
		FrontendBindAddr: "tcp://127.0.0.1:13579",
		BackendBindAddr:  "tcp://127.0.0.1:13580",
	}
	proxy, err := rpcproxy.NewMultiplexingTcpProxy(option)
	if err != nil {
		t.Fatal(err)
	}

	err = proxy.Start()
	if err != nil {
		t.Fatal(err)
	}

	time.Sleep(1 * time.Minute)
}
