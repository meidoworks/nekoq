package localswitch

import (
	"net"
	"net/http"
	"testing"
	"time"

	"github.com/meidoworks/nekoq/shared/netaddons/multiplexer"
)

func TestSimpleSwitch(t *testing.T) {
	const data byte = 147
	rCh := make(chan bool, 1)

	lswitch, err := StartNewLocalSwitch()
	if err != nil {
		t.Fatal(err)
	}
	lswitch.AddTrafficConsumer(1, func(conn net.Conn, meta multiplexer.TrafficMeta) error {
		d := make([]byte, 1)
		_, _ = conn.Read(d)
		rCh <- d[0] == data
		return nil
	})
	conn, err := lswitch.Connect(1)
	if err != nil {
		t.Fatal(err)
	}
	_, _ = conn.Write([]byte{data})

	select {
	case r := <-rCh:
		if !r {
			t.Fatal("unexpected data received")
		}
	case <-time.NewTimer(5 * time.Second).C:
		t.Fatal("timeout")
	}
}

func TestLocalSwitchForHttpServer(t *testing.T) {
	l := NewLocalSwitchNetListener()
	server := &http.Server{Handler: http.NewServeMux()}
	go func() {
		if err := server.Serve(l); err != nil {
			t.Log(err)
		}
	}()

	lswitch, err := StartNewLocalSwitch()
	if err != nil {
		t.Fatal(err)
	}
	lswitch.AddTrafficConsumer(1, func(conn net.Conn, meta multiplexer.TrafficMeta) error {
		l.PublishNetConn(conn)
		return nil
	})

	c := NewLocalSwitchHttpClient(lswitch, 1)
	resp, err := c.Get("http://localhost")
	if err != nil {
		t.Fatal(err)
	}
	t.Log("http status:", resp.Status)
	_ = resp.Body.Close()
}
