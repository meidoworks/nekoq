package httpaddons

import (
	"bufio"
	"fmt"
	"net"
	"net/http"
	"testing"
	"time"
)

func TestLongPolling(t *testing.T) {
	const clientTimeout = 10 * time.Second
	const serverTimeout = 3 * time.Second

	l, err := net.Listen("tcp", "localhost:0")
	if err != nil {
		t.Fatal(err)
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/polling", func(writer http.ResponseWriter, request *http.Request) {
		t.Log("start invoking!!!!!!!!!!!!")
		if err := SendMessage(writer, []byte("{hhhhhhhhhhhhhhhh:lllllllllllllllll}")); err != nil {
			t.Log(err)
		}
		time.Sleep(serverTimeout)
		if err := SendMessage(writer, []byte("ab")); err != nil {
			t.Log(err)
		}
		t.Log("invoked!!!!!!!!!!!!!!")
	})
	srv := &http.Server{Handler: mux}
	go func() {
		err := srv.Serve(l)
		if err != nil {
			t.Log(err)
		}
	}()
	defer srv.Close()
	time.Sleep(time.Second)

	c := new(http.Client)
	c.Timeout = clientTimeout
	resp, err := c.Get(fmt.Sprint("http://", l.Addr().String(), "/polling"))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	for k, v := range resp.Header {
		fmt.Println("header:", k, "=", v)
	}

	bReader := bufio.NewReader(resp.Body)
	// 1st message
	{
		msg, err := PollingMessage(bReader)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("received:", string(msg))
	}
	// 2nd message
	{
		msg, err := PollingMessage(bReader)
		if err != nil {
			t.Fatal(err)
		}
		t.Log("received:", string(msg))
	}
}
