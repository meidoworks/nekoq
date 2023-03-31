package localswitch

import (
	"context"
	"net"
	"net/http"
	"net/http/cookiejar"

	"github.com/sirupsen/logrus"
	"golang.org/x/net/publicsuffix"
)

func NewLocalSwitchHttpClient(lswitch *LocalSwitch, trafficIndex uint8) *http.Client {
	newTransport := http.DefaultTransport.(*http.Transport).Clone()
	newTransport.DialContext = func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := lswitch.Connect(trafficIndex)
		if err != nil {
			return nil, err
		}
		if _localswitchLogger.IsLevelEnabled(logrus.InfoLevel) {
			_localswitchLogger.Infof("http localswitch dialer: convert to localswitch for the connection of %s", addr)
		}
		return conn, nil
	}
	cookieJar, _ := cookiejar.New(&cookiejar.Options{PublicSuffixList: publicsuffix.List})
	c := &http.Client{
		Transport: newTransport,
		Jar:       cookieJar,
	}
	return c
}
