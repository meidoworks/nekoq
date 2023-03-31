package localswitch

import (
	"fmt"
	"net"
	"net/url"
	"time"

	"github.com/meidoworks/nekoq/shared/logging"
	"github.com/meidoworks/nekoq/shared/netaddons/multiplexer"
	"github.com/meidoworks/nekoq/shared/utils"

	"github.com/sirupsen/logrus"
)

var _localswitchLogger = logging.NewLogger("localswitch")

type LocalSwitch struct {
	addr     string
	listener net.Listener

	mux *multiplexer.DedicatedServerConnMultiplexer
}

func (l *LocalSwitch) Connect(trafficIndex uint8) (net.Conn, error) {
	u, err := url.Parse(l.addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.DialTimeout(u.Scheme, u.Host, 10*time.Second)
	if err != nil {
		return nil, err
	}
	if err := multiplexer.ClientConnInitialization(conn, trafficIndex); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return conn, nil
}

func (l *LocalSwitch) AddTrafficConsumer(trafficIndex uint8, consumer multiplexer.TrafficConsumer) {
	l.mux.Consumers = utils.CopyAddMap(l.mux.Consumers, int(trafficIndex), consumer)
}

func (l *LocalSwitch) SwitchAddr() string {
	return l.addr
}

func (l *LocalSwitch) listenLoop() {
	for {
		conn, err := l.listener.Accept()
		if err != nil {
			//FIXME handling accept error
			_localswitchLogger.Errorf("localswitch accept error: %s", err)
			break
		}
		go func() {
			if err := l.mux.ConsumeConn(conn); err != nil {
				//FIXME handling conn consumption error
				_localswitchLogger.Errorf("localswitch general connection consumption error: %s", err)
			}
		}()
	}
}

func StartNewLocalSwitch() (*LocalSwitch, error) {
	listener, err := net.Listen("tcp", "127.0.0.255:0")
	if err != nil {
		return nil, err
	}

	lswitch := &LocalSwitch{
		addr:     fmt.Sprintf("tcp://%s", listener.Addr().String()),
		listener: listener,
		mux:      new(multiplexer.DedicatedServerConnMultiplexer),
	}
	go lswitch.listenLoop()
	if _localswitchLogger.IsLevelEnabled(logrus.InfoLevel) {
		_localswitchLogger.Infof("start localswitch at: %s", lswitch.SwitchAddr())
	}
	return lswitch, nil
}
