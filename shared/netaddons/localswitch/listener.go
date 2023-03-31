package localswitch

import (
	"errors"
	"net"
)

type emptyAddr struct {
}

func (e emptyAddr) Network() string {
	return "localswitch"
}

func (e emptyAddr) String() string {
	return "localswitch"
}

type LocalSwitchNetListener struct {
	connCh chan net.Conn

	closeCh chan struct{}
}

func (l *LocalSwitchNetListener) PublishNetConn(conn net.Conn) {
	//FIXME looks like there may be the case when the listener is closing and a new connection comes, the connection will not be well handled
	select {
	case <-l.closeCh:
		break
	default:
		l.connCh <- conn
	}
}

func NewLocalSwitchNetListener() *LocalSwitchNetListener {
	localListener := &LocalSwitchNetListener{
		connCh:  make(chan net.Conn, 128),
		closeCh: make(chan struct{}),
	}

	return localListener
}

func (l *LocalSwitchNetListener) Accept() (net.Conn, error) {
	select {
	case conn := <-l.connCh:
		return conn, nil
	case <-l.closeCh:
		return nil, errors.New("local listener has been closed")
	}
}

func (l *LocalSwitchNetListener) Close() error {
	close(l.closeCh)
	return nil
}

func (l *LocalSwitchNetListener) Addr() net.Addr {
	return emptyAddr{}
}
