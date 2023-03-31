package multiplexer

import (
	"crypto/rand"
	"errors"
	"io"
	"net"
)

const (
	_HeaderSize = 128
	_Version    = 1

	_PacketTypeClientMeta = 1
	_PacketTypeServerMeta = 2

	_HandshakeStatusSuccess                 = 0
	_HandshakeStatusUnsupportedTrafficIndex = 1
	_HandshakeStatusUpgradeVersion          = 2
)

var inprocToken [32]byte

func init() {
	if n, err := rand.Read(inprocToken[:]); err != nil {
		panic(err)
	} else if n != 32 {
		panic(errors.New("random number not enough"))
	}
}

type TrafficMeta struct {
	Version         byte
	PacketType      byte
	TrafficIndex    uint8
	HandshakeStatus byte

	//TODO token support
	Token [32]byte
}

func (t TrafficMeta) ToData() []byte {
	data := make([]byte, _HeaderSize)
	data[0] = t.Version
	data[1] = t.PacketType
	data[2] = t.TrafficIndex
	data[3] = t.HandshakeStatus
	return data
}

func (t *TrafficMeta) ReadInput(reader io.Reader) error {
	data := make([]byte, _HeaderSize)
	if _, err := io.ReadFull(reader, data); err != nil {
		return err
	}
	t.Version = data[0]
	t.PacketType = data[1]
	t.TrafficIndex = data[2]
	t.HandshakeStatus = data[3]
	return nil
}

type TrafficConsumer func(conn net.Conn, meta TrafficMeta) error

type DedicatedServerConnMultiplexer struct {
	Consumers map[int]TrafficConsumer
}

func (d *DedicatedServerConnMultiplexer) ConsumeConn(conn net.Conn) error {
	ntm := TrafficMeta{}
	if err := ntm.ReadInput(conn); err != nil {
		return err
	}

	if ntm.Version != _Version {
		tm := TrafficMeta{
			Version:         ntm.Version,
			PacketType:      _PacketTypeServerMeta,
			TrafficIndex:    ntm.TrafficIndex,
			HandshakeStatus: _HandshakeStatusUpgradeVersion,
		}
		_, _ = conn.Write(tm.ToData())
		return errors.New("version not supported")
	} else if ntm.PacketType != _PacketTypeClientMeta {
		return errors.New("client meta expected")
	} else if ntm.HandshakeStatus != _HandshakeStatusSuccess {
		return errors.New("unknown handshake status")
	}

	consumer, ok := d.Consumers[int(ntm.TrafficIndex)]
	if !ok {
		tm := TrafficMeta{
			Version:         _Version,
			PacketType:      _PacketTypeServerMeta,
			TrafficIndex:    ntm.TrafficIndex,
			HandshakeStatus: _HandshakeStatusUnsupportedTrafficIndex,
		}
		_, _ = conn.Write(tm.ToData())
		return errors.New("unsupported traffic index")
	} else {
		tm := TrafficMeta{
			Version:         _Version,
			PacketType:      _PacketTypeServerMeta,
			TrafficIndex:    ntm.TrafficIndex,
			HandshakeStatus: _HandshakeStatusSuccess,
		}
		if n, err := conn.Write(tm.ToData()); err != nil {
			return err
		} else if n != len(tm.ToData()) {
			return errors.New("write unexpected length of data")
		}
		return consumer(conn, tm)
	}
}

func ClientConnInitialization(conn net.Conn, trafficIndex uint8) error {
	tm := TrafficMeta{
		Version:         _Version,
		PacketType:      _PacketTypeClientMeta,
		TrafficIndex:    trafficIndex,
		HandshakeStatus: _HandshakeStatusSuccess,
	}

	if n, err := conn.Write(tm.ToData()); err != nil {
		return err
	} else if n != len(tm.ToData()) {
		return errors.New("write unexpected length of data")
	}

	ntm := TrafficMeta{}
	if err := ntm.ReadInput(conn); err != nil {
		return err
	}

	if ntm.Version != _Version {
		return errors.New("version not supported")
	} else if ntm.TrafficIndex != trafficIndex {
		return errors.New("traffic index not matched")
	} else if ntm.PacketType != _PacketTypeServerMeta {
		return errors.New("server meta expected")
	} else if ntm.HandshakeStatus != _HandshakeStatusSuccess {
		switch ntm.HandshakeStatus {
		case _HandshakeStatusUnsupportedTrafficIndex:
			return errors.New("unsupported traffic index")
		case _HandshakeStatusUpgradeVersion:
			return errors.New("need to upgrade version")
		default:
			return errors.New("unknown handshake status")
		}
	}

	return nil
}
