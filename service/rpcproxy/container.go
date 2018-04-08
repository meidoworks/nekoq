package rpcproxy

import (
	"net"
	"sync/atomic"
)

import (
	"goimport.moetang.info/nekoq/packet"
)

type ChannelContainer struct {
	frontendIdGen    uint64
	frontendChannels map[uint64]*Conn
	backendIdGen     uint64
	backendChannels  map[uint64]*Conn
}

type Conn struct {
	Id uint64
	*packet.Conn
}

func NewChannelContainer() *ChannelContainer {
	c := new(ChannelContainer)
	c.frontendIdGen = 1
	c.backendIdGen = 1
	c.frontendChannels = make(map[uint64]*Conn)
	c.backendChannels = make(map[uint64]*Conn)
	return c
}

func (this *ChannelContainer) GetFrontendChannel(id uint64) *Conn {
	v, ok := this.frontendChannels[id]
	if ok {
		return v
	} else {
		return nil
	}
}

func (this *ChannelContainer) GetFrontendId() uint64 {
	return incUint64(&this.frontendIdGen)
}

func (this *ChannelContainer) GetBackendChannel(id uint64) *Conn {
	v, ok := this.backendChannels[id]
	if ok {
		return v
	} else {
		return nil
	}
}

func (this *ChannelContainer) GetBackendId() uint64 {
	return incUint64(&this.backendIdGen)
}

func incUint64(addr *uint64) uint64 {
	r := 0
	for true {
		i := atomic.LoadUint64(addr)
		r = i + 1
		r := atomic.CompareAndSwapUint64(addr, i, r)
		if r {
			break
		}
	}
	return r
}

func (this *ChannelContainer) WrapFrontend(conn *net.TCPConn) *Conn {
	c := packet.WrapTcpConn(conn)
	id := this.GetFrontendId()
	nc := new(Conn)
	nc.Id = id
	nc.Conn = c
	this.frontendChannels[id] = nc
	return nc
}

func (this *ChannelContainer) WrapBackend(conn *net.TCPConn) *Conn {
	c := packet.WrapTcpConn(conn)
	id := this.GetBackendId()
	nc := new(Conn)
	nc.Id = id
	nc.Conn = c
	this.backendChannels[id] = nc
	return nc
}
