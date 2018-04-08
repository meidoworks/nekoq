package rpcproxy

import (
	"errors"
	"log"
	"net"
	"strconv"
	"sync/atomic"
)

import (
	"goimport.moetang.info/nekoq/transport/tcp"
)

var (
	ERROR_ALREADY_STARTED = errors.New("TcpProxy is already started!")
)

type MultiplexingOption struct {
	FrontendBindAddr string
	BackendBindAddr  string
}

func NewMultiplexingTcpProxy(option *MultiplexingOption) (*TcpProxy, error) {
	tcpProxy := new(TcpProxy)
	tcpProxy.frontendUrl = option.FrontendBindAddr
	tcpProxy.backendUrl = option.BackendBindAddr
	tcpProxy.startFlag = 0

	return tcpProxy, nil
}

type TcpProxy struct {
	startFlag int32

	frontendUrl string
	backendUrl  string

	frontendListener *net.TCPListener
	backendListener  *net.TCPListener

	container *ChannelContainer
}

func (this *TcpProxy) Start() error {
	swapped := atomic.CompareAndSwapInt32(&this.startFlag, 0, 1)
	if !swapped {
		return ERROR_ALREADY_STARTED
	}

	fli, err := tcp.NewTcpListener(this.frontendUrl)
	if err != nil {
		return err
	}
	this.frontendListener = fli

	bli, err := tcp.NewTcpListener(this.backendUrl)
	if err != nil {
		return err
	}
	this.backendListener = bli

	initProxy(this)

	return nil
}

func (this *TcpProxy) Close() error {
	if atomic.CompareAndSwapInt32(&this.startFlag, 1, 2) {
		return errors.New("error close proxy. from " + strconv.Itoa(int(atomic.LoadInt32(&this.startFlag))) + " to 2")
	}
	err := this.frontendListener.Close()
	if err != nil {
		return err
	}
	err = this.backendListener.Close()
	if err != nil {
		return err
	}
	return nil
}

func initProxy(proxy *TcpProxy) {
	proxy.container = NewChannelContainer()

	fl := proxy.frontendListener
	go func() {
		for atomic.LoadInt32(&proxy.startFlag) == 1 {
			tcp, err := fl.AcceptTCP()
			if err != nil {
				if atomic.LoadInt32(&proxy.startFlag) == 2 {
					break
				} else {
					log.Println(err)
					break
				}
			}
			//TODO process tcp conn
			proxy.container.WrapFrontend(tcp)
		}
	}()
	bl := proxy.backendListener
	go func() {
		for atomic.LoadInt32(&proxy.startFlag) == 1 {
			tcp, err := bl.AcceptTCP()
			if err != nil {
				if atomic.LoadInt32(&proxy.startFlag) == 2 {
					break
				} else {
					log.Println(err)
					break
				}
			}
			//TODO process tcp conn
			proxy.container.WrapBackend(tcp)
		}
	}()
}
