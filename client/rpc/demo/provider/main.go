package main

import (
	"errors"
	"log"
)

import (
	"goimport.moetang.info/nekoq/packet"
	"goimport.moetang.info/nekoq/packet/rpc"
	"goimport.moetang.info/nekoq/transport"
	"goimport.moetang.info/nekoq/transport/tcp"
)

import (
	"net/http"
	_ "net/http/pprof"
)

var (
	ERROR_UNSUPPORT_OPERATION = errors.New("operation is not support.")
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()

	rpcPacketHandler := new(rpc.RpcHandler)
	rpcPacketHandler.HandleReq = func(req *rpc.RpcReq, conn transport.TransConn) error {
		resp := new(rpc.RpcResp)
		resp.ReqId = req.ReqId
		resp.ResultType = 1222
		resp.Result = []byte("result is: " + string(req.Param))
		err := conn.Write(resp)
		if err != nil {
			log.Println(err)
			conn.Close()
		}
		return nil
	}
	rpcPacketHandler.HandleResp = func(resp *rpc.RpcResp, conn transport.TransConn) error {
		return ERROR_UNSUPPORT_OPERATION
	}

	listener, err := tcp.NewTcpListener("tcp://0.0.0.0:23234")
	if err != nil {
		log.Fatalln(err)
	}

	packet.StartTcpListenerPacketProcessing(listener, func() tcp.TcpHandler {
		return new(RpcProviderHandler)
	}, rpcPacketHandler)
}

type RpcProviderHandler struct {
	packet.AbstractTcpPacketHandler
}
