package main

import (
	"errors"
	"log"
	"time"
)

import (
	"goimport.moetang.info/nekoq/packet"
	"goimport.moetang.info/nekoq/packet/rpc"
	"goimport.moetang.info/nekoq/transport"
	"goimport.moetang.info/nekoq/transport/tcp"
)

var (
	ERROR_UNSUPPORT_OPERATION = errors.New("operation is not support.")
)

func main() {
	tcpConn, err := tcp.NewTcpConn("tcp://127.0.0.1:23234", 10*time.Second)
	if err != nil {
		log.Fatalln(err)
	}

	rpcPacketHandler := new(rpc.RpcHandler)
	rpcPacketHandler.HandleReq = func(req *rpc.RpcReq, conn transport.TransConn) error {
		return ERROR_UNSUPPORT_OPERATION
	}
	rpcPacketHandler.HandleResp = func(resp *rpc.RpcResp, conn transport.TransConn) error {
		log.Println(resp.ReqId, string(resp.Result))
		log.Println(resp)
		log.Println(resp.ResultType)
		return conn.Close()
	}

	packet.StartTcpConnPacketProcessing(tcpConn, func() tcp.TcpHandler {
		return new(RpcComsumerHandler)
	}, rpcPacketHandler)

	time.Sleep(5 * time.Second)
}

type RpcComsumerHandler struct {
	packet.AbstractTcpPacketHandler
}

func (this *RpcComsumerHandler) OnInit(tcpConn *tcp.TcpConn) {
	log.Println("==============================================")
	log.Println("connected!")
	log.Println("==============================================")
	rpcReq := new(rpc.RpcReq)
	rpcReq.TraceId = []byte("1234567812345678123456781234567812345678123456781234567812345678")
	rpcReq.RpcId = []byte("87654321876543218765432187654321")
	rpcReq.ReqId = [4]byte{5, 4, 3, 6}
	rpcReq.ServiceName = "echo:1.0.0"
	rpcReq.Param = []byte("param data is here.")
	err := tcpConn.Write(rpcReq)
	if err != nil {
		log.Println(err)
		tcpConn.Close()
	}
}
