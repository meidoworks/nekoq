package bench

import (
	"errors"
	"log"
	"sync"
	"testing"
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

func BenchmarkRpc(b *testing.B) {
	tcpConn, err := tcp.NewTcpConn("tcp://127.0.0.1:23234", 10*time.Second)
	if err != nil {
		log.Fatalln(err)
	}

	wg := new(sync.WaitGroup)
	wg.Add(b.N)

	rpcPacketHandler := new(rpc.RpcHandler)
	rpcPacketHandler.HandleReq = func(req *rpc.RpcReq, conn transport.TransConn) error {
		return ERROR_UNSUPPORT_OPERATION
	}
	rpcPacketHandler.HandleResp = func(resp *rpc.RpcResp, conn transport.TransConn) error {
		wg.Done()
		return nil
	}

	conn, err := packet.StartTcpConnPacketProcessing(tcpConn, func() tcp.TcpHandler {
		return new(RpcComsumerHandler)
	}, rpcPacketHandler)
	if err != nil {
		log.Fatalln(err)
	}

	for i := 0; i < b.N; i++ {
		OnInit(conn)
	}

	wg.Wait()

	tcpConn.Close()
}

type RpcComsumerHandler struct {
	packet.AbstractTcpPacketHandler
}

func OnInit(tcpConn *tcp.TcpConn) {
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
