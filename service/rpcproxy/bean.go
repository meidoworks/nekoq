package rpcproxy

import "goimport.moetang.info/nekoq/packet/rpc"

type Req struct {
	Id         uint64
	SrcChannel uint64
	ReqPacket  *rpc.RpcReq
}
