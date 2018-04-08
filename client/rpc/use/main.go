package main

import (
	"log"
	"reflect"
)

import (
	"goimport.moetang.info/nekoq-common/context"
	"goimport.moetang.info/nekoq/use/rpc"
	"goimport.moetang.info/nekoq/use/rpc/client"
	"goimport.moetang.info/nekoq/use/rpc/server"
)

func main() {
	go func() {
		methods := map[string]reflect.Value{
			"ping": reflect.ValueOf(Ping),
		}
		serverGlobalConfig := new(server.ServiceConfig)
		serverGlobalConfig.Listen = "tcp://127.0.0.1:14357"
		server, err := server.NewServer(make(map[string]string), methods, serverGlobalConfig)
		if err != nil {
			log.Fatalln(err)
			return
		}
		var _ = server
	}()
	log.Println("server run.")

	var pingfunc PingFunc
	methods := map[string]reflect.Type{
		"ping": reflect.Type(reflect.ValueOf(pingfunc).Type()),
	}
	serviceGlobalConfig := new(client.ServiceConfig)
	serviceGlobalConfig.ConnectionTimeout = 10
	serviceGlobalConfig.ServerAddr = "tcp://127.0.0.1:14357"
	clientApi, err := client.NewServiceClient(make(map[string]string), methods, serviceGlobalConfig)
	if err != nil {
		log.Fatalln(err)
		return
	}

	i, err := clientApi.CallSync("ping", nil, new(context.AppInfo))
	if err != nil {
		log.Fatalln(err)
		return
	}
	log.Println("result:", i.(Pong).Msg)

	f, err := clientApi.CallAsync("ping", nil, new(context.AppInfo))
	if err != nil {
		log.Fatalln("async:", err)
		return
	}
	result, err := f.Get()
	log.Println("result:", result.(Pong).Msg, err)
}

type Pong struct {
	Msg string `codec:"msg"`
}

func Ping() (Pong, rpc.ErrStr) {
	pong := Pong{
		Msg: "pong",
	}
	return pong, ""
}

type PingFunc func() (Pong, rpc.ErrStr)
