package bench

import (
	"log"
	"math/rand"
	"reflect"
	"strconv"
	"testing"
	"time"
)

import (
	"goimport.moetang.info/nekoq-common/context"
	"goimport.moetang.info/nekoq/use/rpc"
	"goimport.moetang.info/nekoq/use/rpc/client"
	"goimport.moetang.info/nekoq/use/rpc/server"
)

func BenchmarkBenchRpc(b *testing.B) {
	rand.Seed(int64(time.Now().Nanosecond()))
	port := strconv.Itoa(rand.Intn(20000) + 10000)
	log.Println("port:", port)
	go func() {
		methods := map[string]reflect.Value{
			"ping": reflect.ValueOf(Ping),
		}
		serverGlobalConfig := new(server.ServiceConfig)
		serverGlobalConfig.Listen = "tcp://127.0.0.1:" + port
		serverApi, err := server.NewServer(make(map[string]string), methods, serverGlobalConfig)
		if err != nil {
			log.Fatalln(err)
			return
		}
		var _ = serverApi
	}()
	time.Sleep(1 * time.Second)
	log.Println("server run.")

	var pingfunc PingFunc
	methods := map[string]reflect.Type{
		"ping": reflect.Type(reflect.ValueOf(pingfunc).Type()),
	}
	serviceGlobalConfig := new(client.ServiceConfig)
	serviceGlobalConfig.ConnectionTimeout = 10
	serviceGlobalConfig.ServerAddr = "tcp://127.0.0.1:" + port
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
	log.Println(i.(Pong).Msg)
	i, err = clientApi.CallSync("ping", nil, new(context.AppInfo))
	if err != nil {
		log.Fatalln(err)
		return
	}
	log.Println(i.(Pong).Msg)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		clientApi.CallSync("ping", nil, new(context.AppInfo))
	}
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
