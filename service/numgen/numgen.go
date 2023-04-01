package numgen

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/clients/namingclient"
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/shared/idgen"
	"github.com/meidoworks/nekoq/shared/netaddons/localswitch"
	"github.com/meidoworks/nekoq/shared/netaddons/multiplexer"
	"github.com/meidoworks/nekoq/shared/thirdpartyshared/ginshared"

	"github.com/gin-gonic/gin"
)

type ServiceNumGen struct {
	engine     *gin.Engine
	cfg        config.NumGenConfig
	namingAddr string

	nodeId    int16
	elementId int32
	area      string

	m sync.Map
}

func (s *ServiceNumGen) GetNumGen(key string) *idgen.IdGen {
	gen, ok := s.m.Load(key)
	if ok {
		return gen.(*idgen.IdGen)
	}

	ele := atomic.AddInt32(&s.elementId, 1)
	g := idgen.NewIdGen(s.nodeId, ele)
	// store
	gen, _ = s.m.LoadOrStore(key, g)

	return gen.(*idgen.IdGen)
}

func NewServiceNumGen(allCfg config.NekoConfig, cfg config.NumGenConfig) (*ServiceNumGen, error) {
	ng := new(ServiceNumGen)
	ng.nodeId = *allCfg.Shared.NodeId
	ng.area = allCfg.Shared.Area

	ng.engine = gin.New()
	ng.cfg = cfg
	ng.namingAddr = allCfg.Shared.NamingAddr

	return ng, nil
}

func (s *ServiceNumGen) StartHttp() error {
	s.engine.Use(gin.Recovery())
	s.engine.Use(func(context *gin.Context) {
		context.Next()
		var err error
		// handling first error to respond
		for _, v := range context.Errors {
			err = v
			break
		}
		if err != nil {
			context.String(http.StatusInternalServerError, err.Error())
		}
	})

	s.engine.GET("/v1/:gen_key/:count", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		key := ctx.Param("gen_key")
		countStr := ctx.Param("count")
		count, err := strconv.Atoi(countStr)
		//FIXME hardcoded max 100 IDs
		if err != nil || count <= 0 || count > 100 {
			return ginshared.RenderStatus(http.StatusBadRequest)
		}

		gen := s.GetNumGen(key)

		ids, err := gen.NextN(count)
		if err != nil {
			return ginshared.RenderError(err)
		}

		var idstrings = make([]string, 0, len(ids))
		for _, v := range ids {
			idstrings = append(idstrings, v.HexString())
		}

		result := strings.Join(idstrings, "\n")
		return ginshared.RenderOKString(result)
	}))

	// start service in LocalSwitch
	{
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			listener := localswitch.NewLocalSwitchNetListener()

			lswitch := inproc.GetLocalSwitch()
			lswitch.AddTrafficConsumer(api.LocalSwitchNumGen, func(conn net.Conn, meta multiplexer.TrafficMeta) error {
				listener.PublishNetConn(conn)
				return nil
			})

			err := s.engine.RunListener(listener)
			if err != nil {
				panic(err)
			}
		})
	}

	// start exposed service
	if !s.cfg.Disable {
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			if err := s.engine.Run(s.cfg.Listen); err != nil {
				panic(err)
			}
		})
		var namingClient *namingclient.NamingClient
		var err error
		nodeName := fmt.Sprint("nekoq_numgen_", s.nodeId)
		if s.namingAddr == api.DefaultConfigLocalSwitchNamingAddress {
			if namingClient, err = namingclient.NewLocalSwitchNamingClient(inproc.GetLocalSwitch(), nodeName); err != nil {
				return err
			}
		} else {
			if namingClient, err = namingclient.NewNamingClient(s.namingAddr, nodeName); err != nil {
				return err
			}
		}
		if err := namingClient.Register(s.cfg.ServiceName, s.area, namingclient.ServiceDesc{
			Port: namingclient.ParsePortFromHost(s.cfg.Listen),
		}); err != nil {
			return err
		}
	}

	return nil
}
