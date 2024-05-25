package numgen

import (
	"errors"
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/meidoworks/nekoq-component/component/comphttp"
	"github.com/meidoworks/nekoq-component/http/chi"

	chiraw "github.com/go-chi/chi"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/clients/namingclient"
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/shared/idgen"
	"github.com/meidoworks/nekoq/shared/netaddons/localswitch"
	"github.com/meidoworks/nekoq/shared/netaddons/multiplexer"
)

type ServiceNumGen struct {
	engine      *chi.ChiHttpApiServer
	cfg         config.NumGenConfig
	namingAddrs []string

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

	ng.engine = chi.NewChiHttpApiServer(&chi.ChiHttpApiServerConfig{
		Addr: cfg.Listen,
	})
	ng.cfg = cfg
	ng.namingAddrs = allCfg.Shared.NamingAddrs

	return ng, nil
}

type chiGenNum struct {
	s *ServiceNumGen
}

func (c chiGenNum) ParentUrl() string {
	return ""
}

func (c chiGenNum) Url() string {
	return "/v1/{gen_key}/{count}"
}

func (c chiGenNum) HttpMethod() []string {
	return []string{http.MethodGet}
}

func (c chiGenNum) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	key := chiraw.URLParam(r, "gen_key")
	countStr := chiraw.URLParam(r, "count")
	count, err := strconv.Atoi(countStr)
	//FIXME hardcoded max 100 IDs
	if err != nil || count <= 0 || count > 100 {
		return chi.RenderStatus(http.StatusBadRequest), nil
	}

	gen := c.s.GetNumGen(key)

	ids, err := gen.NextN(count)
	if err != nil {
		return chi.RenderError(err), nil
	}

	var idstrings = make([]string, 0, len(ids))
	for _, v := range ids {
		idstrings = append(idstrings, v.HexString())
	}

	result := strings.Join(idstrings, "\n")
	return chi.RenderOKString(result), nil
}

func (s *ServiceNumGen) StartHttp() error {

	if err := s.engine.AddHttpApi(chiGenNum{s: s}); err != nil {
		return err
	}

	// start service in LocalSwitch
	{
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			listener := localswitch.NewLocalSwitchNetListener()

			lswitch := inproc.GetLocalSwitch()
			lswitch.AddTrafficConsumer(api.LocalSwitchNumGen, func(conn net.Conn, meta multiplexer.TrafficMeta) error {
				listener.PublishNetConn(conn)
				return nil
			})

			err := s.engine.StartServicingOn(listener)
			if err != nil {
				panic(err)
			}
		})
	}

	// start exposed service
	if !s.cfg.Disable {
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			if err := s.engine.StartServing(); err != nil {
				panic(err)
			}
		})
		var namingClient *namingclient.NamingClient
		var err error
		nodeName := fmt.Sprint("nekoq_numgen_", s.nodeId)
		isLocal, err := validateAndCheckLocal(s.namingAddrs)
		if err != nil {
			return err
		}
		if isLocal {
			if namingClient, err = namingclient.NewLocalSwitchNamingClient(inproc.GetLocalSwitch(), nodeName); err != nil {
				return err
			}
		} else {
			if namingClient, err = namingclient.NewNamingClient(s.namingAddrs, nodeName); err != nil {
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

func validateAndCheckLocal(addrs []string) (bool, error) {
	var localCnt = 0
	for _, v := range addrs {
		if v == api.DefaultConfigLocalSwitchNamingAddress {
			localCnt++
		}
	}
	if localCnt > 0 && localCnt != len(addrs) {
		return false, errors.New("inproc should not be used with network address at the same time")
	}
	return localCnt > 0, nil
}
