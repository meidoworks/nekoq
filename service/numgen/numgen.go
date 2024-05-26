package numgen

import (
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/meidoworks/nekoq-component/http/chi"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/shared/idgen"
)

type ServiceNumGen struct {
	engines []*chi.ChiHttpApiServer

	nodeId    int16
	elementId int32
	area      string

	m sync.Map
}

func (s *ServiceNumGen) GenerateFor(key string, count int) ([]idgen.IdType, error) {
	gen := s.GetNumGen(key)
	return gen.NextN(count)
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

func NewServiceNumGen() (*ServiceNumGen, error) {
	ng := new(ServiceNumGen)
	ng.nodeId = config.Instance.NekoQ.NodeId
	ng.area = config.Instance.NekoQ.Area

	for idx := range config.Instance.Services.NumGen.ServerRealAddress {
		ng.engines = append(ng.engines, chi.NewChiHttpApiServer(&chi.ChiHttpApiServerConfig{
			Addr: fmt.Sprint(config.Instance.Services.NumGen.ServerRealAddress[idx], ":", config.Instance.Services.NumGen.Port),
		}))
	}

	return ng, nil
}

func (s *ServiceNumGen) StartService() error {
	// register inproc service
	inproc.NumGenInst = s

	// register http api
	for _, h := range s.engines {
		if err := h.AddHttpApi(chiGenNum{s: s}); err != nil {
			return err
		}
	}

	// start http service
	for idx := range s.engines {
		h := s.engines[idx]
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			if err := h.StartServing(); err != nil {
				panic(err)
			}
		})
	}

	return nil
}
