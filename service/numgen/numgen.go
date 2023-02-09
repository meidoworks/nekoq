package numgen

import (
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/shared/idgen"
	"github.com/meidoworks/nekoq/shared/thirdpartyshared/ginshared"

	"github.com/gin-gonic/gin"
)

type ServiceNumGen struct {
	engine *gin.Engine
	addr   string

	nodeId    int16
	elementId int32

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

func NewServiceNumGen(nodeId int16, addr string) (*ServiceNumGen, error) {
	ng := new(ServiceNumGen)
	ng.nodeId = nodeId

	ng.engine = gin.New()
	ng.addr = addr

	// Fill in inproc NumGenSpawn
	ng = inproc.NumGenSpawn(ng).(*ServiceNumGen)

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

	return s.engine.Run(s.addr)
}