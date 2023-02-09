package discovery

import (
	"encoding/json"
	"net/http"

	"github.com/meidoworks/nekoq/shared/thirdpartyshared/ginshared"

	"github.com/gin-gonic/gin"
)

type ExternalHttpService struct {
	engine *gin.Engine
	addr   string

	dataStore         *DataStore
	nodeStatusManager *NodeStatusManager
}

func NewHttpService(addr string, ds *DataStore) *ExternalHttpService {
	engine := gin.New()
	engine.Use(gin.Recovery())
	engine.Use(func(context *gin.Context) {
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

	engine.GET("/utility/self_ip", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		return ginshared.RenderOKString(ctx.ClientIP())
	}))

	nodeStatusManager := NewNodeStatusManager()

	registerHandler(engine, ds, nodeStatusManager)

	return &ExternalHttpService{
		engine:            engine,
		addr:              addr,
		dataStore:         ds,
		nodeStatusManager: nodeStatusManager,
	}
}

func (e *ExternalHttpService) StartService() error {
	return e.engine.Run(e.addr)
}

type ServiceInfo struct {
	Host string `json:"host"`
	Port uint16 `json:"port"`
}

func registerHandler(engine *gin.Engine, ds *DataStore, manager *NodeStatusManager) {
	localPeerService := NewLocalPeerService(ds)
	localNodeService := NewLocalNodeService(ds)
	manager.SetFinalizer(localNodeService.Offline)

	engine.GET("/peer/full", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		set, err := localPeerService.FullFetch()
		if err != nil {
			return ginshared.RenderError(err)
		}
		return ginshared.RenderJson(http.StatusOK, set)
	}))
	engine.GET("/peer/incremental/:version", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		version := ctx.Param("version")
		if len(version) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "version is empty")
		}
		incSet, err := localPeerService.IncrementalFetch(version)
		if err != nil {
			return ginshared.RenderError(err)
		}
		return ginshared.RenderJson(http.StatusOK, incSet)
	}))

	engine.PUT("/node/:node_id/:service/:area", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		nodeId := ctx.Param("node_id")
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}

		serviceInfo := new(ServiceInfo)
		if err := ctx.ShouldBind(serviceInfo); err != nil {
			return ginshared.RenderString(http.StatusBadRequest, "service info invalid")
		}

		recordKey := &RecordKey{
			Service: service,
			Area:    area,
			NodeId:  nodeId,
		}
		// start node lifecycle
		if err := manager.StartNode(recordKey); err != nil {
			return ginshared.RenderError(err)
		}

		// register node
		data, _ := json.Marshal(serviceInfo)
		r := &Record{
			Service:       service,
			Area:          area,
			NodeId:        nodeId,
			RecordVersion: 0,
			Tags:          nil,
			ServiceData:   data,
			MetaData:      nil,
		}
		if err := localNodeService.SelfKeepAlive(r); err != nil {
			return ginshared.RenderError(err)
		}

		return ginshared.RenderStatus(http.StatusOK)
	}))
	engine.HEAD("/node/:node_id/:service/:area", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		nodeId := ctx.Param("node_id")
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}

		recordKey := &RecordKey{
			Service: service,
			Area:    area,
			NodeId:  nodeId,
		}
		if err := manager.KeepAlive(recordKey); err != nil {
			return ginshared.RenderError(err)
		}

		return ginshared.RenderStatus(http.StatusOK)
	}))
	engine.DELETE("/node/:node_id/:service/:area", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		nodeId := ctx.Param("node_id")
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}

		recordKey := &RecordKey{
			Service: service,
			Area:    area,
			NodeId:  nodeId,
		}
		if err := manager.Offline(recordKey); err != nil {
			return ginshared.RenderError(err)
		}

		return ginshared.RenderStatus(http.StatusOK)
	}))

	engine.GET("/service/:service/:area", ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}

		rs, err := localNodeService.Fetch(service, area)
		if err != nil {
			return ginshared.RenderError(err)
		}
		return ginshared.RenderJson(http.StatusOK, rs)
	}))
}
