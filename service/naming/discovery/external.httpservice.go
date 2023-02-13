package discovery

import (
	"net/http"
	"time"

	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc/warehouseapi"
	"github.com/meidoworks/nekoq/shared/logging"
	"github.com/meidoworks/nekoq/shared/thirdpartyshared/ginshared"

	"github.com/gin-gonic/gin"
	"github.com/golang/snappy"
)

var (
	_externalHttpServiceLogger = logging.NewLogger("ExternalHttpService")
)

type ExternalHttpService struct {
	engine *gin.Engine
	addr   string

	dataStore         *DataStore
	nodeStatusManager *NodeStatusManager
}

func NewHttpService(cfg *config.NekoConfig, ds *DataStore) (*ExternalHttpService, error) {
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

	areaManager := warehouseapi.WarehouseDiscoveryApi(nil)
	if !cfg.Naming.Discovery.DisableDefaultArea {
		if err := areaManager.PutArea("default", warehouseapi.RootArea); err != nil {
			return nil, err
		}
	}
	registerHandler(engine, ds, nodeStatusManager, areaManager)

	return &ExternalHttpService{
		engine:            engine,
		addr:              cfg.Naming.Discovery.Listen,
		dataStore:         ds,
		nodeStatusManager: nodeStatusManager,
	}, nil
}

func (e *ExternalHttpService) StartService() error {
	return e.engine.Run(e.addr)
}

type ServiceInfo struct {
	// Host 4b+2b/ipv4, 16b+2b/ipv6
	HostAndPort []byte `json:"host_and_port"`
}

func registerHandler(engine *gin.Engine, ds *DataStore, manager *NodeStatusManager, areaManager warehouseapi.DiscoveryUse) {
	localPeerService := NewLocalPeerService(ds)
	localNodeService := NewLocalNodeService(ds, areaManager)
	manager.SetBatchFinalizer(localNodeService.OfflineN)

	peerFull := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		set, err := localPeerService.FullFetch()
		if err != nil {
			return ginshared.RenderError(err)
		}

		start := time.Now()
		rl := len(set.Records)
		data, err := set.MarshalCbor()
		if err != nil {
			return ginshared.RenderError(err)
		}
		compressed := snappy.Encode(nil, data)
		end := time.Now()
		_externalHttpServiceLogger.Infof("PeerFull data:[%d], time cost:[%d], compressed size:[%d], total records:[%d]",
			len(data), (end.Sub(start)).Milliseconds(), len(compressed), rl)
		return ginshared.RenderBinary(http.StatusOK, compressed)
	})
	peerIncremental := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		version := ctx.Param("version")
		if len(version) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "version is empty")
		}
		incSet, err := localPeerService.IncrementalFetch(version)
		if err != nil {
			return ginshared.RenderError(err)
		}
		return ginshared.RenderJson(http.StatusOK, incSet)
	})
	serviceAdd := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		nodeId := ctx.Param("node_id")
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}
		if !validateServiceName(service) {
			return ginshared.RenderString(http.StatusBadRequest, "service name invalid")
		}
		if !validateAreaName(area) {
			return ginshared.RenderString(http.StatusBadRequest, "area invalid")
		}
		if !validateNodeId(nodeId) {
			return ginshared.RenderString(http.StatusBadRequest, "node id invalid")
		}

		serviceInfo := new(ServiceInfo)
		if err := ctx.ShouldBindJSON(serviceInfo); err != nil {
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
		r := &Record{
			Service:       service,
			Area:          area,
			NodeId:        nodeId,
			RecordVersion: 0,
			Tags:          nil,
			ServiceData:   serviceInfo.HostAndPort,
			MetaData:      nil,
		}
		if err := localNodeService.SelfKeepAlive(r); err != nil {
			return ginshared.RenderError(err)
		}

		return ginshared.RenderStatus(http.StatusOK)
	})
	serviceKeepAlive := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		nodeId := ctx.Param("node_id")
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}
		if !validateServiceName(service) {
			return ginshared.RenderString(http.StatusBadRequest, "service name invalid")
		}
		if !validateAreaName(area) {
			return ginshared.RenderString(http.StatusBadRequest, "area invalid")
		}
		if !validateNodeId(nodeId) {
			return ginshared.RenderString(http.StatusBadRequest, "node id invalid")
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
	})
	serviceRemove := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		nodeId := ctx.Param("node_id")
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}
		if !validateServiceName(service) {
			return ginshared.RenderString(http.StatusBadRequest, "service name invalid")
		}
		if !validateAreaName(area) {
			return ginshared.RenderString(http.StatusBadRequest, "area invalid")
		}
		if !validateNodeId(nodeId) {
			return ginshared.RenderString(http.StatusBadRequest, "node id invalid")
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
	})
	queryService := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		service := ctx.Param("service")
		area := ctx.Param("area")
		if len(service) <= 0 || len(area) <= 0 {
			return ginshared.RenderString(http.StatusBadRequest, "parameter invalid")
		}
		if !validateServiceName(service) {
			return ginshared.RenderString(http.StatusBadRequest, "service name invalid")
		}
		if !validateAreaName(area) {
			return ginshared.RenderString(http.StatusBadRequest, "area invalid")
		}

		rs, err := localNodeService.Fetch(service, area)
		if err != nil {
			return ginshared.RenderError(err)
		}
		return ginshared.RenderJson(http.StatusOK, rs)
	})

	engine.GET("/peer/full", peerFull)
	engine.GET("/peer/incremental/:version", peerIncremental)

	engine.PUT("/node/:node_id/:service/:area", serviceAdd)
	engine.HEAD("/node/:node_id/:service/:area", serviceKeepAlive)
	engine.DELETE("/node/:node_id/:service/:area", serviceRemove)

	engine.GET("/service/:service/:area", queryService)
}
