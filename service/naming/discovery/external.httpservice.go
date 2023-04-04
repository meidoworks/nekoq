package discovery

import (
	"encoding/json"
	"net"
	"net/http"
	"time"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/service/naming/cellar"
	"github.com/meidoworks/nekoq/shared/logging"
	"github.com/meidoworks/nekoq/shared/netaddons/localswitch"
	"github.com/meidoworks/nekoq/shared/netaddons/multiplexer"
	"github.com/meidoworks/nekoq/shared/thirdpartyshared/ginshared"

	"github.com/gin-gonic/gin"
	"github.com/golang/snappy"
)

var (
	_externalHttpServiceLogger = logging.NewLogger("ExternalHttpService")
)

type ExternalHttpService struct {
	engine *gin.Engine
	cfg    config.NamingConfig

	dataStore         *DataStore
	nodeStatusManager *NodeStatusManager

	cellarApi cellar.CellarAPI
}

func NewHttpService(cfg *config.NekoConfig, ds *DataStore, cellarApi cellar.CellarAPI) (*ExternalHttpService, error) {
	engine := gin.New()
	engine.Use(gin.Logger())
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

	registerHandler(engine, ds, nodeStatusManager, cellarApi.GetAreaLevelService())

	return &ExternalHttpService{
		engine:            engine,
		cfg:               cfg.Naming,
		dataStore:         ds,
		nodeStatusManager: nodeStatusManager,

		cellarApi: cellarApi,
	}, nil
}

func (e *ExternalHttpService) StartService() error {
	// exposed service
	if !e.cfg.Discovery.Disable {
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			if err := e.engine.Run(e.cfg.Discovery.Listen); err != nil {
				panic(err)
			}
		})
	}
	// inproc service
	api.GetGlobalShutdownHook().AddBlockingTask(func() {
		lswitch := inproc.GetLocalSwitch()
		listener := localswitch.NewLocalSwitchNetListener()
		lswitch.AddTrafficConsumer(api.LocalSwitchDiscovery, func(conn net.Conn, meta multiplexer.TrafficMeta) error {
			listener.PublishNetConn(conn)
			return nil
		})

		err := e.engine.RunListener(listener)
		if err != nil {
			panic(err)
		}
	})
	return nil
}

type ServiceInfo struct {
	// Host 4b+2b/ipv4, 16b+2b/ipv6
	HostAndPort     []byte `json:"host_and_port"`
	IPv6HostAndPort []byte `json:"ipv6_host_and_port"`
}

func registerHandler(engine *gin.Engine, ds *DataStore, manager *NodeStatusManager, areaLevelService *cellar.AreaLevelService) {
	localPeerService := NewLocalPeerService(ds)
	localNodeService := NewLocalNodeService(ds, areaLevelService)
	manager.SetBatchFinalizer(localNodeService.OfflineN)

	peerFull := ginshared.Wrap(func(ctx *gin.Context) ginshared.Render {
		fetchStart := time.Now()
		set, err := localPeerService.FullFetch()
		if err != nil {
			return ginshared.RenderError(err)
		}
		fetchEnd := time.Now()

		start := time.Now()
		rl := set.TotalRecordCount()
		data, err := set.MarshalCbor()
		if err != nil {
			return ginshared.RenderError(err)
		}
		compressed := snappy.Encode(nil, data)
		end := time.Now()
		_externalHttpServiceLogger.Infof("PeerFull fetch time cost:[%d]", fetchEnd.Sub(fetchStart).Milliseconds())
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
		data, err := json.Marshal(serviceInfo)
		if err != nil {
			_externalHttpServiceLogger.Errorf("marshal service info error:%s", err)
			return ginshared.RenderString(http.StatusInternalServerError, "service info error")
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
			NodeId:        nodeId,
			RecordVersion: 0,
			Tags:          nil,
			ServiceData:   data,
			MetaData:      nil,
		}
		rk := &RecordKey{
			Service: service,
			Area:    area,
			NodeId:  nodeId,
		}
		if err := localNodeService.SelfKeepAlive(rk, r); err != nil {
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
