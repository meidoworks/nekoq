package discovery

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/meidoworks/nekoq-component/component/comphttp"
	"github.com/meidoworks/nekoq-component/http/chi"

	"github.com/golang/snappy"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/shared/logging"
)

var (
	_externalHttpServiceLogger = logging.NewLogger("ExternalHttpService")
)

type ExternalHttpService struct {
	engine []*chi.ChiHttpApiServer

	dataStore         *DataStore
	nodeStatusManager *NodeStatusManager
}

func NewHttpService(ds *DataStore) (*ExternalHttpService, error) {
	nodeStatusManager := NewNodeStatusManager()

	var chis []*chi.ChiHttpApiServer
	for _, v := range config.Instance.Services.Naming.ServerRealAddress {
		addr := fmt.Sprint(v, ":", config.Instance.Services.Naming.Discovery.Port)
		chiSrv := chi.NewChiHttpApiServer(&chi.ChiHttpApiServerConfig{
			Addr: addr,
		})
		if err := chiSrv.AddHttpApi(chiSelfIpHandler{}); err != nil {
			return nil, err
		}
		registerHandler(chiSrv, ds, nodeStatusManager)
	}

	return &ExternalHttpService{
		engine:            chis,
		dataStore:         ds,
		nodeStatusManager: nodeStatusManager,
	}, nil
}

func (e *ExternalHttpService) StartService() error {
	// exposed service
	for idx := range e.engine {
		api.GetGlobalShutdownHook().AddBlockingTask(func() {
			if err := e.engine[idx].StartServing(); err != nil {
				panic(err)
			}
		})
	}
	return nil
}

type ServiceInfo struct {
	// Host 4b+2b/ipv4, 16b+2b/ipv6
	HostAndPort     []byte `json:"host_and_port"`
	IPv6HostAndPort []byte `json:"ipv6_host_and_port"`
}

func (s *ServiceInfo) Bind(r *http.Request) error {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	return json.Unmarshal(data, s)
}

type chiSelfIpHandler struct {
}

func (c chiSelfIpHandler) ParentUrl() string {
	return ""
}

func (c chiSelfIpHandler) Url() string {
	return "/utility/self_ip"
}

func (c chiSelfIpHandler) HttpMethod() []string {
	return []string{"GET"}
}

func (c chiSelfIpHandler) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	return chi.RenderOKString(r.RemoteAddr), nil
}

type chiPeerFull struct {
	localPeerService PeerService
}

func (c chiPeerFull) ParentUrl() string {
	return ""
}

func (c chiPeerFull) Url() string {
	return "/peer/full"
}

func (c chiPeerFull) HttpMethod() []string {
	return []string{"GET"}
}

func (c chiPeerFull) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	fetchStart := time.Now()
	set, err := c.localPeerService.FullFetch()
	if err != nil {
		return chi.RenderError(err), nil
	}
	fetchEnd := time.Now()

	start := time.Now()
	rl := set.TotalRecordCount()
	data, err := set.MarshalCbor()
	if err != nil {
		return chi.RenderError(err), nil
	}
	compressed := snappy.Encode(nil, data)
	end := time.Now()
	_externalHttpServiceLogger.Infof("PeerFull fetch time cost:[%d]", fetchEnd.Sub(fetchStart).Milliseconds())
	_externalHttpServiceLogger.Infof("PeerFull data:[%d], time cost:[%d], compressed size:[%d], total records:[%d]",
		len(data), (end.Sub(start)).Milliseconds(), len(compressed), rl)
	return chi.RenderBinary(http.StatusOK, compressed), nil
}

type chiPeerIncremental struct {
	localPeerService PeerService
}

func (c chiPeerIncremental) ParentUrl() string {
	return ""
}

func (c chiPeerIncremental) Url() string {
	return "/peer/incremental/{version}"
}

func (c chiPeerIncremental) HttpMethod() []string {
	return []string{"GET"}
}

func (c chiPeerIncremental) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	version := chi.GetUrlParam(r, "version")
	if len(version) <= 0 {
		return chi.RenderString(http.StatusBadRequest, "version is empty"), nil
	}
	incSet, err := c.localPeerService.IncrementalFetch(version)
	if err != nil {
		return chi.RenderError(err), nil
	}
	return chi.RenderJson(http.StatusOK, incSet), nil
}

type chiServiceAdd struct {
	manager          *NodeStatusManager
	localNodeService NodeService
}

func (c chiServiceAdd) ParentUrl() string {
	return ""
}

func (c chiServiceAdd) Url() string {
	return "/node/{node_id}/{service}/{area}"
}

func (c chiServiceAdd) HttpMethod() []string {
	return []string{"PUT"}
}

func (c chiServiceAdd) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	nodeId := chi.GetUrlParam(r, "node_id")
	service := chi.GetUrlParam(r, "service")
	area := chi.GetUrlParam(r, "area")
	if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
		return chi.RenderString(http.StatusBadRequest, "parameter invalid"), nil
	}
	if !validateServiceName(service) {
		return chi.RenderString(http.StatusBadRequest, "service name invalid"), nil
	}
	if !validateAreaName(area) {
		return chi.RenderString(http.StatusBadRequest, "area invalid"), nil
	}
	if !validateNodeId(nodeId) {
		return chi.RenderString(http.StatusBadRequest, "node id invalid"), nil
	}

	serviceInfo := new(ServiceInfo)
	if err := chi.BindJson(r, serviceInfo); err != nil {
		return chi.RenderString(http.StatusBadRequest, "service info invalid"), nil
	}
	data, err := json.Marshal(serviceInfo)
	if err != nil {
		_externalHttpServiceLogger.Errorf("marshal service info error:%s", err)
		return chi.RenderString(http.StatusInternalServerError, "service info error"), nil
	}

	recordKey := &RecordKey{
		Service: service,
		Area:    area,
		NodeId:  nodeId,
	}
	// start node lifecycle
	if err := c.manager.StartNode(recordKey); err != nil {
		return chi.RenderError(err), nil
	}

	// register node
	record := &Record{
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
	if err := c.localNodeService.SelfKeepAlive(rk, record); err != nil {
		return chi.RenderError(err), nil
	}

	return chi.RenderStatus(http.StatusOK), nil
}

type chiServiceKeepAlive struct {
	manager *NodeStatusManager
}

func (c chiServiceKeepAlive) ParentUrl() string {
	return ""
}

func (c chiServiceKeepAlive) Url() string {
	return "/node/{node_id}/{service}/{area}"
}

func (c chiServiceKeepAlive) HttpMethod() []string {
	return []string{http.MethodHead}
}

func (c chiServiceKeepAlive) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	nodeId := chi.GetUrlParam(r, "node_id")
	service := chi.GetUrlParam(r, "service")
	area := chi.GetUrlParam(r, "area")
	if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
		return chi.RenderString(http.StatusBadRequest, "parameter invalid"), nil
	}
	if !validateServiceName(service) {
		return chi.RenderString(http.StatusBadRequest, "service name invalid"), nil
	}
	if !validateAreaName(area) {
		return chi.RenderString(http.StatusBadRequest, "area invalid"), nil
	}
	if !validateNodeId(nodeId) {
		return chi.RenderString(http.StatusBadRequest, "node id invalid"), nil
	}

	recordKey := &RecordKey{
		Service: service,
		Area:    area,
		NodeId:  nodeId,
	}
	if err := c.manager.KeepAlive(recordKey); err != nil {
		return chi.RenderError(err), nil
	}

	return chi.RenderStatus(http.StatusOK), nil
}

type chiServiceRemove struct {
	manager *NodeStatusManager
}

func (c chiServiceRemove) ParentUrl() string {
	return ""
}

func (c chiServiceRemove) Url() string {
	return "/node/{node_id}/{service}/{area}"
}

func (c chiServiceRemove) HttpMethod() []string {
	return []string{http.MethodDelete}
}

func (c chiServiceRemove) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	nodeId := chi.GetUrlParam(r, "node_id")
	service := chi.GetUrlParam(r, "service")
	area := chi.GetUrlParam(r, "area")
	if len(nodeId) <= 0 || len(service) <= 0 || len(area) <= 0 {
		return chi.RenderString(http.StatusBadRequest, "parameter invalid"), nil
	}
	if !validateServiceName(service) {
		return chi.RenderString(http.StatusBadRequest, "service name invalid"), nil
	}
	if !validateAreaName(area) {
		return chi.RenderString(http.StatusBadRequest, "area invalid"), nil
	}
	if !validateNodeId(nodeId) {
		return chi.RenderString(http.StatusBadRequest, "node id invalid"), nil
	}

	recordKey := &RecordKey{
		Service: service,
		Area:    area,
		NodeId:  nodeId,
	}
	if err := c.manager.Offline(recordKey); err != nil {
		return chi.RenderError(err), nil
	}

	return chi.RenderStatus(http.StatusOK), nil
}

type chiQueryService struct {
	localNodeService NodeService
}

func (c chiQueryService) ParentUrl() string {
	return ""
}

func (c chiQueryService) Url() string {
	return "/service/{service}/{area}"
}

func (c chiQueryService) HttpMethod() []string {
	return []string{http.MethodGet}
}

func (c chiQueryService) Handle(r *http.Request) (comphttp.ResponseHandler[http.ResponseWriter], error) {
	service := chi.GetUrlParam(r, "service")
	area := chi.GetUrlParam(r, "area")
	if len(service) <= 0 || len(area) <= 0 {
		return chi.RenderString(http.StatusBadRequest, "parameter invalid"), nil
	}
	if !validateServiceName(service) {
		return chi.RenderString(http.StatusBadRequest, "service name invalid"), nil
	}
	if !validateAreaName(area) {
		return chi.RenderString(http.StatusBadRequest, "area invalid"), nil
	}

	if r.URL.Query().Get("children") == "1" {
		//TODO support query child area
	}

	rs, err := c.localNodeService.Fetch(service, area)
	if err != nil {
		return chi.RenderError(err), nil
	}
	return chi.RenderJson(http.StatusOK, rs), nil
}

func registerHandler(server *chi.ChiHttpApiServer, ds *DataStore, manager *NodeStatusManager) {
	localPeerService := NewLocalPeerService(ds)
	localNodeService := NewLocalNodeService(ds)
	manager.SetBatchFinalizer(localNodeService.OfflineN)

	if err := server.AddHttpApi(chiPeerFull{localPeerService: localPeerService}); err != nil {
		panic(err)
	}
	if err := server.AddHttpApi(chiPeerIncremental{localPeerService: localPeerService}); err != nil {
		panic(err)
	}
	if err := server.AddHttpApi(chiServiceAdd{
		manager:          manager,
		localNodeService: localNodeService,
	}); err != nil {
		panic(err)
	}
	if err := server.AddHttpApi(chiServiceKeepAlive{
		manager: manager,
	}); err != nil {
		panic(err)
	}
	if err := server.AddHttpApi(chiServiceRemove{manager: manager}); err != nil {
		panic(err)
	}
	if err := server.AddHttpApi(chiQueryService{localNodeService: localNodeService}); err != nil {
		panic(err)
	}

}
