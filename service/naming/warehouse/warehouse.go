package warehouse

import (
	"fmt"

	"github.com/meidoworks/nekoq-component/component/compdb"
	"github.com/meidoworks/nekoq-component/db/consistent/etcd"
	"github.com/meidoworks/nekoq-component/http/chi"
	"github.com/sirupsen/logrus"

	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/inproc"
	"github.com/meidoworks/nekoq/shared/logging"
)

var warehouseLogger = logging.NewLogger("Naming.Warehouse")

func NewWarehouse() *Warehouse {
	return &Warehouse{}
}

type Warehouse struct {
	consistent     compdb.ConsistentStore
	httpServerList []*chi.ChiHttpApiServer

	areaService *AreaService
}

func (w *Warehouse) AreaLevel(area string) ([]string, error) {
	return w.areaService.AreaLevel(area)
}

func (w *Warehouse) PutArea(parentArea, area string) error {
	return w.areaService.PutArea(parentArea, area)
}

func (w *Warehouse) Startup() error {
	// register inproc service
	inproc.WarehouseInst = w

	// prepare consistent store: etcd
	client, err := etcd.NewEtcdClient(&etcd.EtcdClientConfig{
		Endpoints: config.Instance.Services.Naming.External.Etcd.Endpoints,
	})
	if err != nil {
		return err
	}
	w.consistent = client

	// start area service
	w.areaService = &AreaService{
		w: w,
	}
	if err := w.areaService.Startup(); err != nil {
		return err
	}

	// start http service
	w.startHttpServices()

	return nil
}

func (w *Warehouse) startHttpServices() {
	for idx := range config.Instance.Services.Naming.ServerRealAddress {
		go w.httpServiceTask(config.Instance.Services.Naming.ServerRealAddress[idx])
	}
}

func (w *Warehouse) httpServiceTask(addr string) {
	addr = fmt.Sprint(addr, ":", config.Instance.Services.Naming.Warehouse.Port)
	c := chi.NewChiHttpApiServer(&chi.ChiHttpApiServerConfig{
		Addr: addr,
	})

	if err := w.prepareHttpHandler(c); err != nil {
		panic(err)
	}
	w.httpServerList = append(w.httpServerList, c)
	warehouseLogger.Log(logrus.InfoLevel, "start http service on address:", addr)

	if err := c.StartServing(); err != nil {
		panic(err)
	}
}

func (w *Warehouse) prepareHttpHandler(c *chi.ChiHttpApiServer) error {
	for _, v := range httpApis {
		if err := c.AddHttpApi(v); err != nil {
			return err
		}
	}
	return nil
}
