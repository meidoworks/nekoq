package cellar

import (
	"encoding/json"
	"errors"
	"log"
	"os"
	"time"

	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/shared/logging"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
	"gorm.io/plugin/dbresolver"
)

var _cellarLogger = logging.NewLogger("Cellar")

var _ CellarAPI = new(Cellar)

type CellarAPI interface {
	PutData(data CellarData) error
	GetData(area, dataKey string) (CellarData, error)

	GetWatchService() *CellarWatchStore
	GetAreaLevelService() *AreaLevelService
}

type Cellar struct {
	cfg *config.NekoConfig
	db  *gorm.DB

	levelService *AreaLevelService
	watchStore   *CellarWatchStore
}

func (c *Cellar) GetWatchService() *CellarWatchStore {
	return c.watchStore
}

func (c *Cellar) GetAreaLevelService() *AreaLevelService {
	return c.levelService
}

func NewCellar(cfg *config.NekoConfig) (*Cellar, error) {
	c := &Cellar{}
	switch cfg.Naming.Discovery.CellarStorageType {
	case "postgres":
		newLogger := logger.New(
			log.New(os.Stdout, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             2000 * time.Millisecond, // Slow SQL threshold
				LogLevel:                  logger.Info,             // Log level
				IgnoreRecordNotFoundError: true,                    // Ignore ErrRecordNotFound error for logger
				Colorful:                  true,                    // Disable color
			},
		)
		firstSource := cfg.Naming.Discovery.CellarStorageConfig.Sources[0]
		if db, err := gorm.Open(postgres.Open(firstSource), &gorm.Config{
			Logger: newLogger,
		}); err != nil {
			return nil, err
		} else {
			c.db = db
			var sources []gorm.Dialector
			for _, v := range cfg.Naming.Discovery.CellarStorageConfig.Sources {
				sources = append(sources, postgres.Open(v))
			}
			var replicas []gorm.Dialector
			for _, v := range cfg.Naming.Discovery.CellarStorageConfig.Replicas {
				replicas = append(replicas, postgres.Open(v))
			}
			if err := c.db.Use(dbresolver.Register(dbresolver.Config{
				Sources:           sources,
				Replicas:          replicas,
				Policy:            dbresolver.RandomPolicy{},
				TraceResolverMode: true,
			})); err != nil {
				return nil, err
			}
		}
	default:
		panic(errors.New("unsupported cellar storage type"))
	}

	store := newCellarWatchStore(c)
	// waiting for storage startup(including loading full data set) since other services will depend on the data
	if err := store.startWatchStore(); err != nil {
		return nil, err
	}
	c.watchStore = store

	// watch and update area levels
	c.levelService = newAreaLevelService()
	initializerCh := make(chan struct{}, 1)
	go c.watchAreaLevelsTask(store, initializerCh)
	<-initializerCh
	_cellarLogger.Infof("Celler initialized!")

	return c, nil
}

type areaEntry map[string]struct {
	Parent    string
	Attribute map[string]string
}

func (c *Cellar) watchAreaLevelsTask(store *CellarWatchStore, initCh chan struct{}) {
	areaVersion := 0
	for {
		watchId := store.NextWatcherSequence()
		ch := make(chan []*CellarData, 1)
		data, err := store.RetrieveAndWatch(watchId, []WatchKey{{
			Area:    "top",
			DataKey: "nekoq.area_levels",
			Version: areaVersion,
		}}, ch)
		if err != nil {
			_cellarLogger.Errorf("RetrieveAndWatch AreaLevels failed: %s", err)
			time.Sleep(1 * time.Second) // wait 1 second and continue
			continue
		}
		if len(data) <= 0 || data[0] == nil {
			_cellarLogger.Errorf("RetrieveAndWatch get empty result.")
			time.Sleep(1 * time.Second) // wait 1 second and continue
			continue
		}
		areaData := data[0].DataContent

		areaMap := areaEntry{}
		if err := json.Unmarshal(areaData, &areaMap); err != nil {
			_cellarLogger.Errorf("RetrieveAndWatch unmarshal result failed:%s", err)
			time.Sleep(1 * time.Second) // wait 1 second and continue
			continue
		}
		if err := c.levelService.refreshAreaLevels(areaMap); err != nil {
			_cellarLogger.Errorf("RetrieveAndWatch refreshAreaLevels:%s", err)
			time.Sleep(1 * time.Second) // wait 1 second and continue
			continue
		}

		areaVersion = data[0].DataVersion
		_cellarLogger.Infof("successfully processed new area list.")
		select {
		case initCh <- struct{}{}:
		default:
		}

		_ = <-ch // wait update but won't process the data since next round will be trigger for new data
	}
}
