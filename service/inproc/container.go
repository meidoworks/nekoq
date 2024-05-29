package inproc

import (
	"errors"

	"github.com/meidoworks/nekoq/api"
	"github.com/meidoworks/nekoq/shared/idgen"
)

var ErrNoAreaFound = errors.New("no area found")
var ErrAreaExists = errors.New("area exists")
var ErrBadAreaData = errors.New("bad area data")
var ErrAreaFormatInvalid = errors.New("invalid area format")

type Warehouse interface {
	AreaLevel(area string) ([]string, error)
	PutArea(parentArea, area string) error

	Get(key string) ([]byte, error)
	Set(key string, value []byte) error
	SetIfNotExists(key string, value []byte) error
	Del(key string) error

	WatchFolder(folder string) (<-chan api.WatchEvent, func(), error)

	Leader(key string) (string, error)
	AcquireLeader(key string, node string) (string, error)
}

var WarehouseInst Warehouse

type NumGen interface {
	GenerateFor(key string, count int) ([]idgen.IdType, error)
}

var NumGenInst NumGen
