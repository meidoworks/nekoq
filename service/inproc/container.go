package inproc

import (
	"errors"

	"github.com/meidoworks/nekoq/shared/idgen"
)

var ErrNoAreaFound = errors.New("no area found")
var ErrAreaExists = errors.New("area exists")
var ErrBadAreaData = errors.New("bad area data")
var ErrAreaFormatInvalid = errors.New("invalid area format")

type Warehouse interface {
	AreaLevel(area string) ([]string, error)
	PutArea(parentArea, area string) error
}

var WarehouseInst Warehouse

type NumGen interface {
	GenerateFor(key string, count int) ([]idgen.IdType, error)
}

var NumGenInst NumGen
