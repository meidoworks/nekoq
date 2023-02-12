package warehouseapi

import (
	"errors"
)

var (
	ErrAreaNotFound      = errors.New("area not found")
	ErrAreaExists        = errors.New("area exists")
	ErrAreaAttrNotFound  = errors.New("area attribute not found")
	ErrAreaSelfReference = errors.New("area parent field self reference")
	ErrAreaHasFrozen     = errors.New("area has frozen")
	ErrAreaNotFrozen     = errors.New("area not frozen")
	ErrParentAreaIllegal = errors.New("parent area illegal")
	ErrAreaInUse         = errors.New("area is in use")
)

const (
	RootArea = ""
)

type Area interface {
	Area() string
}

// DiscoveryUse is used for discovery to retrieve area hierarchy and metadata information
type DiscoveryUse interface {
	// AreaLevels returns level hierarchy from the given area
	// Response slice includes all areas up to the root(included) in child->parent order
	AreaLevels(area string) ([]Area, error)
	AreaAttrRecursively(area, attr string) ([]byte, error)
	AreaAttr(area, attr string) ([]byte, error)

	PutArea(area, parent string) error
	PutAreaAttr(area, attr string, data []byte) ([]byte, error)
	// FreezeArea free the area
	// This will block any structure change on the area including creating child area, etc.
	// Service discovery is still available: register/fetch
	FreezeArea(area string) error
	UnfreezeArea(area string) error
	// DeleteFrozenArea delete froze area permanently
	// Must setup acl to avoid unexpected deletion
	DeleteFrozenArea(area string) error
}

var warehouseDiscovery DiscoveryUse

func WarehouseDiscoveryApi(wd DiscoveryUse) DiscoveryUse {
	if wd == nil {
		return warehouseDiscovery
	} else {
		warehouseDiscovery = wd
		return wd
	}
}
