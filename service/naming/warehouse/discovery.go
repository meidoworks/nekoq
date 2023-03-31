package warehouse

import (
	"sync"

	"github.com/meidoworks/nekoq/service/naming/warehouseapi"
)

func init() {
	//FIXME inject real persistent area manager before anyone use it
	warehouseapi.WarehouseDiscoveryApi(newLocalMemory())
}

type AreaLevel struct {
	area string `json:"area"`

	attr map[string][]byte

	parent string // parent area
	state  string // ok/frozen
}

func (a *AreaLevel) Area() string {
	return a.area
}

func (a *AreaLevel) available() bool {
	return a.state == "ok"
}

type localMemory struct {
	sync.RWMutex

	areaMap map[string]*AreaLevel
}

func newLocalMemory() *localMemory {
	return (&localMemory{
		areaMap: map[string]*AreaLevel{},
	}).init()
}

func (l *localMemory) init() *localMemory {
	l.areaMap[warehouseapi.RootArea] = &AreaLevel{
		area:   warehouseapi.RootArea,
		attr:   map[string][]byte{},
		parent: warehouseapi.RootArea,
		state:  "ok",
	}
	return l
}

func (l *localMemory) AreaLevels(area string) ([]warehouseapi.Area, error) {
	r := make([]warehouseapi.Area, 0, 8)

	l.RLock()
	defer l.RUnlock()
	for {
		if v, ok := l.areaMap[area]; !ok {
			return nil, warehouseapi.ErrAreaNotFound
		} else {
			r = append(r, v)
			area = v.parent
			if v.area == warehouseapi.RootArea {
				// reach root, exit
				break
			}
		}
	}

	return append(r), nil
}

func (l *localMemory) AreaAttrRecursively(area, attr string) ([]byte, error) {
	l.RLock()
	defer l.RUnlock()

	for {
		if v, ok := l.areaMap[area]; !ok {
			return nil, warehouseapi.ErrAreaNotFound
		} else if a, ok := v.attr[attr]; !ok {
			area = v.parent
			if v.area == warehouseapi.RootArea {
				// reach root, exit
				break
			}
		} else {
			return a, nil
		}
	}

	return nil, warehouseapi.ErrAreaAttrNotFound
}

func (l *localMemory) AreaAttr(area, attr string) ([]byte, error) {
	l.RLock()
	defer l.RUnlock()

	if v, ok := l.areaMap[area]; !ok {
		return nil, warehouseapi.ErrAreaNotFound
	} else if a, ok := v.attr[attr]; !ok {
		return nil, warehouseapi.ErrAreaAttrNotFound
	} else {
		return a, nil
	}
}

func (l *localMemory) PutArea(area, parent string) error {
	if area == parent {
		return warehouseapi.ErrAreaSelfReference
	}

	l.Lock()
	defer l.Unlock()

	if _, ok := l.areaMap[area]; ok {
		return warehouseapi.ErrAreaExists
	}
	if v, ok := l.areaMap[parent]; ok {
		if !v.available() {
			return warehouseapi.ErrParentAreaIllegal
		}
		l.areaMap[area] = &AreaLevel{
			area:   area,
			attr:   map[string][]byte{},
			parent: v.area,
			state:  "ok",
		}
		return nil
	} else {
		return warehouseapi.ErrAreaNotFound
	}
}

func (l *localMemory) PutAreaAttr(area, attr string, data []byte) ([]byte, error) {
	l.Lock()
	defer l.Unlock()

	if v, ok := l.areaMap[area]; ok {
		if !v.available() {
			return nil, warehouseapi.ErrAreaHasFrozen
		}
		val := v.attr[attr]
		v.attr[attr] = data
		return val, nil
	} else {
		return nil, warehouseapi.ErrAreaNotFound
	}
}

func (l *localMemory) FreezeArea(area string) error {
	l.Lock()
	defer l.Unlock()

	if v, ok := l.areaMap[area]; ok {
		v.state = "frozen"
		return nil
	} else {
		return warehouseapi.ErrAreaNotFound
	}
}

func (l *localMemory) UnfreezeArea(area string) error {
	l.Lock()
	defer l.Unlock()

	if v, ok := l.areaMap[area]; ok {
		v.state = "ok"
		return nil
	} else {
		return warehouseapi.ErrAreaNotFound
	}
}

func (l *localMemory) DeleteFrozenArea(area string) error {
	l.Lock()
	defer l.Unlock()

	if v, ok := l.areaMap[area]; ok {
		if v.available() {
			return warehouseapi.ErrAreaNotFrozen
		}
		for _, a := range l.areaMap {
			if a.parent == area {
				return warehouseapi.ErrAreaInUse
			}
		}
		delete(l.areaMap, area)
		return nil
	} else {
		return warehouseapi.ErrAreaNotFound
	}
}
