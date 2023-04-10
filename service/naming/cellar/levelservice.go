package cellar

import (
	"errors"
	"sync"
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
	ErrHasDuplicatedArea = errors.New("has duplicated area")
)

type AreaKey struct {
	Area string
}

type Area struct {
	AreaKey
	parent     AreaKey
	Attributes map[string]string
}

type AreaLevelServiceApi interface {
	AreaLevels(area string) ([]AreaKey, error)
}

type AreaLevelService struct {
	areaMap map[string]Area

	internalBarrier sync.Mutex

	//TODO Attr/AttrRecursively/PutArea/PutAttr/Freeze/Unfreeze/Delete
}

func newAreaLevelService() *AreaLevelService {
	return &AreaLevelService{
		areaMap: map[string]Area{},
	}
}

func (l *AreaLevelService) refreshAreaLevels(data areaEntry) error {
	areaMap := make(map[string]Area)

	// build area map
	for k, v := range data {
		var attr map[string]string
		if v.Attribute == nil {
			attr = map[string]string{}
		} else {
			attr = v.Attribute
		}
		areaMap[k] = Area{
			AreaKey:    AreaKey{Area: k},
			parent:     AreaKey{Area: v.Parent},
			Attributes: attr,
		}
	}

	// validate area map
	{
		// validate parent area
		for _, v := range areaMap {
			if v.Area == "top" {
				continue
			}
			_, ok := areaMap[v.parent.Area]
			if !ok {
				return ErrParentAreaIllegal
			}
		}
	}

	l.internalBarrier.Lock()
	l.areaMap = areaMap
	l.internalBarrier.Unlock()

	return nil
}

func (l *AreaLevelService) AreaLevels(area string) ([]AreaKey, error) {
	m := l.areaMap
	var r []AreaKey
	for {
		areaEntry, ok := m[area]
		if !ok {
			return nil, ErrAreaNotFound
		}
		r = append(r, areaEntry.AreaKey)
		if areaEntry.Area == "top" {
			break
		} else {
			area = areaEntry.parent.Area
		}
	}
	return r, nil
}
