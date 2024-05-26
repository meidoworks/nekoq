package warehouse

import (
	"encoding/json"
	"time"

	"github.com/meidoworks/nekoq-component/component/compdb"
	"github.com/meidoworks/nekoq-component/component/shared"

	"github.com/meidoworks/nekoq/rules"
	"github.com/meidoworks/nekoq/service/inproc"
)

const areaPath = "/_sys/nekoq.naming.area/"
const defaultAreaLevel = "default"

type AreaService struct {
	w *Warehouse

	watchCancelFn shared.CancelFn
	eventChannel  <-chan compdb.WatchEvent

	area     *area
	stripped map[string]*areaKey
}

func (a *AreaService) Startup() error {
	evCh, cancelFn, err := a.w.consistent.WatchFolder(areaPath)
	if err != nil {
		return err
	}
	a.watchCancelFn = cancelFn
	a.eventChannel = evCh

	if err := a.initAreaData(<-evCh); err != nil {
		return err
	}
	go a.areaDataUpdateLoop()

	return nil
}

func (a *AreaService) AreaLevel(area string) (result []string, err error) {
	stripped := a.stripped
	item, ok := stripped[area]
	if !ok {
		return nil, inproc.ErrNoAreaFound
	}
	for i := 0; i < 100; i++ { // set limitation to avoid infinite loop when data is corrupted
		result = append(result, item.Level)
		if item.Level == defaultAreaLevel {
			return
		}
		item, ok = stripped[item.Parent]
		if !ok {
			return nil, inproc.ErrBadAreaData
		}
	}
	return nil, inproc.ErrBadAreaData
}

func (a *AreaService) PutArea(parentArea, newArea string) error {
	if !rules.CheckAreaAllowedCharacters(parentArea) || !rules.CheckAreaAllowedCharacters(newArea) {
		return inproc.ErrAreaFormatInvalid
	}
	stripped := a.stripped
	if _, ok := stripped[newArea]; ok {
		return inproc.ErrAreaExists
	}
	if node, ok := stripped[parentArea]; !ok {
		return inproc.ErrNoAreaFound
	} else {
		m := node.areaNode.Children
		if m == nil {
			m = map[string]*area{}
			node.areaNode.Children = m
		}
		m[newArea] = &area{
			Level:    newArea,
			Desc:     "area:" + newArea,
			Children: nil,
		}
		data, err := a.area.Marshal()
		if err != nil {
			return err
		}
		if err := a.w.consistent.Set(areaPath, string(data)); err != nil {
			return err
		}
		return nil
	}
}

func (a *AreaService) initAreaData(event compdb.WatchEvent) error {
	// no data found
	if len(event.Ev) == 0 {
		d, err := defaultArea.Marshal()
		if err != nil {
			return err
		}
		if err := a.w.consistent.SetIfNotExists(areaPath, string(d)); err != nil {
			return err
		}
		return nil
	}
	// has data
	for _, v := range event.Ev {
		if v.Key != areaPath {
			continue
		}
		data, err := a.w.consistent.Get(areaPath)
		if err != nil {
			return err
		}
		if data == "" {
			d, err := defaultArea.Marshal()
			if err != nil {
				return err
			}
			data = string(d)
			if err := a.w.consistent.SetIfNotExists(areaPath, data); err != nil {
				return err
			}
		}
		if err := a.updateArea(data); err != nil {
			return err
		}
		break
	}
	return nil
}

func (a *AreaService) updateArea(data string) error {
	newArea := new(area)
	if err := newArea.Unmarshal([]byte(data)); err != nil {
		return err
	}
	ch := make(chan *area)
	go func() {
		ch <- newArea
	}()
	a.area = <-ch

	// strip area
	m := map[string]*areaKey{}
	a.walkArea(a.area, m, "") // empty parent for top level entry
	a.stripped = m

	return nil
}

func (a *AreaService) walkArea(areaItem *area, m map[string]*areaKey, parent string) {
	m[areaItem.Level] = &areaKey{Level: areaItem.Level, Parent: parent, areaNode: areaItem}
	if areaItem.Children == nil {
		return
	}
	for _, child := range areaItem.Children {
		a.walkArea(child, m, areaItem.Level)
	}
}

func (a *AreaService) areaDataUpdateLoop() {
	for {
		ev, ok := <-a.eventChannel
		if !ok {
			break
		}
		// retry loop
	RetryLoop:
		for i := 0; i < 100; i++ {
			for _, v := range ev.Ev {
				if v.Key != areaPath {
					continue
				}
				data, err := a.w.consistent.Get(areaPath)
				if err != nil {
					//FIXME log the error
					break
				}
				if err := a.updateArea(data); err != nil {
					//FIXME log the error
					break
				}
				break RetryLoop
			}
			time.Sleep(1 * time.Second) // wait 1s for next retry
		}
	}
}

type areaKey struct {
	Level    string
	Parent   string
	areaNode *area
}

type area struct {
	Level    string
	Desc     string
	Children map[string]*area
	Attr     map[string]string
}

func (a *area) Marshal() ([]byte, error) {
	return json.Marshal(a)
}

func (a *area) Unmarshal(data []byte) error {
	return json.Unmarshal(data, a)
}

var defaultArea = &area{
	Level: defaultAreaLevel,
	Desc:  "top area",
}
