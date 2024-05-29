package warehouse

import (
	"errors"
	"strings"

	"github.com/meidoworks/nekoq-component/component/compdb"

	"github.com/meidoworks/nekoq/api"
)

func (w *Warehouse) Get(key string) ([]byte, error) {
	s, err := w.consistent.Get(key)
	return []byte(s), err
}

func (w *Warehouse) Set(key string, value []byte) error {
	return w.consistent.Set(key, string(value))
}

func (w *Warehouse) SetIfNotExists(key string, value []byte) error {
	return w.consistent.SetIfNotExists(key, string(value))
}

func (w *Warehouse) Del(key string) error {
	return w.consistent.Del(key)
}

func (w *Warehouse) WatchFolder(folder string) (<-chan api.WatchEvent, func(), error) {
	if !strings.HasSuffix(folder, "/") {
		return nil, nil, errors.New("folder must end with /")
	}
	ch, cancelFn, err := w.consistent.WatchFolder(folder)
	if err != nil {
		return nil, nil, err
	}
	rch := make(chan api.WatchEvent, 64)
	go func() {
		for {
			item, ok := <-ch
			if !ok {
				close(rch)
				break
			}
			ev := api.WatchEvent{
				Path: item.Path,
				Ev: make([]struct {
					Key       string
					EventType api.WatchEventType
				}, len(item.Ev)),
			}
			for idx, v := range item.Ev {
				var et api.WatchEventType
				switch v.EventType {
				default:
					//FIXME alert on unknown mapping
					fallthrough
				case compdb.WatchEventUnknown:
					et = api.WatchEventUnknown
				case compdb.WatchEventFresh:
					et = api.WatchEventFresh
				case compdb.WatchEventCreated:
					et = api.WatchEventCreated
				case compdb.WatchEventModified:
					et = api.WatchEventModified
				case compdb.WatchEventDelete:
					et = api.WatchEventDelete
				}
				ev.Ev[idx] = struct {
					Key       string
					EventType api.WatchEventType
				}{Key: v.Key, EventType: et}
			}
			rch <- ev
		}
	}()
	return rch, cancelFn, nil
}
