package cellar

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/meidoworks/nekoq/shared/logging"
)

var _cellarWatchLogger = logging.NewLogger("CellarWatch")

type WatchChannel chan<- []*CellarData

type WatchKey struct {
	Area    string
	DataKey string
	Version int
}

func (w *WatchKey) UniqueKey() string {
	return w.Area + "/" + w.DataKey
}

func newCellarWatchStore(cellar *Cellar) *CellarWatchStore {
	return &CellarWatchStore{
		cellar:  cellar,
		storage: map[string]*CellarData{},
		watcher: map[string]map[int64]int{},
		watcherNotify: map[int64]struct {
			WatchChannel
			List []WatchKey
		}{},
		watcherSequence: 0,
	}
}

type CellarWatchStore struct {
	lastScannedId int

	cellar *Cellar

	lock          sync.Mutex
	storage       map[string]*CellarData // this may reduce the complexity of implementations but requires additional work for area based search
	watcher       map[string]map[int64]int
	watcherNotify map[int64]struct {
		WatchChannel
		List []WatchKey
	}

	watcherSequence int64
}

func (c *CellarWatchStore) startWatchStore() error {
	if err := c.LoadFull(); err != nil {
		return err
	}
	go c.scanUpdateLoop()
	return nil
}

func (c *CellarWatchStore) NextWatcherSequence() int64 {
	return atomic.AddInt64(&c.watcherSequence, 1)
}

// RetrieveAndWatch will respond the latest data immediately and start watching updates from the offset of the latest data
func (c *CellarWatchStore) RetrieveAndWatch(watcher int64, watchList []WatchKey, watchChannel WatchChannel) ([]*CellarData, error) {
	dataList := make([]*CellarData, 0, len(watchList))
	newWatchList := make([]WatchKey, 0, len(watchList))

	storage := c.storage
	dataWatcher := c.watcher
	// Get latest and watch changes
	{
		c.lock.Lock()
		defer c.lock.Unlock()
		for _, v := range watchList {
			data, ok := storage[v.UniqueKey()]
			if ok {
				newWatchList = append(newWatchList, WatchKey{
					Area:    data.Area,
					DataKey: data.DataKey,
					Version: data.DataVersion,
				})
				dataList = append(dataList, data)
			} else {
				dataList = append(dataList, nil)
			}
		}
		// add watch list
		for _, v := range dataList {
			if v != nil {
				watcherMap, ok := dataWatcher[v.UniqueKey()]
				if !ok {
					watcherMap = map[int64]int{}
					dataWatcher[v.UniqueKey()] = watcherMap
				}
				watcherMap[watcher] = v.DataVersion
			} else {
				// if watch a non-existing item, in order to avoid DoS attack, skip it
			}
		}
		// add watchChannel
		c.watcherNotify[watcher] = struct {
			WatchChannel
			List []WatchKey
		}{WatchChannel: watchChannel, List: newWatchList}
	}

	return dataList, nil
}

func (c *CellarWatchStore) Unwatch(watcher int64) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	n, ok := c.watcherNotify[watcher]
	if !ok {
		return nil
	}
	for _, v := range n.List {
		watchMap, ok := c.watcher[v.UniqueKey()]
		if ok {
			delete(watchMap, watcher)
		}
	}
	return nil
}

func (c *CellarWatchStore) updateCellarData(dataList []*CellarData) error {
	c.lock.Lock()
	defer c.lock.Unlock()

	// update
	changeList := func(list []*CellarData) []*CellarData {
		changeList := make([]*CellarData, 0, 64)
		storage := c.storage
		for _, v := range list {
			oldData, ok := storage[v.UniqueKey()]
			if !ok {
				// case: not exist
				storage[v.UniqueKey()] = v
				changeList = append(changeList, v)
			} else if oldData.DataVersion < v.DataVersion {
				// case: data version obsolete
				storage[v.UniqueKey()] = v
				changeList = append(changeList, v)
			} else {
				// case: data version is latest
				// do nothing
			}
		}
		return changeList
	}(dataList)

	// calculate watchers
	changeMap := map[string]*CellarData{}
	for _, v := range changeList {
		changeMap[v.UniqueKey()] = v
	}
	watcherResult := func(changeMap map[string]*CellarData) map[int64][]*CellarData {
		r := make(map[int64][]*CellarData)
		for k, data := range changeMap {
			watcherMap := c.watcher[k]
			for watcherId, watcherVersion := range watcherMap {
				if watcherVersion < data.DataVersion {
					l := append(r[watcherId], data)
					r[watcherId] = l
				}
			}
		}
		return r
	}(changeMap)

	// notify watchers
	func() {
		for w, l := range watcherResult {
			ch, ok := c.watcherNotify[w]
			if ok {
				select {
				case ch.WatchChannel <- l:
				default:
					_cellarWatchLogger.Warnf("notify failed. watcher channel is full. watcher:%d", w)
				}
			}
		}
	}()

	return nil
}

func (c *CellarWatchStore) LoadFull() error {
	// 1. load lastScannedId_0
	var lastScannedId0 int
	if change, err := c.cellar.queryLatestChange(); err != nil {
		return err
	} else {
		lastScannedId0 = change.ChangeId
	}
	// 2. load full data
	var startDataId = 0
	for {
		_cellarWatchLogger.Infof("load full data from data id: %d", startDataId)
		if r, err := c.cellar.queryCellarDataPaging(startDataId, 100); err != nil {
			return err
		} else if err = c.updateCellarData(r); err != nil {
			return err
		} else if len(r) > 0 {
			startDataId = r[len(r)-1].DataId
		} else {
			break
		}
	}
	// 3. setup lastScannedId
	var lastScannedId int
	if change, err := c.cellar.queryLatestChange(); err != nil {
		return err
	} else {
		lastScannedId = change.ChangeId
	}
	// 4. query incremental from lastScannedId_0 to lastScannedId
	//    And set a max attempts(if exceeded, go to update loop for more data) to avoid infinity loop under heavy update system
	for i := 0; i < 10; i++ {
		_cellarWatchLogger.Infof("load incremental data from change id: %d", lastScannedId0)
		if r, err := c.cellar.queryCellarDataChangesPaging(lastScannedId0, lastScannedId, 1000); err != nil {
			return err
		} else if len(r) == 0 {
			break
		} else {
			var req [][]interface{}
			for _, v := range r {
				req = append(req, []interface{}{v.Area, v.DataKey})
			}
			if rData, err := c.cellar.queryCellarDataByAreaDataKeyPairList(req); err != nil {
				return err
			} else if err = c.updateCellarData(rData); err != nil {
				return err
			}
			lastScannedId0 = r[len(r)-1].ChangeId
		}
	}
	// 5. mark lastScannedId
	c.lastScannedId = lastScannedId

	return nil
}

func (c *CellarWatchStore) scanUpdateLoop() {
	ticker := time.NewTicker(10 * time.Second)
	for {
		<-ticker.C

		err := func() error {
			var lastDataChangeId int
			if change, err := c.cellar.queryLatestChange(); err != nil {
				return err
			} else {
				lastDataChangeId = change.ChangeId
			}
			for {
				// 1. query incremental changes
				changes, err := c.cellar.queryCellarDataChangesPaging(c.lastScannedId, lastDataChangeId, 1000)
				if err != nil {
					return err
				}
				if len(changes) == 0 {
					return nil
				}
				// 2. query data & process data update
				var req [][]interface{}
				for _, v := range changes {
					req = append(req, []interface{}{v.Area, v.DataKey})
				}
				if rData, err := c.cellar.queryCellarDataByAreaDataKeyPairList(req); err != nil {
					return err
				} else if err = c.updateCellarData(rData); err != nil {
					return err
				} else {
					// 3. update index
					c.lastScannedId = changes[len(changes)-1].ChangeId
				}
				_cellarWatchLogger.Infof("scan update - partial finished!")
			}
		}()
		if err != nil {
			_cellarWatchLogger.Errorf("scan update failed: %s", err)
		} else {
			// This may not be able to finish in time, if under a heavy update system
			// But the actual update operation happens for every part. It won't cause severe issue.
			// Refer to #3 in the inner function and #4 in LoadFull function
			_cellarWatchLogger.Infof("scan update - round finished")
		}
	}
}
