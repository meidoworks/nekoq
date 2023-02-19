package badger

import (
	"encoding/binary"
	"errors"
	"time"

	"github.com/meidoworks/nekoq/shared/logging"

	"github.com/dgraph-io/badger/v3"
)

var _badgerAtomicStorageLogger = logging.NewLogger("BadgerAtomicStorage")

type atomicStorage struct {
	db *badger.DB
}

func (a *atomicStorage) GetCount(key string) (int, error) {
	txn := a.db.NewTransaction(false)
	defer txn.Discard()

	item, err := txn.Get([]byte(key))
	if err != nil {
		return 0, err
	}
	v, err2 := item.ValueCopy(nil)
	if err2 != nil {
		return 0, err2
	}
	old := int(binary.LittleEndian.Uint64(v))

	return old, nil
}

func (a *atomicStorage) IncrementBy(key string, val int) (int, error) {
	if val <= 0 {
		return 0, errors.New("val should be greater than 0")
	}

	keyVal := []byte(key)
	for {
		r, err := func() (int, error) {
			txn := a.db.NewTransaction(true)
			defer txn.Discard()

			var newVal []byte
			var newRtn int

			item, err := txn.Get(keyVal)
			if err == badger.ErrKeyNotFound {
				newVal = make([]byte, 8)
				binary.LittleEndian.PutUint64(newVal, uint64(val))
			} else if err != nil {
				return 0, err
			} else {
				v, err := item.ValueCopy(nil)
				if err != nil {
					return 0, err
				}
				old := int(binary.LittleEndian.Uint64(v))
				newV := old + val
				newVal = make([]byte, 8)
				binary.LittleEndian.PutUint64(newVal, uint64(newV))
				newRtn = newV
			}

			if err := txn.Set(keyVal, newVal); err != nil {
				return 0, err
			}

			if err := txn.Commit(); err != nil {
				return 0, err
			}

			return newRtn, nil
		}()
		if err == badger.ErrConflict {
			continue
		} else if err != nil {
			return 0, err
		} else {
			return r, nil
		}
	}

}

func (a *atomicStorage) DecrementBy(key string, val int) (int, error) {
	if val <= 0 {
		return 0, errors.New("val should be greater than 0")
	}

	keyVal := []byte(key)
	for {
		r, err := func() (int, error) {
			txn := a.db.NewTransaction(true)
			defer txn.Discard()

			var newVal []byte
			var newRtn int

			item, err := txn.Get(keyVal)
			if err == badger.ErrKeyNotFound {
				newVal = make([]byte, 8)
				binary.LittleEndian.PutUint64(newVal, uint64(-val))
			} else if err != nil {
				return 0, err
			} else {
				v, err := item.ValueCopy(nil)
				if err != nil {
					return 0, err
				}
				old := int(binary.LittleEndian.Uint64(v))
				newV := old - val
				newVal = make([]byte, 8)
				binary.LittleEndian.PutUint64(newVal, uint64(newV))
				newRtn = newV
			}

			if err := txn.Set(keyVal, newVal); err != nil {
				return 0, err
			}

			if err := txn.Commit(); err != nil {
				return 0, err
			}

			return newRtn, nil
		}()
		if err == badger.ErrConflict {
			continue
		} else if err != nil {
			return 0, err
		} else {
			return r, nil
		}
	}

}

func (a *atomicStorage) SetIfAbsent(key string, value []byte) ([]byte, bool, error) {
	for {
		v, put, err := func() ([]byte, bool, error) {
			txn := a.db.NewTransaction(true)
			defer txn.Discard()

			item, err := txn.Get([]byte(key))
			if err == nil {
				v, err := item.ValueCopy(nil)
				if err != nil {
					return nil, false, err
				}
				return v, false, nil
			} else if err != badger.ErrKeyNotFound {
				return nil, false, err
			}

			// not exist
			if err := txn.Set([]byte(key), value); err != nil {
				return nil, false, err
			}

			if err := txn.Commit(); err == badger.ErrConflict {
				return nil, false, err
			} else if err != nil {
				return nil, false, err
			}

			return value, true, nil
		}()
		if err == badger.ErrConflict {
			continue
		} else if err != nil {
			return nil, false, err
		} else {
			return v, put, nil
		}
	}
}

func NewAtomicDB(path string, inMemory bool) (*atomicStorage, error) {
	opt := badger.DefaultOptions(path).WithInMemory(inMemory)
	db, err := badger.Open(opt)
	if err != nil {
		return nil, err
	}

	go func() {
		ticker := time.NewTicker(5 * time.Minute)
		defer ticker.Stop()
		for range ticker.C {
			// handling discard data
			db.SetDiscardTs(db.MaxVersion())
			// This gc handling comes from official document
			if !inMemory {
			again:
				err := db.RunValueLogGC(0.7)
				//FIXME may have to handle some specific error
				if err == nil {
					goto again
				}
			}
			// handling sync
			if err := db.Sync(); err != nil {
				_badgerAtomicStorageLogger.Errorf("invoke sync failed:[%s]", err)
			}
		}
	}()

	return &atomicStorage{
		db: db,
	}, nil
}
