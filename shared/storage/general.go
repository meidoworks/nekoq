package storage

import "github.com/meidoworks/nekoq/shared/storage/badger"

type AtomicStorage interface {
	IncrementBy(key string, val int) (int, error)
	DecrementBy(key string, val int) (int, error)
	GetCount(key string) (int, error)

	SetIfAbsent(key string, value []byte) ([]byte, bool, error)
}

func NewBadgerAtomicStorage(path string) (AtomicStorage, error) {
	return badger.NewAtomicDB(path, false)
}

func NewBadgerAtomicStorageInMemory() (AtomicStorage, error) {
	return badger.NewAtomicDB("", true)
}

type VersionStorage interface {
	SetWithVersion(key string, val []byte, ver uint64) error
}
