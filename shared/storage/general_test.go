package storage_test

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/meidoworks/nekoq/shared/storage"
)

func TestNewBadgerAtomicStorageInMemoryIncrDecr(t *testing.T) {
	stor, err := storage.NewBadgerAtomicStorageInMemory()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("n_proc:", runtime.GOMAXPROCS(-1))

	const workerCnt = 10
	const taskCnt = 10000

	wg := new(sync.WaitGroup)
	wg.Add(10 * 2)

	for i := 0; i < workerCnt; i++ {
		go func() {
			for idx := 0; idx < taskCnt; idx++ {
				_, err := stor.IncrementBy("1", 1)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}
	for i := 0; i < workerCnt; i++ {
		go func() {
			for idx := 0; idx < taskCnt; idx++ {
				_, err := stor.DecrementBy("1", 1)
				if err != nil {
					panic(err)
				}
			}
			wg.Done()
		}()
	}

	wg.Wait()

	if val, err := stor.GetCount("1"); err != nil {
		t.Fatal(err)
	} else if val != 0 {
		t.Fatal("value mismatch")
	}
}

func TestNewBadgerAtomicStorageInMemorySetIfNotExists(t *testing.T) {
	stor, err := storage.NewBadgerAtomicStorageInMemory()
	if err != nil {
		t.Fatal(err)
	}

	t.Log("n_proc:", runtime.GOMAXPROCS(-1))

	var count uint32 = 0

	ch := make(chan struct{}, 1)

	for i := 0; i < 100; i++ {
		go func() {
			<-ch
			data, put, err := stor.SetIfAbsent("key1", []byte("hello"))
			if err != nil {
				panic(err)
			}

			if put {
				atomic.AddUint32(&count, 1)
			}

			fmt.Println(string(data), put)
		}()
	}

	close(ch)

	time.Sleep(1 * time.Second)

	if count > 1 {
		t.Fatal("more than one request has modified the value")
	}
}
