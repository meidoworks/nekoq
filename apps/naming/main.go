package main

import (
	"fmt"
	"runtime"

	"github.com/meidoworks/nekoq/service/naming"
)

func main() {
	fmt.Println("system proc count:", runtime.GOMAXPROCS(-1))
	err := naming.SyncStartNaming(":9302")
	if err != nil {
		panic(err)
	}
}
