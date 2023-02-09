package main

import "github.com/meidoworks/nekoq/service/naming"

func main() {
	err := naming.SyncStartNaming(":9302")
	if err != nil {
		panic(err)
	}
}
