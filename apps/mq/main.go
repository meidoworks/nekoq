package main

import "goimport.moetang.info/nekoq/apps/mq/lib"

func main() {
	err := lib.Run()
	if err != nil {
		panic(err)
	}
}
