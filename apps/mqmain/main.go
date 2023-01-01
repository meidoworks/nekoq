package main

import "github.com/meidoworks/nekoq/service/mq"

func main() {
	err := mq.Run(":9301")
	if err != nil {
		panic(err)
	}
}
