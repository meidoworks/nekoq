package mq

import (
	"fmt"
	"log"
)

func LogInfo(v ...interface{}) {
	fmt.Println(v...)
}

func LogError(v ...interface{}) {
	log.Println(v...)
}
