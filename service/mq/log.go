package mq

import (
	"fmt"
	"log"
)

func logInfo(v ...interface{}) {
	fmt.Println(v...)
}

func logError(v ...interface{}) {
	log.Println(v...)
}
