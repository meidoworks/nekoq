package main

import (
	"flag"
	"fmt"
	"goimport.moetang.info/nekoq/service/naming/impl"
	"goimport.moetang.info/nekoq/service/naming/impl/mem/tool"
	"log"
	"os"
)

var (
	nodeInfoFile string
)

func init() {
	flag.StringVar(&nodeInfoFile, "file", "", "-file=XXX")
}

func main() {
	flag.Parse()

	if nodeInfoFile == "" {
		log.Fatalln("file is empty.")
	}

	f, err := os.Open(nodeInfoFile)
	if err != nil {
		log.Fatalln("open file [", nodeInfoFile, "] error.", err)
	}

	record, err := tool.FromReader(f)
	if err != nil {
		log.Fatalln("read record error.", err)
	}

	fmt.Println(record)

	logfile, err := impl.NewLog("./logfile.data")
	if err != nil {
		log.Fatalln("create log error.", err)
	}

	logfile.WriteLogRecord()
}
