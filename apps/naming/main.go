package main

import (
	"fmt"
	"log"
	"net/http"
	"os"
	"runtime"
	"runtime/pprof"
	"runtime/trace"

	"github.com/meidoworks/nekoq/service/naming"
)

func main() {

	container := &struct {
		file      *os.File
		traceFile *os.File
	}{}

	go func() {
		mux := http.NewServeMux()
		mux.HandleFunc("/profile/cpu_start", func(writer http.ResponseWriter, request *http.Request) {
			if container.file != nil {
				writer.WriteHeader(http.StatusConflict)
				return
			}
			os.Remove("cpu.prof")
			file, err := os.Create("cpu.prof")
			if err != nil {
				log.Println(err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			container.file = file
			pprof.StartCPUProfile(file)
			writer.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/profile/cpu_stop", func(writer http.ResponseWriter, request *http.Request) {
			if container.file == nil {
				writer.WriteHeader(http.StatusOK)
				return
			}
			pprof.StopCPUProfile()
			container.file = nil
			writer.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/profile/mem_dump", func(writer http.ResponseWriter, request *http.Request) {
			os.Remove("mem.heap")
			file, err := os.Create("mem.heap")
			if err != nil {
				log.Println(err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			defer file.Close()
			runtime.GC()
			pprof.WriteHeapProfile(file)
			writer.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/profile/trace_start", func(writer http.ResponseWriter, request *http.Request) {
			if container.traceFile != nil {
				writer.WriteHeader(http.StatusConflict)
				return
			}
			os.Remove("trace.out")
			file, err := os.Create("trace.out")
			if err != nil {
				log.Println(err)
				writer.WriteHeader(http.StatusInternalServerError)
				return
			}
			container.traceFile = file
			trace.Start(file)
			writer.WriteHeader(http.StatusOK)
		})
		mux.HandleFunc("/profile/trace_stop", func(writer http.ResponseWriter, request *http.Request) {
			if container.traceFile == nil {
				writer.WriteHeader(http.StatusOK)
				return
			}
			trace.Stop()
			container.traceFile = nil
			writer.WriteHeader(http.StatusOK)
		})
		if err := http.ListenAndServe(":9303", mux); err != nil {
			panic(err)
		}
	}()

	fmt.Println("system proc count:", runtime.GOMAXPROCS(-1))
	err := naming.SyncStartNaming(":9302")
	if err != nil {
		panic(err)
	}
}
