package mq

import (
	"context"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"runtime/pprof"
	"time"
)

func Run(listener string) error {
	if false {
		f, err := os.Create("cpu.pprof")
		if err != nil {
			panic(err)
		}
		defer f.Close()
		// pprof for CPU
		pprof.StartCPUProfile(f)
		defer pprof.StopCPUProfile()
	}

	if err := InitBroker(); err != nil {
		return err
	}

	l, err := net.Listen("tcp", listener)
	if err != nil {
		return err
	}
	log.Printf("listening on %v", l.Addr())

	s := &http.Server{
		Handler:      messagingHandler{},
		ReadTimeout:  time.Second * 10,
		WriteTimeout: time.Second * 10,
	}
	errc := make(chan error, 1)
	go func() {
		errc <- s.Serve(l)
	}()

	log.Println("NekoQ has been started!")

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	select {
	case err := <-errc:
		log.Printf("failed to serve: %v", err)
	case sig := <-sigs:
		log.Printf("terminating: %v", sig)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second*10)
	defer cancel()

	return s.Shutdown(ctx)
}
