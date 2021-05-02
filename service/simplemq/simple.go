package simplemq

import (
	"context"
	"log"
	"net/http"
	"time"
)

const (
	IN_FLIGHT_SIZE                    = 64
	RESCHEDULE_INTERVAL               = 10 * 60 * 1000 //10min
	LOAD_TOPIC_QUEUE_MAPPING_INTERVAL = 60             // 1min
)

type SimpleMq struct {
	AppCtx *AppCtx

	DbConfig *DbConfig

	httpListenAddr string

	loadTopicQueueMappingCh chan struct{}
	stopCh                  chan struct{}
}

func NewSimpleMq(debugMode bool, dbCfg *DbConfig, listen string) (*SimpleMq, error) {
	appCtx := new(AppCtx)
	appCtx.DebugOutput = debugMode
	return &SimpleMq{
		AppCtx:         appCtx,
		DbConfig:       dbCfg,
		httpListenAddr: listen,
	}, nil
}

func (this *SimpleMq) Init() error {
	this.loadTopicQueueMappingCh = make(chan struct{})
	this.AppCtx.SubMessageRequestQueue = make(chan *MessageRequest, IN_FLIGHT_SIZE)
	this.AppCtx.SubAckMessageRequestQueue = make(chan *MessageAckRequest, IN_FLIGHT_SIZE)
	this.stopCh = make(chan struct{})
	if err := InitDb(this.AppCtx, this.DbConfig); err != nil {
		return err
	}
	return nil
}

func (this *SimpleMq) SyncStart() error {
	go this.loadMqConfigWorker()
	go this.QueryMessageWorker()

	hh := new(httpHandler)
	hh.AppCtx = this.AppCtx

	mux := http.NewServeMux()
	mux.HandleFunc("/simplemq", hh.handleWebsocket)
	server := &http.Server{Addr: this.httpListenAddr, Handler: mux}
	this.AppCtx.httpServer = server
	if err := server.ListenAndServe(); err != nil {
		return err
	}

	//TODO

	<-this.stopCh
	return nil
}

func (this *SimpleMq) Stop() error {
	close(this.loadTopicQueueMappingCh)
	err := this.AppCtx.httpServer.Shutdown(context.Background())
	if err != nil {
		return err
	}
	//TODO close all websocket connections
	//TODO etc.

	close(this.stopCh)
	return nil
}

func (this *SimpleMq) loadMqConfigWorker() {
	timer := time.NewTicker(LOAD_TOPIC_QUEUE_MAPPING_INTERVAL * time.Second)
	defer func() {
		timer.Stop()
	}()

	if this.AppCtx.DebugOutput {
		log.Println("[DEBUG] trigger load mq mapping for the first time")
	}
	ch := make(chan struct {
		M   map[string]map[string]TopicQueueMapping
		Err error
	})
	// memory barrier
	go func() {
		m, err := LoadTopicQueueMapping(this.AppCtx)
		ch <- struct {
			M   map[string]map[string]TopicQueueMapping
			Err error
		}{M: m, Err: err}
	}()
	elem := <-ch
	m, err := elem.M, elem.Err
	if err != nil {
		log.Println("[ERROR] load latest mq mapping failed. wait for next schedule. info:", err)
		panic(err)
	} else {
		// replace map
		this.AppCtx.mapping = m
		if this.AppCtx.DebugOutput {
			log.Println("[DEBUG] loaded data: ", PrintJson(m))
		}
	}

LOOP:
	for {
		select {
		case t := <-timer.C:
			if this.AppCtx.DebugOutput {
				log.Println("[DEBUG] trigger load mq mapping at", t)
			}
			ch := make(chan struct {
				M   map[string]map[string]TopicQueueMapping
				Err error
			})
			// memory barrier
			go func() {
				m, err := LoadTopicQueueMapping(this.AppCtx)
				ch <- struct {
					M   map[string]map[string]TopicQueueMapping
					Err error
				}{M: m, Err: err}
			}()
			elem := <-ch
			m, err := elem.M, elem.Err
			if err != nil {
				log.Println("[ERROR] load latest mq mapping failed. wait for next schedule. info:", err)
			} else {
				// replace map
				this.AppCtx.mapping = m
				if this.AppCtx.DebugOutput {
					log.Println("[DEBUG] loaded data: ", PrintJson(m))
				}
			}
		case <-this.loadTopicQueueMappingCh:
			log.Println("[INFO] finish load mq mapping")
			break LOOP
		}
	}
}
