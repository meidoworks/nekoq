package main

import (
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/numgen"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/afero"
)

var (
	configFile string
	nodeId     int
)

func init() {
	flag.StringVar(&configFile, "c", "nekoq.toml", "-c=nekoq.toml")
	flag.IntVar(&nodeId, "id", 0, "-id=1 (ignored if config file is specified)")
}

func main() {
	nekoCfg := new(config.NekoConfig)

	if len(configFile) > 0 {
		fs := afero.NewOsFs()
		cfgData, err := afero.ReadFile(fs, configFile)
		if err != nil {
			panic(err)
		}
		if err := toml.Unmarshal(cfgData, nekoCfg); err != nil {
			panic(err)
		}
	} else {
		if nodeId >= 0 && nekoCfg.NekoQ.NodeId == nil {
			var id = int16(nodeId)
			nekoCfg.NekoQ.NodeId = &id
			nekoCfg.MergeDefault()
		}
	}

	if err := nekoCfg.Validate(); err != nil {
		panic(err)
	}

	startService(nekoCfg)

	waiting()
}

func waiting() {
	errc := make(chan error, 1)
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, os.Interrupt)
	select {
	case err := <-errc:
		log.Printf("failed to serve: %v", err)
	case sig := <-sigs:
		log.Printf("terminating: %v", sig)
	}
}

func startService(cfg *config.NekoConfig) {
	if numgenService, err := numgen.NewServiceNumGen(*cfg.NekoQ.NodeId, cfg.NumGen); err != nil {
		panic(err)
	} else if err := numgenService.StartHttp(); err != nil {
		panic(err)
	}
}
