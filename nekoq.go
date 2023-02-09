package main

import (
	"flag"
	"time"

	"github.com/meidoworks/nekoq/config"

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

	time.Sleep(1 * time.Hour)
}
