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
)

func init() {
	flag.StringVar(&configFile, "c", "nekoq.toml", "-c=nekoq.toml")
}

func main() {
	fs := afero.NewOsFs()
	cfgData, err := afero.ReadFile(fs, configFile)
	if err != nil {
		panic(err)
	}

	nekoCfg := new(config.NekoConfig)
	if err := toml.Unmarshal(cfgData, nekoCfg); err != nil {
		panic(err)
	}
	if err := nekoCfg.Validate(); err != nil {
		panic(err)
	}

	time.Sleep(1 * time.Hour)
}
