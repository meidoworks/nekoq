package main

import (
	"errors"
	"flag"
	"log"
	"os"
	"os/signal"

	"github.com/meidoworks/nekoq/config"
	"github.com/meidoworks/nekoq/service/naming"
	"github.com/meidoworks/nekoq/service/numgen"

	"github.com/pelletier/go-toml/v2"
	"github.com/spf13/afero"
)

var (
	configFile           string
	generateSampleConfig bool
)

func init() {
	flag.StringVar(&configFile, "c", "nekoq.toml", "-c=nekoq.toml")
	flag.BoolVar(&generateSampleConfig, "gencfg", false, "-gencfg")

	flag.Parse()
}

func main() {
	func() {
		if generateSampleConfig {
			data, err := toml.Marshal(config.Instance)
			if err != nil {
				panic(err)
			}
			fs := afero.NewOsFs()
			f, err := fs.Create("nekoq.toml.example")
			if err != nil {
				panic(err)
			}
			defer f.Close()
			f.Write(data)
			os.Exit(1)
		}
	}()

	var configData []byte

	if len(configFile) > 0 {
		fs := afero.NewOsFs()
		cfgData, err := afero.ReadFile(fs, configFile)
		if err != nil {
			panic(err)
		}
		configData = cfgData
	} else {
		panic(errors.New("nekoq config file not specified"))
	}

	// new configuration file
	{
		if err := toml.Unmarshal(configData, &config.Instance); err != nil {
			panic(err)
		}
		if err := config.ValidateInstanceConfiguration(); err != nil {
			panic(err)
		}
	}

	startService()

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

func startService() {
	// discovery
	if err := naming.StartNaming(); err != nil {
		panic(err)
	}
	// numgen
	if numgenService, err := numgen.NewServiceNumGen(); err != nil {
		panic(err)
	} else if err := numgenService.StartService(); err != nil {
		panic(err)
	}
}
