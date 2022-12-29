package main

import (
	"fmt"
	"io"
	"os"

	"github.com/pelletier/go-toml"

	"github.com/meidoworks/nekoq/service/simplemq"
)

type Config struct {
	Connection string
	Listen     string
}

func main() {
	c := new(Config)
	f, err := os.Open("simplemq.toml")
	if err != nil {
		panic(err)
	}
	d, err := io.ReadAll(f)
	if err != nil {
		panic(err)
	}
	err = toml.Unmarshal(d, c)
	if err != nil {
		panic(err)
	}

	fmt.Println(c)

	dbCfg := &simplemq.DbConfig{
		ConnString: c.Connection,
	}

	simpleMq, err := simplemq.NewSimpleMq(true, dbCfg, c.Listen)
	if err != nil {
		panic(err)
	}
	if err := simpleMq.Init(); err != nil {
		panic(err)
	}
	if err := simpleMq.SyncStart(); err != nil {
		panic(err)
	}
}
