package config

import (
	"errors"
	"io"

	"github.com/pelletier/go-toml/v2"
)

var _defaultConfig = &NekoConfig{
	NekoQ: struct {
		NodeId *int16 `toml:"node_id"`
	}{},
	NumGen: NumGenConfig{
		Listen: ":9301",
	},
	MQ: MQConfig{},
	Naming: NamingConfig{
		Discovery: struct {
			Listen string `toml:"listen"`
			Peers  []struct {
				Address string `toml:"address"`
				NodeId  int16  `toml:"node_id"`
			} `toml:"peers"`
		}{
			Listen: ":9302",
		},
	},
}

func WriteDefault(w io.Writer) error {
	data, err := toml.Marshal(_defaultConfig)
	if err != nil {
		return err
	}
	_, err = w.Write(data)
	if err != nil {
		return err
	}
	return nil
}

type NekoConfig struct {
	NekoQ struct {
		NodeId *int16 `toml:"node_id"`
	} `toml:"nekoq"`

	NumGen NumGenConfig `toml:"numgen"`

	MQ     MQConfig     `toml:"mq"`
	Naming NamingConfig `toml:"naming"`
}

type NumGenConfig struct {
	Listen string `toml:"listen"`
}

type MQConfig struct {
}

type NamingConfig struct {
	Discovery struct {
		Listen string `toml:"listen"`
		Peers  []struct {
			Address string `toml:"address"`
			NodeId  int16  `toml:"node_id"` // Skip self: if node_id == current node_id, then skip
		} `toml:"peers"`
	} `toml:"discovery"`
}

func (n *NekoConfig) Validate() error {
	if n.NekoQ.NodeId == nil {
		return errors.New("node id is empty")
	}
	return nil
}

func (n *NekoConfig) MergeDefault() {
	//TODO apply default to current config
}
