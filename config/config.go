package config

import (
	"errors"
	"io"

	"github.com/meidoworks/nekoq/api"

	"github.com/pelletier/go-toml/v2"
)

var _defaultConfig = &NekoConfig{
	Shared: struct {
		NodeId *int16 `toml:"node_id"`
		Area   string `toml:"area"`

		NamingAddr string `toml:"naming_address"`
	}{
		Area:       "default",
		NamingAddr: api.DefaultConfigLocalSwitchNamingAddress,
	},
	NumGen: NumGenConfig{
		Disable:     false,
		Listen:      ":9301",
		ServiceName: "nekoq.numgen",
	},

	Naming: NamingConfig{
		Discovery: struct {
			Disable bool   `toml:"disable"`
			Listen  string `toml:"listen"`
			Peers   []struct {
				Address string `toml:"address"`
				NodeId  int16  `toml:"node_id"`
			} `toml:"peers"`
			DisableDefaultArea bool `toml:"disable_default_area"`

			CellarListen      string `toml:"cellar_listen"`
			CellarStorageType string `toml:"cellar_storage_type"`
			CellarStorageAddr string `toml:"cellar_storage_address"`
		}{
			Listen: ":9302",

			CellarListen:      ":9303",
			CellarStorageType: "postgres",
			CellarStorageAddr: "host=localhost user=admin password=admin dbname=nekoq_cellar port=5432 sslmode=disable TimeZone=Asia/Shanghai",
		},
	},
	MQ: MQConfig{},
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
	Shared struct {
		NodeId *int16 `toml:"node_id"`
		Area   string `toml:"area"`

		NamingAddr string `toml:"naming_address"`
	} `toml:"shared"`

	NumGen NumGenConfig `toml:"numgen"`

	MQ     MQConfig     `toml:"mq"`
	Naming NamingConfig `toml:"naming"`
}

type NumGenConfig struct {
	Disable     bool   `toml:"disable"`
	Listen      string `toml:"listen"`
	ServiceName string `toml:"service_name"`
}

type MQConfig struct {
}

type NamingConfig struct {
	Discovery struct {
		Disable bool   `toml:"disable"`
		Listen  string `toml:"listen"`
		Peers   []struct {
			Address string `toml:"address"`
			NodeId  int16  `toml:"node_id"` // Skip self: if node_id == current node_id, then skip
		} `toml:"peers"`
		DisableDefaultArea bool `toml:"disable_default_area"`

		CellarListen      string `toml:"cellar_listen"`
		CellarStorageType string `toml:"cellar_storage_type"`
		CellarStorageAddr string `toml:"cellar_storage_address"`
	} `toml:"discovery"`
}

func (n *NekoConfig) Validate() error {
	if n.Shared.NodeId == nil {
		return errors.New("node id is empty")
	}
	return nil
}

func (n *NekoConfig) MergeDefault() *NekoConfig {
	//TODO apply default to current config
	return _defaultConfig
}
