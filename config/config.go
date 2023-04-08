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

		NamingAddrs []string `toml:"naming_addresses"`
	}{
		Area:        "default",
		NamingAddrs: []string{api.DefaultConfigLocalSwitchNamingAddress},
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

			CellarStorageType   string `toml:"cellar_storage_type"`
			CellarStorageConfig struct {
				Sources  []string `toml:"sources"`
				Replicas []string `toml:"replicas"`
			} `toml:"cellar_storage_config"`
		}{
			Listen: ":9302",

			CellarStorageType: "postgres",
			CellarStorageConfig: struct {
				Sources  []string `toml:"sources"`
				Replicas []string `toml:"replicas"`
			}{
				Sources:  []string{"host=localhost user=admin password=admin dbname=nekoq_cellar port=5432 sslmode=disable TimeZone=Asia/Shanghai"},
				Replicas: []string{},
			},
		},
	},

	MQ: MQConfig{
		ClusterName: "shared_cluster",
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
	Shared struct {
		NodeId *int16 `toml:"node_id"`
		Area   string `toml:"area"`

		NamingAddrs []string `toml:"naming_addresses"`
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
	ClusterName string `toml:"cluster_name"`
}

type NamingConfig struct {
	Discovery struct {
		Disable bool   `toml:"disable"`
		Listen  string `toml:"listen"`
		Peers   []struct {
			Address string `toml:"address"`
			NodeId  int16  `toml:"node_id"` // Skip self: if node_id == current node_id, then skip
		} `toml:"peers"`

		CellarStorageType   string `toml:"cellar_storage_type"`
		CellarStorageConfig struct {
			Sources  []string `toml:"sources"`
			Replicas []string `toml:"replicas"`
		} `toml:"cellar_storage_config"`
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
