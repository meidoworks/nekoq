package config

import "errors"

type NekoConfig struct {
	NekoQ struct {
		NodeId *int16 `toml:"node_id"`
	} `toml:"nekoq"`

	IdGenConf IdGenConfig `toml:"idgen"`
}

type IdGenConfig struct {
}

func (n *NekoConfig) Validate() error {
	if n.NekoQ.NodeId == nil {
		return errors.New("node id is empty")
	}
	return nil
}
