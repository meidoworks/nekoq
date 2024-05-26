package config

var _defaultConfiguration = struct {
	NekoQ struct {
		NodeId int16 `toml:"node_id"`
		//ExtId  int32  `toml:"ext_id"` // for extending node id if no more room to traditional node id
		Area string `toml:"area"`
	} `toml:"nekoq"`

	Services struct {
		NumGen struct {
			ServerRealAddress []string `toml:"server_real_address"`
			Port              int      `toml:"port"`
		} `toml:"numgen"`
		Naming struct {
			ServerRealAddress []string `toml:"server_real_address"`
			//ClusterRealAddress []string `toml:"cluster_real_address"` // for communication inside cluster
			External struct {
				Etcd struct {
					Endpoints []string `toml:"endpoints"`
				} `toml:"etcd"`
			} `toml:"external"`
			Warehouse struct {
				Port int `toml:"port"`
			} `toml:"warehouse"`
			Discovery struct {
				Port int `toml:"port"`
			} `toml:"discovery"`
		} `toml:"naming"`
	} `toml:"services"`
}{}

var Instance = _defaultConfiguration

func ValidateInstanceConfiguration() error {
	return nil
}
