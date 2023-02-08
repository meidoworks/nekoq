package discovery

type Record struct {
	Service string `json:"service"`
	Area    string `json:"area"`
	NodeId  string `json:"node_id"`
	Version int64  `json:"version"`

	Tags        []string `json:"tags"`
	ServiceData []byte   `json:"service_data"`
	MetaData    []byte   `json:"meta_data"`
}

type IncrementalRecord struct {
	Record
	Operation string // change/remove
}

type FullSet struct {
	CurrentVersion string `json:"current_version"`

	Records []*Record `json:"records"`
}

type IncrementalSet struct {
	CurrentVersion string `json:"current_version"`
	ReSync         bool   `json:"re_sync"`

	Records []*IncrementalRecord `json:"records"`
}

// PeerService peer service in the same area
type PeerService interface {
	FullFetch() (*FullSet, error)
	IncrementalFetch(lastVersion string) (*IncrementalSet, error)
}

// EdgePeerService peer service across multi-level and multi-area
type EdgePeerService interface {
	PeerService
}

// ------------------------------------------------------------

type CustomInfo struct {
	Version int64 // used for determining update

	Data []byte
}

type ControlData struct {
	Record           // service, area, node_id are required as primary key
	Operation string // apply/recover
}

// NodeService node service for processing node request
// TODO working in progress
type NodeService interface {
	SelfKeepAlive()
	SlimKeepAlive()
	Offline()
	ControlData(data *ControlData) error

	CustomInformation(info *CustomInfo) error

	Fetch()
	Search()
}

type Node struct {
}
