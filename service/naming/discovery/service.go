package discovery

import "fmt"

type RecordKey struct {
	Service string `json:"service"`
	Area    string `json:"area"`
	NodeId  string `json:"node_id"`
}

func (r *RecordKey) GetKey() string {
	return fmt.Sprint(r.Service, "||", r.Area, "||", r.NodeId)
}

type Record struct {
	Service string `json:"service"`
	Area    string `json:"area"`
	NodeId  string `json:"node_id"`

	RecordVersion int64 `json:"record_version"`

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
	Operation string // apply/recover

	MergeOperation string // change/remove
	Record                // service, area, node_id are required as primary key
}

// NodeService node service for processing node request
type NodeService interface {
	SelfKeepAlive(record *Record) error
	SlimKeepAlive(key *RecordKey) error
	Offline(key *RecordKey) error
	OfflineN(keys []*RecordKey) error
	ControlData(data *ControlData) error //TODO WIP

	CustomInformation(info *CustomInfo) error //TODO WIP

	Fetch(service, area string) ([]*Record, error)
}

type Node struct {
}
