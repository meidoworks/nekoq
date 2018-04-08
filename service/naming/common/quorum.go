package common

type Quorum interface {
	GetNodeId() string
	GetUniqId() int32
}
