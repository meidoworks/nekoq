package impl

import (
	"github.com/meidoworks/nekoq/service/naming/common"
)

var _ common.Quorum = &memNamingTree{}
var _ common.NamingTree = &memNamingTree{}

type memNamingTree struct {
	NodeId string
	UniqId int32
}

func (m memNamingTree) GetNodeId() string {
	//TODO implement me
	panic("implement me")
}

func (m memNamingTree) GetUniqId() int32 {
	//TODO implement me
	panic("implement me")
}

func (m memNamingTree) ParentPath() (common.NamingTree, common.Path, error) {
	//TODO implement me
	panic("implement me")
}

func (m memNamingTree) GetPath(s string) (common.Path, error) {
	//TODO implement me
	panic("implement me")
}

func (m memNamingTree) GetPathAndWatch(change common.OnPathSelfChange) (common.Path, error) {
	//TODO implement me
	panic("implement me")
}

func (m memNamingTree) SessionManager() common.SessionMgr {
	//TODO implement me
	panic("implement me")
}

func NewMemNamingTree(nodeId string, uniqId int32) *memNamingTree {
	m := &memNamingTree{
		NodeId: nodeId,
		UniqId: uniqId,
	}

	return m
}
