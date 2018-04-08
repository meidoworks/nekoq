package impl

import (
	"goimport.moetang.info/nekoq/service/naming/common"
	"goimport.moetang.info/nekoq/util/id"
)

var _ common.Quorum = &memNamingTree{}
var _ common.NamingTree = &memNamingTree{}

type memNamingTree struct {
	NodeId string
	UniqId int32

	//TODO
	idgen *id.Id
}

func NewMemNamingTree(nodeId string, uniqId int32) *memNamingTree {
	m := &memNamingTree{
		NodeId: nodeId,
		UniqId: uniqId,

		idgen: id.NewId(uniqId),
	}

	return m
}
