package common

type NamingTree interface {
	ParentPath() (NamingTree, Path, error) // parent tree, path in parent tree, error
	GetPath(string) (Path, error)
	GetPathAndWatch(OnPathSelfChange) (Path, error)

	SessionManager() SessionMgr
}

type RootNamingTree NamingTree

type Path interface {
	// self
	GetOption() *PathOption
	SetOption(*PathOption) error
	GetOptionAndWatch(OnOptionChange) *PathOption
	// 1. sub structure
	SubNamingTreeOperation() SubNamingTreeOps
	// 2. structure
	SubPathOperation() SubNamingTreeOps
	// 3. temporary sub path
	TempSubPathOperation() SubNamingTreeOps
	// 4. data - a
	DataOperation() DataOps
	// 4. data - b
	AttributeOperation() AttriOps
}

type SubNamingTreeOps interface {
	NewSubNamingTree(*SubTreeCreatingOption) (NamingTree, error)
	GetSubNamingTree() (NamingTree, error)
	DeleteSubNamingTree() error
}

type SubPathOps interface {
	// path, isSeq, option, data
	NewSubPath(string, bool, *PathOption, []byte) (Path, error)
	GetSubPath(string) ([]Path, error)
	GetSubPaths() ([]Path, error)
	GetSubPathAndWatch(OnSubPathChange) ([]Path, error)
	DeleteSubPath(string) (Path, error)
}

type TempSubPathOps interface {
	// path, isSeq, option, data
	NewTempSubPath(string, bool, *TempPathOption, []byte) (TempPath, error)
	GetTempSubPath(string) (TempPath, error)
	GetTempSubPaths() ([]TempPath, error)
	GetTempSubPathAndWatch(OnTempSubPathChange) ([]TempPath, error)
	DeleteTempSubPath(string) (TempPath, error)
}

type DataOps interface {
	GetValue() (HistoryVersion, []byte, error)                      //version, data, error
	GetValueAndWatch(OnValueChange) (HistoryVersion, []byte, error) //version, data, error
	SetValue([]byte) (HistoryVersion, error)                        //version, error
	GetHistoryValues() (map[HistoryVersion][]byte, error)
	GetHistoryValue(version HistoryVersion) ([]byte, error)
	GetHistoryValuesFrom(HistoryVersion) (map[HistoryVersion][]byte, error)

	EnhancedStore() //TODO enhanced storage
}

type AttriOps interface {
	GetAttr0() ([]byte, error)
	SetAttr0([]byte) error
	CompareAndSetAttr0(old []byte, new []byte) (bool, error)
	// ownerFullPath must be a temp path
	AcquireLockAttr0(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr1() ([]byte, error)
	SetAttr1([]byte) error
	CompareAndSetAttr1(old []byte, new []byte) (bool, error)
	AcquireLockAttr1(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr2() ([]byte, error)
	SetAttr2([]byte) error
	CompareAndSetAttr2(old []byte, new []byte) (bool, error)
	AcquireLockAttr2(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr3() ([]byte, error)
	SetAttr3([]byte) error
	CompareAndSetAttr3(old []byte, new []byte) (bool, error)
	AcquireLockAttr3(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr4() ([]byte, error)
	SetAttr4([]byte) error
	CompareAndSetAttr4(old []byte, new []byte) (bool, error)
	AcquireLockAttr4(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr5() ([]byte, error)
	SetAttr5([]byte) error
	CompareAndSetAttr5(old []byte, new []byte) (bool, error)
	AcquireLockAttr5(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr6() ([]byte, error)
	SetAttr6([]byte) error
	CompareAndSetAttr6(old []byte, new []byte) (bool, error)
	AcquireLockAttr6(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr7() ([]byte, error)
	SetAttr7([]byte) error
	CompareAndSetAttr7(old []byte, new []byte) (bool, error)
	AcquireLockAttr7(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr8() ([]byte, error)
	SetAttr8([]byte) error
	CompareAndSetAttr8(old []byte, new []byte) (bool, error)
	AcquireLockAttr8(timeSec int64, block bool, ownerFullPath string) (bool, error)
	GetAttr9() ([]byte, error)
	SetAttr9([]byte) error
	CompareAndSetAttr9(old []byte, new []byte) (bool, error)
	AcquireLockAttr9(timeSec int64, block bool, ownerFullPath string) (bool, error)
}

type TempPath interface {
	// self
	GetOption() *PathOption
	SetOption(*PathOption) error
	GetOptionAndWatch(OnTempOptionChange) *TempPathOption
	// 1. data - a
	DataOperation() DataOps
	// 1. data - b
	AttributeOperation() AttriOps
}

type SessionMgr interface {
	// session manager in quorum
	NewSession(keepaliveInMillis uint64) (*Session, error)
	DestroySession(session *Session) error
	KeepAlive(session *Session) (bool, error) // sessionExists, error
	WatchSessionInvalid(*Session, OnSessionInvalid) error
}

type Session struct {
	// generated, cannot change
	SessionId [2]int64
}

type PathOption struct {
	// read only
	CreateTime uint64
	ModifyTime uint64
	AccessTime uint64
	// quorum status - normal, changing
	QuorumStatus int
	// last quorum change result - none, success, fail
	LastQuorumChangeResult int
	// last quorum change time
	LastQuorumChangeTime int64

	// specify quorum node rule of sub tree
	QuorumNodeRuleList []string
	// group or sub groups of quorum nodes can access
	LocalOnly bool
	// quorum size
	QuorumSize int

	// options - can change
	Acl      []string
	ReadOnly bool
}

type TempPathOption struct {
	// read only
	CreateTime uint64
	ModifyTime uint64
	AccessTime uint64

	// options - can change
	Acl      []string
	ReadOnly bool

	// options - cannot change once created
	CreatorSession *Session
}

type SubTreeCreatingOption struct {
	// specify quorum node rule of sub tree
	QuorumNodeRuleList []string
	// group or sub groups of quorum nodes can see/access
	LocalOnly bool
	// quorum size
	QuorumSize int
}

type HistoryVersion struct {
	ver [2]int64
}
