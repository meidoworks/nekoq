package common

const (
	SYSTEM_PATH                  = "/_system"
	SYSTEM_NAMING_NODE_LIST_PATH = "/_system/node"          // grouped, dynamic. for declaring all live nodes
	SYSTEM_NAMING_NODE_TOPOLOGY  = "/_system/node_topology" // global, static.
	SYSTEM_NAMING_NODE_INFO      = "/_system/node_info"     // global, static, node unique. for storing node all info.
	CONFIG_PATH                  = "/_config"               // static configs
	SYSTEM_NAMING_NODE_UNIQ_ID   = "/_system/node_uniq_id"  // system uniq id
)

const (
	NODE_NAMING = 1
	NODE_NORMAL = 2
	NODE_CLIENT = 3

	USER_ADMIN  = 1 // can access all paths and data without 'LocalOnly' or 'Acl' field
	USER_NORMAL = 2
	USER_GUEST  = 3

	NAMING_NODE_TYPE_QUORUM   = 1
	NAMING_NODE_TYPE_OBSERVER = 2
)

const (
	NAMING_NODE_FIELD_ID    = "node_id"
	NAMING_NODE_FIELD_PUBID = "pub_id"
	NAMING_NODE_FIELD_PRIID = "pri_id"
)
