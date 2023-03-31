# Naming

## 1. Concepts

Naming service consists of the following functions

* Discovery
    * Discovery is a service registry supporting various use cases.
    * It supports dynamic and static service registration in clustered env.
    * Discovery nodes are loosely clustered using data replication between each other.
    * Clients are suggested to register the service in more than one cluster node in order to achieve high availability.
    * Clients are suggested to have unique node id in order to avoid potential registration conflicts and client status
      issue. And this will enable manual management for the certain node.
    * Discovery cluster can have different peer list and this allows flexibly scale up.
    * Discovery nodes in the cluster are suggested to have the same peer list to make every node in the cluster have the
      same and whole data.
    * Refer to the following feature section for other advanced features.
* Warehouse
    * Warehouse is a distributed system for storage which providing consistency in the cluster.

## 2. Features

discovery:

* [X] discovery service
  * [X] Client API services - service lifecycle management
  * [X] Peer API services - discovery cluster synchronization
* [X] discovery: peer full/incremental sync
* [X] discovery: client state report/lifecycle management
* [X] discovery: support multiple register from single client
* [ ] discovery: Polling(watch) Operation and API
* [ ] discovery: gracefully shutdown
* [ ] discovery: Custom information overwriting a service
* [ ] discovery: revision(version) support for service(not recommended) and custom information update
* [X] discovery: hierarchy discovery support, environment support & environment inherit
    * For retrieving services, this requires area configuration available in warehouse DiscoveryUse.
    * For register services, area can be any value even not existing in warehouse DiscoveryUse.
    * By default, a default area `default` will be created for general purpose.
* [ ] discovery: service tagging/grouping - e.g. active/standby, canary/grey/blue-green release
* [ ] discovery: manual service management - e.g. downgrade/priority
* [ ] discovery: multiple tenant
* [ ] discovery optimization: merge update into batch to save history slot
* [ ] discovery optimization: Streaming API for large data sync
* [ ] discovery optimization: Reduce calculation and network transport(especially for fan-out scenario)
* [ ] discovery security: authentication

warehouse:

* [ ] warehouse service
* [ ] warehouse kv
* [ ] warehouse directory
* [ ] warehouse discovery storage
* [ ] warehouse storage
* [ ] warehouse consensus algorithm - raft
* [ ] warehouse listening api
* [ ] warehouse more algorithm - paxos/gossip/consistent hash

## 3. Document: discovery

#### 3.1 Service, Area, NodeId

Unique service are recognized by the combination of the following:

* Service
    * Service here can be the following types:
        * Traditional service name like full JAVA class name
        * Or the name of service in kubernetes
        * Details: refer to the document of discovery below
* Area
* NodeId

Service data

* Tag: group nodes in one service
* ServiceData: service data of the record
* MetaData: metadata to describe the service record

#### 3.2 Keep Alive model

##### 3.2.1 Grain of registered service

Discovery requires client to register and keep alive for each service.

This allows register different services distributed in different node or even cluster.

The drawback is overhead increase compared to register per node.

##### 3.2.2 Keep Alive Operation

Generally, to keep alive a service only requires client to send keep alive operation.

In order to make service data up-to-date, clients are suggested to re-register the service after a long period.

This will cause overhead of computing and synchronization. So this behaviour is optional on client side.

#### 3.3 Performance problem

* large provider cluster in a single service
* mass consumer count for a single service
* mass service registered in a single node

#### 3.4 Service Lifecycle & TTL of each stage

* Service keepalive: suggested >5s and < Time of ServiceTTL
* (Optional)Service re-register: > several minutes
* Service check interval: 2s
* Service TTL before cleanup: 20s + 0~10s
* Peer Sync TTL before expired: 20s
* Peer TTL before cleanup: 60s
* Peer fetching update: 1s

#### 3.5 Service discovery hierarchy in data center model and other service models

Hierarchies for service communications:

* Direct
    * Can talk to peer directly.
    * In the `same` tier(network/naming scope).
* Proxy/Gateway
    * Regardless of whether peer can talk to each other or not, one node should talk to the proxy to access peer.
    * Examples reverse proxy(nginx/etc.), kubernetes service, network zoning(firewall isolated/etc.), etc.
    * Can be treated as direct/inside subnet.
* Inside subnet - e.g. NAT/L4-L7 Translation
    * Node outside the subnet cannot access peers inside subnet.
    * Access should be via a translation/gateway mechanism.
    * Both or the inside service have to be exposed using the mechanism. So that the other side could access.
    * Subnet can have multiple layers.
    * In `multiper` tiers.
    * And is in the `tree` model.

For the case of using private network between multiple sites, it is the network architecture. It doesn't change any
assumption to the above.

#### 3.6 Cluster management

In order to avoid impact on the service, any operation(join/leave) to the cluster should be performed during idle
period. Because new node join or node leave will cause peer initialize full data set or cleanup data set. This will have
heavy impact on computing and memory resource and will cause GC which will blocking business.

When a new node joint the cluster, it's better to wait for several seconds(depending on the size of data set) before
starting processing request. This will make peer sync happen finish ahead of user request.

#### 3.A Alternative implementations

* Performance: using client side register & keepalive to replicate to peers
    * This approach will reduce the cost of recording change history on the source server
    * Syncing data in keepalive aims to replicate data to newly joint server. But this will cause high bandwidth
      consumption.
    * Discovery currently keeps history inside the server and replicate the data only when someone ask for updates.
      This will reduce the cost of bandwidth but have to think about the amount of history to track.

* Ready to accept requests
    * Since discovery relies on peer talk to reach cluster consistent which is not strict consistency, it may have data
      issue to start serving before getting data from all peers.
    * One solution is waiting for all peers to respond data then start processing client request.
    * There are some other features have to be considered:
        * One discovery server may start before peers and may also keep this state for a long time. So this should allow
          incoming request.
        * Discovery server may shut down for maintenance(even majority of the nodes do). In this case, other servers
          should work as normal.
        * Discovery supports asymmetric peer. Peer list is maintained per discovery server not the whole cluster.
    * So to avoid data out-of-sync issue, when a new discovery service online, it's better to make sure peer status is
      healthy, then dispatch client requests to this server.

* Service field used for service definition
    * 3 types of service scope
        * Per application node
        * Per service
        * Hybrid
    * Per service
        * Traditional service discovery
        * Fine-grained
        * MetaData can be attached per node per service
        * Cons: resource consumption - computing/memory/bandwidth
    * Per application node
        * Like domain based service discovery
        * Resource friendly
        * Cons: require all node associated with the service name should provide same service implementations
            * Possible solution: directly retrieve peer metadata before invoking service
        * Cons: metadata only supports per node
            * Possible solution: same to the above
        * Otherwise, request may fail if running on the node, which not supporting the request.
    * Hybrid
        * Combining the solutions of per service and per application node
        * Providing fine-grained service discovery and reduce the cost of resources

#### 3.B UseCase

##### 3.B.1 Nested Environment

In some testing environments, user can create isolated environments based on a shared environment.

This allows user to access shared services without deploying them in the separate environment.

The solution using discovery is:

* Create new `area` for the new test env and link to the parent env by parent `area`
* Register services in the new test env with the new `area`
* Fetch desired service as normal

##### 3.B.2 Application groups for various purpose

For the purpose of increasing reliability of a large application, usually the application will be split into several
groups.

When client calling specific api, the traffic will be sent to the specific group which is part of the application that
provide the api.

Discovery doesn't provide mechanism to directly support grouping, since group function is not the key attribute of a
service.

However, alternatives are still available to easily support:

* Opt1. Adding group prefix to a service
    * Both service provider and consumer have to add group prefix while using discovery
    * On consumer side, add group prefix in the service name when fetching services according to the grouping rule
    * Pros: avoid unexpected services sending to consumer and prevent unexpected calls
    * Cons: requires provider and consumer manually configuration, even though rpc framework would do the stuffs.
* Opt2. Adding tag to a service
    * Add tag for the group when registering a service
    * On consumer side, the addresses with the tag are filter out according to the grouping rule
    * Pros: can automatically specify the tag by merging operational configurations
    * Cons: consumers may fetch all services beyond the tag, and it can cause misuse of the data
* When registering service, provider can determine whether to register the service or not if the provider is not in the
  grouping rule

The `area` field is not recommended as group name. Because by design `area` represents hard isolation between two
clusters(area).

Using `area` indicates direct access are prohibited and a gateway/proxy in the middle for the communication. But
grouping doesn't limit the direct access, instead it indicates direct access available. This(using `area` as group)
assumption and solution would be broken if any acl according to its definition enforced between areas in the future.

## 4. Document: Warehouse

#### 4.1 Design

##### Component - storage

* Stable storage with version
* Logs
* Snapshot storages with version

Records must satisfy:

* snapshot <= stable
* snapshot + log >= stable

Prerequisite to the following operations

* Storage: support write with version comparison atomically

Flows of operations - Stable storage/Snapshot storages(not all log operations included):

(If the backend storage supports MVCC, dump flow seems to be much easier)

* Read
    * Normal state
        1. not in snapshot process
        2. read stable
    * In snapshot dumping state
        1. in snapshot process
        2. read temp map by key(build if empty or not available)
        3. if not found, read stable
* Write
    * Normal state and in snapshot dumping state
        * goroutine pipeline
            * N write requester goroutines -> (dispatch queue) -> dispatch goroutine -> (write event channel) ->
              normal/dumping
              state worker
    * Write event
        1. normal state write worker
            * write stable with version(+key-value with version) if version is greater than db
        2. dumping state write worker
            * write temporary key-value storage with version
    * Dump start event
        1. Dumping state write worker: read stable with version
        2. Dumping state write worker: write snapshot with version
        3. Dumping state write worker: send dump finish
    * Dump finish event
        1. Normal state write worker: apply all temporary key-value to stable if version is greater than db
* Dump snapshot related operations
    * dump start
        1. Write requester: send dump start event to write dispatch queue
        2. Dispatcher: close write event channel, which is used by write worker, to stop all workers
        3. Dispatcher: waitgroup to wait for all workers exit
        4. Dispatcher: init new temporary key-value storage
        5. Dispatcher: start snapshot process with snapshot version(=dump log entry id)
        6. Dispatcher: start new dumping state write workers with new waitgroup
        7. Dispatcher: dispatch dump start operation to write worker and then continue dispatching write operations
    * dump finish
        1. Dispatcher: close write event channel, which is used by write worker, to stop all workers
        2. Dispatcher: waitgroup to wait for all workers exit
        3. Dispatcher: end snapshot process
        4. Dispatcher: start new write workers with new waitgroup
        5. Dispatcher: cleanup temporary key-value storage
        6. Dispatcher: continue dispatching write operations
* Full sync-up(new node join or peer expired)
    1. fetch snapshot with version
    2. fetch incremental logs
* Cold startup(+after disaster recovery)
    1. read stable with version
    2. apply logs from version
* Scheduled job
    * cleanup logs(scheduled by time/version)
        1. read snapshot with version
        2. cleanup logs before version
    * cleanup in-snapshot logs
        1. find latest in-snapshot log container
        2. cleanup the other in-snapshot log containers
    * trigger snapshot
        1. refer to dump snapshot

Flow of operations - Logs(rest of log operations included)

* Write(master node)
    1. raft write with stable storage and log storage
* Cold startup(+after disaster recovery)
    * Same to the description above

Flow of operations - consistency

* Read
    * Opt1. read local
    * Opt2. read quorum
* Write
    * raft write

##### Component - consensus algorithm

Currently, using mature raft library for consensus algorithm implementation.


