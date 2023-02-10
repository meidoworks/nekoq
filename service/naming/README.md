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

## 2. Features

* [X] Discovery service
* [X] Discovery: peer full/incremental sync
* [X] Discovery: client state report
* [X] Discovery: support multiple register from single client
* [ ] Discovery: Polling(watch) Operation and API
* [ ] Discovery: gracefully shutdown
* [ ] Discovery: Custom information over a service
* [ ] Discovery: revision(version) support for service(not recommended) and custom information update
* [ ] Discovery optimization: merge update into batch to save history slot
* [ ] Discovery optimization: Streaming API for large data sync
* [ ] Discovery optimization: Reduce calculation and network transport(especially for fan-out scenario)
* [ ] Discovery security: authentication

## 3. Document: discovery

#### 3.1 Service, Area, NodeId

Unique service are recognized by the combination of the following:

* Service
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
* Service check interval: 5s
* Service TTL before cleanup: 20s
* Peer Sync TTL before expired: 20s
* Peer TTL before cleanup: 60s
* Peer fetching update: 1s
