# Naming

## 1. Concepts

Naming service consists of the following functions

* Discovery
    * Discovery is a service registry supporting various use cases.
    * It supports dynamic and static service registration in clustered env.
    * Discovery nodes are loosely clustered using data replication between each other.
    * Clients are suggested to register the service in more than one cluster node in order to achieve high availability.
    * Clients are suggested to have unique node id in order to avoid potential registration conflicts and client status
      issue.
    * Discovery cluster can have different peer list and this allows flexibly scale up.
    * Discovery nodes in the cluster are suggested to have the same peer list to make every node in the cluster have the
      same and whole data.
    * Refer to the following feature section for other advanced features.
* Warehouse

## 2. Features

* [ ] Discovery service
* [ ] Discovery: peer full/incremental sync
* [ ] Discovery: client state report
* [ ] Discovery: support multiple register from single client
* [ ] Discovery: Polling(watch) Operation and API
* [ ] Discovery: gracefully shutdown
* [ ] Discovery optimization: Streaming API for large data sync
* [ ] Discovery optimization: Reduce calculation and network transport(especially for fan-out scenario)
* [ ] Discovery security: authentication


