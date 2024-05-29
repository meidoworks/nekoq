NekoQ
=====

NekoQ

# 1. Services

* [X] NumGen
    * Distributed ID generator
* [X] Naming.Discovery
    * Service discovery
* [X] Naming.Warehouse
    * Distributed data storage
* [ ] MQ
* [ ] Scheduling

# 2. TODO List

* [ ] General: graceful startup and shutdown handling
    * [ ] Organize service dependencies/service dependency graph
    * [ ] Lifecycle of services
    * [ ] Cleanup resources
* [ ] General: graceful service deregister
* [ ] General: error handling and detailed error information
* [ ] General: logging/profiling/debugging support
    * [ ] Including: management portal for viewing data in the process
* [ ] General: configurations
    * [ ] Additional: All services can be turned off
* [ ] General: service registration
    * [ ] Cluster mode: self if api -> registration
    * [ ] LocalSwitch mode: NIC IP -> registration
        * By default, all services are always available in LocalSwitch
* [ ] General: tuning for critical usecase - high TPS/large data set/broadcast/network bandwidth/etc
    * [ ] Speed
    * [ ] Space/Resource consumption
* [ ] General: change magic number to configuration
* [ ] General: Security - AuthZ/AuthN for APIs
* [ ] General: reorganize package structure
* [ ] General: more testing/fuzz
* [ ] General: better organized documents including key parameter configurations
* [ ] General: Simple configuration for usecases and flexible scale-out with shared nothing/minimum
* [ ] General: (Any improvements and suggestions)

Note: These are the general items to be implemented. For more details, refer to the document of each service.

# 3. How to use

## 3.1 LocalSwitch mode(single model, test only)

## 3.2 Cluster mode

# 4. Service documents

* [MQ Service](DOC.MQ.md)
* [NumGen](service/numgen/doc.md)
* [Naming](service/naming/README.md)
