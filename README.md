NekoQ
=====

NekoQ

# 1. Features

* [X] NumGen
    * Default: Enabled
    * Depend on: Discovery
* [X] Discovery(Naming)
    * Default: Enabled
* [ ] Warehouse(Naming)
* [ ] MQ

# 2. TODO List

* [ ] General: graceful startup and shutdown handling
    * [ ] Organize service dependencies/service dependency graph
    * [ ] Lifecycle of services
    * [ ] Cleanup resources
* [ ] General: graceful service deregister
* [ ] General: error handling and detailed error information
* [ ] General: profiling/debugging support
* [ ] General: configurations
* [ ] General: service registration
    * [ ] Cluster mode: self if api -> registration
    * [ ] LocalSwitch mode: NIC IP -> registration
        * By default, all services are always available in LocalSwitch
* [ ] General: (Any improvements and suggestions)

Note: These are the general items to be implemented. For more details, refer to the document of each service.

# 3. How to use

## 3.1 LocalSwitch mode(single model, test only)

## 3.2 Cluster mode

# 4. Service documents

* [MQ Service](DOC.MQ.md)

