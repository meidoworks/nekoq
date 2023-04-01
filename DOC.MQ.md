# MQ Service

## 1. Features

## 2. TODO List

## 3. Usage

## 4. Design

### 4.1 Metadata(topic/queue/binding) change flow

May support the following operations and status

* Create/Change
* Archive
* Delete(archived before)
* Migrating(failover/scaleout)

##### Flow

```text
1. New metadata item request from mq service/management portal
2. Allocation service processes the request
2.1. Collect information - mq service nodes
2.2. Allocate mq service nodes for the metadata item - based on assignment in request or rules
2.3. Persist the allocation in order for mq service to retrieve during startup
2.4. Deliver the allocation to the related mq service nodes to apply the change
```

### 4.2 Metadata query flow

```text
1. (When startup or update) MQ service node sends query request
2. Query the allocations from persistent storage
3. MQ service prepares the mechanisms according to the allocations
```

### 4.3 Metadata holding node change(failover/scaleout/etc.) flow

```text
TBD - Coordination between source node and target node
```

