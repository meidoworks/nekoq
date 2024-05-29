# MQ Service

## 1. Features

* [x] [MQ PubSub Concept]: metadata management:
    * Auto metadata creation: simply use of mq
    * Management portal of metadata: For operation purpose. Centralize, Standard process, Minimum impact.
* [x] [MQ PubSub]: Metadata management:
    * Topic/Queue/PublishGroup/SubscribeGroup creation
    * Topic-Queue/Topic-PublishGroup/Queue-SubscribeGroup binding

## 2. TODO List

* [ ] [MQ Config]: validate configurations
* [ ] [MQ PubSub]: Metadata management enhancement 1:
    * Topic/Queue/PublishGroup/SubscribeGroup deletion
    * Topic-Queue/Topic-PublishGroup/Queue-SubscribeGroup unbinding
* [ ] [MQ PubSub]: publish support simple & wildcard binding key rule
* [ ] [MQ PubSub]: validate input parameters

## 3. Usage

Notes:

1. Make sure no message created before removing topic-queue binding. For QoS > 0, if topic-queue binding is removed
   while still sending messages, some messages may not be able to commit for consuming in the queue whose binding is
   removed

## 4. Design

### 4.1 Metadata(topic/queue/binding) change flow

##### Flows by using warehouse db.consistent api

Write node:

1. prerequisite check
2. lock local storage for the following serialized write operation
3. add new entry in warehouse if the key does not exist
4. get latest data from warehouse of the key
5. write data to local storage for API to use
6. unlock local storage

Peer node:

1. listen changes from warehouse
2. lock local storage
3. write data to local storage for API to use
4. unlock local storage
