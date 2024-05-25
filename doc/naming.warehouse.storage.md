# Warehouse storage reference documents

## 1. Consistent storage for warehouse

Warehouse is a durable and fault-tolerant data service, which is used for configuration and other data persistent.

In order to achieve fault-tolerant, a consistent storage is required.

As of current design, external implementations are used in warehouse for convenient.

But the following sections will describe an approach of a new implementation which providing reliable, high performance
consistent storage.

#### 1.1 Design

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



