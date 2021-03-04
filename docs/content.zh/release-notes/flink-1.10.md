---
title: "Release Notes - Flink 1.10"
---
<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# Release Notes - Flink 1.10

These release notes discuss important aspects, such as configuration, behavior,
or dependencies, that changed between Flink 1.9 and Flink 1.10. Please read
these notes carefully if you are planning to upgrade your Flink version to 1.10.

### Clusters & Deployment
#### FileSystems should be loaded via Plugin Architecture 
##### [FLINK-11956](https://issues.apache.org/jira/browse/FLINK-11956)

s3-hadoop and s3-presto filesystems do no longer use class relocations and need
to be loaded through [plugins]({{< ref "docs/deployment/filesystems/overview" >}}#pluggable-file-systems)
but now seamlessly integrate with all credential providers. Other filesystems
are strongly recommended to be only used as plugins as we will continue to
remove relocations.

#### Flink Client respects Classloading Policy 
##### [FLINK-13749](https://issues.apache.org/jira/browse/FLINK-13749)

The Flink client now also respects the configured classloading policy, i.e.,
`parent-first` or `child-first` classloading. Previously, only cluster
components such as the job manager or task manager supported this setting.
This does mean that users might get different behaviour in their programs, in
which case they should configure the classloading policy explicitly to use
`parent-first` classloading, which was the previous (hard-coded) behaviour.

#### Enable spreading out Tasks evenly across all TaskManagers 
##### [FLINK-12122](https://issues.apache.org/jira/browse/FLINK-12122)

When
[FLIP-6](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65147077)
was rolled out with Flink 1.5.0, we changed how slots are allocated from
TaskManagers (TMs). Instead of evenly allocating the slots from all registered
TMs, we had the tendency to exhaust a TM before using another one. To use a
scheduling strategy that is more similar to the pre-FLIP-6 behaviour, where
Flink tries to spread out the workload across all currently available TMs, one
can set `cluster.evenly-spread-out-slots: true` in the `flink-conf.yaml`.

#### Directory Structure Change for highly available Artifacts 
##### [FLINK-13633](https://issues.apache.org/jira/browse/FLINK-13633)

All highly available artifacts stored by Flink will now be stored under
`HA_STORAGE_DIR/HA_CLUSTER_ID` with `HA_STORAGE_DIR` configured by
`high-availability.storageDir` and `HA_CLUSTER_ID` configured by
`high-availability.cluster-id`.

#### Resources and JARs shipped via --yarnship will be ordered in the Classpath 
##### [FLINK-13127](https://issues.apache.org/jira/browse/FLINK-13127)

When using the `--yarnship` command line option, resource directories and jar
files will be added to the classpath in lexicographical order with resources
directories appearing first.

#### Removal of --yn/--yarncontainer Command Line Options 
##### [FLINK-12362](https://issues.apache.org/jira/browse/FLINK-12362)

The Flink CLI no longer supports the deprecated command line options
`-yn/--yarncontainer`, which were used to specify the number of containers to
start on YARN. This option has been deprecated since the introduction of
[FLIP-6](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=65147077).
All Flink users are advised to remove this command line option.

#### Removal of --yst/--yarnstreaming Command Line Options 
##### [FLINK-14957](https://issues.apache.org/jira/browse/FLINK-14957)

The Flink CLI no longer supports the deprecated command line options
`-yst/--yarnstreaming`, which were used to disable eager pre-allocation of memory.
All Flink users are advised to remove this command line option.

#### Mesos Integration will reject expired Offers faster 
##### [FLINK-14029](https://issues.apache.org/jira/browse/FLINK-14029)

Flink's Mesos integration now rejects all expired offers instead of only 4.
This improves the situation where Fenzo holds on to a lot of expired offers
without giving them back to the Mesos resource manager.

#### Scheduler Rearchitecture 
##### [FLINK-14651](https://issues.apache.org/jira/browse/FLINK-14651)

Flink's scheduler was refactored with the goal of making scheduling strategies
customizable in the future. Using the legacy scheduler is discouraged as it will
be removed in a future release. However, users that experience issues related to
scheduling can fallback to the legacy scheduler by setting
`jobmanager.scheduler` to `legacy` in their `flink-conf.yaml` for the time
being. Note, however, that using the legacy scheduler with the [Pipelined Region
Failover Strategy]({{< ref "docs/dev/execution/task_failure_recovery" >}}#restart-pipelined-region-failover-strategy)
enabled has the following caveats:

* Exceptions that caused a job to restart will not be shown on the job overview page of the Web UI 
##### [FLINK-15917](https://issues.apache.org/jira/browse/FLINK-15917)
.
However, exceptions that cause a job to fail (e.g., when all restart attempts exhausted) will still be shown.
* The `uptime` metric will not be reset after restarting a job due to task failure 
##### [FLINK-15918](https://issues.apache.org/jira/browse/FLINK-15918)
.

Note that in the default `flink-conf.yaml`, the Pipelined Region Failover
Strategy is already enabled. That is, users that want to use the legacy
scheduler and cannot accept aforementioned caveats should make sure that
`jobmanager.execution.failover-strategy` is set to `full` or not set at all.

#### Java 11 Support 
##### [FLINK-10725](https://issues.apache.org/jira/browse/FLINK-10725)

Beginning from this release, Flink can be compiled and run with Java 11. All
Java 8 artifacts can be also used with Java 11. This means that users that want
to run Flink with Java 11 do not have to compile Flink themselves.

When starting Flink with Java 11, the following warnings may be logged:

    WARNING: An illegal reflective access operation has occurred
    WARNING: Illegal reflective access by org.apache.flink.core.memory.MemoryUtils (file:/opt/flink/flink-1.10.0/lib/flink-dist_2.11-1.10.0.jar) to constructor java.nio.DirectByteBuffer(long,int)
    WARNING: Please consider reporting this to the maintainers of org.apache.flink.core.memory.MemoryUtils
    WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    WARNING: All illegal access operations will be denied in a future release

    WARNING: An illegal reflective access operation has occurred
    WARNING: Illegal reflective access by org.apache.flink.api.java.ClosureCleaner (file:/home/flinkuser/.m2/repository/org/apache/flink/flink-core/1.10.0/flink-core-1.10.0.jar) to field java.lang.String.value
    WARNING: Please consider reporting this to the maintainers of org.apache.flink.api.java.ClosureCleaner
    WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    WARNING: All illegal access operations will be denied in a future release

    WARNING: An illegal reflective access operation has occurred
    WARNING: Illegal reflective access by org.jboss.netty.util.internal.ByteBufferUtil (file:/home/flinkuser/.m2/repository/io/netty/netty/3.10.6.Final/netty-3.10.6.Final.jar) to method java.nio.DirectByteBuffer.cleaner()
    WARNING: Please consider reporting this to the maintainers of org.jboss.netty.util.internal.ByteBufferUtil
    WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    WARNING: All illegal access operations will be denied in a future release

    WARNING: An illegal reflective access operation has occurred
    WARNING: Illegal reflective access by com.esotericsoftware.kryo.util.UnsafeUtil (file:/home/flinkuser/.m2/repository/com/esotericsoftware/kryo/kryo/2.24.0/kryo-2.24.0.jar) to constructor java.nio.DirectByteBuffer(long,int,java.lang.Object)
    WARNING: Please consider reporting this to the maintainers of com.esotericsoftware.kryo.util.UnsafeUtil
    WARNING: Use --illegal-access=warn to enable warnings of further illegal reflective access operations
    WARNING: All illegal access operations will be denied in a future release

These warnings are considered harmless and will be addressed in future Flink
releases.

Lastly, note that the connectors for Cassandra, Hive, HBase, and Kafka 0.8--0.11
have not been tested with Java 11 because the respective projects did not
provide Java 11 support at the time of the Flink 1.10.0 release.


### Memory Management
#### New Task Executor Memory Model 
##### [FLINK-13980](https://issues.apache.org/jira/browse/FLINK-13980)

With
[FLIP-49](https://cwiki.apache.org/confluence/display/FLINK/FLIP-49%3A+Unified+Memory+Configuration+for+TaskExecutors),
a new memory model has been introduced for the task executor. New configuration
options have been introduced to control the memory consumption of the task
executor process. This affects all types of deployments: standalone, YARN,
Mesos, and the new active Kubernetes integration. The memory model of the job
manager process has not been changed yet but it is planned to be updated as
well.

If you try to reuse your previous Flink configuration without any adjustments,
the new memory model can result in differently computed memory parameters for
the JVM and, thus, performance changes.

Please, check [the user documentation](../deployment/memory/mem_setup.html) for more details.

##### Deprecation and breaking changes
The following options have been removed and have no effect anymore:

<table class="table">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Deprecated/removed config option</th>
      <th class="text-left">Note</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>taskmanager.memory.fraction</td>
      <td>
        Check also the description of the new option
        <code class="highlighter-rouge">taskmanager.memory.managed.fraction</code>
        but it has different semantics and the value of the deprecated option
        usually has to be adjusted
      </td>
    </tr>
    <tr>
      <td>taskmanager.memory.off-heap</td>
      <td>Support for on-heap managed memory has been removed, leaving off-heap managed memory as the only possibility</td>
    </tr>
    <tr>
      <td>taskmanager.memory.preallocate</td>
      <td>Pre-allocation is no longer supported, and managed memory is always allocated lazily</td>
    </tr>
  </tbody>
</table>


The following options, if used, are interpreted as other new options in order to
maintain backwards compatibility where it makes sense:

<table class="table">
  <thead>
    <tr>
      <th class="text-left" style="width: 30%">Deprecated config option</th>
      <th class="text-left">Interpreted as</th>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td>taskmanager.heap.size</td>
      <td>
        <ul>
          <li>taskmanager.memory.flink.size for standalone deployment</li>
          <li>taskmanager.memory.process.size for containerized deployments</li>
        </ul>
      </td>
    </tr>
    <tr>
      <td>taskmanager.memory.size</td>
      <td>taskmanager.memory.managed.size</td>
    </tr>
    <tr>
      <td>taskmanager.network.memory.min</td>
      <td>taskmanager.memory.network.min</td>
    </tr>
    <tr>
      <td>taskmanager.network.memory.max</td>
      <td>taskmanager.memory.network.max</td>
    </tr>
    <tr>
      <td>taskmanager.network.memory.fraction</td>
      <td>taskmanager.memory.network.fraction</td>
    </tr>
  </tbody>
</table>


The container cut-off configuration options, `containerized.heap-cutoff-ratio`
and `containerized.heap-cutoff-min`, have no effect for task executor processes
anymore but they still have the same semantics for the JobManager process.

#### RocksDB State Backend Memory Control 
##### [FLINK-7289](https://issues.apache.org/jira/browse/FLINK-7289)

Together with the introduction of the [new Task Executor Memory
Model](#new-task-executor-memory-model-flink-13980), the memory consumption of the RocksDB state backend will be
limited by the total amount of Flink Managed Memory, which can be configured via
`taskmanager.memory.managed.size` or `taskmanager.memory.managed.fraction`.
Furthermore, users can tune RocksDB's write/read memory ratio
(`state.backend.rocksdb.memory.write-buffer-ratio`, by default `0.5`) and the
reserved memory fraction for indices/filters
(`state.backend.rocksdb.memory.high-prio-pool-ratio`, by default `0.1`). More
details and advanced configuration options can be found in the [Flink user
documentation]({{< ref "docs/ops/state/large_state_tuning" >}}#tuning-rocksdb-memory).

#### Fine-grained Operator Resource Management 
##### [FLINK-14058](https://issues.apache.org/jira/browse/FLINK-14058)

Config options `table.exec.resource.external-buffer-memory`,
`table.exec.resource.hash-agg.memory`, `table.exec.resource.hash-join.memory`,
and `table.exec.resource.sort.memory` have been deprecated. Beginning from Flink
1.10, these config options are interpreted as weight hints instead of absolute
memory requirements. Flink choses sensible default weight hints which should
not be adjustment by users.


### Table API & SQL
#### Rename of ANY Type to RAW Type 
##### [FLINK-14904](https://issues.apache.org/jira/browse/FLINK-14904)

The identifier `raw` is a reserved keyword now and must be escaped with
backticks when used as a SQL field or function name.

#### Rename of Table Connector Properties 
##### [FLINK-14649](https://issues.apache.org/jira/browse/FLINK-14649)

Some indexed properties for table connectors have been flattened and renamed
for a better user experience when writing DDL statements. This affects the
Kafka Connector properties `connector.properties` and
`connector.specific-offsets`. Furthermore, the Elasticsearch Connector
property `connector.hosts` is affected. The aforementioned, old properties are
deprecated and will be removed in future versions. Please consult the [Table
Connectors documentation]({{< ref "docs/connectors/table/overview" >}})
for the new property names.

#### Methods for interacting with temporary Tables & Views 
##### [FLINK-14490](https://issues.apache.org/jira/browse/FLINK-14490)

Methods `registerTable()`/`registerDataStream()`/`registerDataSet()` have been
deprecated in favor of `createTemporaryView()`, which better adheres to the
corresponding SQL term.

The `scan()` method has been deprecated in favor of the `from()` method.

Methods `registerTableSource()`/`registerTableSink()` become deprecated in favor
of `ConnectTableDescriptor#createTemporaryTable()`. The `ConnectTableDescriptor`
approach expects only a set of string properties as a description of a
TableSource or TableSink instead of an instance of a class in case of the
deprecated methods. This in return makes it possible to reliably store those
definitions in catalogs.

Method `insertInto(String path, String... pathContinued)` has been removed in
favor of in `insertInto(String path)`.

All the newly introduced methods accept a String identifier which will be
parsed into a 3-part identifier. The parser supports quoting the identifier.
It also requires escaping any reserved SQL keywords.

#### Removal of ExternalCatalog API 
##### [FLINK-13697](https://issues.apache.org/jira/browse/FLINK-13697)

The deprecated `ExternalCatalog` API has been dropped. This includes:

* `ExternalCatalog` (and all dependent classes, e.g., `ExternalTable`)
* `SchematicDescriptor`, `MetadataDescriptor`, `StatisticsDescriptor`

Users are advised to use the [new Catalog API]({{< ref "docs/dev/table/catalogs" >}}#catalog-api).


### Configuration
#### Introduction of Type Information for ConfigOptions 
##### [FLINK-14493](https://issues.apache.org/jira/browse/FLINK-14493)

Getters of `org.apache.flink.configuration.Configuration` throw
`IllegalArgumentException` now if the configured value cannot be parsed into
the required type. In previous Flink releases the default value was returned
in such cases.

#### Increase of default Restart Delay 
##### [FLINK-13884](https://issues.apache.org/jira/browse/FLINK-13884)

The default restart delay for all shipped restart strategies, i.e., `fixed-delay`
and `failure-rate`, has been raised to 1 s (from originally 0 s).

#### Simplification of Cluster-Level Restart Strategy Configuration 
##### [FLINK-13921](https://issues.apache.org/jira/browse/FLINK-13921)

Previously, if the user had set `restart-strategy.fixed-delay.attempts` or
`restart-strategy.fixed-delay.delay` but had not configured the option
`restart-strategy`, the cluster-level restart strategy would have been
`fixed-delay`. Now the cluster-level restart strategy is only determined by
the config option `restart-strategy` and whether checkpointing is enabled. See
[_"Task Failure Recovery"_]({{< ref "docs/dev/execution/task_failure_recovery" >}})
for details.

#### Disable memory-mapped BoundedBlockingSubpartition by default 
##### [FLINK-14952](https://issues.apache.org/jira/browse/FLINK-14952)

The config option `taskmanager.network.bounded-blocking-subpartition-type` has
been renamed to `taskmanager.network.blocking-shuffle.type`. Moreover, the
default value of the aforementioned config option has been changed from `auto`
to `file`. The reason is that TaskManagers running on YARN with `auto`, could
easily exceed the memory budget of their container, due to incorrectly accounted
memory-mapped files memory usage.

#### Removal of non-credit-based Network Flow Control 
##### [FLINK-14516](https://issues.apache.org/jira/browse/FLINK-14516)

The non-credit-based network flow control code was removed alongside of the
configuration option `taskmanager.network.credit-model`. Flink will now always
use credit-based flow control.

#### Removal of HighAvailabilityOptions#HA_JOB_DELAY 
##### [FLINK-13885](https://issues.apache.org/jira/browse/FLINK-13885)

The configuration option `high-availability.job.delay` has been removed
since it is no longer used.


### State
#### Enable Background Cleanup of State with TTL by default 
##### [FLINK-14898](https://issues.apache.org/jira/browse/FLINK-14898)

[Background cleanup of expired state with TTL]({{< ref "docs/dev/datastream/fault-tolerance/state" >}}#cleanup-of-expired-state)
is activated by default now for all state backends shipped with Flink.
Note that the RocksDB state backend implements background cleanup by employing
a compaction filter. This has the caveat that even if a Flink job does not
store state with TTL, a minor performance penalty during compaction is incurred.
Users that experience noticeable performance degradation during RocksDB
compaction can disable the TTL compaction filter by setting the config option
`state.backend.rocksdb.ttl.compaction.filter.enabled` to `false`.

#### Deprecation of StateTtlConfig#Builder#cleanupInBackground() 
##### [FLINK-15606](https://issues.apache.org/jira/browse/FLINK-15606)

`StateTtlConfig#Builder#cleanupInBackground()` has been deprecated because the
background cleanup of state with TTL is already enabled by default.

#### Timers are stored in RocksDB by default when using RocksDBStateBackend 
##### [FLINK-15637](https://issues.apache.org/jira/browse/FLINK-15637)

The default timer store has been changed from Heap to RocksDB for the RocksDB
state backend to support asynchronous snapshots for timer state and better
scalability, with less than 5% performance cost. Users that find the performance
decline critical can set `state.backend.rocksdb.timer-service.factory` to `HEAP`
in `flink-conf.yaml` to restore the old behavior.

#### Removal of StateTtlConfig#TimeCharacteristic 
##### [FLINK-15605](https://issues.apache.org/jira/browse/FLINK-15605)

`StateTtlConfig#TimeCharacteristic` has been removed in favor of
`StateTtlConfig#TtlTimeCharacteristic`.

#### New efficient Method to check if MapState is empty 
##### [FLINK-13034](https://issues.apache.org/jira/browse/FLINK-13034)

We have added a new method `MapState#isEmpty()` which enables users to check
whether a map state is empty. The new method is 40% faster than
`mapState.keys().iterator().hasNext()` when using the RocksDB state backend.

#### RocksDB Upgrade 
##### [FLINK-14483](https://issues.apache.org/jira/browse/FLINK-14483)

We have again released our own RocksDB build (FRocksDB) which is based on
RocksDB version 5.17.2 with several feature backports for the [Write Buffer
Manager](https://github.com/facebook/rocksdb/wiki/Write-Buffer-Manager) to
enable limiting RocksDB's memory usage. The decision to release our own
RocksDB build was made because later RocksDB versions suffer from a
[performance regression under certain
workloads](https://github.com/facebook/rocksdb/issues/5774).

#### RocksDB Logging disabled by default 
##### [FLINK-15068](https://issues.apache.org/jira/browse/FLINK-15068)

Logging in RocksDB (e.g., logging related to flush, compaction, memtable
creation, etc.) has been disabled by default to prevent disk space from being
filled up unexpectedly. Users that need to enable logging should implement their
own `RocksDBOptionsFactory` that creates `DBOptions` instances with
`InfoLogLevel` set to `INFO_LEVEL`.

#### Improved RocksDB Savepoint Recovery 
##### [FLINK-12785](https://issues.apache.org/jira/browse/FLINK-12785)

In previous Flink releases users may encounter an `OutOfMemoryError` when
restoring from a RocksDB savepoint containing large KV pairs. For that reason
we introduced a configurable memory limit in the `RocksDBWriteBatchWrapper`
with a default value of 2 MB. RocksDB's WriteBatch will flush before the
consumed memory limit is reached. If needed, the limit can be tuned via the
`state.backend.rocksdb.write-batch-size` config option in `flink-conf.yaml`.


### PyFlink
#### Python 2 Support dropped 
##### [FLINK-14469](https://issues.apache.org/jira/browse/FLINK-14469)

Beginning from this release, PyFlink does not support Python 2. This is because [Python 2 has
reached end of life on January 1,
2020](https://www.python.org/doc/sunset-python-2/), and several third-party
projects that PyFlink depends on are also dropping Python 2 support.


### Monitoring
#### InfluxdbReporter skips Inf and NaN 
##### [FLINK-12147](https://issues.apache.org/jira/browse/FLINK-12147)

The `InfluxdbReporter` now silently skips values that are unsupported by
InfluxDB, such as `Double.POSITIVE_INFINITY`, `Double.NEGATIVE_INFINITY`,
`Double.NaN`, etc.


### Connectors
#### Kinesis Connector License Change 
##### [FLINK-12847](https://issues.apache.org/jira/browse/FLINK-12847)

flink-connector-kinesis is now licensed under the Apache License, Version 2.0,
and its artifacts will be deployed to Maven central as part of the Flink
releases. Users no longer need to build the Kinesis connector from source themselves.


### Miscellaneous Interface Changes
#### ExecutionConfig#getGlobalJobParameters() cannot return null anymore 
##### [FLINK-9787](https://issues.apache.org/jira/browse/FLINK-9787)

`ExecutionConfig#getGlobalJobParameters` has been changed to never return
`null`. Conversely,
`ExecutionConfig#setGlobalJobParameters(GlobalJobParameters)` will not accept
`null` values anymore.

#### Change of contract in MasterTriggerRestoreHook interface 
##### [FLINK-14344](https://issues.apache.org/jira/browse/FLINK-14344)

Implementations of `MasterTriggerRestoreHook#triggerCheckpoint(long, long,
Executor)` must be non-blocking now. Any blocking operation should be executed
asynchronously, e.g., using the given executor.

#### Client-/ and Server-Side Separation of HA Services 
##### [FLINK-13750](https://issues.apache.org/jira/browse/FLINK-13750)

The `HighAvailabilityServices` have been split up into client-side
`ClientHighAvailabilityServices` and cluster-side `HighAvailabilityServices`.
When implementing custom high availability services, users should follow this
separation by overriding the factory method
`HighAvailabilityServicesFactory#createClientHAServices(Configuration)`.
Moreover, `HighAvailabilityServices#getWebMonitorLeaderRetriever()` should no
longer be implemented since it has been deprecated.

#### Deprecation of HighAvailabilityServices#getWebMonitorLeaderElectionService() 
##### [FLINK-13977](https://issues.apache.org/jira/browse/FLINK-13977)

Implementations of `HighAvailabilityServices` should implement
`HighAvailabilityServices#getClusterRestEndpointLeaderElectionService()` instead
of `HighAvailabilityServices#getWebMonitorLeaderElectionService()`.

#### Interface Change in LeaderElectionService 
##### [FLINK-14287](https://issues.apache.org/jira/browse/FLINK-14287)

`LeaderElectionService#confirmLeadership(UUID, String)` now takes an
additional second argument, which is the address under which the leader will be
reachable. All custom `LeaderElectionService` implementations will need to be
updated accordingly.

#### Deprecation of Checkpoint Lock 
##### [FLINK-14857](https://issues.apache.org/jira/browse/FLINK-14857)

The method
`org.apache.flink.streaming.runtime.tasks.StreamTask#getCheckpointLock()` is
deprecated now. Users should use `MailboxExecutor` to run actions that require
synchronization with the task's thread (e.g. collecting output produced by an
external thread). The methods `MailboxExecutor#yield()` or
`MailboxExecutor#tryYield()` can be used for actions that need to give up
control to other actions temporarily, e.g., if the current operator is
blocked. The `MailboxExecutor` can be accessed by using
`YieldingOperatorFactory` (see `AsyncWaitOperator` for an example usage).

#### Deprecation of OptionsFactory and ConfigurableOptionsFactory interfaces 
##### [FLINK-14926](https://issues.apache.org/jira/browse/FLINK-14926)

Interfaces `OptionsFactory` and `ConfigurableOptionsFactory` have been
deprecated in favor of `RocksDBOptionsFactory` and
`ConfigurableRocksDBOptionsFactory`, respectively.

#### Incompatibility of serialized JobGraphs 
##### [FLINK-14594](https://issues.apache.org/jira/browse/FLINK-14594)

Serialized `JobGraphs` which set the `ResourceSpec` created by Flink versions < `1.10` are no longer compatible with Flink >= `1.10`. 
If you want to migrate these jobs to Flink >= `1.10` you will have to stop the job with a savepoint and then resume it from this savepoint on the Flink >= `1.10` cluster.
