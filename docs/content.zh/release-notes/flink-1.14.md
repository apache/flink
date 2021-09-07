---
title: "Release Notes - Flink 1.14"
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

# Release Notes - Flink 1.14

These release notes discuss important aspects, such as configuration, behavior, or dependencies,
that changed between Flink 1.13 and Flink 1.14. Please read these notes carefully if you are
planning to upgrade your Flink version to 1.14.

### Known Issues

#### Memory leak with Pulsar connector on Java 11

Netty, that Pulsar client uses underneath, allocates memory differently on Java 11 and 8. With Java
11 it will allocate memory from the pool of Java Direct Memory and is affected by the
MaxDirectMemory limit. Current Pulsar client has no configuration for controlling the memory limits,
which can lead to OOM(s).

Users are advised to use Pulsar connector with Java 8 or overprovision memory for Flink. See 
[here]({{< ref "docs/deployment/memory/mem_setup_tm" >}}) for documentation how to configure memory
for Flink. The proper solution can be tracked in [FLINK-24302](https://issues.apache.org/jira/browse/FLINK-24302).

### Table & SQL

#### Use pipeline name consistently across DataStream API and Table API

##### [FLINK-23646](https://issues.apache.org/jira/browse/FLINK-23646)

The default job name for DataStream API programs in batch mode has changed from `"Flink Streaming Job"` to
`"Flink Batch Job"`. A custom name can be set with config option `pipeline.name`.

#### Propagate unique keys for fromChangelogStream

##### [FLINK-24033](https://issues.apache.org/jira/browse/FLINK-24033)

Compared to 1.13.2, `StreamTableEnvironment.fromChangelogStream` might produce a different stream
because primary keys were not properly considered before.

#### Support new type inference for Table#flatMap

##### [FLINK-16769](https://issues.apache.org/jira/browse/FLINK-16769)

`Table.flatMap()` supports the new type system now. Users are requested to upgrade their functions.

#### Add Scala implicit conversions for new API methods

##### [FLINK-22590](https://issues.apache.org/jira/browse/FLINK-22590)

The Scala implicits that convert between DataStream API and Table API have been updated to the new
methods of FLIP-136.

The changes might require an update of pipelines that used `toTable` or implicit conversions from
`Table` to `DataStream[Row]`.

#### Remove YAML environment file support in SQL Client

##### [FLINK-22540](https://issues.apache.org/jira/browse/FLINK-22540)

The sql-client-defaults.yaml YAML file was deprecated in 1.13 release and now it is completely removed
in this release. As an alternative, you can use the `-i` startup option to execute an initialization SQL
file to setup the SQL Client session. The initialization SQL file can use Flink DDLs to
define available catalogs, table sources and sinks, user-defined functions, and other properties
required for execution and deployment.

See more: https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/dev/table/sqlclient/#initialize-session-using-sql-files

#### Remove the legacy planner code base

##### [FLINK-22864](https://issues.apache.org/jira/browse/FLINK-22864)

The old Table/SQL planner has been removed. BatchTableEnvironment and DataSet API interop with Table
API are not supported anymore. Use the unified TableEnvironment for batch and stream processing with
the new planner or the DataStream API in batch execution mode.

Users are encouraged to update their pipelines. Otherwise Flink 1.13 is the last version that offers
the old functionality.

#### Remove "blink" suffix from table modules

##### [FLINK-22879](https://issues.apache.org/jira/browse/FLINK-22879)

The following Maven modules have been renamed:
* flink-table-planner-blink -> flink-table-planner
* flink-table-runtime-blink -> flink-table-runtime
* flink-table-uber-blink ->flink-table-uber

It might be required to update job JAR dependencies. Note that
flink-table-planner and flink-table-uber used to contain the legacy planner before Flink 1.14 and
now contain the only officially supported planner (i.e. previously known as 'Blink' planner).

#### Remove BatchTableEnvironment and related API classes

##### [FLINK-22877](https://issues.apache.org/jira/browse/FLINK-22877)

Due to the removal of BatchTableEnvironment, BatchTableSource and BatchTableSink have been removed
as well. Use DynamicTableSource and DynamicTableSink instead. They support the old InputFormat and
OutputFormat interfaces as runtime providers if necessary.

#### Remove TableEnvironment#connect

##### [FLINK-23063](https://issues.apache.org/jira/browse/FLINK-23063)

The deprecated `TableEnvironment#connect()` method has been removed. Use the
new `TableEnvironment#createTemporaryTable(String, TableDescriptor)` to create tables
programatically. Please note that this method only supports sources and sinks that comply with
FLIP-95. This is also indicated by the new property design `'connector'='kafka'` instead of `'connector.type'='kafka'`.

#### Deprecate toAppendStream and toRetractStream

##### [FLINK-23330](https://issues.apache.org/jira/browse/FLINK-23330)

The outdated variants of `StreamTableEnvironment.{fromDataStream|toAppendStream|toRetractStream)`
have been deprecated. Use the `(from|to)(Data|Changelog)Stream` alternatives introduced in 1.13.

#### Remove old connectors and formats stack around descriptors

##### [FLINK-23513](https://issues.apache.org/jira/browse/FLINK-23513)

The legacy versions of the SQL Kafka connector and SQL Elasticsearch connector have been removed
together with their corresponding legacy formats. DDL or descriptors that still use `'connector.type='` or
`'format.type='` options need to be updated to the new connector and formats available via the `'connector='` option.

#### Drop BatchTableSource/Sink HBaseTableSource/Sink and related classes

##### [FLINK-22623](https://issues.apache.org/jira/browse/FLINK-22623)

The HBaseTableSource/Sink and related classes including various HBaseInputFormats and
HBaseSinkFunction have been removed. It is possible to read via Table & SQL API and convert the
Table to DataStream API (or vice versa) if necessary. DataSet API is not supported anymore.

#### Drop BatchTableSource ParquetTableSource and related classes

##### [FLINK-22622](https://issues.apache.org/jira/browse/FLINK-22622)

The ParquetTableSource and related classes including various ParquetInputFormats have been removed.
Use the filesystem connector with a Parquet format as a replacement. It is possible to read via
Table & SQL API and convert the Table to DataStream API if necessary. DataSet API is not supported
anymore.

#### Drop BatchTableSource OrcTableSource and related classes

##### [FLINK-22620](https://issues.apache.org/jira/browse/FLINK-22620)

The OrcTableSource and related classes (including OrcInputFormat) have been removed. Use the
filesystem connector with an ORC format as a replacement. It is possible to read via Table & SQL API
and convert the Table to DataStream API if necessary. DataSet API is not supported anymore.

#### Drop usages of BatchTableEnvironment and old planner in Python

##### [FLINK-22619](https://issues.apache.org/jira/browse/FLINK-22619)

The Python API does not offer a dedicated BatchTableEnvironment anymore. Instead, users can switch
to the unified TableEnvironment for both batch and stream processing. Only the Blink planner (the
only remaining planner in 1.14) is supported.

#### Migrate ModuleFactory to the new factory stack

##### [FLINK-23720](https://issues.apache.org/jira/browse/FLINK-23720)

The `LOAD/UNLOAD MODULE` architecture for table modules has been updated to the new factory stack of
FLIP-95. Users of this feature should update their `ModuleFactory` implementations.

#### Migrate Table API to new KafkaSink

##### [FLINK-23639](https://issues.apache.org/jira/browse/FLINK-23639)

Table API/SQL write to Kafka with the new `KafkaSink`. When migrating from a query writing to Kafka
in exactly-once mode from an earlier Flink version, make sure to terminate the old application with
stop-with-savepoint to avoid lingering Kafka transactions. To run in exactly-once processing mode,
the sink needs a user-configured and unique transaction prefix, such that transactions of different
applications do not interfere with each other.

### DataStream API

#### Allow @TypeInfo annotation on POJO field declarations

##### [FLINK-12141](https://issues.apache.org/jira/browse/FLINK-12141)

`@TypeInfo` annotations can now also be used on POJO fields which, for example, can help to define
custom serializers for third-party classes that can otherwise not be annotated themselves.

#### Clarify SourceFunction#cancel() contract about interrupting

##### [FLINK-23527](https://issues.apache.org/jira/browse/FLINK-23527)

Contract of the `SourceFunction#cancel()` method with respect to interruptions has been clarified:
- source itself shouldn’t be interrupting the source thread
- interrupt shouldn’t be expected in the clean cancellation case

#### Expose a consistent GlobalDataExchangeMode

##### [FLINK-23402](https://issues.apache.org/jira/browse/FLINK-23402)

The default DataStream API shuffle mode for batch executions has been changed to blocking exchanges
for all edges of the stream graph. A new option `execution.batch-shuffle-mode` allows to change it
to pipelined behavior if necessary.

### Python API

#### Support loopback mode to allow Python UDF worker and client reuse the same Python VM

##### [FLINK-21222](https://issues.apache.org/jira/browse/FLINK-21222)

Instead of launching a separate Python process, it will reuse the Python process of the client side
to execute Python UDFs if you are running jobs locally. This makes it easy for Python UDFs
debugging.

#### Support Python UDF chaining in Python DataStream API

##### [FLINK-22913](https://issues.apache.org/jira/browse/FLINK-22913)

The job graph of Python DataStream API jobs may be different from before as the Python functions
will be chained as much as possible to optimize the performance. You could disable Python functions
chaining by setting `python.operator-chaining.enabled` as `false` explicitly.

### Connectors

#### Implement FLIP-179: Expose Standardized Operator Metrics

##### [FLINK-23652](https://issues.apache.org/jira/browse/FLINK-23652)

Connectors using unified Source and Sink interface expose certain standardized metrics
automatically.

#### Port KafkaSink to new Unified Sink API (FLIP-143)

##### [FLINK-22902](https://issues.apache.org/jira/browse/FLINK-22902)

`KafkaSink` supersedes `FlinkKafkaProducer` and provides efficient exactly-once and at-least-once
writing with the new unified sink interface supporting both batch and streaming mode of DataStream
API. To upgrade, please stop with savepoint. To run in exactly-once processing mode, `KafkaSink`
needs a user-configured and unique transaction prefix, such that transactions of different
applications do not interfere with each other.

#### Deprecate FlinkKafkaConsumer

##### [FLINK-24055](https://issues.apache.org/jira/browse/FLINK-24055)

`FlinkKafkaConsumer` has been deprecated in favor of `KafkaSource`. To upgrade to the new version,
please store the offsets in Kafka with `setCommitOffsetsOnCheckpoints` in the
old `FlinkKafkaConsumer` and then stop with a savepoint. When resuming from the savepoint, please
use `setStartingOffsets(OffsetsInitializer.committedOffsets())` in the new `KafkaSourceBuilder` to
transfer the offsets to the new source.

#### InputStatus should not contain END_OF_RECOVERY

##### [FLINK-23474](https://issues.apache.org/jira/browse/FLINK-23474)

`InputStatus.END_OF_RECOVERY` was removed. It was an internal flag that should never be returned from
SourceReaders. Returning that value in earlier versions might lead to misbehaviour.

#### Connector-base exposes dependency to flink-core.

##### [FLINK-22964](https://issues.apache.org/jira/browse/FLINK-22964)

Connectors do not transitively hold a reference to `flink-core` anymore. That means that a fat jar
with a connector does not include `flink-core` with this fix.

### Runtime & Coordination

#### Increase akka.ask.timeout for tests using the MiniCluster

##### [FLINK-23906](https://issues.apache.org/jira/browse/FLINK-23906)

The default `akka.ask.timeout` used by the `MiniCluster` has been increased to 5 minutes. If you
want to use a smaller value, then you have to set it explicitly in the passed configuration.

The change is due to the fact that messages cannot get lost in a single-process minicluster, so this
timeout (which otherwise helps to detect message loss in distributed setups) has no benefit here.

The increased timeout reduces the number of false-positive timeouts, for example during heavy tests
on loaded CI/CD workers or during debugging.

#### The node IP obtained in NodePort mode is a VIP

##### [FLINK-23507](https://issues.apache.org/jira/browse/FLINK-23507)

When using `kubernetes.rest-service.exposed.type=NodePort`, connection string for Rest gateway is
now correctly constructed in form of `<nodeIp>:<nodePort>` instead of
`<kubernetesApiServerUrl>:<nodePort>`. This may be a breaking change for some users.

This also introduces a new config option `kubernetes.rest-service.exposed.node-port-address-type` that
lets you select `<nodeIp>` from a desired range.

#### Timeout heartbeat if the heartbeat target is no longer reachable

##### [FLINK-23209](https://issues.apache.org/jira/browse/FLINK-23209)

Flink now supports to detect dead TaskManagers via the number of consecutive failed heartbeat RPCs.
The threshold until a TaskManager is marked as unreachable can be configured
via `heartbeat.rpc-failure-threshold`. This can speed up the detection of dead TaskManagers
significantly.

#### RPCs fail faster when target is unreachable

##### [FLINK-23202](https://issues.apache.org/jira/browse/FLINK-23202)

The same way Flink detects unreachable heartbeat targets faster, Flink now also immediately fails
RPCs where the target is known by the OS to be unreachable on a network level, instead of waiting
for a timeout (`akka.ask.timeout`).

One effect this are faster task failovers, because cancelling tasks on a dead TaskExecutor no longer
gets delayed by the RPC timeout.

If this faster failover is a problem in certain setups (which might rely on the fast that external
systems hit timeouts), we recommend configuring the application's restart strategy with a restart
delay.

#### Changes in accounting of IOExceptions when triggering checkpoints on JobManager

##### [FLINK-23189](https://issues.apache.org/jira/browse/FLINK-23189)

In previous versions, IOExceptions thrown from the JobManager, would not fail the entire Job. We
changed the way we bookkeep those exceptions and now they do increase the number of checkpoint
failures.

The number of tolerable checkpoint failures can be adjusted or disabled via:
`org.apache.flink.streaming.api.environment.CheckpointConfig#setTolerableCheckpointFailureNumber` (
which is set to 0 by default).

#### Refine ShuffleMaster lifecycle management for pluggable shuffle service framework

##### [FLINK-22910](https://issues.apache.org/jira/browse/FLINK-22910)

We improved the ShuffleMaster interface by adding some lifecycle methods, including open, close,
registerJob and unregisterJob. Besides, the ShuffleMaster now becomes a cluster level service which
can be shared by multiple jobs. This is a breaking change to the pluggable shuffle service framework
and the customized shuffle plugin needs to adapt to the new interface accordingly.

#### Group job specific ZooKeeper HA services under common jobs/<JobID> zNode

##### [FLINK-22636](https://issues.apache.org/jira/browse/FLINK-22636)

The ZooKeeper job-specific HA services are now grouped under a zNode with the respective `JobID`.
Moreover, the config options `high-availability.zookeeper.path.latch`
, `high-availability.zookeeper.path.leader`, `high-availability.zookeeper.path.checkpoints`
and `high-availability.zookeeper.path.checkpoint-counter` have been removed and, thus, have no
longer an effect.

#### Fallback value for taskmanager.slot.timeout

##### [FLINK-22002](https://issues.apache.org/jira/browse/FLINK-22002)

The config option `taskmanager.slot.timeout` falls now back to `akka.ask.timeout` if no value has
been configured. Previously the default value for `taskmanager.slot.timeout` was `10 s`. 

#### DuplicateJobSubmissionException after JobManager failover

##### [FLINK-21928](https://issues.apache.org/jira/browse/FLINK-21928)

The fix for this problem only works if the ApplicationMode is used with a single job submission and
if the user code does not access the `JobExecutionResult`. If any of these conditions is violated,
then Flink cannot guarantee that the whole Flink application is executed.

Additionally, it is still required that the user cleans up the corresponding HA entries for the
running jobs registry because these entries won't be reliably cleaned up when encountering the
situation described by FLINK-21928.

#### Zookeeper node under leader and leaderlatch is not deleted after job finished

##### [FLINK-20695](https://issues.apache.org/jira/browse/FLINK-20695)

The `HighAvailabilityServices` have received a new method `cleanupJobData` which can be implemented
in order to clean up job related HA data after a given job has terminated.

#### Optimize scheduler performance for large-scale jobs

##### [FLINK-21110](https://issues.apache.org/jira/browse/FLINK-21110)

The performance of scheduler has been improved to reduce the time of execution graph creation,
task deployment and task failover. This improvement is significant to large scale jobs which
currently may spend minutes on the processes mentioned above. This improvement also helps to avoid
cases that the job manager main thread gets blocked for too long and leads to heartbeat timeout.

### Checkpoints

#### The semantic of alignmentTimeout configuration has changed the meaning

##### [FLINK-23041](https://issues.apache.org/jira/browse/FLINK-23041)

The semantic of alignmentTimeout configuration has changed the meaning and now it's measured as the
time between the start of a checkpoint(on the checkpoint coordinator) and the time when the
checkpoint barrier is received by a task.

#### Disabled unaligned checkpoints for BROADCAST exchanges

##### [FLINK-22815](https://issues.apache.org/jira/browse/FLINK-22815)

Broadcast partitioning can not work with unaligned checkpointing. There are no guarantees that
records are consumed at the same rate in all channels. This can result in some tasks applying state
changes corresponding to a certain broadcasted event while others don't. In turn upon restore, it
may lead to an inconsistent state.

#### DefaultCompletedCheckpointStore drops unrecoverable checkpoints silently

##### [FLINK-22502](https://issues.apache.org/jira/browse/FLINK-22502)

On recovery, if a failure occurs during retrieval of a checkpoint, the job is restarted (instead of
skipping the checkpoint in some circumstances). This prevents potential consistency violations.

#### Removal of CompletedCheckpointRecover#recover() method.

##### [FLINK-22483](https://issues.apache.org/jira/browse/FLINK-22483)

Flink no longer re-loads checkpoint metadata from the external storage before restoring the task
state after the failover (except when the JobManager fails over / changes leadership). This results
in less external I/O and faster failover.

Please note that this changes a public interfaces around `CompletedCheckpointStore`, that we allow
overriding by providing custom implementation of HA Services.

#### Removal of the deprecated CheckpointConfig#setPreferCheckpointForRecovery method.

##### [FLINK-20427](https://issues.apache.org/jira/browse/FLINK-20427)

Deprecated method CheckpointConfig#setPreferCheckpointForRecovery was removed, because preferring
older checkpoints over newer savepoints for recovery can lead to data loss.

### Dependency upgrades

#### Bump up RocksDb version to 6.20.3

##### [FLINK-14482](https://issues.apache.org/jira/browse/FLINK-14482)

RocksDB has been upgraded to 6.20.3. Compared to the previous version, the new version contains lots
of bug fixes, ARM platform supported, musl library supported, and more attractive features. However,
the new version can entail at most 8% performance regression according to our tests.

See the corresponding ticket for more information.

#### Support configuration of the RocksDB info logging via configuration

##### [FLINK-23812](https://issues.apache.org/jira/browse/FLINK-23812)

With RocksDB bumped to 6.20.3 (FLINK-14482), you can now also configure a rolling info logging
strategy by configuring it accordingly via newly added state.backend.rocksdb.log.* settings. This
can be helpful for debugging RocksDB (performance) issues in containerized environments where the
local data dir is volatile but the logs should be retained on a separate volume mount.

#### Make flink-runtime scala-free

##### [FLINK-14105](https://issues.apache.org/jira/browse/FLINK-14105)

Flink's akka dependency is now loaded with a separate classloader and no longer accessible from the
outside.

Resulting from that various modules, most prominently flink-runtime, no longer have a
scala suffix in their artifactId.

#### Drop Mesos support

##### [FLINK-23118](https://issues.apache.org/jira/browse/FLINK-23118)

The support for Apache Mesos has been removed.
