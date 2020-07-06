---
title: "Release Notes - Flink 1.11"
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


These release notes discuss important aspects, such as configuration, behavior,
or dependencies, that changed between Flink 1.10 and Flink 1.11. Please read
these notes carefully if you are planning to upgrade your Flink version to 1.11.

* This will be replaced by the TOC
{:toc}

### Clusters & Deployment
#### Support for Application Mode ([FLIP-85](https://cwiki.apache.org/confluence/display/FLINK/FLIP-85+Flink+Application+Mode))
The user can now submit applications and choose to execute their `main()` method on the cluster rather than the client.
This allows for more light-weight application submission. For more details,
see the [Application Mode documentation](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/#application-mode).
 
#### Web Submission behaves the same as detached mode.
With [FLINK-16657](https://issues.apache.org/jira/browse/FLINK-16657) the web submission logic changes and it exposes
the same behavior as submitting a job through the CLI in detached mode. This implies that, for instance, jobs based on 
the DataSet API that were using sinks like `print()`, `count()` or `collect()` will now throw an exception while 
before the output was simply never printed. See also comments on related [PR](https://github.com/apache/flink/pull/11460).

#### Support for Hadoop 3.0.0 and higher ([FLINK-11086](https://issues.apache.org/jira/browse/FLINK-11086))
Flink project does not provide any updated "flink-shaded-hadoop-*" jars.
Users need to provide Hadoop dependencies through the HADOOP_CLASSPATH environment variable (recommended) or via `lib/` folder.
Also, the `include-hadoop` Maven profile has been removed.

#### `flink-csv` and `flink-json` are bundled in lib folder ([FLINK-18173](https://issues.apache.org/jira/browse/FLINK-18173))
There is no need to download manually jar files for `flink-csv` and `flink-json` formats as they are now bundled in the `lib` folder.

#### Removal of `LegacyScheduler` ([FLINK-15629](https://issues.apache.org/jira/browse/FLINK-15629))
Flink no longer supports the legacy scheduler. 
Hence, setting `jobmanager.scheduler: legacy` will no longer work and fail with an `IllegalArgumentException`. 
The only valid option for `jobmanager.scheduler` is the default value `ng`.

#### Bind user code class loader to lifetime of a slot ([FLINK-16408](https://issues.apache.org/jira/browse/FLINK-16408))
The user code class loader is being reused by the `TaskExecutor` as long as there is at least a single slot allocated for the respective job. 
This changes Flink's recovery behaviour slightly so that it will not reload static fields.
The benefit is that this change drastically reduces pressure on the JVM's metaspace.

#### Replaced `slave` file name with `workers` ([FLINK-18307](https://issues.apache.org/jira/browse/FLINK-18307))
For Standalone Setups, the file with the worker nodes is no longer called `slaves` but `workers`.
Previous setups that use the `start-cluster.sh` and `stop-cluster.sh` scripts need to rename that file.

#### Flink Docker Integration Improvements
The examples of `Dockerfiles` and docker image `build.sh` scripts have been removed from [the Flink Github repository](https://github.com/apache/flink). The examples will no longer be maintained by community in the Flink Github repository, including the examples of integration with Bluemix. Therefore, the following modules have been deleted from the Flink Github repository:
- `flink-contrib/docker-flink`
- `flink-container/docker`
- `flink-container/kubernetes`

Check the updated user documentation for [Flink Docker integration](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html) instead. It now describes in detail how to [use](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html#how-to-run-a-flink-image) and [customize](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html#customize-flink-image) [the Flink official docker image](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html#docker-hub-flink-images): configuration options, logging, plugins, adding more dependencies and installing software. The documentation also includes examples for Session and Job cluster deployments with:
- [docker run](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html#how-to-run-flink-image)
- [docker compose](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html#flink-with-docker-compose)
- [docker swarm](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/docker.html#flink-with-docker-swarm)
- [standalone Kubernetes](https://ci.apache.org/projects/flink/flink-docs-master/ops/deployment/kubernetes.html)

### Memory Management
#### New JobManager Memory Model
##### Overview
With [FLIP-116](https://cwiki.apache.org/confluence/display/FLINK/FLIP-116%3A+Unified+Memory+Configuration+for+Job+Managers), a new memory model has been introduced for the JobManager. New configuration options have been introduced to control the memory consumption of the JobManager process. This affects all types of deployments: standalone, YARN, Mesos, and the new active Kubernetes integration.

Please, check the user documentation for [more details](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_setup_jobmanager.html).

If you try to reuse your previous Flink configuration without any adjustments, the new memory model can result in differently computed memory parameters for the JVM and, thus, performance changes or even failures.
In order to start the JobManager process, you have to specify at least one of the following options [`jobmanager.memory.flink.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-flink-size), [`jobmanager.memory.process.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-process-size) or [`jobmanager.memory.heap.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-heap-size).
See also [the migration guide](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_migration.html#migrate-job-manager-memory-configuration) for more information.

##### Deprecation and breaking changes
The following options are deprecated:
 * `jobmanager.heap.size`
 * `jobmanager.heap.mb`

If these deprecated options are still used, they will be interpreted as one of the following new options in order to maintain backwards compatibility:
 * [JVM Heap](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_setup_jobmanager.html#configure-jvm-heap) ([`jobmanager.memory.heap.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-heap-size)) for standalone and Mesos deployments
 * [Total Process Memory](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_setup_jobmanager.html#configure-total-memory) ([`jobmanager.memory.process.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-process-size)) for containerized deployments (Kubernetes and Yarn)

The following options have been removed and have no effect anymore:
 * `containerized.heap-cutoff-ratio`
 * `containerized.heap-cutoff-min`

There is [no container cut-off](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_migration.html#container-cut-off-memory) anymore.

##### JVM arguments
The `direct` and `metaspace` memory of the JobManager's JVM process are now limited by configurable values:
 * [`jobmanager.memory.off-heap.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-off-heap-size)
 * [`jobmanager.memory.jvm-metaspace.size`](https://ci.apache.org/projects/flink/flink-docs-master/ops/config.html#jobmanager-memory-jvm-metaspace-size)

See also [JVM Parameters](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_setup.html#jvm-parameters).

<span class="label label-warning">Attention</span> These new limits can produce the respective `OutOfMemoryError` exceptions if they are not configured properly or there is a respective memory leak. See also [the troubleshooting guide](https://ci.apache.org/projects/flink/flink-docs-master/ops/memory/mem_trouble.html#outofmemoryerror-direct-buffer-memory).

#### Removal of deprecated mesos.resourcemanager.tasks.mem ([FLINK-15198](https://issues.apache.org/jira/browse/FLINK-15198))
The `mesos.resourcemanager.tasks.mem` option, deprecated in 1.10 in favour of `taskmanager.memory.process.size`, has been completely removed and will have no effect anymore in 1.11+.

### Table API & SQL
#### Blink is now the default planner ([FLINK-16934](https://issues.apache.org/jira/browse/FLINK-16934))
The default table planner has been changed to blink.

#### Changed package structure for Table API ([FLINK-15947](https://issues.apache.org/jira/browse/FLINK-15947))
Due to various issues with packages `org.apache.flink.table.api.scala/java` all classes from those packages were relocated. 
Moreover the scala expressions were moved to `org.apache.flink.table.api` as announced in Flink 1.9.

If you used one of:
* `org.apache.flink.table.api.java.StreamTableEnvironment`
* `org.apache.flink.table.api.scala.StreamTableEnvironment`
* `org.apache.flink.table.api.java.BatchTableEnvironment`
* `org.apache.flink.table.api.scala.BatchTableEnvironment` 

And you do not convert to/from DataStream, switch to:
* `org.apache.flink.table.api.TableEnvironment` 

If you do convert to/from DataStream/DataSet, change your imports to one of:
* `org.apache.flink.table.api.bridge.java.StreamTableEnvironment`
* `org.apache.flink.table.api.bridge.scala.StreamTableEnvironment`
* `org.apache.flink.table.api.bridge.java.BatchTableEnvironment`
* `org.apache.flink.table.api.bridge.scala.BatchTableEnvironment` 

For the Scala expressions use the import:
* `org.apache.flink.table.api._` instead of `org.apache.flink.table.api.bridge.scala._` 

Additionally, if you use Scala's implicit conversions to/from DataStream/DataSet, import `org.apache.flink.table.api.bridge.scala._` instead of `org.apache.flink.table.api.scala._`

#### Removal of deprecated `StreamTableSink` ([FLINK-16362](https://issues.apache.org/jira/browse/FLINK-16362))
The existing `StreamTableSink` implementations should remove `emitDataStream` method.

#### Removal of `BatchTableSink#emitDataSet` ([FLINK-16535](https://issues.apache.org/jira/browse/FLINK-16535))
The existing `BatchTableSink` implementations should rename `emitDataSet` to `consumeDataSet` and return `DataSink`.
  
#### Corrected execution behavior of TableEnvironment.execute() and StreamTableEnvironment.execute() ([FLINK-16363](https://issues.apache.org/jira/browse/FLINK-16363))
In previous versions, `TableEnvironment.execute()` and `StreamExecutionEnvironment.execute()` can both trigger table and DataStream programs.
Since Flink 1.11.0, table programs can only be triggered by `TableEnvironment.execute()`. 
Once table program is converted into DataStream program (through `toAppendStream()` or `toRetractStream()` method), it can only be triggered by `StreamExecutionEnvironment.execute()`.

#### Corrected execution behavior of ExecutionEnvironment.execute() and BatchTableEnvironment.execute() ([FLINK-17126](https://issues.apache.org/jira/browse/FLINK-17126))
In previous versions, `BatchTableEnvironment.execute()` and `ExecutionEnvironment.execute()` can both trigger table and DataSet programs for legacy batch planner.
Since Flink 1.11.0, batch table programs can only be triggered by `BatchEnvironment.execute()`.
Once table program is converted into DataSet program (through `toDataSet()` method), it can only be triggered by `ExecutionEnvironment.execute()`.

#### Added a changeflag to Row type ([FLINK-16998](https://issues.apache.org/jira/browse/FLINK-16998))
An additional change flag called `RowKind` was added to the `Row` type.
This changed the serialization format and will trigger a state migration.

### Configuration
#### Renamed log4j-yarn-session.properties and logback-yarn.xml properties files ([FLINK-17527](https://issues.apache.org/jira/browse/FLINK-17527))
The logging properties files `log4j-yarn-session.properties` and `logback-yarn.xml` have been renamed to `log4j-session.properties` and `logback-session.xml`.
Moreover, `yarn-session.sh` and `kubernetes-session.sh` use these logging properties files.

### State
#### Removal of deprecated background cleanup toggle (State TTL) ([FLINK-15620](https://issues.apache.org/jira/browse/FLINK-15620))
The `StateTtlConfig#cleanupInBackground` has been removed, because the method was deprecated and the background TTL was enabled by default in 1.10.

####  Removal of deprecated option to disable TTL compaction filter ([FLINK-15621](https://issues.apache.org/jira/browse/FLINK-15621))
The TTL compaction filter in RocksDB has been enabled in 1.10 by default and it is now always enabled in 1.11+.
Because of that the following option and methods have been removed in 1.11: 
- `state.backend.rocksdb.ttl.compaction.filter.enabled`
- `StateTtlConfig#cleanupInRocksdbCompactFilter()`
- `RocksDBStateBackend#isTtlCompactionFilterEnabled`
- `RocksDBStateBackend#enableTtlCompactionFilter`
- `RocksDBStateBackend#disableTtlCompactionFilter`
- (state_backend.py) `is_ttl_compaction_filter_enabled`
- (state_backend.py) `enable_ttl_compaction_filter`
- (state_backend.py) `disable_ttl_compaction_filter`

#### Changed argument type of StateBackendFactory#createFromConfig ([FLINK-16913](https://issues.apache.org/jira/browse/FLINK-16913))
Starting from Flink 1.11 the `StateBackendFactory#createFromConfig` interface now takes `ReadableConfig` instead of `Configuration`.
A `Configuration` class is still a valid argument to that method, as it implements the ReadableConfig interface.
Implementors of custom `StateBackend` should adjust their implementations.

#### Removal of deprecated OptionsFactory and ConfigurableOptionsFactory classes ([FLINK-18242](https://issues.apache.org/jira/browse/FLINK-18242))
The deprecated `OptionsFactory` and `ConfigurableOptionsFactory` classes have been removed.
Please use `RocksDBOptionsFactory` and `ConfigurableRocksDBOptionsFactory` instead.
Please also recompile your application codes if any class extends `DefaultConfigurableOptionsFactory`.

#### Enabled by default setTotalOrderSeek ([FLINK-17800](https://issues.apache.org/jira/browse/FLINK-17800))
Since Flink-1.11 the option `setTotalOrderSeek` will be enabled by default for RocksDB's `ReadOptions`.
This is in order to prevent user from miss using `optimizeForPointLookup`.
For backward compatibility we support customizing `ReadOptions` through `RocksDBOptionsFactory`.
Please set `setTotalOrderSeek` back to false if any performance regression observed (it shouldn't happen according to our testing).

#### Increased default size of `state.backend.fs.memory-threshold` ([FLINK-17865](https://issues.apache.org/jira/browse/FLINK-17865))
The default value of `state.backend.fs.memory-threshold` has been increased from 1K to 20K to prevent too many small files created on remote FS for small states.
Jobs with large parallelism on source or stateful operators may have "JM OOM" or "RPC message exceeding maximum frame size" problem with this change.
If you encounter such issues please manually set the configuration back to 1K.

### PyFlink
#### Throw exceptions for the unsupported data types ([FLINK-16606](https://issues.apache.org/jira/browse/FLINK-16606))
DataTypes can be configured with some parameters, e.g., precision.
However in previous releases, the precision provided by users was not taking any effect and default value for the precision was being used.
To avoid confusion since Flink 1.11 exceptions will be thrown if the value is not supported to make it more visible to users. 
Changes include:
- the precision for `TimeType` can only be `0`
- the length for `VarBinaryType`/`VarCharType` can only be `0x7fffffff`
- the precision/scale for `DecimalType` can only be `38`/`18`
- the precision for `TimestampType`/`LocalZonedTimestampType` can only be `3`
- the resolution for `DayTimeIntervalType` can only be `SECOND` and the `fractionalPrecision` can only be `3`
- the resolution for `YearMonthIntervalType` can only be `MONTH` and the `yearPrecision` can only be `2`
- the `CharType`/`BinaryType`/`ZonedTimestampType` is not supported

### Monitoring
#### Converted all MetricReporters to plugins ([FLINK-16963](https://issues.apache.org/jira/browse/FLINK-16963))
All MetricReporters that come with Flink have been converted to plugins.
They should no longer be placed into `/lib` directory (doing so may result in dependency conflicts!), but `/plugins/<some_directory>` instead.

#### Changed of DataDog's metric reporter Counter metrics ([FLINK-15438](https://issues.apache.org/jira/browse/FLINK-15438))
The DataDog metrics reporter now reports counts as the number of events over the reporting interval, instead of the total count. 
This aligns the count semantics with the DataDog documentation.

#### Switch to Log4j 2 by default ([FLINK-15672](https://issues.apache.org/jira/browse/FLINK-15672))
Flink now uses Log4j2 by default. 
Users who wish to revert back to Log4j1 can find instructions to do so in the logging documentation.

#### Changed behaviour of JobManager API's log request ([FLINK-16303](https://issues.apache.org/jira/browse/FLINK-16303))
Requesting an unavailable log or stdout file from the JobManager's HTTP server returns status code 404 now. 
In previous releases, the HTTP server would return a file with `(file unavailable)` as its content.

#### Removal of lastCheckpointAlignmentBuffered metric ([FLINK-16404](https://issues.apache.org/jira/browse/FLINK-16404))
Note that the metric `lastCheckpointAlignmentBuffered` has been removed, because the upstream task will not send any data after emitting a checkpoint barrier until the alignment has been completed on the downstream side. 
The web UI still displays this value but it is always `0` now. 

### Connectors
#### Dropped Kafka 0.8/0.9 connectors ([FLINK-15115](https://issues.apache.org/jira/browse/FLINK-15115))
The Kafka 0.8 and 0.9 connectors are no longer under active development and were removed.

#### Dropped Elasticsearch 2.x connector ([FLINK-16046](https://issues.apache.org/jira/browse/FLINK-16046))
The Elasticsearch 2 connector is no longer under active development and were removed.
Prior version of these connectors will continue to work with Flink. 

#### Removal of deprecated `KafkaPartitioner` ([FLINK-15862](https://issues.apache.org/jira/browse/FLINK-15862))
Deprecated `KafkaPartitioner` was removed. Please see the release notes of Flink 1.3.0 how to migrate from that interface.

#### Refined fallback filesystems to only handle specific filesystems ([FLINK-16015](https://issues.apache.org/jira/browse/FLINK-16015))
By default, if there is an official filesystem plugin for a given schema, it will not be allowed to use fallback filesystem factories (like HADOOP libraries on the classpath) to load it. 
Added `fs.allowed-fallback-filesystems` configuration option to override this behaviour.

#### Deprecation of FileSystem#getKind ([FLINK-16400](https://issues.apache.org/jira/browse/FLINK-16400))
`org.apache.flink.core.fs.FileSystem#getKind` method has been formally deprecated, as it was not used by Flink.

### Runtime
#### Streaming jobs will always fail immediately on failures in synchronous part of a checkpoint ([FLINK-17350](https://issues.apache.org/jira/browse/FLINK-17350))
Failures in synchronous part of checkpointing (like an exceptions thrown by an operator) will fail its Task (and job) immediately, regardless of the configuration parameters.
Since Flink 1.5 such failures could be ignored by setting `setTolerableCheckpointFailureNumber(...)` or its deprecated `setFailTaskOnCheckpointError(...)` predecessor.
Now both options will only affect asynchronous failures.

#### Checkpoint timeouts are no longer ignored by CheckpointConfig#setTolerableCheckpointFailureNumber ([FLINK-17351](https://issues.apache.org/jira/browse/FLINK-17351))
Checkpoint timeouts will now be treated as normal checkpoint failures and checked against value configured by `CheckpointConfig#setTolerableCheckpointFailureNumber(...)`.

### Miscellaneous Interface Changes
#### Removal of deprecated StreamTask#getCheckpointLock() ([FLINK-12484](https://issues.apache.org/jira/browse/FLINK-12484))
DataStream API no longer provides `StreamTask#getCheckpointLock` method, which was deprecated in Flink 1.10. 
Users should use `MailboxExecutor` to run actions that require synchronization with the task's thread (e.g. collecting output produced by an external thread). 
`MailboxExecutor#yield` or `MailboxExecutor#tryYield` methods can be used for actions that should give control to other actions temporarily (equivalent of `StreamTask#getCheckpointLock().wait()`), if the current operator is blocked. 
`MailboxExecutor` can be accessed by using `YieldingOperatorFactory`. Example usage can be found in the `AsyncWaitOperator`.

Note, `SourceFunction.SourceContext.getCheckpointLock` is still available for custom implementations of `SourceFunction` interface. 

#### Reversed dependency from flink-streaming-java to flink-client ([FLINK-15090](https://issues.apache.org/jira/browse/FLINK-15090))
Starting from Flink 1.11.0, the `flink-streaming-java` module does not have a dependency on `flink-clients` anymore. If your project was depending on this transitive dependency you now have to add `flink-clients` as an explicit dependency.

#### AsyncWaitOperator is chainable again ([FLINK-16219](https://issues.apache.org/jira/browse/FLINK-16219))
`AsyncWaitOperator` will be allowed to be chained by default with all operators, except of tasks with `SourceFunction`.
This mostly revert limitation introduced as a bug fix for [FLINK-13063](https://issues.apache.org/jira/browse/FLINK-13063).

#### Changed argument types of ShuffleEnvironment#createInputGates and #createResultPartitionWriters methods ([FLINK-16586](https://issues.apache.org/jira/browse/FLINK-16586))
The argument type of methods `ShuffleEnvironment#createInputGates` and `#createResultPartitionWriters` are adjusted from `Collection` to `List` for satisfying the order guarantee requirement in unaligned checkpoint.
It will break the compatibility if users already implemented a custom `ShuffleService` based on `ShuffleServiceFactory` interface.

#### Deprecation of CompositeTypeSerializerSnapshot#isOuterSnapshotCompatible ([FLINK-17520](https://issues.apache.org/jira/browse/FLINK-17520))
The `boolean isOuterSnapshotCompatible(TypeSerializer)` on the `CompositeTypeSerializerSnapshot` class has been deprecated, in favor of a new `OuterSchemaCompatibility resolveOuterSchemaCompatibility(TypeSerializer)` method.
Please implement that instead.
Compared to the old method, the new method allows composite serializers to signal state schema migration based on outer schema and configuration.

#### Removal of deprecated TimestampExtractor ([FLINK-17655](https://issues.apache.org/jira/browse/FLINK-17655))
The long-deprecated `TimestampExtractor` was removed along with API methods in the DataStream API.
Please use the new `TimestampAssigner` and `WatermarkStrategies` for working with timestamps and watermarks in the DataStream API.

#### Deprecation of ListCheckpointed interface ([FLINK-6258](https://issues.apache.org/jira/browse/FLINK-6258))
The `ListCheckpointed` interface has been deprecated because it uses Java Serialization for checkpointing state which is problematic for savepoint compatibility.
Use the `CheckpointedFunction` interface instead, which gives more control over state serialization.

#### Removal of deprecated state access methods ([FLINK-17376](https://issues.apache.org/jira/browse/FLINK-17376))
We removed deprecated state access methods `RuntimeContext#getFoldingState()`, `OperatorStateStore#getSerializableListState()` and `OperatorStateStore#getOperatorState()`.
This means that some code that was compiled against Flink 1.10 will not work with a Flink 1.11 cluster.
An example of this is our Kafka connector which internally used `OperatorStateStore.getSerializableListState`.
