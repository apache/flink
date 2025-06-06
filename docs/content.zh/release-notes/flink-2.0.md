---
title: "Release Notes - Flink 2.0"
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

# Release notes - Flink 2.0

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 1.20 and Flink 2.0. Please read these notes carefully if you are 
planning to upgrade your Flink version to 2.0.

## New Features & Behavior Changes

### State & Checkpoints

#### Disaggregated State Storage and Management

##### [FLINK-32070](https://issues.apache.org/jira/browse/FLINK-32070)

The past decade has witnessed a dramatic shift in Flink's deployment mode, workload patterns, and hardware improvements. We've moved from the map-reduce era where workers are computation-storage tightly coupled nodes to a cloud-native world where containerized deployments on Kubernetes become standard. To enable Flink's Cloud-Native future, we introduce Disaggregated State Storage and Management that uses remote storage as primary storage in Flink 2.0.

This new architecture solves the following challenges brought in the cloud-native era for Flink.
1. Local Disk Constraints in containerization
2. Spiky Resource Usage caused by compaction in the current state model
3. Fast Rescaling for jobs with large states (hundreds of Terabytes)
4. Light and Fast Checkpoint in a native way

While extending the state store to interact with remote DFS seems like a straightforward solution, but it is insufficient due to Flink's existing blocking execution model. To overcome this limitation, Flink 2.0 introduces an asynchronous execution model alongside a disaggregated state backend, with newly designed SQL operators performing asynchronous state access in parallel.

#### Native file copy support

##### [FLINK-35739](https://issues.apache.org/jira/browse/FLINK-35739)

Users can now configure Flink to use [s5cmd](https://github.com/peak/s5cmd) to speed up downloading files from S3 during the recovery process, when using RocksDB, at least a factor of 2.

#### New efficiency improvements for AdaptiveScheduler

##### [FLINK-35549](https://issues.apache.org/jira/browse/FLINK-35549)

This enables the user to synchronize checkpointing and rescaling in the AdaptiveScheduler, so that minimize reprocessing time.

### Runtime & Coordination

#### Further Optimization of Adaptive Batch Execution

##### [FLINK-36333](https://issues.apache.org/jira/browse/FLINK-36333), [FLINK-36159](https://issues.apache.org/jira/browse/FLINK-36159)

Flink possesses adaptive batch execution capabilities that optimize execution plans based on runtime information to enhance performance. Key features include dynamic partition pruning, Runtime Filter, and automatic parallelism adjustment based on data volume. In Flink 2.0, we have further strengthened these capabilities with two new optimizations:

*[Adaptive Broadcast Join](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/adaptive_batch/#adaptive-broadcast-join)* - Compared to Shuffled Hash Join and Sort Merge Join, Broadcast Join eliminates the need for large-scale data shuffling and sorting, delivering superior execution efficiency. However, its applicability depends on one side of the input being sufficiently small; otherwise, performance or stability issues may arise. During the static SQL optimization phase, accurately estimating the input data volume of a Join operator is challenging, making it difficult to determine whether Broadcast Join is suitable. By enabling adaptive execution optimization, Flink dynamically captures the actual input conditions of Join operators at runtime and automatically switches to Broadcast Join when criteria are met, significantly improving execution efficiency.

*[Automatic Join Skew Optimization](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/adaptive_batch/#adaptive-skewed-join-optimization)* - In Join operations, frequent occurrences of specific keys may lead to significant disparities in data volumes processed by downstream Join tasks. Tasks handling larger data volumes can become long-tail bottlenecks, severely delaying overall job execution. Through the Adaptive Skewed Join optimization, Flink leverages runtime statistical information from Join operator inputs to dynamically split skewed data partitions while ensuring the integrity of Join results. This effectively mitigates long-tail latency caused by data skew.

See more details about the capabilities and usages of Flink's [Adaptive Batch Execution](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/deployment/adaptive_batch/).

#### Adaptive Scheduler respects `execution.state-recovery.from-local` flag now

##### [FLINK-36201](https://issues.apache.org/jira/browse/FLINK-36201)

AdaptiveScheduler now respects `execution.state-recovery.from-local` flag, which defaults to false. As a result you now need to opt-in to make [local recovery](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/ops/state/large_state_tuning/#task-local-recovery) work.

#### Align the desired and sufficient resources definition in Executing and WaitForResources states

##### [FLINK-36014](https://issues.apache.org/jira/browse/FLINK-36014)

The new configuration `jobmanager.adaptive-scheduler.executing.resource-stabilization-timeout` for the AdaptiveScheduler was introduced. It defines a duration for which the JobManager delays the scaling operation after a resource change until sufficient resources are available.

The existing configuration `jobmanager.adaptive-scheduler.min-parallelism-increase` was deprecated in Flink 2.0.

#### Incorrect watermark idleness timeout accounting when subtask is backpressured/blocked

##### [FLINK-35886](https://issues.apache.org/jira/browse/FLINK-35886)

For detecting idleness, the way idleness timeout is calculated has changed. Previously the time, when source or source's split was backpressured or blocked due to watermark alignment, was incorrectly accounted for when determining idleness timeout. This could lead to a situation where sources or some splits were incorrectly switching to idle, while they were being unable to make any progress and had some more records to emit, which in turn could result in incorrectly calculated watermarks and erroneous late data. This has been fixed for 2.0.

This change will introduce a new public api `org.apache.flink.api.common.eventtime.WatermarkGeneratorSupplier.Context#getInputActivityClock`. However, this will not create compatibility problems for users upgrading from prior Flink versions.

### Table & SQL

#### Materialized Table

##### [FLINK-35187](https://issues.apache.org/jira/browse/FLINK-35187)

Materialized Tables represent a cornerstone of our vision to unify stream and batch processing paradigms. These tables enable users to declaratively manage both real-time and historical data through a single pipeline, eliminating the need for separate codebases or workflows.

In this release, with a focus on production-grade operability, we have done critical enhancements to simplify lifecycle management and execution in real-world environments:

**Query Modifications** - Materialized Tables now support schema and query updates, enabling seamless iteration of business logic without reprocessing historical data. This is vital for production scenarios requiring rapid schema evolution and computational adjustments.

**Kubernetes/Yarn Submission** - Beyond standalone clusters, Flink 2.0 extends native support for submitting Materialized Table refresh jobs to YARN and Kubernetes clusters. This allows users to seamlessly integrate refresh workflows into their production-grade infrastructure, leveraging standardized resource management, fault tolerance, and scalability.

**Ecosystem Integration** - Collaborating with the [Apache Paimon](https://paimon.apache.org/) community, Materialized Tables now integrate natively with Paimon’s lake storage format, combining Flink’s stream-batch compute with Paimon’s high-performance ACID transactions for unified data serving.

By streamlining modifications and execution on production infrastructure, Materialized Tables empower teams to unify streaming and batch pipelines with higher reliability. Future releases will deepen production support, including integration with a production-ready schedulers to enable policy-driven refresh automation.

#### SQL gateway supports application mode

##### [FLINK-36702](https://issues.apache.org/jira/browse/FLINK-36702)

SQL gateway now supports executing SQL jobs in application mode, serving as a replacement of the removed per-job deployment mode.

#### SQL Syntax Enhancements

##### [FLINK-31836](https://issues.apache.org/jira/browse/FLINK-31836)

Flink SQL now supports C-style escape strings. See the [documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/sql/queries/overview/#syntax) for more details.

A new `QUALIFY` clause has been added as a more concise syntax for filtering outputs of window functions. See the [Top-N](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/sql/queries/topn/) and [Deduplication](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/table/sql/queries/deduplication/) examples.

### Connectors

#### Support Custom Data Distribution for Input Stream of Lookup Join

##### [FLINK-35652](https://issues.apache.org/jira/browse/FLINK-35652)

Lookup Join is an important feature in Flink, It is typically used to enrich a table with data that is queried from an external system. If we interact with the external systems for each incoming record, we incur significant network I/O and RPC overhead. Therefore, most connectors supporting lookup introduce caching to reduce the per-record level query overhead, such as Hbase and JDBC. However, because the data distribution of Lookup Join's input stream is arbitrary, the cache hit rate is sometimes unsatisfactory. External systems may have different requirements for data distribution on the input side, and Flink does not have this knowledge. Flink 2.0 introduces a mechanism for the connector to tell the Flink planner its desired input stream data distribution or partitioning strategy. This can significantly reduce the amount of cached data and improve performance of Lookup Joins.

#### Sink with topologies should not participate in unaligned checkpoint

##### [FLINK-36287](https://issues.apache.org/jira/browse/FLINK-36287)

In Flink 2.0, we disable unaligned checkpoints for all connections of operators within the sink expansion(committer, and any pre/post commit topology). This is necessary because committables need to be at the respective operators on `notifyCheckpointComplete` or else we can't commit all side effects, which violates the contract of `notifyCheckpointComplete`.

### Configuration

#### Migrate string configuration key to ConfigOption

##### [FLINK-34079](https://issues.apache.org/jira/browse/FLINK-34079)

We have deprecated all `setXxx` and `getXxx` methods except `getString(String key, String defaultValue)` and `setString(String key, String value)`, such as: `setInteger`, `setLong`, `getInteger` and `getLong` etc. We strongly recommend users use `get(ConfigOption<T> option)` and `set(ConfigOption<T> option, T value)` methods directly.

#### Remove flink-conf.yaml from flink dist

##### [FLINK-33677](https://issues.apache.org/jira/browse/FLINK-33677)

[FLIP-366](https://cwiki.apache.org/confluence/x/YZuzDw) introduces support for parsing standard YAML files for Flink configuration. A new configuration file named config.yaml, which adheres to the standard YAML format, has been introduced.

In the Flink 2.0, we have removed the old configuration file from the flink-dist, along with support for the old configuration parser. Users are now required to use the new config.yaml file when starting the cluster and submitting jobs. They can utilize the [migration tool](https://nightlies.apache.org/flink/flink-docs-release-2.0/zh/docs/deployment/config/#migration-tool) to assist in migrating legacy configuration files to the new format.

### MISC

#### DataStream V2 API

##### [FLINK-34547](https://issues.apache.org/jira/browse/FLINK-34547)

The DataStream API is one of the two main APIs that Flink provides for writing data processing programs. As an API that was introduced practically since day 1 of the project and has been evolved for nearly a decade, we are observing more and more problems with it. Addressing these problems breaks the existing DataStream API, which makes in-place refactor impractical. Therefore, we propose to introduce a new set of APIs, the DataStream API V2, to gradually replace the original DataStream API.

In Flink 2.0, we provide the experimental version of the new DataStream V2 API. It contains the low-level building blocks (DataStream, ProcessFunction, Partitioning), context and primitives like state, time service, watermark processing. At the same time, we also provide some high-level extensions, such as window and join. The high level extensions make it is simpler to work with the APIs for many cases, though if you want more flexibility / control, you can use the low level APIs.

See DataStream API(V2) [Documentation](https://nightlies.apache.org/flink/flink-docs-release-2.0/docs/dev/datastream-v2/overview/) for more details

**NOTICE:** The new DataStream API is currently in the experimental stage and is not yet stable, thus not recommended for production usage at the moment.

#### Java Supports

##### [FLINK-33163](https://issues.apache.org/jira/browse/FLINK-33163)

Starting the 2.0 version, Flink officially supports Java 21.

The default and recommended Java version is changed to Java 17 (previously Java 11). This change mainly effects the docker images and building Flink from source.

Meanwhile, Java 8 is no longer supported.

#### Serialization Improvements

##### [FLINK-34037](https://issues.apache.org/jira/browse/FLINK-34037)

Flink 2.0 introduces much more efficient built-in serializers for collection types (i.e., Map / List / Set), which are enabled by default.

We have also upgraded Kryo to version 5.6, which is faster, more memory efficient, and has better supports for newer Java versions.

# Highlight Breaking Changes

## API

The following sets of APIs have been completely removed.
- **DataSet API** - Please migrate to DataStream API, or Table API/SQL as applicable. See also ["How to Migrate from DataSet to DataStream"](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/datastream/dataset_migration/).
- **Scala DataStream and DataSet API** - Please migrate to the Java DataStream API.
- **SourceFunction, SinkFunction and Sink V1** - Please migrate to Source and Sink V2.
- **TableSource and TableSink** - Please migrate to DynamicTableSource and DynamicTableSink. See also "User-defined Sources & Sinks".
- **TableSchema, TableColumn and Types** - Please migrate to Schema, Column and DataTypes respectively.

Some deprecated methods have been removed from DataStream API. See also the [list of breaking programming APIs](#list-of-breaking-change-programming-apis-a-namebreaking_programming_apis-).

Some deprecated fields have been removed from the REST API. See also the [list of breaking REST APIs](#list-of-rest-apis-changes).

**NOTICE:** You may find some of the removed APIs still exist in the code base, usually in a different package. They are for internal usage only and can be changed / removed anytime without notifications. Please **DO NOT USE** them.

### Connector Adaption Plan

As SourceFunction, SinkFunction and SinkV1 being removed, existing connectors depending on these APIs will not work on the Flink 2.x series. Here’s the plan for adapting connectors that officially maintained by community.
- A new version of Kafka, Paimon, JDBC and ElasticSearch connectors, adapted to the API changes, will be released right after the release of Flink 2.0.0.
- We plan to gradually migrate the remaining officially maintained connectors within 3 subsequent minor releases (i.e., by Flink 2.3).

## Configuration

Configuration options that met the following criteria are removed in Flink 2.0. See also the [list of removed configuration options](#list-of-removed-configuration-options-a-nameremoved_configs-).
- Annotated as `@Public` and have been deprecated for at least 2 minor releases.
- Annotated as `@PublicEvolving` and have been deprecated for at least 1 minor releases.

The legacy configuration file `flink-conf.yaml` is no longer supported. Please use `config.yaml` that uses standard YAML format instead. A migration tool is provided to convert a legacy `flink-conf.yaml` into a new `config.yaml`. See "Migrate from flink-conf.yaml to config.yaml" for more details.

Configuration APIs that takes java objects as arguments are removed from `StreamExecutionEnvironment` and `ExecutionConfig`. They should now be set via `Configuration` and `ConfigOption`. See also the list of breaking programming APIs.

To avoid exposing internal interfaces, User-Defined Functions no longer have full access to `ExecutionConfig`. Instead, necessary functions such as `createSerializer()`, `getGlobalJobParameters()` and `isObjectReuseEnabled()` can now be accessed from the `RuntimeContext` directly.

## Misc

- State Compatibility is not guaranteed between 1.x and 2.x.
- Java 8 is no longer supported. The minimum Java version supported by Flink is Java 11.
- The Per-job deployment mode has been removed. Please use the Application mode instead.
- Legacy Mode of Hybrid Shuffle is removed.

# Appendix

## List of breaking change programming APIs <a name="breaking_programming_apis" />

### Removed Classes
- `org.apache.flink.api.common.ExecutionConfig$SerializableSerializer`
- `org.apache.flink.api.common.ExecutionMode`
- `org.apache.flink.api.common.InputDependencyConstraint`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies$ExponentialDelayRestartStrategyConfiguration`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies$FailureRateRestartStrategyConfiguration`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies$FallbackRestartStrategyConfiguration`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies$FixedDelayRestartStrategyConfiguration`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies$NoRestartStrategyConfiguration`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies$RestartStrategyConfiguration`
- `org.apache.flink.api.common.restartstrategy.RestartStrategies`
- `org.apache.flink.api.common.time.Time`
- `org.apache.flink.api.connector.sink.Committer`
- `org.apache.flink.api.connector.sink.GlobalCommitter`
- `org.apache.flink.api.connector.sink.Sink$InitContext`
- `org.apache.flink.api.connector.sink.Sink$ProcessingTimeService$ProcessingTimeCallback`
- `org.apache.flink.api.connector.sink.Sink$ProcessingTimeService`
- `org.apache.flink.api.connector.sink.SinkWriter$Context`
- `org.apache.flink.api.connector.sink.SinkWriter`
- `org.apache.flink.api.connector.sink.Sink`
- `org.apache.flink.api.connector.sink2.Sink$InitContextWrapper`
- `org.apache.flink.api.connector.sink2.Sink$InitContext`
- `org.apache.flink.api.connector.sink2.StatefulSink$StatefulSinkWriter`
- `org.apache.flink.api.connector.sink2.StatefulSink$WithCompatibleState`
- `org.apache.flink.api.connector.sink2.StatefulSink`
- `org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink$PrecommittingSinkWriter`
- `org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink`
- `org.apache.flink.api.java.CollectionEnvironment`
- `org.apache.flink.api.java.DataSet`
- `org.apache.flink.api.java.ExecutionEnvironmentFactory`
- `org.apache.flink.api.java.ExecutionEnvironment`
- `org.apache.flink.api.java.LocalEnvironment`
- `org.apache.flink.api.java.RemoteEnvironment`
- `org.apache.flink.api.java.aggregation.Aggregations`
- `org.apache.flink.api.java.aggregation.UnsupportedAggregationTypeException`
- `org.apache.flink.api.java.functions.FlatMapIterator`
- `org.apache.flink.api.java.functions.FunctionAnnotation$ForwardedFieldsFirst`
- `org.apache.flink.api.java.functions.FunctionAnnotation$ForwardedFieldsSecond`
- `org.apache.flink.api.java.functions.FunctionAnnotation$ForwardedFields`
- `org.apache.flink.api.java.functions.FunctionAnnotation$NonForwardedFieldsFirst`
- `org.apache.flink.api.java.functions.FunctionAnnotation$NonForwardedFieldsSecond`
- `org.apache.flink.api.java.functions.FunctionAnnotation$NonForwardedFields`
- `org.apache.flink.api.java.functions.FunctionAnnotation$ReadFieldsFirst`
- `org.apache.flink.api.java.functions.FunctionAnnotation$ReadFieldsSecond`
- `org.apache.flink.api.java.functions.FunctionAnnotation$ReadFields`
- `org.apache.flink.api.java.functions.FunctionAnnotation`
- `org.apache.flink.api.java.functions.GroupReduceIterator`
- `org.apache.flink.api.java.io.CollectionInputFormat`
- `org.apache.flink.api.java.io.CsvOutputFormat`
- `org.apache.flink.api.java.io.CsvReader`
- `org.apache.flink.api.java.io.DiscardingOutputFormat`
- `org.apache.flink.api.java.io.IteratorInputFormat`
- `org.apache.flink.api.java.io.LocalCollectionOutputFormat`
- `org.apache.flink.api.java.io.ParallelIteratorInputFormat`
- `org.apache.flink.api.java.io.PrimitiveInputFormat`
- `org.apache.flink.api.java.io.PrintingOutputFormat`
- `org.apache.flink.api.java.io.RowCsvInputFormat`
- `org.apache.flink.api.java.io.SplitDataProperties$SourcePartitionerMarker`
- `org.apache.flink.api.java.io.SplitDataProperties`
- `org.apache.flink.api.java.io.TextInputFormat`
- `org.apache.flink.api.java.io.TextOutputFormat$TextFormatter`
- `org.apache.flink.api.java.io.TextOutputFormat`
- `org.apache.flink.api.java.io.TextValueInputFormat`
- `org.apache.flink.api.java.io.TypeSerializerInputFormat`
- `org.apache.flink.api.java.io.TypeSerializerOutputFormat`
- `org.apache.flink.api.java.operators.AggregateOperator`
- `org.apache.flink.api.java.operators.CoGroupOperator$CoGroupOperatorSets`
- `org.apache.flink.api.java.operators.CoGroupOperator`
- `org.apache.flink.api.java.operators.CrossOperator$DefaultCross`
- `org.apache.flink.api.java.operators.CrossOperator$ProjectCross`
- `org.apache.flink.api.java.operators.CrossOperator`
- `org.apache.flink.api.java.operators.CustomUnaryOperation`
- `org.apache.flink.api.java.operators.DataSink`
- `org.apache.flink.api.java.operators.DataSource`
- `org.apache.flink.api.java.operators.DeltaIteration$SolutionSetPlaceHolder`
- `org.apache.flink.api.java.operators.DeltaIteration$WorksetPlaceHolder`
- `org.apache.flink.api.java.operators.DeltaIterationResultSet`
- `org.apache.flink.api.java.operators.DeltaIteration`
- `org.apache.flink.api.java.operators.DistinctOperator`
- `org.apache.flink.api.java.operators.FilterOperator`
- `org.apache.flink.api.java.operators.FlatMapOperator`
- `org.apache.flink.api.java.operators.GroupCombineOperator`
- `org.apache.flink.api.java.operators.GroupReduceOperator`
- `org.apache.flink.api.java.operators.Grouping`
- `org.apache.flink.api.java.operators.IterativeDataSet`
- `org.apache.flink.api.java.operators.JoinOperator$DefaultJoin`
- `org.apache.flink.api.java.operators.JoinOperator$EquiJoin`
- `org.apache.flink.api.java.operators.JoinOperator$JoinOperatorSets$JoinOperatorSetsPredicate`
- `org.apache.flink.api.java.operators.JoinOperator$JoinOperatorSets`
- `org.apache.flink.api.java.operators.JoinOperator$ProjectJoin`
- `org.apache.flink.api.java.operators.JoinOperator`
- `org.apache.flink.api.java.operators.MapOperator`
- `org.apache.flink.api.java.operators.MapPartitionOperator`
- `org.apache.flink.api.java.operators.Operator`
- `org.apache.flink.api.java.operators.PartitionOperator`
- `org.apache.flink.api.java.operators.ProjectOperator`
- `org.apache.flink.api.java.operators.ReduceOperator`
- `org.apache.flink.api.java.operators.SingleInputOperator`
- `org.apache.flink.api.java.operators.SingleInputUdfOperator`
- `org.apache.flink.api.java.operators.SortPartitionOperator`
- `org.apache.flink.api.java.operators.SortedGrouping`
- `org.apache.flink.api.java.operators.TwoInputOperator`
- `org.apache.flink.api.java.operators.TwoInputUdfOperator`
- `org.apache.flink.api.java.operators.UdfOperator`
- `org.apache.flink.api.java.operators.UnionOperator`
- `org.apache.flink.api.java.operators.UnsortedGrouping`
- `org.apache.flink.api.java.operators.join.JoinFunctionAssigner`
- `org.apache.flink.api.java.operators.join.JoinOperatorSetsBase$JoinOperatorSetsPredicateBase`
- `org.apache.flink.api.java.operators.join.JoinOperatorSetsBase`
- `org.apache.flink.api.java.operators.join.JoinType`
- `org.apache.flink.api.java.summarize.BooleanColumnSummary`
- `org.apache.flink.api.java.summarize.ColumnSummary`
- `org.apache.flink.api.java.summarize.NumericColumnSummary`
- `org.apache.flink.api.java.summarize.ObjectColumnSummary`
- `org.apache.flink.api.java.summarize.StringColumnSummary`
- `org.apache.flink.api.java.utils.AbstractParameterTool`
- `org.apache.flink.api.java.utils.DataSetUtils`
- `org.apache.flink.api.java.utils.MultipleParameterTool`
- `org.apache.flink.api.java.utils.ParameterTool`
- `org.apache.flink.configuration.AkkaOptions`
- `org.apache.flink.connector.file.src.reader.FileRecordFormat$Reader`
- `org.apache.flink.connector.file.src.reader.FileRecordFormat`
- `org.apache.flink.connector.testframe.external.sink.DataStreamSinkV1ExternalContext`
- `org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend$PriorityQueueStateType`
- `org.apache.flink.core.execution.RestoreMode`
- `org.apache.flink.datastream.api.stream.KeyedPartitionStream$TwoKeyedPartitionStreams`
- `org.apache.flink.datastream.api.stream.NonKeyedPartitionStream$TwoNonKeyedPartitionStreams`
- `org.apache.flink.formats.avro.AvroRowDeserializationSchema`
- `org.apache.flink.formats.csv.CsvRowDeserializationSchema$Builder`
- `org.apache.flink.formats.csv.CsvRowDeserializationSchema`
- `org.apache.flink.formats.csv.CsvRowSerializationSchema$Builder`
- `org.apache.flink.formats.csv.CsvRowSerializationSchema`
- `org.apache.flink.formats.json.JsonRowDeserializationSchema$Builder`
- `org.apache.flink.formats.json.JsonRowDeserializationSchema`
- `org.apache.flink.formats.json.JsonRowSerializationSchema$Builder`
- `org.apache.flink.formats.json.JsonRowSerializationSchema`
- `org.apache.flink.metrics.reporter.InstantiateViaFactory`
- `org.apache.flink.metrics.reporter.InterceptInstantiationViaReflection`
- `org.apache.flink.runtime.jobgraph.SavepointConfigOptions`
- `org.apache.flink.runtime.state.CheckpointListener`
- `org.apache.flink.runtime.state.filesystem.FsStateBackendFactory`
- `org.apache.flink.runtime.state.filesystem.FsStateBackend`
- `org.apache.flink.runtime.state.memory.MemoryStateBackendFactory`
- `org.apache.flink.runtime.state.memory.MemoryStateBackend`
- `org.apache.flink.state.api.BootstrapTransformation`
- `org.apache.flink.state.api.EvictingWindowReader`
- `org.apache.flink.state.api.ExistingSavepoint`
- `org.apache.flink.state.api.KeyedOperatorTransformation`
- `org.apache.flink.state.api.NewSavepoint`
- `org.apache.flink.state.api.OneInputOperatorTransformation`
- `org.apache.flink.state.api.Savepoint`
- `org.apache.flink.state.api.WindowReader`
- `org.apache.flink.state.api.WindowedOperatorTransformation`
- `org.apache.flink.state.api.WritableSavepoint`
- `org.apache.flink.state.forst.fs.ByteBufferReadableFSDataInputStream`
- `org.apache.flink.state.forst.fs.ByteBufferWritableFSDataOutputStream`
- `org.apache.flink.state.forst.fs.ForStFlinkFileSystem`
- `org.apache.flink.streaming.api.TimeCharacteristic`
- `org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource$CheckpointTrigger`
- `org.apache.flink.streaming.api.checkpoint.ExternallyInducedSource`
- `org.apache.flink.streaming.api.connector.sink2.WithPostCommitTopology`
- `org.apache.flink.streaming.api.connector.sink2.WithPreCommitTopology`
- `org.apache.flink.streaming.api.connector.sink2.WithPreWriteTopology`
- `org.apache.flink.streaming.api.datastream.IterativeStream$ConnectedIterativeStreams`
- `org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup`
- `org.apache.flink.streaming.api.environment.ExecutionCheckpointingOptions`
- `org.apache.flink.streaming.api.environment.StreamPipelineOptions`
- `org.apache.flink.streaming.api.functions.AscendingTimestampExtractor`
- `org.apache.flink.streaming.api.functions.sink.DiscardingSink`
- `org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction`
- `org.apache.flink.streaming.api.functions.sink.PrintSinkFunction`
- `org.apache.flink.streaming.api.functions.sink.RichSinkFunction`
- `org.apache.flink.streaming.api.functions.sink.SinkFunction$Context`
- `org.apache.flink.streaming.api.functions.sink.SinkFunction`
- `org.apache.flink.streaming.api.functions.sink.SocketClientSink`
- `org.apache.flink.streaming.api.functions.sink.TwoPhaseCommitSinkFunction`
- `org.apache.flink.streaming.api.functions.sink.WriteFormatAsCsv`
- `org.apache.flink.streaming.api.functions.sink.WriteFormatAsText`
- `org.apache.flink.streaming.api.functions.sink.WriteFormat`
- `org.apache.flink.streaming.api.functions.sink.WriteSinkFunctionByMillis`
- `org.apache.flink.streaming.api.functions.sink.WriteSinkFunction`
- `org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink$BulkFormatBuilder`
- `org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink$DefaultBulkFormatBuilder`
- `org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink$DefaultRowFormatBuilder`
- `org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink$RowFormatBuilder`
- `org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink`
- `org.apache.flink.streaming.api.functions.source.FromElementsFunction`
- `org.apache.flink.streaming.api.functions.source.FromIteratorFunction`
- `org.apache.flink.streaming.api.functions.source.FromSplittableIteratorFunction`
- `org.apache.flink.streaming.api.functions.source.MessageAcknowledgingSourceBase`
- `org.apache.flink.streaming.api.functions.source.MultipleIdsMessageAcknowledgingSourceBase`
- `org.apache.flink.streaming.api.functions.source.ParallelSourceFunction`
- `org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction`
- `org.apache.flink.streaming.api.functions.source.RichSourceFunction`
- `org.apache.flink.streaming.api.functions.source.SocketTextStreamFunction`
- `org.apache.flink.streaming.api.functions.source.SourceFunction$SourceContext`
- `org.apache.flink.streaming.api.functions.source.SourceFunction`
- `org.apache.flink.streaming.api.functions.source.StatefulSequenceSource`
- `org.apache.flink.streaming.api.functions.source.datagen.DataGeneratorSource`
- `org.apache.flink.streaming.api.functions.windowing.RichProcessAllWindowFunction`
- `org.apache.flink.streaming.api.functions.windowing.RichProcessWindowFunction`
- `org.apache.flink.streaming.api.operators.SetupableStreamOperator`
- `org.apache.flink.streaming.api.operators.YieldingOperatorFactory`
- `org.apache.flink.streaming.api.windowing.time.Time`
- `org.apache.flink.streaming.util.serialization.AbstractDeserializationSchema`
- `org.apache.flink.streaming.util.serialization.DeserializationSchema`
- `org.apache.flink.streaming.util.serialization.SerializationSchema`
- `org.apache.flink.streaming.util.serialization.SimpleStringSchema`
- `org.apache.flink.streaming.util.serialization.TypeInformationSerializationSchema`
- `org.apache.flink.table.api.TableColumn$ComputedColumn`
- `org.apache.flink.table.api.TableColumn$MetadataColumn`
- `org.apache.flink.table.api.TableColumn$PhysicalColumn`
- `org.apache.flink.table.api.TableColumn`
- `org.apache.flink.table.api.TableSchema$Builder`
- `org.apache.flink.table.api.TableSchema`
- `org.apache.flink.table.api.constraints.Constraint$ConstraintType`
- `org.apache.flink.table.api.constraints.Constraint`
- `org.apache.flink.table.api.constraints.UniqueConstraint`
- `org.apache.flink.table.connector.sink.SinkFunctionProvider`
- `org.apache.flink.table.connector.sink.SinkProvider`
- `org.apache.flink.table.connector.source.AsyncTableFunctionProvider`
- `org.apache.flink.table.connector.source.SourceFunctionProvider`
- `org.apache.flink.table.connector.source.TableFunctionProvider`
- `org.apache.flink.table.descriptors.Descriptor`
- `org.apache.flink.table.descriptors.RowtimeValidator`
- `org.apache.flink.table.descriptors.Rowtime`
- `org.apache.flink.table.descriptors.SchemaValidator`
- `org.apache.flink.table.descriptors.Schema`
- `org.apache.flink.table.factories.StreamTableSinkFactory`
- `org.apache.flink.table.factories.StreamTableSourceFactory`
- `org.apache.flink.table.factories.TableFactory`
- `org.apache.flink.table.factories.TableSinkFactory$Context`
- `org.apache.flink.table.factories.TableSinkFactory`
- `org.apache.flink.table.factories.TableSourceFactory$Context`
- `org.apache.flink.table.factories.TableSourceFactory`
- `org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator$`
- `org.apache.flink.table.planner.plan.metadata.FlinkRelMdRowCount$`
- `org.apache.flink.table.planner.plan.optimize.RelNodeBlockPlanBuilder$`
- `org.apache.flink.table.planner.plan.rules.logical.JoinDeriveNullFilterRule$`
- `org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalJoinRuleBase$`
- `org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortMergeJoinRule$`
- `org.apache.flink.table.planner.plan.rules.physical.batch.BatchPhysicalSortRule$`
- `org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule$`
- `org.apache.flink.table.planner.plan.utils.FlinkRexUtil$`
- `org.apache.flink.table.sinks.AppendStreamTableSink`
- `org.apache.flink.table.sinks.OutputFormatTableSink`
- `org.apache.flink.table.sinks.OverwritableTableSink`
- `org.apache.flink.table.sinks.PartitionableTableSink`
- `org.apache.flink.table.sinks.RetractStreamTableSink`
- `org.apache.flink.table.sinks.TableSink`
- `org.apache.flink.table.sinks.UpsertStreamTableSink`
- `org.apache.flink.table.sources.DefinedFieldMapping`
- `org.apache.flink.table.sources.DefinedProctimeAttribute`
- `org.apache.flink.table.sources.DefinedRowtimeAttributes`
- `org.apache.flink.table.sources.FieldComputer`
- `org.apache.flink.table.sources.InputFormatTableSource`
- `org.apache.flink.table.sources.LimitableTableSource`
- `org.apache.flink.table.sources.LookupableTableSource`
- `org.apache.flink.table.sources.NestedFieldsProjectableTableSource`
- `org.apache.flink.table.sources.PartitionableTableSource`
- `org.apache.flink.table.sources.ProjectableTableSource`
- `org.apache.flink.table.sources.TableSource`
- `org.apache.flink.table.sources.tsextractors.ExistingField`
- `org.apache.flink.table.sources.tsextractors.StreamRecordTimestamp`
- `org.apache.flink.table.sources.tsextractors.TimestampExtractor`
- `org.apache.flink.table.types.logical.TypeInformationRawType`
- `org.apache.flink.table.utils.TypeStringUtils`
- `org.apache.flink.walkthrough.common.sink.AlertSink`
- `org.apache.flink.walkthrough.common.source.TransactionSource`


### Modified Classes
- `org.apache.flink.table.api.bridge.java.StreamTableEnvironment`
    - method removed:
        - `void registerDataStream(java.lang.String, org.apache.flink.streaming.api.datastream.DataStream<T>)`
        - `void registerFunction(java.lang.String, org.apache.flink.table.functions.TableFunction<T>)`
        - `void registerFunction(java.lang.String, org.apache.flink.table.functions.AggregateFunction<T,ACC>)`
        - `void registerFunction(java.lang.String, org.apache.flink.table.functions.TableAggregateFunction<T,ACC>)`
- `org.apache.flink.table.api.config.ExecutionConfigOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> TABLE_EXEC_LEGACY_TRANSFORMATION_UIDS`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> TABLE_EXEC_SHUFFLE_MODE`
- `org.apache.flink.table.api.config.LookupJoinHintOptions`
    - method modified:
        - `org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableSet<org.apache.flink.configuration.ConfigOption><org.apache.flink.configuration.ConfigOption> (<-org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet<org.apache.flink.configuration.ConfigOption><org.apache.flink.configuration.ConfigOption>) getRequiredOptions()`
        - `org.apache.flink.shaded.guava32.com.google.common.collect.ImmutableSet<org.apache.flink.configuration.ConfigOption><org.apache.flink.configuration.ConfigOption> (<-org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableSet<org.apache.flink.configuration.ConfigOption><org.apache.flink.configuration.ConfigOption>) getSupportedOptions()`
- `org.apache.flink.table.api.config.OptimizerConfigOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> TABLE_OPTIMIZER_SOURCE_PREDICATE_PUSHDOWN_ENABLED`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED`
- `org.apache.flink.table.api.dataview.ListView`
    - field removed:
        - `TRANSIENT(-) org.apache.flink.api.common.typeinfo.TypeInformation<?> elementType`
    - constructor removed:
        - `ListView(org.apache.flink.api.common.typeinfo.TypeInformation<?>)`
- `org.apache.flink.table.api.dataview.MapView`
    - field removed:
        - `TRANSIENT(-) org.apache.flink.api.common.typeinfo.TypeInformation<?> valueType`
        - `TRANSIENT(-) org.apache.flink.api.common.typeinfo.TypeInformation<?> keyType`
    - constructor removed:
        - `MapView(org.apache.flink.api.common.typeinfo.TypeInformation<?>, org.apache.flink.api.common.typeinfo.TypeInformation<?>)`
- `org.apache.flink.table.api.EnvironmentSettings`
    - method removed:
        - `org.apache.flink.table.api.EnvironmentSettings fromConfiguration(org.apache.flink.configuration.ReadableConfig)`
        - `org.apache.flink.configuration.Configuration toConfiguration()`
- `org.apache.flink.table.api.internal.BaseExpressions`
    - method removed:
        - `java.lang.Object cast(org.apache.flink.api.common.typeinfo.TypeInformation<?>)`
- `org.apache.flink.table.api.OverWindow`
    - method modified:
        - `java.util.Optional<org.apache.flink.table.expressions.Expression> (<-org.apache.flink.table.expressions.Expression<org.apache.flink.table.expressions.Expression>) getPreceding()`
- `org.apache.flink.table.api.Table`
    - method modified:
        - `org.apache.flink.table.legacy.api.TableSchema (<-org.apache.flink.table.api.TableSchema) getSchema()`
- `org.apache.flink.table.api.TableConfig`
    - constructor modified:
        - `PRIVATE (<- PUBLIC) TableConfig()`
    - method removed:
        - `long getMaxIdleStateRetentionTime()`
        - `long getMinIdleStateRetentionTime()`
        - `void setIdleStateRetentionTime(org.apache.flink.api.common.time.Time, org.apache.flink.api.common.time.Time)`
- `org.apache.flink.table.api.TableDescriptor`
    - method removed:
        - `org.apache.flink.table.api.TableDescriptor$Builder forManaged()`
- `org.apache.flink.table.api.TableResult`
    - method removed:
        - `org.apache.flink.table.api.TableSchema getTableSchema()`
- `org.apache.flink.table.catalog.Catalog`
    - method removed:
        - `java.util.Optional<org.apache.flink.table.factories.TableFactory> getTableFactory()`
        - `boolean supportsManagedTable()`
- `org.apache.flink.table.catalog.CatalogBaseTable`
    - method modified:
        - `org.apache.flink.table.legacy.api.TableSchema (<-org.apache.flink.table.api.TableSchema) getSchema()`
- `org.apache.flink.table.catalog.CatalogFunction`
    - method removed:
        - `boolean isGeneric()`
- `org.apache.flink.table.catalog.CatalogTable`
    - method removed:
        - `org.apache.flink.table.catalog.CatalogTable of(org.apache.flink.table.api.Schema, java.lang.String, java.util.List<java.lang.String>, java.util.Map<java.lang.String,java.lang.String>)`
        - `org.apache.flink.table.catalog.CatalogTable of(org.apache.flink.table.api.Schema, java.lang.String, java.util.List<java.lang.String>, java.util.Map<java.lang.String,java.lang.String>, java.lang.Long)`
        - `java.util.Map<java.lang.String,java.lang.String> toProperties()`
- `org.apache.flink.table.catalog.ResolvedCatalogBaseTable`
    - method modified:
        - `org.apache.flink.table.legacy.api.TableSchema (<-org.apache.flink.table.api.TableSchema) getSchema()`
- `org.apache.flink.table.connector.sink.DataStreamSinkProvider`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.streaming.api.datastream.DataStreamSink<?><?> consumeDataStream(org.apache.flink.table.connector.ProviderContext, org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData><org.apache.flink.table.data.RowData>)`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<?> consumeDataStream(org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData>)`
- `org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown`
    - method removed:
        - `void applyProjection(int[][])`
    - method modified:
        - `(<- NON_ABSTRACT) void applyProjection(int[][], org.apache.flink.table.types.DataType)`
- `org.apache.flink.table.connector.source.DataStreamScanProvider`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData><org.apache.flink.table.data.RowData> produceDataStream(org.apache.flink.table.connector.ProviderContext, org.apache.flink.streaming.api.environment.StreamExecutionEnvironment)`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.table.data.RowData> produceDataStream(org.apache.flink.streaming.api.environment.StreamExecutionEnvironment)`
- `org.apache.flink.table.expressions.CallExpression`
    - constructor removed:
        - `CallExpression(org.apache.flink.table.functions.FunctionIdentifier, org.apache.flink.table.functions.FunctionDefinition, java.util.List<org.apache.flink.table.expressions.ResolvedExpression>, org.apache.flink.table.types.DataType)`
        - `CallExpression(org.apache.flink.table.functions.FunctionDefinition, java.util.List<org.apache.flink.table.expressions.ResolvedExpression>, org.apache.flink.table.types.DataType)`
- `org.apache.flink.table.factories.FactoryUtil`
    - method removed:
        - `org.apache.flink.table.connector.sink.DynamicTableSink createDynamicTableSink(org.apache.flink.table.factories.DynamicTableSinkFactory, org.apache.flink.table.catalog.ObjectIdentifier, org.apache.flink.table.catalog.ResolvedCatalogTable, org.apache.flink.configuration.ReadableConfig, java.lang.ClassLoader, boolean)`
        - `org.apache.flink.table.connector.source.DynamicTableSource createDynamicTableSource(org.apache.flink.table.factories.DynamicTableSourceFactory, org.apache.flink.table.catalog.ObjectIdentifier, org.apache.flink.table.catalog.ResolvedCatalogTable, org.apache.flink.configuration.ReadableConfig, java.lang.ClassLoader, boolean)`
        - `org.apache.flink.table.connector.sink.DynamicTableSink createTableSink(org.apache.flink.table.catalog.Catalog, org.apache.flink.table.catalog.ObjectIdentifier, org.apache.flink.table.catalog.ResolvedCatalogTable, org.apache.flink.configuration.ReadableConfig, java.lang.ClassLoader, boolean)`
        - `org.apache.flink.table.connector.source.DynamicTableSource createTableSource(org.apache.flink.table.catalog.Catalog, org.apache.flink.table.catalog.ObjectIdentifier, org.apache.flink.table.catalog.ResolvedCatalogTable, org.apache.flink.configuration.ReadableConfig, java.lang.ClassLoader, boolean)`
- `org.apache.flink.table.factories.FunctionDefinitionFactory`
    - method removed:
        - `org.apache.flink.table.functions.FunctionDefinition createFunctionDefinition(java.lang.String, org.apache.flink.table.catalog.CatalogFunction)`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.table.functions.FunctionDefinition createFunctionDefinition(java.lang.String, org.apache.flink.table.catalog.CatalogFunction, org.apache.flink.table.factories.FunctionDefinitionFactory$Context)`
- `org.apache.flink.table.functions.FunctionContext`
    - constructor removed:
        - `FunctionContext(org.apache.flink.api.common.functions.RuntimeContext, java.lang.ClassLoader, org.apache.flink.configuration.Configuration)`
- `org.apache.flink.table.plan.stats.ColumnStats`
    - constructor removed:
        - `ColumnStats(java.lang.Long, java.lang.Long, java.lang.Double, java.lang.Integer, java.lang.Number, java.lang.Number)`
    - method removed:
        - `java.lang.Number getMaxValue()`
        - `java.lang.Number getMinValue()`
- `org.apache.flink.table.types.logical.SymbolType`
    - constructor removed:
        - `SymbolType(boolean, java.lang.Class<T>)`
        - `SymbolType(java.lang.Class<T>)`
- `org.apache.flink.table.types.logical.utils.LogicalTypeParser`
    - method removed:
        - `org.apache.flink.table.types.logical.LogicalType parse(java.lang.String)`
- `org.apache.flink.api.common.state.v2.StateIterator`
    - method removed:
        - `org.apache.flink.api.common.state.v2.StateFuture<java.util.Collection<U>> onNext(java.util.function.Function<T,org.apache.flink.api.common.state.v2.StateFuture<U>>)`
        - `org.apache.flink.api.common.state.v2.StateFuture<java.lang.Void> onNext(java.util.function.Consumer<T>)`
- `org.apache.flink.table.api.ImplicitExpressionConversions`
    - method removed:
        - `org.apache.flink.table.expressions.Expression toTimestampLtz(org.apache.flink.table.expressions.Expression, org.apache.flink.table.expressions.Expression)`
        - `SYNTHETIC(-) org.apache.flink.table.expressions.Expression toTimestampLtz$(org.apache.flink.table.api.ImplicitExpressionConversions, org.apache.flink.table.expressions.Expression, org.apache.flink.table.expressions.Expression)`
- `org.apache.flink.api.common.eventtime.WatermarksWithIdleness`
    - constructor removed:
        - `WatermarksWithIdleness(org.apache.flink.api.common.eventtime.WatermarkGenerator<T>, java.time.Duration)`
- `org.apache.flink.api.common.ExecutionConfig`
    - field removed:
        - `int PARALLELISM_AUTO_MAX`
    - method removed:
        - `void addDefaultKryoSerializer(java.lang.Class<?>, com.esotericsoftware.kryo.Serializer<?>)`
        - `void addDefaultKryoSerializer(java.lang.Class<?>, java.lang.Class<? extends com.esotericsoftware.kryo.Serializer<? extends ?>>)`
        - `boolean canEqual(java.lang.Object)`
        - `void disableAutoTypeRegistration()`
        - `void disableForceAvro()`
        - `void disableForceKryo()`
        - `void disableGenericTypes()`
        - `void enableForceAvro()`
        - `void enableForceKryo()`
        - `void enableGenericTypes()`
        - `int getAsyncInflightRecordsLimit()`
        - `int getAsyncStateBufferSize()`
        - `long getAsyncStateBufferTimeout()`
        - `org.apache.flink.api.common.InputDependencyConstraint getDefaultInputDependencyConstraint()`
        - `java.util.LinkedHashMap<java.lang.Class<?>,java.lang.Class<com.esotericsoftware.kryo.Serializer<? extends ?>>> getDefaultKryoSerializerClasses()`
        - `java.util.LinkedHashMap<java.lang.Class<?>,org.apache.flink.api.common.ExecutionConfig$SerializableSerializer<?>> getDefaultKryoSerializers()`
        - `org.apache.flink.api.common.ExecutionMode getExecutionMode()`
        - `long getExecutionRetryDelay()`
        - `int getNumberOfExecutionRetries()`
        - `java.util.LinkedHashSet<java.lang.Class<?>> getRegisteredKryoTypes()`
        - `java.util.LinkedHashSet<java.lang.Class<?>> getRegisteredPojoTypes()`
        - `java.util.LinkedHashMap<java.lang.Class<?>,java.lang.Class<com.esotericsoftware.kryo.Serializer<? extends ?>>> getRegisteredTypesWithKryoSerializerClasses()`
        - `java.util.LinkedHashMap<java.lang.Class<?>,org.apache.flink.api.common.ExecutionConfig$SerializableSerializer<?>> getRegisteredTypesWithKryoSerializers()`
        - `org.apache.flink.api.common.restartstrategy.RestartStrategies$RestartStrategyConfiguration getRestartStrategy()`
        - `boolean hasGenericTypesDisabled()`
        - `boolean isAutoTypeRegistrationDisabled()`
        - `boolean isForceAvroEnabled()`
        - `boolean isForceKryoEnabled()`
        - `void registerKryoType(java.lang.Class<?>)`
        - `void registerPojoType(java.lang.Class<?>)`
        - `void registerTypeWithKryoSerializer(java.lang.Class<?>, com.esotericsoftware.kryo.Serializer<?>)`
        - `void registerTypeWithKryoSerializer(java.lang.Class<?>, java.lang.Class<? extends com.esotericsoftware.kryo.Serializer>)`
        - `org.apache.flink.api.common.ExecutionConfig setAsyncInflightRecordsLimit(int)`
        - `org.apache.flink.api.common.ExecutionConfig setAsyncStateBufferSize(int)`
        - `org.apache.flink.api.common.ExecutionConfig setAsyncStateBufferTimeout(long)`
        - `void setDefaultInputDependencyConstraint(org.apache.flink.api.common.InputDependencyConstraint)`
        - `void setExecutionMode(org.apache.flink.api.common.ExecutionMode)`
        - `org.apache.flink.api.common.ExecutionConfig setExecutionRetryDelay(long)`
        - `org.apache.flink.api.common.ExecutionConfig setNumberOfExecutionRetries(int)`
        - `void setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies$RestartStrategyConfiguration)`
- `org.apache.flink.api.common.functions.RichFunction`
    - method removed:
        - `void open(org.apache.flink.configuration.Configuration)`
    - method modified:
        - `(<- NON_ABSTRACT) void open(org.apache.flink.api.common.functions.OpenContext)`
- `org.apache.flink.api.common.functions.RuntimeContext`
    - method removed:
        - `int getAttemptNumber()`
        - `org.apache.flink.api.common.ExecutionConfig getExecutionConfig()`
        - `int getIndexOfThisSubtask()`
        - `org.apache.flink.api.common.JobID getJobId()`
        - `int getMaxNumberOfParallelSubtasks()`
        - `int getNumberOfParallelSubtasks()`
        - `java.lang.String getTaskName()`
        - `java.lang.String getTaskNameWithSubtasks()`
- `org.apache.flink.api.common.io.BinaryInputFormat`
    - field removed:
        - `java.lang.String BLOCK_SIZE_PARAMETER_KEY`
- `org.apache.flink.api.common.io.BinaryOutputFormat`
    - field removed:
        - `java.lang.String BLOCK_SIZE_PARAMETER_KEY`
- `org.apache.flink.api.common.io.FileInputFormat`
    - field removed:
        - `java.lang.String ENUMERATE_NESTED_FILES_FLAG`
    - method removed:
        - `org.apache.flink.core.fs.Path getFilePath()`
        - `boolean supportsMultiPaths()`
- `org.apache.flink.api.common.io.FileOutputFormat`
    - field removed:
        - `java.lang.String FILE_PARAMETER_KEY`
- `org.apache.flink.api.common.io.FinalizeOnMaster`
    - method removed:
        - `void finalizeGlobal(int)`
    - method modified:
        - `(<- NON_ABSTRACT) void finalizeGlobal(org.apache.flink.api.common.io.FinalizeOnMaster$FinalizationContext)`
- `org.apache.flink.api.common.io.OutputFormat`
    - method removed:
        - `void open(int, int)`
    - method modified:
        - `(<- NON_ABSTRACT) void open(org.apache.flink.api.common.io.OutputFormat$InitializationContext)`
- `org.apache.flink.api.common.JobExecutionResult`
    - method removed:
        - `org.apache.flink.api.common.JobExecutionResult fromJobSubmissionResult(org.apache.flink.api.common.JobSubmissionResult)`
        - `java.lang.Integer getIntCounterResult(java.lang.String)`
- `org.apache.flink.api.common.serialization.SerializerConfig`
    - method removed:
        - `java.util.LinkedHashMap<java.lang.Class<?>,org.apache.flink.api.common.ExecutionConfig$SerializableSerializer<?>> getDefaultKryoSerializers()`
        - `java.util.LinkedHashMap<java.lang.Class<?>,org.apache.flink.api.common.ExecutionConfig$SerializableSerializer<?>> getRegisteredTypesWithKryoSerializers()`
- `org.apache.flink.api.common.state.StateTtlConfig`
    - method removed:
        - `org.apache.flink.api.common.time.Time getTtl()`
        - `org.apache.flink.api.common.state.StateTtlConfig$Builder newBuilder(org.apache.flink.api.common.time.Time)`
- `org.apache.flink.api.common.state.StateTtlConfig$Builder`
    - constructor removed:
        - `StateTtlConfig$Builder(org.apache.flink.api.common.time.Time)`
    - method removed:
        - `org.apache.flink.api.common.state.StateTtlConfig$Builder setTtl(org.apache.flink.api.common.time.Time)`
- `org.apache.flink.api.common.typeinfo.TypeInformation`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.api.common.typeutils.TypeSerializer<T><T> createSerializer(org.apache.flink.api.common.serialization.SerializerConfig)`
    - method removed:
        - `org.apache.flink.api.common.typeutils.TypeSerializer<T> createSerializer(org.apache.flink.api.common.ExecutionConfig)`
- `org.apache.flink.api.common.typeutils.TypeSerializerSnapshot`
    - method removed:
        - `org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility<T> resolveSchemaCompatibility(org.apache.flink.api.common.typeutils.TypeSerializer<T>)`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility<T><T> resolveSchemaCompatibility(org.apache.flink.api.common.typeutils.TypeSerializerSnapshot<T><T>)`
- `org.apache.flink.api.connector.sink2.Sink`
    - method removed:
        - `org.apache.flink.api.connector.sink2.SinkWriter<InputT> createWriter(org.apache.flink.api.connector.sink2.Sink$InitContext)`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.api.connector.sink2.SinkWriter<InputT><InputT> createWriter(org.apache.flink.api.connector.sink2.WriterInitContext)`
- `org.apache.flink.api.java.typeutils.PojoTypeInfo`
    - method removed:
        - `org.apache.flink.api.java.typeutils.runtime.PojoSerializer<T> createPojoSerializer(org.apache.flink.api.common.ExecutionConfig)`
- `org.apache.flink.api.java.typeutils.RowTypeInfo`
    - method removed:
        - `org.apache.flink.api.common.typeutils.TypeSerializer<org.apache.flink.types.Row> createLegacySerializer(org.apache.flink.api.common.serialization.SerializerConfig)`
- `org.apache.flink.configuration.CheckpointingOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> LOCAL_RECOVERY`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> STATE_BACKEND`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> ASYNC_SNAPSHOTS`
- `org.apache.flink.configuration.ClusterOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> FINE_GRAINED_SHUFFLE_MODE_ALL_BLOCKING`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> EVENLY_SPREAD_OUT_SLOTS_STRATEGY`
- `org.apache.flink.configuration.ConfigConstants`
    - field removed:
        - `java.lang.String HA_ZOOKEEPER_LEADER_PATH`
        - `double DEFAULT_AKKA_WATCH_THRESHOLD`
        - `int DEFAULT_JOB_MANAGER_IPC_PORT`
        - `java.lang.String JOB_MANAGER_WEB_TMPDIR_KEY`
        - `int DEFAULT_TASK_MANAGER_MEMORY_SEGMENT_SIZE`
        - `java.lang.String METRICS_SCOPE_NAMING_TASK`
        - `java.lang.String ZOOKEEPER_NAMESPACE_KEY`
        - `int DEFAULT_AKKA_DISPATCHER_THROUGHPUT`
        - `java.lang.String RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS`
        - `java.lang.String MESOS_MASTER_URL`
        - `java.lang.String FLINK_BASE_DIR_PATH_KEY`
        - `java.lang.String JOB_MANAGER_WEB_SSL_ENABLED`
        - `java.lang.String YARN_APPLICATION_TAGS`
        - `java.lang.String HDFS_SITE_CONFIG`
        - `java.lang.String EXECUTION_RETRY_DELAY_KEY`
        - `int DEFAULT_MESOS_ARTIFACT_SERVER_PORT`
        - `boolean DEFAULT_SECURITY_SSL_VERIFY_HOSTNAME`
        - `java.lang.String CONTAINERIZED_HEAP_CUTOFF_MIN`
        - `java.lang.String YARN_HEARTBEAT_DELAY_SECONDS`
        - `java.lang.String AKKA_SSL_ENABLED`
        - `java.lang.String HA_MODE`
        - `java.lang.String ZOOKEEPER_MESOS_WORKERS_PATH`
        - `boolean DEFAULT_ZOOKEEPER_SASL_DISABLE`
        - `java.lang.String METRICS_SCOPE_DELIMITER`
        - `java.lang.String LOCAL_NUMBER_RESOURCE_MANAGER`
        - `java.lang.String AKKA_TCP_TIMEOUT`
        - `java.lang.String METRICS_SCOPE_NAMING_OPERATOR`
        - `java.lang.String ZOOKEEPER_RECOVERY_PATH`
        - `int DEFAULT_ZOOKEEPER_LEADER_PORT`
        - `java.lang.String DEFAULT_ZOOKEEPER_LATCH_PATH`
        - `int DEFAULT_ZOOKEEPER_PEER_PORT`
        - `java.lang.String METRICS_SCOPE_NAMING_TM_JOB`
        - `int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES`
        - `java.lang.String HA_ZOOKEEPER_SESSION_TIMEOUT`
        - `java.lang.String FLINK_JVM_OPTIONS`
        - `java.lang.String HA_ZOOKEEPER_CHECKPOINT_COUNTER_PATH`
        - `java.lang.String METRICS_SCOPE_NAMING_JM`
        - `java.lang.String DEFAULT_YARN_JOB_MANAGER_PORT`
        - `boolean DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_DISABLE`
        - `java.lang.String HA_ZOOKEEPER_QUORUM_KEY`
        - `boolean DEFAULT_JOB_MANAGER_WEB_SUBMIT_ENABLED`
        - `java.lang.String JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE`
        - `java.lang.String ZOOKEEPER_JOBGRAPHS_PATH`
        - `java.lang.String ZOOKEEPER_SASL_SERVICE_NAME`
        - `java.lang.String DEFAULT_AKKA_LOOKUP_TIMEOUT`
        - `java.lang.String RESTART_STRATEGY_FAILURE_RATE_MAX_FAILURES_PER_INTERVAL`
        - `java.lang.String JOB_MANAGER_WEB_PORT_KEY`
        - `java.lang.String METRICS_LATENCY_HISTORY_SIZE`
        - `int DEFAULT_BLOB_FETCH_BACKLOG`
        - `java.lang.String JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL`
        - `float DEFAULT_SORT_SPILLING_THRESHOLD`
        - `java.lang.String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_INTERVAL`
        - `java.lang.String CONTAINERIZED_MASTER_ENV_PREFIX`
        - `int DEFAULT_JOB_MANAGER_WEB_ARCHIVE_COUNT`
        - `java.lang.String TASK_MANAGER_HOSTNAME_KEY`
        - `java.lang.String AKKA_WATCH_HEARTBEAT_INTERVAL`
        - `java.lang.String DEFAULT_TASK_MANAGER_TMP_PATH`
        - `int DEFAULT_EXECUTION_RETRIES`
        - `int DEFAULT_JOB_MANAGER_WEB_FRONTEND_PORT`
        - `java.lang.String JOB_MANAGER_WEB_LOG_PATH_KEY`
        - `java.lang.String TASK_MANAGER_MEMORY_SIZE_KEY`
        - `java.lang.String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_NAME`
        - `java.lang.String TASK_MANAGER_DATA_PORT_KEY`
        - `java.lang.String ZOOKEEPER_CHECKPOINTS_PATH`
        - `java.lang.String HA_JOB_MANAGER_PORT`
        - `java.lang.String TASK_MANAGER_REFUSED_REGISTRATION_PAUSE`
        - `java.lang.String CONTAINERIZED_HEAP_CUTOFF_RATIO`
        - `java.lang.String DEFAULT_SORT_SPILLING_THRESHOLD_KEY`
        - `java.lang.String YARN_CONTAINER_START_COMMAND_TEMPLATE`
        - `boolean DEFAULT_JOB_MANAGER_WEB_SSL_ENABLED`
        - `java.lang.String LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL`
        - `java.lang.String JOB_MANAGER_WEB_CHECKPOINTS_DISABLE`
        - `java.lang.String DEFAULT_ZOOKEEPER_LEADER_PATH`
        - `int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_DELAY`
        - `java.lang.String DEFAULT_TASK_MANAGER_MAX_REGISTRATION_PAUSE`
        - `java.lang.String METRICS_REPORTERS_LIST`
        - `java.lang.String DEFAULT_RECOVERY_MODE`
        - `int DEFAULT_METRICS_LATENCY_HISTORY_SIZE`
        - `java.lang.String TASK_MANAGER_INITIAL_REGISTRATION_PAUSE`
        - `java.lang.String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE`
        - `int DEFAULT_JOB_MANAGER_WEB_CHECKPOINTS_HISTORY_SIZE`
        - `java.lang.String YARN_PROPERTIES_FILE_LOCATION`
        - `java.lang.String RECOVERY_JOB_MANAGER_PORT`
        - `boolean DEFAULT_SECURITY_SSL_ENABLED`
        - `java.lang.String MESOS_FAILOVER_TIMEOUT_SECONDS`
        - `java.lang.String RUNTIME_HASH_JOIN_BLOOM_FILTERS_KEY`
        - `java.lang.String ZOOKEEPER_LEADER_PATH`
        - `java.lang.String ZOOKEEPER_MAX_RETRY_ATTEMPTS`
        - `java.lang.String HA_ZOOKEEPER_CHECKPOINTS_PATH`
        - `java.lang.String MESOS_RESOURCEMANAGER_FRAMEWORK_ROLE`
        - `int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_REFRESH_INTERVAL`
        - `java.lang.String DEFAULT_ZOOKEEPER_MESOS_WORKERS_PATH`
        - `java.lang.String JOB_MANAGER_IPC_PORT_KEY`
        - `java.lang.String AKKA_WATCH_HEARTBEAT_PAUSE`
        - `java.lang.String MESOS_RESOURCEMANAGER_FRAMEWORK_NAME`
        - `java.lang.String DELIMITED_FORMAT_MAX_SAMPLE_LENGTH_KEY`
        - `java.lang.String STATE_BACKEND`
        - `java.lang.String MESOS_RESOURCEMANAGER_FRAMEWORK_PRINCIPAL`
        - `long DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS`
        - `java.lang.String DEFAULT_AKKA_CLIENT_TIMEOUT`
        - `int DEFAULT_SPILLING_MAX_FAN`
        - `java.lang.String TASK_MANAGER_IPC_PORT_KEY`
        - `java.lang.String TASK_MANAGER_MEMORY_OFF_HEAP_KEY`
        - `boolean DEFAULT_FILESYSTEM_OVERWRITE`
        - `boolean DEFAULT_USE_LARGE_RECORD_HANDLER`
        - `java.lang.String HA_ZOOKEEPER_JOBGRAPHS_PATH`
        - `boolean DEFAULT_BLOB_SERVICE_SSL_ENABLED`
        - `java.lang.String ZOOKEEPER_SESSION_TIMEOUT`
        - `java.lang.String TASK_MANAGER_NETWORK_DEFAULT_IO_MODE`
        - `java.lang.String SECURITY_SSL_TRUSTSTORE_PASSWORD`
        - `int DEFAULT_ZOOKEEPER_MAX_RETRY_ATTEMPTS`
        - `java.lang.String AKKA_STARTUP_TIMEOUT`
        - `java.lang.String TASK_MANAGER_TMP_DIR_KEY`
        - `java.lang.String USE_LARGE_RECORD_HANDLER_KEY`
        - `java.lang.String DEFAULT_ZOOKEEPER_DIR_KEY`
        - `int DEFAULT_YARN_MIN_HEAP_CUTOFF`
        - `java.lang.String TASK_MANAGER_DATA_SSL_ENABLED`
        - `java.lang.String HDFS_DEFAULT_CONFIG`
        - `boolean DEFAULT_TASK_MANAGER_DATA_SSL_ENABLED`
        - `java.lang.String DEFAULT_ZOOKEEPER_JOBGRAPHS_PATH`
        - `java.lang.String HA_ZOOKEEPER_MESOS_WORKERS_PATH`
        - `java.lang.String BLOB_STORAGE_DIRECTORY_KEY`
        - `java.lang.String DEFAULT_STATE_BACKEND`
        - `java.lang.String HA_ZOOKEEPER_RETRY_WAIT`
        - `java.lang.String AKKA_ASK_TIMEOUT`
        - `java.lang.String JOB_MANAGER_WEB_SUBMIT_ENABLED_KEY`
        - `java.lang.String DEFAULT_ZOOKEEPER_NAMESPACE_KEY`
        - `java.lang.String DEFAULT_ZOOKEEPER_CHECKPOINTS_PATH`
        - `int DEFAULT_LOCAL_NUMBER_JOB_MANAGER`
        - `java.lang.String AKKA_TRANSPORT_HEARTBEAT_INTERVAL`
        - `java.lang.String DEFAULT_ZOOKEEPER_CHECKPOINT_COUNTER_PATH`
        - `java.lang.String FS_STREAM_OPENING_TIMEOUT_KEY`
        - `java.lang.String SECURITY_SSL_TRUSTSTORE`
        - `java.lang.String METRICS_SCOPE_NAMING_JM_JOB`
        - `java.lang.String MESOS_INITIAL_TASKS`
        - `java.lang.String AKKA_FRAMESIZE`
        - `int DEFAULT_ZOOKEEPER_INIT_LIMIT`
        - `java.lang.String JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL`
        - `java.lang.String SECURITY_SSL_KEYSTORE`
        - `boolean DEFAULT_MESOS_ARTIFACT_SERVER_SSL_ENABLED`
        - `java.lang.String HA_ZOOKEEPER_MAX_RETRY_ATTEMPTS`
        - `int DEFAULT_PARALLELISM`
        - `java.lang.String RECOVERY_MODE`
        - `java.lang.String EXECUTION_RETRIES_KEY`
        - `java.lang.String METRICS_REPORTER_SCOPE_DELIMITER`
        - `java.lang.String LOCAL_START_WEBSERVER`
        - `java.lang.String LOCAL_NUMBER_JOB_MANAGER`
        - `java.lang.String RESTART_STRATEGY`
        - `java.lang.String ZOOKEEPER_QUORUM_KEY`
        - `int DEFAULT_MESOS_FAILOVER_TIMEOUT_SECS`
        - `boolean DEFAULT_TASK_MANAGER_MEMORY_PRE_ALLOCATE`
        - `int DEFAULT_LOCAL_NUMBER_RESOURCE_MANAGER`
        - `java.lang.String HA_ZOOKEEPER_CLIENT_ACL`
        - `java.lang.String METRICS_REPORTER_FACTORY_CLASS_SUFFIX`
        - `boolean DEFAULT_FILESYSTEM_ALWAYS_CREATE_DIRECTORY`
        - `java.lang.String BLOB_FETCH_CONCURRENT_KEY`
        - `java.lang.String FILESYSTEM_DEFAULT_OVERWRITE_KEY`
        - `java.lang.String RESOURCE_MANAGER_IPC_PORT_KEY`
        - `java.lang.String DEFAULT_AKKA_ASK_TIMEOUT`
        - `int DEFAULT_ZOOKEEPER_CLIENT_PORT`
        - `double DEFAULT_AKKA_TRANSPORT_THRESHOLD`
        - `java.lang.String DEFAULT_AKKA_FRAMESIZE`
        - `java.lang.String TASK_MANAGER_NUM_TASK_SLOTS`
        - `java.lang.String YARN_APPLICATION_MASTER_ENV_PREFIX`
        - `java.lang.String JOB_MANAGER_WEB_BACK_PRESSURE_DELAY`
        - `long DEFAULT_TASK_CANCELLATION_INTERVAL_MILLIS`
        - `java.lang.String TASK_MANAGER_MEMORY_PRE_ALLOCATE_KEY`
        - `java.lang.String FILESYSTEM_SCHEME`
        - `java.lang.String TASK_MANAGER_MAX_REGISTRATION_DURATION`
        - `java.lang.String HA_ZOOKEEPER_DIR_KEY`
        - `java.lang.String DEFAULT_MESOS_RESOURCEMANAGER_FRAMEWORK_USER`
        - `java.lang.String DEFAULT_FILESYSTEM_SCHEME`
        - `java.lang.String MESOS_RESOURCEMANAGER_FRAMEWORK_SECRET`
        - `int DEFAULT_DELIMITED_FORMAT_MAX_SAMPLE_LEN`
        - `java.lang.String ENV_FLINK_BIN_DIR`
        - `float DEFAULT_YARN_HEAP_CUTOFF_RATIO`
        - `java.lang.String SAVEPOINT_FS_DIRECTORY_KEY`
        - `java.lang.String AKKA_JVM_EXIT_ON_FATAL_ERROR`
        - `java.lang.String ZOOKEEPER_RETRY_WAIT`
        - `java.lang.String HA_ZOOKEEPER_NAMESPACE_KEY`
        - `java.lang.String ZOOKEEPER_CONNECTION_TIMEOUT`
        - `java.lang.String TASK_MANAGER_NETWORK_NUM_BUFFERS_KEY`
        - `java.lang.String JOB_MANAGER_WEB_ARCHIVE_COUNT`
        - `int DEFAULT_RESOURCE_MANAGER_IPC_PORT`
        - `int DEFAULT_JOB_MANAGER_WEB_BACK_PRESSURE_CLEAN_UP_INTERVAL`
        - `java.lang.String YARN_REALLOCATE_FAILED_CONTAINERS`
        - `java.lang.String SECURITY_SSL_KEYSTORE_PASSWORD`
        - `java.lang.String DEFAULT_HA_JOB_MANAGER_PORT`
        - `java.lang.String BLOB_FETCH_RETRIES_KEY`
        - `java.lang.String METRICS_REPORTER_EXCLUDED_VARIABLES`
        - `java.lang.String DEFAULT_SECURITY_SSL_PROTOCOL`
        - `java.lang.String RECOVERY_JOB_DELAY`
        - `java.lang.String TASK_CANCELLATION_INTERVAL_MILLIS`
        - `java.lang.String YARN_APPLICATION_MASTER_PORT`
        - `int DEFAULT_TASK_MANAGER_DATA_PORT`
        - `java.lang.String RESTART_STRATEGY_FAILURE_RATE_FAILURE_RATE_INTERVAL`
        - `java.lang.String YARN_TASK_MANAGER_ENV_PREFIX`
        - `int DEFAULT_DELIMITED_FORMAT_MIN_LINE_SAMPLES`
        - `java.lang.String AKKA_LOG_LIFECYCLE_EVENTS`
        - `boolean DEFAULT_AKKA_LOG_LIFECYCLE_EVENTS`
        - `java.lang.String SECURITY_SSL_ENABLED`
        - `int DEFAULT_DELIMITED_FORMAT_MAX_LINE_SAMPLES`
        - `java.lang.String LOCAL_NUMBER_TASK_MANAGER`
        - `java.lang.String DEFAULT_TASK_MANAGER_REFUSED_REGISTRATION_PAUSE`
        - `java.lang.String DEFAULT_SECURITY_SSL_ALGORITHMS`
        - `java.lang.String MESOS_MAX_FAILED_TASKS`
        - `int DEFAULT_TASK_MANAGER_IPC_PORT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> DEFAULT_JOB_MANAGER_WEB_FRONTEND_ADDRESS`
        - `java.lang.String SECURITY_SSL_ALGORITHMS`
        - `int DEFAULT_ZOOKEEPER_CONNECTION_TIMEOUT`
        - `java.lang.String YARN_HEAP_CUTOFF_RATIO`
        - `java.lang.String HA_ZOOKEEPER_LATCH_PATH`
        - `int DEFAULT_ZOOKEEPER_SESSION_TIMEOUT`
        - `java.lang.String DEFAULT_SPILLING_MAX_FAN_KEY`
        - `java.lang.String AKKA_WATCH_THRESHOLD`
        - `java.lang.String TASK_MANAGER_DEBUG_MEMORY_USAGE_LOG_INTERVAL_MS`
        - `java.lang.String HA_ZOOKEEPER_STORAGE_PATH`
        - `java.lang.String DEFAULT_BLOB_SERVER_PORT`
        - `java.lang.String AKKA_TRANSPORT_THRESHOLD`
        - `java.lang.String ZOOKEEPER_CHECKPOINT_COUNTER_PATH`
        - `boolean DEFAULT_RUNTIME_HASH_JOIN_BLOOM_FILTERS`
        - `int DEFAULT_BLOB_FETCH_CONCURRENT`
        - `java.lang.String BLOB_SERVER_PORT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> RESTART_STRATEGY_FIXED_DELAY_DELAY`
        - `java.lang.String METRICS_REPORTER_CLASS_SUFFIX`
        - `java.lang.String ZOOKEEPER_DIR_KEY`
        - `java.lang.String JOB_MANAGER_IPC_ADDRESS_KEY`
        - `int DEFAULT_TASK_MANAGER_NETWORK_NUM_BUFFERS`
        - `java.lang.String DEFAULT_AKKA_TRANSPORT_HEARTBEAT_PAUSE`
        - `java.lang.String MESOS_ARTIFACT_SERVER_SSL_ENABLED`
        - `java.lang.String RESTART_STRATEGY_FAILURE_RATE_DELAY`
        - `java.lang.String DELIMITED_FORMAT_MIN_LINE_SAMPLES_KEY`
        - `java.lang.String BLOB_FETCH_BACKLOG_KEY`
        - `java.lang.String FILESYSTEM_OUTPUT_ALWAYS_CREATE_DIRECTORY_KEY`
        - `java.lang.String DEFAULT_TASK_MANAGER_MAX_REGISTRATION_DURATION`
        - `java.lang.String TASK_MANAGER_LOG_PATH_KEY`
        - `java.lang.String DEFAULT_TASK_MANAGER_NETWORK_DEFAULT_IO_MODE`
        - `int DEFAULT_YARN_HEAP_CUTOFF`
        - `java.lang.String SECURITY_SSL_PROTOCOL`
        - `java.lang.String JOB_MANAGER_WEB_BACK_PRESSURE_NUM_SAMPLES`
        - `java.lang.String CHECKPOINTS_DIRECTORY_KEY`
        - `java.lang.String DELIMITED_FORMAT_MAX_LINE_SAMPLES_KEY`
        - `java.lang.String PATH_HADOOP_CONFIG`
        - `java.lang.String ZOOKEEPER_SASL_DISABLE`
        - `java.lang.String AKKA_LOOKUP_TIMEOUT`
        - `java.lang.String YARN_HEAP_CUTOFF_MIN`
        - `java.lang.String AKKA_CLIENT_TIMEOUT`
        - `int DEFAULT_ZOOKEEPER_SYNC_LIMIT`
        - `java.lang.String DEFAULT_HA_MODE`
        - `java.lang.String CONTAINERIZED_TASK_MANAGER_ENV_PREFIX`
        - `java.lang.String HA_ZOOKEEPER_CONNECTION_TIMEOUT`
        - `java.lang.String METRICS_REPORTER_ADDITIONAL_VARIABLES`
        - `java.lang.String MESOS_ARTIFACT_SERVER_PORT_KEY`
        - `java.lang.String TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD`
        - `java.lang.String TASK_MANAGER_MEMORY_SEGMENT_SIZE_KEY`
        - `java.lang.String YARN_APPLICATION_ATTEMPTS`
        - `java.lang.String AKKA_TRANSPORT_HEARTBEAT_PAUSE`
        - `java.lang.String DEFAULT_TASK_MANAGER_INITIAL_REGISTRATION_PAUSE`
        - `java.lang.String SECURITY_SSL_VERIFY_HOSTNAME`
        - `java.lang.String DEFAULT_PARALLELISM_KEY`
        - `java.lang.String AKKA_DISPATCHER_THROUGHPUT`
        - `java.lang.String TASK_MANAGER_MEMORY_FRACTION_KEY`
        - `java.lang.String JOB_MANAGER_WEB_UPLOAD_DIR_KEY`
        - `java.lang.String SECURITY_SSL_KEY_PASSWORD`
        - `int DEFAULT_BLOB_FETCH_RETRIES`
        - `java.lang.String MESOS_RESOURCEMANAGER_FRAMEWORK_USER`
        - `java.lang.String BLOB_SERVICE_SSL_ENABLED`
        - `java.lang.String DEFAULT_YARN_APPLICATION_MASTER_PORT`
        - `java.lang.String METRICS_SCOPE_NAMING_TM`
        - `java.lang.String TASK_MANAGER_MAX_REGISTARTION_PAUSE`
        - `long DEFAULT_LIBRARY_CACHE_MANAGER_CLEANUP_INTERVAL`
        - `int DEFAULT_FS_STREAM_OPENING_TIMEOUT`
        - `java.lang.String YARN_VCORES`
        - `java.lang.String YARN_MAX_FAILED_CONTAINERS`
        - `java.lang.String METRICS_REPORTER_INTERVAL_SUFFIX`
        - `java.lang.String DEFAULT_HA_ZOOKEEPER_CLIENT_ACL`
        - `float DEFAULT_MEMORY_MANAGER_MEMORY_FRACTION`
        - `java.lang.String SAVEPOINT_DIRECTORY_KEY`
        - `int DEFAULT_ZOOKEEPER_RETRY_WAIT`
        - `java.lang.String ZOOKEEPER_LATCH_PATH`
        - `java.lang.String DEFAULT_RECOVERY_JOB_MANAGER_PORT`
        - `boolean DEFAULT_TASK_MANAGER_DEBUG_MEMORY_USAGE_START_LOG_THREAD`
        - `boolean DEFAULT_AKKA_SSL_ENABLED`
- `org.apache.flink.configuration.ConfigOption`
    - method removed:
        - `java.lang.Iterable<java.lang.String> deprecatedKeys()`
        - `boolean hasDeprecatedKeys()`
- `org.apache.flink.configuration.ConfigOptions$OptionBuilder`
    - method removed:
        - `org.apache.flink.configuration.ConfigOption<T> defaultValue(java.lang.Object)`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> noDefaultValue()`
- `org.apache.flink.configuration.Configuration`
    - method removed:
        - `boolean getBoolean(java.lang.String, boolean)`
        - `boolean getBoolean(org.apache.flink.configuration.ConfigOption<java.lang.Boolean>)`
        - `boolean getBoolean(org.apache.flink.configuration.ConfigOption<java.lang.Boolean>, boolean)`
        - `byte[] getBytes(java.lang.String, byte[])`
        - `java.lang.Class<T> getClass(java.lang.String, java.lang.Class<? extends T>, java.lang.ClassLoader)`
        - `double getDouble(java.lang.String, double)`
        - `double getDouble(org.apache.flink.configuration.ConfigOption<java.lang.Double>)`
        - `double getDouble(org.apache.flink.configuration.ConfigOption<java.lang.Double>, double)`
        - `float getFloat(java.lang.String, float)`
        - `float getFloat(org.apache.flink.configuration.ConfigOption<java.lang.Float>)`
        - `float getFloat(org.apache.flink.configuration.ConfigOption<java.lang.Float>, float)`
        - `int getInteger(java.lang.String, int)`
        - `int getInteger(org.apache.flink.configuration.ConfigOption<java.lang.Integer>)`
        - `int getInteger(org.apache.flink.configuration.ConfigOption<java.lang.Integer>, int)`
        - `long getLong(java.lang.String, long)`
        - `long getLong(org.apache.flink.configuration.ConfigOption<java.lang.Long>)`
        - `long getLong(org.apache.flink.configuration.ConfigOption<java.lang.Long>, long)`
        - `java.lang.String getString(org.apache.flink.configuration.ConfigOption<java.lang.String>)`
        - `java.lang.String getString(org.apache.flink.configuration.ConfigOption<java.lang.String>, java.lang.String)`
        - `void setBoolean(java.lang.String, boolean)`
        - `void setBoolean(org.apache.flink.configuration.ConfigOption<java.lang.Boolean>, boolean)`
        - `void setBytes(java.lang.String, byte[])`
        - `void setClass(java.lang.String, java.lang.Class<?>)`
        - `void setDouble(java.lang.String, double)`
        - `void setDouble(org.apache.flink.configuration.ConfigOption<java.lang.Double>, double)`
        - `void setFloat(java.lang.String, float)`
        - `void setFloat(org.apache.flink.configuration.ConfigOption<java.lang.Float>, float)`
        - `void setInteger(java.lang.String, int)`
        - `void setInteger(org.apache.flink.configuration.ConfigOption<java.lang.Integer>, int)`
        - `void setLong(java.lang.String, long)`
        - `void setLong(org.apache.flink.configuration.ConfigOption<java.lang.Long>, long)`
        - `void setString(org.apache.flink.configuration.ConfigOption<java.lang.String>, java.lang.String)`
- `org.apache.flink.configuration.ExecutionOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Long> ASYNC_STATE_BUFFER_TIMEOUT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> ASYNC_INFLIGHT_RECORDS_LIMIT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> ASYNC_STATE_BUFFER_SIZE`
- `org.apache.flink.configuration.HighAvailabilityOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> HA_ZOOKEEPER_JOBGRAPHS_PATH`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> ZOOKEEPER_RUNNING_JOB_REGISTRY_PATH`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> HA_JOB_DELAY`
- `org.apache.flink.configuration.JobManagerOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> JOB_MANAGER_HEAP_MEMORY_MB`
        - `org.apache.flink.configuration.ConfigOption<java.time.Duration> BLOCK_SLOW_NODE_DURATION`
        - `org.apache.flink.configuration.ConfigOption<org.apache.flink.configuration.MemorySize> ADAPTIVE_BATCH_SCHEDULER_AVG_DATA_VOLUME_PER_TASK`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> SPECULATIVE_MAX_CONCURRENT_EXECUTIONS`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> ADAPTIVE_BATCH_SCHEDULER_DEFAULT_SOURCE_PARALLELISM`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> ADAPTIVE_BATCH_SCHEDULER_MAX_PARALLELISM`
        - `org.apache.flink.configuration.ConfigOption<org.apache.flink.configuration.MemorySize> JOB_MANAGER_HEAP_MEMORY`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> ADAPTIVE_BATCH_SCHEDULER_MIN_PARALLELISM`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> SPECULATIVE_ENABLED`
- `org.apache.flink.configuration.JobManagerOptions$SchedulerType`
    - field removed:
        - `org.apache.flink.configuration.JobManagerOptions$SchedulerType Ng`
- `org.apache.flink.configuration.MetricOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> REPORTER_CLASS`
- `org.apache.flink.configuration.NettyShuffleEnvironmentOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NUM_THREADS_CLIENT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NETWORK_BUFFERS_PER_CHANNEL`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> HYBRID_SHUFFLE_SPILLED_INDEX_REGION_GROUP_SIZE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> CONNECT_BACKLOG`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> NETWORK_HYBRID_SHUFFLE_ENABLE_NEW_MODE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Float> NETWORK_BUFFERS_MEMORY_FRACTION`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NUM_ARENAS`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NUM_THREADS_SERVER`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Long> HYBRID_SHUFFLE_NUM_RETAINED_IN_MEMORY_REGIONS_MAX`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> NETWORK_BUFFERS_MEMORY_MIN`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> TRANSPORT_TYPE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> NETWORK_BLOCKING_SHUFFLE_TYPE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NETWORK_MAX_OVERDRAFT_BUFFERS_PER_GATE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Long> NETWORK_EXCLUSIVE_BUFFERS_REQUEST_TIMEOUT_MILLISECONDS`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> BATCH_SHUFFLE_COMPRESSION_ENABLED`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> SEND_RECEIVE_BUFFER_SIZE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> NETWORK_BUFFERS_MEMORY_MAX`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NETWORK_EXTRA_BUFFERS_PER_GATE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NETWORK_SORT_SHUFFLE_MIN_PARALLELISM`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NETWORK_NUM_BUFFERS`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> NETWORK_MAX_BUFFERS_PER_CHANNEL`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> MAX_NUM_TCP_CONNECTIONS`
- `org.apache.flink.configuration.PipelineOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.util.List<java.lang.String>> KRYO_DEFAULT_SERIALIZERS`
        - `org.apache.flink.configuration.ConfigOption<java.util.List<java.lang.String>> POJO_REGISTERED_CLASSES`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> AUTO_TYPE_REGISTRATION`
        - `org.apache.flink.configuration.ConfigOption<java.util.List<java.lang.String>> KRYO_REGISTERED_CLASSES`
- `org.apache.flink.configuration.ResourceManagerOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> LOCAL_NUMBER_RESOURCE_MANAGER`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> TASK_MANAGER_RELEASE_WHEN_RESULT_CONSUMED`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Long> SLOT_MANAGER_TASK_MANAGER_TIMEOUT`
- `org.apache.flink.configuration.SecurityOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Double> KERBEROS_TOKENS_RENEWAL_TIME_RATIO`
        - `org.apache.flink.configuration.ConfigOption<java.time.Duration> KERBEROS_TOKENS_RENEWAL_RETRY_BACKOFF`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> KERBEROS_FETCH_DELEGATION_TOKEN`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> SSL_ENABLED`
- `org.apache.flink.configuration.StateBackendOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> LATENCY_TRACK_HISTORY_SIZE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> LATENCY_TRACK_STATE_NAME_AS_VARIABLE`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> LATENCY_TRACK_ENABLED`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> LATENCY_TRACK_SAMPLE_INTERVAL`
- `org.apache.flink.configuration.TaskManagerOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.time.Duration> REGISTRATION_MAX_BACKOFF`
        - `org.apache.flink.configuration.ConfigOption<java.time.Duration> INITIAL_REGISTRATION_BACKOFF`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> EXIT_ON_FATAL_AKKA_ERROR`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> TASK_MANAGER_HEAP_MEMORY_MB`
        - `java.lang.String MANAGED_MEMORY_CONSUMER_NAME_DATAPROC`
        - `org.apache.flink.configuration.ConfigOption<org.apache.flink.configuration.MemorySize> TASK_MANAGER_HEAP_MEMORY`
        - `org.apache.flink.configuration.ConfigOption<java.time.Duration> REFUSED_REGISTRATION_BACKOFF`
- `org.apache.flink.configuration.TaskManagerOptions$TaskManagerLoadBalanceMode`
    - method removed:
        - `org.apache.flink.configuration.TaskManagerOptions$TaskManagerLoadBalanceMode loadFromConfiguration(org.apache.flink.configuration.Configuration)`
- `org.apache.flink.configuration.WebOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> BACKPRESSURE_DELAY`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> PORT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> BACKPRESSURE_REFRESH_INTERVAL`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> BACKPRESSURE_NUM_SAMPLES`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> ADDRESS`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> BACKPRESSURE_CLEANUP_INTERVAL`
        - `org.apache.flink.configuration.ConfigOption<java.lang.Boolean> SSL_ENABLED`
- `org.apache.flink.connector.base.sink.AsyncSinkBase`
    - interface removed:
        - `org.apache.flink.api.connector.sink2.StatefulSink`
- `org.apache.flink.connector.base.sink.writer.AsyncSinkWriter`
    - constructor removed:
        - `AsyncSinkWriter(org.apache.flink.connector.base.sink.writer.ElementConverter<InputT,RequestEntryT>, org.apache.flink.api.connector.sink2.Sink$InitContext, int, int, int, long, long, long)`
        - `AsyncSinkWriter(org.apache.flink.connector.base.sink.writer.ElementConverter<InputT,RequestEntryT>, org.apache.flink.api.connector.sink2.Sink$InitContext, int, int, int, long, long, long, java.util.Collection<org.apache.flink.connector.base.sink.writer.BufferedRequestState<RequestEntryT>>)`
        - `AsyncSinkWriter(org.apache.flink.connector.base.sink.writer.ElementConverter<InputT,RequestEntryT>, org.apache.flink.api.connector.sink2.Sink$InitContext, org.apache.flink.connector.base.sink.writer.config.AsyncSinkWriterConfiguration, java.util.Collection<org.apache.flink.connector.base.sink.writer.BufferedRequestState<RequestEntryT>>)`
- `org.apache.flink.connector.base.sink.writer.ElementConverter`
    - method removed:
        - `void open(org.apache.flink.api.connector.sink2.Sink$InitContext)`
- `org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager`
    - constructor removed:
        - `SingleThreadFetcherManager(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, java.util.function.Supplier<org.apache.flink.connector.base.source.reader.splitreader.SplitReader<E,SplitT>>)`
        - `SingleThreadFetcherManager(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, java.util.function.Supplier<org.apache.flink.connector.base.source.reader.splitreader.SplitReader<E,SplitT>>, org.apache.flink.configuration.Configuration)`
        - `SingleThreadFetcherManager(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, java.util.function.Supplier<org.apache.flink.connector.base.source.reader.splitreader.SplitReader<E,SplitT>>, org.apache.flink.configuration.Configuration, java.util.function.Consumer<java.util.Collection<java.lang.String>>)`
- `org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager`
    - constructor removed:
        - `SplitFetcherManager(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, java.util.function.Supplier<org.apache.flink.connector.base.source.reader.splitreader.SplitReader<E,SplitT>>, org.apache.flink.configuration.Configuration, java.util.function.Consumer<java.util.Collection<java.lang.String>>)`
        - `SplitFetcherManager(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, java.util.function.Supplier<org.apache.flink.connector.base.source.reader.splitreader.SplitReader<E,SplitT>>, org.apache.flink.configuration.Configuration)`
- `org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase`
    - constructor removed:
        - `SingleThreadMultiplexSourceReaderBase(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, java.util.function.Supplier<org.apache.flink.connector.base.source.reader.splitreader.SplitReader<E,SplitT>>, org.apache.flink.connector.base.source.reader.RecordEmitter<E,T,SplitStateT>, org.apache.flink.configuration.Configuration, org.apache.flink.api.connector.source.SourceReaderContext)`
        - `SingleThreadMultiplexSourceReaderBase(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager<E,SplitT>, org.apache.flink.connector.base.source.reader.RecordEmitter<E,T,SplitStateT>, org.apache.flink.configuration.Configuration, org.apache.flink.api.connector.source.SourceReaderContext)`
        - `SingleThreadMultiplexSourceReaderBase(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, org.apache.flink.connector.base.source.reader.fetcher.SingleThreadFetcherManager<E,SplitT>, org.apache.flink.connector.base.source.reader.RecordEmitter<E,T,SplitStateT>, org.apache.flink.connector.base.source.reader.RecordEvaluator<T>, org.apache.flink.configuration.Configuration, org.apache.flink.api.connector.source.SourceReaderContext)`
- `org.apache.flink.connector.base.source.reader.SourceReaderBase`
    - constructor removed:
        - `SourceReaderBase(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager<E,SplitT>, org.apache.flink.connector.base.source.reader.RecordEmitter<E,T,SplitStateT>, org.apache.flink.configuration.Configuration, org.apache.flink.api.connector.source.SourceReaderContext)`
        - `SourceReaderBase(org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue<org.apache.flink.connector.base.source.reader.RecordsWithSplitIds<E>>, org.apache.flink.connector.base.source.reader.fetcher.SplitFetcherManager<E,SplitT>, org.apache.flink.connector.base.source.reader.RecordEmitter<E,T,SplitStateT>, org.apache.flink.connector.base.source.reader.RecordEvaluator<T>, org.apache.flink.configuration.Configuration, org.apache.flink.api.connector.source.SourceReaderContext)`
- `org.apache.flink.contrib.streaming.state.ConfigurableRocksDBOptionsFactory`
    - interface removed:
        - `org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory`
    - method removed:
        - `org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory configure(org.apache.flink.configuration.ReadableConfig)`
- `org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend`
    - method removed:
        - `org.apache.flink.contrib.streaming.state.RocksDBMemoryConfiguration getMemoryConfiguration()`
        - `org.apache.flink.contrib.streaming.state.PredefinedOptions getPredefinedOptions()`
        - `org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend$PriorityQueueStateType getPriorityQueueStateType()`
        - `org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory getRocksDBOptions()`
        - `void setPredefinedOptions(org.apache.flink.contrib.streaming.state.PredefinedOptions)`
        - `void setPriorityQueueStateType(org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend$PriorityQueueStateType)`
        - `void setRocksDBMemoryFactory(org.apache.flink.contrib.streaming.state.RocksDBMemoryControllerUtils$RocksDBMemoryFactory)`
        - `void setRocksDBOptions(org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory)`
- `org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions`
    - method removed:
        - `org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions fromConfig(org.apache.flink.configuration.ReadableConfig)`
- `org.apache.flink.contrib.streaming.state.RocksDBOptionsFactory`
    - method removed:
        - `org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions createNativeMetricsOptions(org.apache.flink.contrib.streaming.state.RocksDBNativeMetricOptions)`
- `org.apache.flink.core.execution.JobClient`
    - method removed:
        - `java.util.concurrent.CompletableFuture<java.lang.String> stopWithSavepoint(boolean, java.lang.String)`
        - `java.util.concurrent.CompletableFuture<java.lang.String> triggerSavepoint(java.lang.String)`
- `org.apache.flink.core.failure.FailureEnricher$Context`
    - method removed:
        - `org.apache.flink.api.common.JobID getJobId()`
        - `java.lang.String getJobName()`
- `org.apache.flink.core.fs.FileSystem`
    - method removed:
        - `org.apache.flink.core.fs.FileSystemKind getKind()`
- `org.apache.flink.core.fs.Path`
    - interface removed:
        - `org.apache.flink.core.io.IOReadableWritable`
- `org.apache.flink.datastream.api.context.StateManager`
    - method modified:
        - `org.apache.flink.api.common.state.v2.ListState<T>(<- <org.apache.flink.api.common.state.ListState<T>>) (<-java.util.Optional<T>(<- <org.apache.flink.api.common.state.ListState<T>>)) getState(org.apache.flink.api.common.state.ListStateDeclaration<T><T>)`
        - `org.apache.flink.api.common.state.v2.ValueState<T>(<- <org.apache.flink.api.common.state.ValueState<T>>) (<-java.util.Optional<T>(<- <org.apache.flink.api.common.state.ValueState<T>>)) getState(org.apache.flink.api.common.state.ValueStateDeclaration<T><T>)`
        - `org.apache.flink.api.common.state.v2.MapState<K,V>(<- <org.apache.flink.api.common.state.MapState<K,V>>) (<-java.util.Optional<K,V>(<- <org.apache.flink.api.common.state.MapState<K,V>>)) getState(org.apache.flink.api.common.state.MapStateDeclaration<K,V><K,V>)`
        - `org.apache.flink.api.common.state.v2.ReducingState<T>(<- <org.apache.flink.api.common.state.ReducingState<T>>) (<-java.util.Optional<T>(<- <org.apache.flink.api.common.state.ReducingState<T>>)) getState(org.apache.flink.api.common.state.ReducingStateDeclaration<T><T>)`
        - `org.apache.flink.api.common.state.v2.AggregatingState<IN,OUT>(<- <org.apache.flink.api.common.state.AggregatingState<IN,OUT>>) (<-java.util.Optional<IN,OUT>(<- <org.apache.flink.api.common.state.AggregatingState<IN,OUT>>)) getState(org.apache.flink.api.common.state.AggregatingStateDeclaration<IN,ACC,OUT><IN,ACC,OUT>)`
        - `org.apache.flink.api.common.state.BroadcastState<K,V>(<- <org.apache.flink.api.common.state.BroadcastState<K,V>>) (<-java.util.Optional<K,V>(<- <org.apache.flink.api.common.state.BroadcastState<K,V>>)) getState(org.apache.flink.api.common.state.BroadcastStateDeclaration<K,V><K,V>)`
- `org.apache.flink.datastream.api.ExecutionEnvironment`
    - method modified:
        - `org.apache.flink.datastream.api.stream.NonKeyedPartitionStream$ProcessConfigurableAndNonKeyedPartitionStream<OUT><OUT> (<-org.apache.flink.datastream.api.stream.NonKeyedPartitionStream<OUT><OUT>) fromSource(org.apache.flink.api.connector.dsv2.Source<OUT><OUT>, java.lang.String)`
- `org.apache.flink.datastream.api.function.ProcessFunction`
    - method removed:
        - `void open()`
- `org.apache.flink.datastream.api.function.TwoOutputApplyPartitionFunction`
    - method removed:
        - `void apply(org.apache.flink.datastream.api.common.Collector<OUT1>, org.apache.flink.datastream.api.common.Collector<OUT2>, org.apache.flink.datastream.api.context.PartitionedContext)`
- `org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction`
    - method removed:
        - `void onProcessingTimer(long, org.apache.flink.datastream.api.common.Collector<OUT1>, org.apache.flink.datastream.api.common.Collector<OUT2>, org.apache.flink.datastream.api.context.PartitionedContext)`
        - `void processRecord(java.lang.Object, org.apache.flink.datastream.api.common.Collector<OUT1>, org.apache.flink.datastream.api.common.Collector<OUT2>, org.apache.flink.datastream.api.context.PartitionedContext)`
- `org.apache.flink.datastream.api.stream.KeyedPartitionStream`
    - method modified:
        - `org.apache.flink.datastream.api.stream.KeyedPartitionStream$ProcessConfigurableAndTwoKeyedPartitionStreams<K,OUT1,OUT2><K,OUT1,OUT2> (<-org.apache.flink.datastream.api.stream.KeyedPartitionStream$TwoKeyedPartitionStreams<K,OUT1,OUT2><K,OUT1,OUT2>) process(org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction<T,OUT1,OUT2><T,OUT1,OUT2>, org.apache.flink.api.java.functions.KeySelector<OUT1,K><OUT1,K>, org.apache.flink.api.java.functions.KeySelector<OUT2,K><OUT2,K>)`
        - `org.apache.flink.datastream.api.stream.NonKeyedPartitionStream$ProcessConfigurableAndTwoNonKeyedPartitionStream<OUT1,OUT2><OUT1,OUT2> (<-org.apache.flink.datastream.api.stream.NonKeyedPartitionStream$TwoNonKeyedPartitionStreams<OUT1,OUT2><OUT1,OUT2>) process(org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction<T,OUT1,OUT2><T,OUT1,OUT2>)`
- `org.apache.flink.datastream.api.stream.NonKeyedPartitionStream`
    - method modified:
        - `org.apache.flink.datastream.api.stream.NonKeyedPartitionStream$ProcessConfigurableAndTwoNonKeyedPartitionStream<OUT1,OUT2><OUT1,OUT2> (<-org.apache.flink.datastream.api.stream.NonKeyedPartitionStream$TwoNonKeyedPartitionStreams<OUT1,OUT2><OUT1,OUT2>) process(org.apache.flink.datastream.api.function.TwoOutputStreamProcessFunction<T,OUT1,OUT2><T,OUT1,OUT2>)`
- `org.apache.flink.streaming.api.connector.sink2.CommittableMessage`
    - method removed:
        - `java.util.OptionalLong getCheckpointId()`
- `org.apache.flink.streaming.api.connector.sink2.CommittableSummary`
    - constructor removed:
        - `CommittableSummary(int, int, java.lang.Long, int, int, int)`
- `org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage`
    - constructor removed:
        - `CommittableWithLineage(java.lang.Object, java.lang.Long, int)`
- `org.apache.flink.streaming.api.datastream.AllWindowedStream`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.AllWindowedStream<T,W> allowedLateness(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<R> apply(org.apache.flink.api.common.functions.ReduceFunction<T>, org.apache.flink.streaming.api.functions.windowing.AllWindowFunction<T,R,W>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<R> apply(org.apache.flink.api.common.functions.ReduceFunction<T>, org.apache.flink.streaming.api.functions.windowing.AllWindowFunction<T,R,W>, org.apache.flink.api.common.typeinfo.TypeInformation<R>)`
- `org.apache.flink.streaming.api.datastream.CoGroupedStreams$WithWindow`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.CoGroupedStreams$WithWindow<T1,T2,KEY,W> allowedLateness(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> with(org.apache.flink.api.common.functions.CoGroupFunction<T1,T2,T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> with(org.apache.flink.api.common.functions.CoGroupFunction<T1,T2,T>, org.apache.flink.api.common.typeinfo.TypeInformation<T>)`
    - method modified:
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T><T> (<-org.apache.flink.streaming.api.datastream.DataStream<T><T>) apply(org.apache.flink.api.common.functions.CoGroupFunction<T1,T2,T><T1,T2,T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T><T> (<-org.apache.flink.streaming.api.datastream.DataStream<T><T>) apply(org.apache.flink.api.common.functions.CoGroupFunction<T1,T2,T><T1,T2,T>, org.apache.flink.api.common.typeinfo.TypeInformation<T><T>)`
- `org.apache.flink.streaming.api.datastream.DataStream`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> addSink(org.apache.flink.streaming.api.functions.sink.SinkFunction<T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks<T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> assignTimestampsAndWatermarks(org.apache.flink.streaming.api.functions.AssignerWithPunctuatedWatermarks<T>)`
        - `org.apache.flink.streaming.api.datastream.IterativeStream<T> iterate()`
        - `org.apache.flink.streaming.api.datastream.IterativeStream<T> iterate(long)`
        - `org.apache.flink.streaming.api.datastream.KeyedStream<T,org.apache.flink.api.java.tuple.Tuple> keyBy(int[])`
        - `org.apache.flink.streaming.api.datastream.KeyedStream<T,org.apache.flink.api.java.tuple.Tuple> keyBy(java.lang.String[])`
        - `org.apache.flink.streaming.api.datastream.DataStream<T> partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, int)`
        - `org.apache.flink.streaming.api.datastream.DataStream<T> partitionCustom(org.apache.flink.api.common.functions.Partitioner<K>, java.lang.String)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> sinkTo(org.apache.flink.api.connector.sink.Sink<T,?,?,?>)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> sinkTo(org.apache.flink.api.connector.sink.Sink<T,?,?,?>, org.apache.flink.streaming.api.datastream.CustomSinkOperatorUidHashes)`
        - `org.apache.flink.streaming.api.datastream.AllWindowedStream<T,org.apache.flink.streaming.api.windowing.windows.TimeWindow> timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.AllWindowedStream<T,org.apache.flink.streaming.api.windowing.windows.TimeWindow> timeWindowAll(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> writeAsCsv(java.lang.String)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> writeAsCsv(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode, java.lang.String, java.lang.String)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> writeAsText(java.lang.String)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSink<T> writeAsText(java.lang.String, org.apache.flink.core.fs.FileSystem$WriteMode)`
- `org.apache.flink.streaming.api.datastream.DataStreamUtils`
    - method removed:
        - `java.util.Iterator<OUT> collect(org.apache.flink.streaming.api.datastream.DataStream<OUT>)`
        - `java.util.Iterator<OUT> collect(org.apache.flink.streaming.api.datastream.DataStream<OUT>, java.lang.String)`
        - `java.util.List<E> collectBoundedStream(org.apache.flink.streaming.api.datastream.DataStream<E>, java.lang.String)`
        - `java.util.List<E> collectRecordsFromUnboundedStream(org.apache.flink.streaming.api.operators.collect.ClientAndIterator<E>, int)`
        - `java.util.List<E> collectUnboundedStream(org.apache.flink.streaming.api.datastream.DataStream<E>, int, java.lang.String)`
        - `org.apache.flink.streaming.api.operators.collect.ClientAndIterator<OUT> collectWithClient(org.apache.flink.streaming.api.datastream.DataStream<OUT>, java.lang.String)`
- `org.apache.flink.streaming.api.datastream.JoinedStreams$WithWindow`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.JoinedStreams$WithWindow<T1,T2,KEY,W> allowedLateness(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> with(org.apache.flink.api.common.functions.JoinFunction<T1,T2,T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> with(org.apache.flink.api.common.functions.FlatJoinFunction<T1,T2,T>, org.apache.flink.api.common.typeinfo.TypeInformation<T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> with(org.apache.flink.api.common.functions.FlatJoinFunction<T1,T2,T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T> with(org.apache.flink.api.common.functions.JoinFunction<T1,T2,T>, org.apache.flink.api.common.typeinfo.TypeInformation<T>)`
    - method modified:
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T><T> (<-org.apache.flink.streaming.api.datastream.DataStream<T><T>) apply(org.apache.flink.api.common.functions.JoinFunction<T1,T2,T><T1,T2,T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T><T> (<-org.apache.flink.streaming.api.datastream.DataStream<T><T>) apply(org.apache.flink.api.common.functions.FlatJoinFunction<T1,T2,T><T1,T2,T>, org.apache.flink.api.common.typeinfo.TypeInformation<T><T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T><T> (<-org.apache.flink.streaming.api.datastream.DataStream<T><T>) apply(org.apache.flink.api.common.functions.FlatJoinFunction<T1,T2,T><T1,T2,T>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<T><T> (<-org.apache.flink.streaming.api.datastream.DataStream<T><T>) apply(org.apache.flink.api.common.functions.JoinFunction<T1,T2,T><T1,T2,T>, org.apache.flink.api.common.typeinfo.TypeInformation<T><T>)`
- `org.apache.flink.streaming.api.datastream.KeyedStream`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.WindowedStream<T,KEY,org.apache.flink.streaming.api.windowing.windows.TimeWindow> timeWindow(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.WindowedStream<T,KEY,org.apache.flink.streaming.api.windowing.windows.TimeWindow> timeWindow(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.datastream.KeyedStream$IntervalJoin`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.KeyedStream$IntervalJoined<T1,T2,KEY> between(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.datastream.WindowedStream`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.WindowedStream<T,K,W> allowedLateness(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<R> apply(org.apache.flink.api.common.functions.ReduceFunction<T>, org.apache.flink.streaming.api.functions.windowing.WindowFunction<T,R,K,W>)`
        - `org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator<R> apply(org.apache.flink.api.common.functions.ReduceFunction<T>, org.apache.flink.streaming.api.functions.windowing.WindowFunction<T,R,K,W>, org.apache.flink.api.common.typeinfo.TypeInformation<R>)`
- `org.apache.flink.streaming.api.environment.CheckpointConfig`
    - field removed:
        - `int UNDEFINED_TOLERABLE_CHECKPOINT_NUMBER`
        - `long DEFAULT_TIMEOUT`
        - `long DEFAULT_MIN_PAUSE_BETWEEN_CHECKPOINTS`
        - `org.apache.flink.streaming.api.CheckpointingMode DEFAULT_MODE`
        - `int DEFAULT_MAX_CONCURRENT_CHECKPOINTS`
        - `int DEFAULT_CHECKPOINT_ID_OF_IGNORED_IN_FLIGHT_DATA`
    - method removed:
        - `void enableExternalizedCheckpoints(org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup)`
        - `java.time.Duration getAlignmentTimeout()`
        - `org.apache.flink.runtime.state.CheckpointStorage getCheckpointStorage()`
        - `org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup getExternalizedCheckpointCleanup()`
        - `boolean isFailOnCheckpointingErrors()`
        - `boolean isForceCheckpointing()`
        - `void setAlignmentTimeout(java.time.Duration)`
        - `void setCheckpointStorage(org.apache.flink.runtime.state.CheckpointStorage)`
        - `void setCheckpointStorage(java.lang.String)`
        - `void setCheckpointStorage(java.net.URI)`
        - `void setCheckpointStorage(org.apache.flink.core.fs.Path)`
        - `void setExternalizedCheckpointCleanup(org.apache.flink.streaming.api.environment.CheckpointConfig$ExternalizedCheckpointCleanup)`
        - `void setFailOnCheckpointingErrors(boolean)`
        - `void setForceCheckpointing(boolean)`
- `org.apache.flink.streaming.api.environment.RemoteStreamEnvironment`
    - method removed:
        - `org.apache.flink.configuration.Configuration getClientConfiguration()`
- `org.apache.flink.streaming.api.environment.StreamExecutionEnvironment`
    - field removed:
        - `java.lang.String DEFAULT_JOB_NAME`
    - method removed:
        - `void addDefaultKryoSerializer(java.lang.Class<?>, com.esotericsoftware.kryo.Serializer<?>)`
        - `void addDefaultKryoSerializer(java.lang.Class<?>, java.lang.Class<? extends com.esotericsoftware.kryo.Serializer<? extends ?>>)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSource<OUT> addSource(org.apache.flink.streaming.api.functions.source.SourceFunction<OUT>)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSource<OUT> addSource(org.apache.flink.streaming.api.functions.source.SourceFunction<OUT>, java.lang.String)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSource<OUT> addSource(org.apache.flink.streaming.api.functions.source.SourceFunction<OUT>, org.apache.flink.api.common.typeinfo.TypeInformation<OUT>)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSource<OUT> addSource(org.apache.flink.streaming.api.functions.source.SourceFunction<OUT>, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<OUT>)`
        - `org.apache.flink.streaming.api.environment.StreamExecutionEnvironment enableCheckpointing(long, org.apache.flink.streaming.api.CheckpointingMode, boolean)`
        - `org.apache.flink.streaming.api.environment.StreamExecutionEnvironment enableCheckpointing()`
        - `int getNumberOfExecutionRetries()`
        - `org.apache.flink.api.common.restartstrategy.RestartStrategies$RestartStrategyConfiguration getRestartStrategy()`
        - `org.apache.flink.runtime.state.StateBackend getStateBackend()`
        - `org.apache.flink.streaming.api.TimeCharacteristic getStreamTimeCharacteristic()`
        - `boolean isForceCheckpointing()`
        - `org.apache.flink.streaming.api.datastream.DataStream<java.lang.String> readFileStream(java.lang.String, long, org.apache.flink.streaming.api.functions.source.FileMonitoringFunction$WatchType)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSource<java.lang.String> readTextFile(java.lang.String)`
        - `org.apache.flink.streaming.api.datastream.DataStreamSource<java.lang.String> readTextFile(java.lang.String, java.lang.String)`
        - `void registerType(java.lang.Class<?>)`
        - `void registerTypeWithKryoSerializer(java.lang.Class<?>, com.esotericsoftware.kryo.Serializer<?>)`
        - `void registerTypeWithKryoSerializer(java.lang.Class<?>, java.lang.Class<? extends com.esotericsoftware.kryo.Serializer>)`
        - `void setNumberOfExecutionRetries(int)`
        - `void setRestartStrategy(org.apache.flink.api.common.restartstrategy.RestartStrategies$RestartStrategyConfiguration)`
        - `org.apache.flink.streaming.api.environment.StreamExecutionEnvironment setStateBackend(org.apache.flink.runtime.state.StateBackend)`
        - `void setStreamTimeCharacteristic(org.apache.flink.streaming.api.TimeCharacteristic)`
- `org.apache.flink.streaming.api.operators.AbstractStreamOperator`
    - interface removed:
        - `org.apache.flink.streaming.api.operators.SetupableStreamOperator`
    - method modified:
        - `(<- NON_FINAL) void initializeState(org.apache.flink.streaming.api.operators.StreamTaskStateInitializer)`
        - `PROTECTED (<- PUBLIC) void setProcessingTimeService(org.apache.flink.streaming.runtime.tasks.ProcessingTimeService)`
        - `PROTECTED (<- PUBLIC) void setup(org.apache.flink.streaming.runtime.tasks.StreamTask<?,?><?,?>, org.apache.flink.streaming.api.graph.StreamConfig, org.apache.flink.streaming.api.operators.Output<org.apache.flink.streaming.runtime.streamrecord.StreamRecord<OUT>><org.apache.flink.streaming.runtime.streamrecord.StreamRecord<OUT>>)`
- `org.apache.flink.streaming.api.operators.AbstractStreamOperatorV2`
    - method modified:
        - `(<- NON_FINAL) void initializeState(org.apache.flink.streaming.api.operators.StreamTaskStateInitializer)`
- `org.apache.flink.streaming.api.operators.AbstractUdfStreamOperator`
    - method modified:
        - `PROTECTED (<- PUBLIC) void setup(org.apache.flink.streaming.runtime.tasks.StreamTask<?,?><?,?>, org.apache.flink.streaming.api.graph.StreamConfig, org.apache.flink.streaming.api.operators.Output<org.apache.flink.streaming.runtime.streamrecord.StreamRecord<OUT>><org.apache.flink.streaming.runtime.streamrecord.StreamRecord<OUT>>)`
- `org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows withGap(org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.assigners.ProcessingTimeSessionWindows withGap(org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.assigners.SlidingEventTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.assigners.WindowStagger)`
- `org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows of(org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.time.Time, org.apache.flink.streaming.api.windowing.assigners.WindowStagger)`
- `org.apache.flink.streaming.api.windowing.assigners.WindowAssigner`
    - method modified:
        - `(<- NON_ABSTRACT) org.apache.flink.streaming.api.windowing.triggers.Trigger<T,W><T,W> getDefaultTrigger()`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.triggers.Trigger<T,W> getDefaultTrigger(org.apache.flink.streaming.api.environment.StreamExecutionEnvironment)`
- `org.apache.flink.streaming.api.windowing.evictors.TimeEvictor`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.evictors.TimeEvictor<W> of(org.apache.flink.streaming.api.windowing.time.Time)`
        - `org.apache.flink.streaming.api.windowing.evictors.TimeEvictor<W> of(org.apache.flink.streaming.api.windowing.time.Time, boolean)`
- `org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.triggers.ContinuousEventTimeTrigger<W> of(org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger`
    - method removed:
        - `org.apache.flink.streaming.api.windowing.triggers.ContinuousProcessingTimeTrigger<W> of(org.apache.flink.streaming.api.windowing.time.Time)`
- `org.apache.flink.streaming.api.windowing.triggers.Trigger$TriggerContext`
    - method removed:
        - `org.apache.flink.api.common.state.ValueState<S> getKeyValueState(java.lang.String, java.lang.Class<S>, java.io.Serializable)`
        - `org.apache.flink.api.common.state.ValueState<S> getKeyValueState(java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<S>, java.io.Serializable)`
- `org.apache.flink.streaming.experimental.CollectSink`
    - interface removed:
        - `org.apache.flink.streaming.api.functions.sink.SinkFunction`
    - superclass modified:
        - `org.apache.flink.streaming.api.functions.sink.legacy.RichSinkFunction (<- org.apache.flink.streaming.api.functions.sink.RichSinkFunction)`
- `org.apache.flink.types.DoubleValue`
    - interface removed:
        - `org.apache.flink.types.Key`
- `org.apache.flink.types.FloatValue`
    - interface removed:
        - `org.apache.flink.types.Key`
- `org.apache.flink.types.NormalizableKey`
    - interface removed:
        - `org.apache.flink.core.io.IOReadableWritable`
        - `org.apache.flink.types.Value`
        - `org.apache.flink.types.Key`
        - `java.io.Serializable`
- `org.apache.flink.test.junit5.MiniClusterExtension`
    - method removed:
        - `org.apache.flink.test.util.TestEnvironment getTestEnvironment()`
- `org.apache.flink.metrics.prometheus.PrometheusPushGatewayReporterOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> PORT`
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> HOST`
- `org.apache.flink.formats.csv.CsvReaderFormat`
    - method removed:
        - `org.apache.flink.formats.csv.CsvReaderFormat<T> forSchema(org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvMapper, org.apache.flink.shaded.jackson2.com.fasterxml.jackson.dataformat.csv.CsvSchema, org.apache.flink.api.common.typeinfo.TypeInformation<T>)`
- `org.apache.flink.state.forst.ForStOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.String> REMOTE_DIRECTORY`
- `org.apache.flink.state.forst.ForStOptionsFactory`
    - method removed:
        - `org.rocksdb.ColumnFamilyOptions createColumnOptions(org.rocksdb.ColumnFamilyOptions, java.util.Collection<java.lang.AutoCloseable>)`
        - `org.rocksdb.DBOptions createDBOptions(org.rocksdb.DBOptions, java.util.Collection<java.lang.AutoCloseable>)`
        - `org.rocksdb.ReadOptions createReadOptions(org.rocksdb.ReadOptions, java.util.Collection<java.lang.AutoCloseable>)`
        - `org.rocksdb.WriteOptions createWriteOptions(org.rocksdb.WriteOptions, java.util.Collection<java.lang.AutoCloseable>)`
- `org.apache.flink.table.client.config.SqlClientOptions`
    - field removed:
        - `org.apache.flink.configuration.ConfigOption<java.lang.Integer> DISPLAY_MAX_COLUMN_WIDTH`
- `org.apache.flink.table.runtime.typeutils.SortedMapTypeInfo`
    - method removed:
        - `org.apache.flink.api.common.typeutils.TypeSerializer<java.util.SortedMap<K,V>> createSerializer(org.apache.flink.api.common.ExecutionConfig)`
- `org.apache.flink.connector.file.sink.FileSink`
    - method removed:
        - `org.apache.flink.api.connector.sink2.SinkWriter<IN> createWriter(org.apache.flink.api.connector.sink2.Sink$InitContext)`
- `org.apache.flink.connector.file.src.FileSource`
    - method removed:
        - `org.apache.flink.connector.file.src.FileSource$FileSourceBuilder<T> forRecordFileFormat(org.apache.flink.connector.file.src.reader.FileRecordFormat<T>, org.apache.flink.core.fs.Path[])`
- `org.apache.flink.connector.file.src.FileSourceSplit`
    - constructor removed:
        - `FileSourceSplit(java.lang.String, org.apache.flink.core.fs.Path, long, long)`
        - `FileSourceSplit(java.lang.String, org.apache.flink.core.fs.Path, long, long, java.lang.String[])`
        - `FileSourceSplit(java.lang.String, org.apache.flink.core.fs.Path, long, long, java.lang.String[], org.apache.flink.connector.file.src.util.CheckpointedPosition)`
- `org.apache.flink.state.api.functions.KeyedStateReaderFunction`
    - method removed:
        - `void open(org.apache.flink.configuration.Configuration)`
- `org.apache.flink.state.api.OperatorTransformation`
    - method removed:
        - `org.apache.flink.state.api.OneInputOperatorTransformation<T> bootstrapWith(org.apache.flink.api.java.DataSet<T>)`
- `org.apache.flink.state.api.SavepointReader`
    - method removed:
        - `org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.api.java.tuple.Tuple2<K,V>> readBroadcastState(java.lang.String, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<K>, org.apache.flink.api.common.typeinfo.TypeInformation<V>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<org.apache.flink.api.java.tuple.Tuple2<K,V>> readBroadcastState(java.lang.String, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<K>, org.apache.flink.api.common.typeinfo.TypeInformation<V>, org.apache.flink.api.common.typeutils.TypeSerializer<K>, org.apache.flink.api.common.typeutils.TypeSerializer<V>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<OUT> readKeyedState(java.lang.String, org.apache.flink.state.api.functions.KeyedStateReaderFunction<K,OUT>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<OUT> readKeyedState(java.lang.String, org.apache.flink.state.api.functions.KeyedStateReaderFunction<K,OUT>, org.apache.flink.api.common.typeinfo.TypeInformation<K>, org.apache.flink.api.common.typeinfo.TypeInformation<OUT>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<T> readListState(java.lang.String, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<T>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<T> readListState(java.lang.String, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<T>, org.apache.flink.api.common.typeutils.TypeSerializer<T>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<T> readUnionState(java.lang.String, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<T>)`
        - `org.apache.flink.streaming.api.datastream.DataStream<T> readUnionState(java.lang.String, java.lang.String, org.apache.flink.api.common.typeinfo.TypeInformation<T>, org.apache.flink.api.common.typeutils.TypeSerializer<T>)`
- `org.apache.flink.state.api.SavepointWriter`
    - method removed:
        - `org.apache.flink.state.api.SavepointWriter fromExistingSavepoint(java.lang.String)`
        - `org.apache.flink.state.api.SavepointWriter fromExistingSavepoint(java.lang.String, org.apache.flink.runtime.state.StateBackend)`
        - `org.apache.flink.state.api.SavepointWriter newSavepoint(int)`
        - `org.apache.flink.state.api.SavepointWriter newSavepoint(org.apache.flink.runtime.state.StateBackend, int)`
        - `org.apache.flink.state.api.SavepointWriter removeOperator(java.lang.String)`
        - `org.apache.flink.state.api.SavepointWriter withOperator(java.lang.String, org.apache.flink.state.api.StateBootstrapTransformation<T>)`
- `org.apache.flink.state.api.SavepointWriterOperatorFactory`
    - method modified:
        - `org.apache.flink.streaming.api.operators.StreamOperatorFactory<org.apache.flink.state.api.output.TaggedOperatorSubtaskState><org.apache.flink.state.api.output.TaggedOperatorSubtaskState> (<-org.apache.flink.streaming.api.operators.StreamOperator<org.apache.flink.state.api.output.TaggedOperatorSubtaskState><org.apache.flink.state.api.output.TaggedOperatorSubtaskState>) createOperator(long, org.apache.flink.core.fs.Path)`

## List of removed configuration options <a name="removed_configs" />

- cluster.evenly-spread-out-slots
- execution.async-state.buffer-size
- execution.async-state.buffer-timeout
- execution.async-state.in-flight-records-limit
- fine-grained.shuffle-mode.all-blocking
- high-availability.job.delay
- high-availability.zookeeper.path.jobgraphs
- high-availability.zookeeper.path.running-registry
- jobmanager.adaptive-batch-scheduler.avg-data-volume-per-task
- jobmanager.adaptive-batch-scheduler.default-source-parallelism
- jobmanager.adaptive-batch-scheduler.max-parallelism
- jobmanager.adaptive-batch-scheduler.min-parallelism
- jobmanager.adaptive-batch-scheduler.speculative.block-slow-node-duration
- jobmanager.adaptive-batch-scheduler.speculative.enabled
- jobmanager.adaptive-batch-scheduler.speculative.max-concurrent-executions
- jobmanager.heap.mb
- jobmanager.heap.size
- jobmanager.web.address
- jobmanager.web.backpressure.cleanup-interval
- jobmanager.web.backpressure.delay-between-samples
- jobmanager.web.backpressure.num-samples
- jobmanager.web.backpressure.refresh-interval
- jobmanager.web.port;
- jobmanager.web.ssl.enabled
- local.number-resourcemanager
- pipeline.auto-type-registration
- pipeline.default-kryo-serializers
- pipeline.registered-kryo-types
- pipeline.registered-pojo-types
- recovery.job.delay
- resourcemanager.taskmanager-release.wait.result.consumed
- security.kerberos.fetch.delegation-token
- security.kerberos.tokens.renewal.retry.backoff
- security.kerberos.tokens.renewal.time-ratio
- security.ssl.enabled
- slotmanager.taskmanager-timeout
- sql-client.display.max-column-width
- state.backend.async
- state.backend.forst.remote-dir
- state.backend.latency-track.history-size
- state.backend.latency-track.keyed-state-enabled
- state.backend.latency-track.sample-interval
- state.backend.latency-track.state-name-as-variable
- state.backend.local-recovery
- state.backend.rocksdb.checkpointdir
- state.backend.type
- streaming-source.consume-order
- table.exec.deduplicate.insert-and-updateafter-sensitive.enabled
- table.exec.deduplicate.mini-batch.compact-changes.enabled
- table.exec.legacy-transformation-uids
- table.exec.shuffle-mode
- table.exec.topn-cache-size
- table.optimizer.source.aggregate-pushdown-enabled
- table.optimizer.source.predicate-pushdown-enabled
- table.optimizer.sql-to-rel.project.merge.enabled
- taskmanager.exit-on-fatal-akka-error
- taskmanager.heap.mb
- taskmanager.heap.size
- taskmanager.initial-registration-pause
- taskmanager.max-registration-pause
- taskmanager.net.client.numThreads
- taskmanager.net.num-arenas
- taskmanager.net.sendReceiveBufferSize
- taskmanager.net.server.backlog
- taskmanager.net.server.numThreads
- taskmanager.net.transport
- taskmanager.network.batch-shuffle.compression.enabled
- taskmanager.network.blocking-shuffle.compression.enabled
- taskmanager.network.blocking-shuffle.type
- taskmanager.network.hybrid-shuffle.enable-new-mode
- taskmanager.network.hybrid-shuffle.num-retained-in-memory-regions-max
- taskmanager.network.hybrid-shuffle.spill-index-region-group-size
- taskmanager.network.hybrid-shuffle.spill-index-segment-size
- taskmanager.network.max-num-tcp-connections
- taskmanager.network.memory.buffers-per-channel
- taskmanager.network.memory.exclusive-buffers-request-timeout-ms
- taskmanager.network.memory.floating-buffers-per-gate
- taskmanager.network.memory.fraction
- taskmanager.network.memory.max
- taskmanager.network.memory.max-buffers-per-channel
- taskmanager.network.memory.max-overdraft-buffers-per-gate
- taskmanager.network.memory.min
- taskmanager.network.netty.client.numThreads
- taskmanager.network.netty.num-arenas
- taskmanager.network.netty.sendReceiveBufferSize
- taskmanager.network.netty.server.backlog
- taskmanager.network.netty.server.numThreads
- taskmanager.network.netty.transport
- taskmanager.network.numberOfBuffers
- taskmanager.network.sort-shuffle.min-parallelism
- taskmanager.refused-registration-pause
- taskmanager.registration.initial-backoff
- taskmanager.registration.max-backoff
- taskmanager.registration.refused-backoff
- web.address
- web.backpressure.cleanup-interval
- web.backpressure.delay-between-samples
- web.backpressure.num-samples
- web.backpressure.refresh-interval
- web.port
- web.ssl.enabled

## List of REST APIs changes

| REST API                                                                                                                                                                                                                                                                                                                                                                                       | Changes                                                                                                                                                         |
|------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| /taskmanagers/:taskmanagerid                                                                                                                                                                                                                                                                                                                                                                   | In its response, "metrics.memorySegmentsAvailable" and "metrics.memorySegmentsTotal" are removed.                                                               |
| /jobs/:jobid/config                                                                                                                                                                                                                                                                                                                                                                            | In its response, the "execution-mode" property is removed.                                                                                                      |
| /jars/:jarid/run                                                                                                                                                                                                                                                                                                                                                                               | In its request, the internal type of "claimMode" and "restoreMode" are changed from RestoreMode to RecoveryClaimMode, but their json structure is not affected. |
| /jobs/:jobid/vertices/:vertexid<br />/jobs/:jobid/vertices/:vertexid/subtasks/accumulators<br />/jobs/:jobid/vertices/:vertexid/subtasks/:subtaskindex<br />/jobs/:jobid/vertices/:vertexid/subtasks/:subtaskindex/attempts/:attempt<br />/jobs/:jobid/vertices/:vertexid/subtasktimes<br />/jobs/:jobid/vertices/:vertexid/taskmanagers<br />/jobs/:jobid/taskmanagers/:taskmanagerid/log-url | In their responses, the "host", "subtasks.host" or "taskmanagers.host" property is removed.                                                                     |

## List of removed CLI options

- sql-client.sh:
    - `-u,--update <SQL update statement>` is removed: Please use option `-f` to submit update.
      statement.
- flink-client:
    - `run-application` action is removed: Please use `run -t kubernetes-application` to run Kubernetes Application mode.
