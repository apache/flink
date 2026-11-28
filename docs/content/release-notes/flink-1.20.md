---
title: "Release Notes - Flink 1.20"
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

# Release notes - Flink 1.20

These release notes discuss important aspects, such as configuration, behavior or dependencies,
that changed between Flink 1.19 and Flink 1.20. Please read these notes carefully if you are 
planning to upgrade your Flink version to 1.20.

### Checkpoints

#### Unified File Merging Mechanism for Checkpoints

##### [FLINK-32070](https://issues.apache.org/jira/browse/FLINK-32070)

The unified file merging mechanism for checkpointing is introduced to Flink 1.20 as an MVP ("minimum viable product") feature.
It combines multiple small checkpoint files to into fewer larger files, which reduces the number of file creation
and file deletion operations and alleviates the pressure of file system metadata management during checkpoints.

The mechanism can be enabled by setting `execution.checkpointing.file-merging.enabled` to `true`. For more advanced options
and the principles behind this feature, please refer to the Checkpointing documentation.

#### Reorganize State & Checkpointing & Recovery Configuration

##### [FLINK-34255](https://issues.apache.org/jira/browse/FLINK-34255)

Currently, all the options about state and checkpointing are reorganized and categorized by
prefixes as listed below:

1. `execution.checkpointing.*`: all configurations associated with checkpointing and savepoint.
2. `execution.state-recovery.*`: all configurations pertinent to state recovery.
3. `state.*`: all configurations related to the state accessing.
    1. `state.backend.*`: specific options for individual state backends, such as RocksDB.
    2. `state.changelog.*`: configurations for the changelog, as outlined in FLIP-158, including the options for the "Durable Short-term Log" (DSTL).
    3. `state.latency-track.*`: configurations related to the latency tracking of state access.

In the meantime, all the original options scattered everywhere are annotated as `@Deprecated`.

#### Use common thread pools when transferring RocksDB state files

##### [FLINK-35501](https://issues.apache.org/jira/browse/FLINK-35501)

The semantics of `state.backend.rocksdb.checkpoint.transfer.thread.num` changed slightly:
If negative, the common (TM) IO thread pool is used (see `cluster.io-pool.size`) for up/downloading RocksDB files.

#### Expose RocksDB bloom filter metrics

##### [FLINK-34386](https://issues.apache.org/jira/browse/FLINK-34386)

We expose some RocksDB bloom filter metrics to monitor the effectiveness of bloom filter optimization:

`BLOOM_FILTER_USEFUL`: times bloom filter has avoided file reads.
`BLOOM_FILTER_FULL_POSITIVE`: times bloom FullFilter has not avoided the reads.
`BLOOM_FILTER_FULL_TRUE_POSITIVE`: times bloom FullFilter has not avoided the reads and data actually exist.

#### Manually Compact Small SST Files

##### [FLINK-26050](https://issues.apache.org/jira/browse/FLINK-26050)

In some cases, the number of files produced by RocksDB state backend grows indefinitely.
This might cause task state info (TDD and checkpoint ACK) to exceed RPC message size and fail recovery/checkpoint
in addition to having lots of small files.

In Flink 1.20, you can manually merge such files in the background using RocksDB API via setting `state.backend.rocksdb.manual-compaction.min-interval` value `> 0`(zero disables manual compaction).

### Runtime & Coordination

#### Support Job Recovery from JobMaster Failures for Batch Jobs

##### [FLINK-33892](https://issues.apache.org/jira/browse/FLINK-33892)

In 1.20, we introduced a batch job recovery mechanism to enable batch jobs to recover as much progress as possible 
after a JobMaster failover, avoiding the need to rerun tasks that have already been finished.

More information about this feature and how to enable it could be found in the [documentation](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/ops/batch/recovery_from_job_master_failure/)

#### Extend Curator config option for Zookeeper configuration

##### [FLINK-33376](https://issues.apache.org/jira/browse/FLINK-33376)

Adds support for the following curator parameters: 
`high-availability.zookeeper.client.authorization` (corresponding curator parameter: `authorization`),
`high-availability.zookeeper.client.max-close-wait` (corresponding curator parameter: `maxCloseWaitMs`), 
`high-availability.zookeeper.client.simulated-session-expiration-percent` (corresponding curator parameter: `simulatedSessionExpirationPercent`).

#### More fine-grained timer processing

##### [FLINK-20217](https://issues.apache.org/jira/browse/FLINK-20217)

Firing timers can now be interrupted to speed up checkpointing. Timers that were interrupted by a checkpoint,
will be fired shortly after checkpoint completes. 

By default, this features is disabled. To enabled it please set `execution.checkpointing.unaligned.interruptible-timers.enabled` to `true`.
Currently supported only by all `TableStreamOperators` and `CepOperator`.

#### Add numFiredTimers and numFiredTimersPerSecond metrics

##### [FLINK-35065](https://issues.apache.org/jira/browse/FLINK-35065)

Currently, there is no way of knowing how many timers are being fired by Flink, so it's impossible to distinguish,
even using code profiling, if the operator is firing only a couple of heavy timers per second using ~100% of the CPU time,
vs firing thousands of timer per seconds.

We added the following metrics to address this issue:

- `numFiredTimers`: total number of fired timers per operator
- `numFiredTimersPerSecond`: per second rate of firing timers per operator

#### Support EndOfStreamTrigger and isOutputOnlyAfterEndOfStream Operator Attribute to Optimize Task Deployment

##### [FLINK-34371](https://issues.apache.org/jira/browse/FLINK-34371)

For operators that only generates outputs after all inputs have been consumed, they are now optimized
to run in blocking mode, and the other operators in the same job will wait to start until these operators
have finished. Such operators include windowing with `GlobalWindows#createWithEndOfStreamTrigger`, 
sorting, and etc.

### SDK

#### Support Full Partition Processing On Non-keyed DataStream

##### [FLINK-34543](https://issues.apache.org/jira/browse/FLINK-34543)

Before 1.20, the `DataStream` API did not directly support aggregations on non-keyed streams (subtask-scope aggregations).
As a workaround, users could assign the subtask id to the records before turning the stream into a keyed stream which incurred additional overhead.
Flink 1.20 adds built-in support for these operations via the `FullPartitionWindow` API.

### Table SQL / API

#### Introduce a New Materialized Table for Simplifying Data Pipelines

##### [FLINK-35187](https://issues.apache.org/jira/browse/FLINK-35187)

In Flink 1.20, We introduced Materialized Tables abstraction in Flink SQL, a new table type designed to simplify both batch and stream
data pipelines, while providing a consistent development experience.

Materialized tables are defined with a query and a data freshness specification. The engine automatically derives the table
schema and creates a data refresh pipeline to maintain the query result with the requested freshness. Users are relieved from
the burden of comprehending the concepts and differences between streaming and batch processing, and they do not have to directly
maintain Flink streaming or batch jobs. All operations are done on Materialized tables, which can significantly accelerate ETL pipeline
development.

More information about this feature can be found here: [Materialized Table Overview](https://nightlies.apache.org/flink/flink-docs-release-1.20/docs/dev/table/materialized-table/overview/)

#### Introduce Catalog-related Syntax

##### [FLINK-34914](https://issues.apache.org/jira/browse/FLINK-34914)

With the growing adoption of Flink SQL, implementations of Flink's `Catalog` interface play an increasingly important role. 
Today, Flink features a JDBC and a Hive catalog implementation and other open source projects such as Apache Paimon integrate with this interface as well.

Now in Flink 1.20, you can use the `DQL` syntax to obtain detailed metadata from existing catalogs, and the `DDL` syntax to modify metadata such as properties or comment in the specified catalog.

#### Harden correctness for non-deterministic updates present in the changelog pipeline

##### [FLINK-27849](https://issues.apache.org/jira/browse/FLINK-27849)

For complex streaming jobs, now it's possible to detect and resolve potential correctness issues 
before running.

#### Add DISTRIBUTED BY clause

##### [FLINK-33494](https://issues.apache.org/jira/browse/FLINK-33494)

Many SQL engines expose the concepts of `Partitioning`, `Bucketing`, or `Clustering`. We propose to introduce 
the concept of `Bucketing` to Flink.

Buckets enable load balancing in an external storage system by splitting data into disjoint subsets. It depends
heavily on the semantics of the underlying connector. However, a user can influence the bucketing behavior by
specifying the number of buckets, the distribution algorithm, and (if the algorithm allows it) the columns which
are used for target bucket calculation. All bucketing components (i.e. bucket number, distribution algorithm, bucket key columns)
are optional in the SQL syntax.

#### Exclude the mini-batch assigner operator if it serves no purpose

##### [FLINK-32622](https://issues.apache.org/jira/browse/FLINK-32622)

Currently, when users configure mini-batch for their SQL jobs, Flink always includes the mini-batch assigner operator in the job plan, even if there are no aggregate or join operators in the job.

The mini-batch operator will generate unnecessary events, leading to performance issues. If the mini-batch is not needed for specific jobs, Flink will avoid adding the mini-batch assigner, even when users enable the mini-batch mechanism.

#### Support for RETURNING clause of JSON_QUERY

##### [FLINK-35216](https://issues.apache.org/jira/browse/FLINK-35216)

We support `RETURNING` clause for `JSON_QUERY` in 1.20 to match the SQL standard.

- sql: `JSON_QUERY(jsonValue, path [RETURNING <dataType>] [ { WITHOUT | WITH CONDITIONAL | WITH UNCONDITIONAL } [ ARRAY ] WRAPPER ] [ { NULL | EMPTY ARRAY | EMPTY OBJECT | ERROR } ON EMPTY ] [ { NULL | EMPTY ARRAY | EMPTY OBJECT | ERROR } ON ERROR ])`
- table: `STRING.jsonQuery(path [, returnType [, JsonQueryWrapper [, JsonQueryOnEmptyOrError, JsonQueryOnEmptyOrError ] ] ])`

### Connectors

#### Support dynamic parallelism inference for HiveSource

##### [FLINK-35293](https://issues.apache.org/jira/browse/FLINK-35293)

In Flink 1.20, we have introduced support for dynamic source parallelism inference in batch jobs for the Hive source connector.
This allows the connector to dynamically determine parallelism based on the actual partitions with dynamic partition pruning.
Additionally, we have introduced a new configuration option - `table.exec.hive.infer-source-parallelism.mode` to enable
users to choose between static and dynamic inference modes for source parallelism. By default, the mode is set to `dynamic`.
Users may configure it to `static` for static inference, `dynamic` for dynamic inference, or `none` to disable automatic parallelism inference altogether.

It should be noted that in Flink 1.20, the previous configuration option `table.exec.hive.infer-source-parallelism` has been marked as deprecated, 
but it will continue to serve as a switch for automatic parallelism inference until it is fully phased out.

#### Promote Unified Sink API V2 to Public and Deprecate SinkFunction

##### [FLINK-35378](https://issues.apache.org/jira/browse/FLINK-35378)

`TwoPhaseCommitSinkFunction`, `SocketClientSink`, `RichSinkFunction`, `PrintSinkFunction` and `DiscardingSink`
have been deprecated in favor of the new `org.apache.flink.streaming.api.functions.sink.v2.*` interfaces.

#### Support for HTTP connect and timeout options while writes in GCS connector

##### [FLINK-32877](https://issues.apache.org/jira/browse/FLINK-32877)

The current GCS connector uses the gcs java storage library and bypasses the hadoop gcs connector 
which supports multiple http options. There are situations where GCS takes longer to provide a response
for a PUT operation than the default value.

This change will allow users to customize their connect time and read timeout based on their application

#### Introduce timeout configuration to AsyncSink

##### [FLINK-35435](https://issues.apache.org/jira/browse/FLINK-35435)

In Flink 1.20, We have introduced timeout configuration to `AsyncSink` with `retryOnTimeout`
and `failOnTimeout` mechanisms to ensure the writer doesn't block on un-acknowledged requests.

### Formats

#### Allow set ReadDefaultValues to false for non-primitive types on Proto3

##### [FLINK-33817](https://issues.apache.org/jira/browse/FLINK-33817)

Protobuf format now supports set `protobuf.read-default-values = false` for non-primitive types on pb3.
Since the default value for `protobuf.read-default-values` is `false`, this is a breaking change, 
if you are using pb3, you can set 'protobuf.read-default-values' = 'true' to keep the original behavior.

### REST API & Web UI

#### Show JobType on WebUI

##### [FLINK-29481](https://issues.apache.org/jira/browse/FLINK-29481)

We display the JobType on the Flink WebUI. The job maintainer or platform administrator can easily
see whether the Flink Job is running in Streaming or Batch Mode, which is useful for troubleshooting.

#### Expose SlotId / SlotSharingGroup in Rest API

##### [FLINK-20090](https://issues.apache.org/jira/browse/FLINK-20090)

We have exposed slot sharing group information in the REST API, which would be useful to monitor how tasks are assigned to task slots.

### Configuration

#### Using proper type for configuration

##### [FLINK-35359](https://issues.apache.org/jira/browse/FLINK-35359)

The following configurations have been updated to the `Duration` type in a backward-compatible manner:
- `client.heartbeat.interval`
- `client.heartbeat.timeout`
- `cluster.registration.error-delay`
- `cluster.registration.initial-timeout`
- `cluster.registration.max-timeout`
- `cluster.registration.refused-registration-delay`
- `cluster.services.shutdown-timeout`
- `heartbeat.interval`
- `heartbeat.timeout`
- `high-availability.zookeeper.client.connection-timeout`
- `high-availability.zookeeper.client.retry-wait`
- `high-availability.zookeeper.client.session-timeout`
- `historyserver.archive.fs.refresh-interval`
- `historyserver.web.refresh-interval`
- `metrics.fetcher.update-interval`
- `metrics.latency.interval`
- `metrics.reporter.influxdb.connectTimeout`
- `metrics.reporter.influxdb.writeTimeout`
- `metrics.system-resource-probing-interval`
- `pekko.startup-timeout`
- `pekko.tcp.timeout`
- `resourcemanager.job.timeout`
- `resourcemanager.standalone.start-up-time`
- `resourcemanager.taskmanager-timeout`
- `rest.await-leader-timeout`
- `rest.connection-timeout`
- `rest.idleness-timeout`
- `rest.retry.delay`
- `slot.idle.timeout`
- `slot.request.timeout`
- `task.cancellation.interval`
- `task.cancellation.timeout`
- `task.cancellation.timers.timeout`
- `taskmanager.debug.memory.log-interval`
- `web.refresh-interval`
- `web.timeout`
- `yarn.heartbeat.container-request-interval`

The following configurations have been updated to the `Enum` type in a backward-compatible manner:
- `taskmanager.network.compression.codec`
- `table.optimizer.agg-phase-strategy`

The following configurations have been updated to the `Int` type in a backward-compatible manner:
- `yarn.application-attempts`

#### Deprecate Some Runtime Configuration for Flink 2.0

##### [FLINK-35461](https://issues.apache.org/jira/browse/FLINK-35461)

The following configurations have been deprecated as we are phasing out the hash-based blocking shuffle:
- `taskmanager.network.sort-shuffle.min-parallelism`
- `taskmanager.network.blocking-shuffle.type`

The following configurations have been deprecated as we are phasing out the legacy hybrid shuffle:
- `taskmanager.network.hybrid-shuffle.spill-index-region-group-size`
- `taskmanager.network.hybrid-shuffle.num-retained-in-memory-regions-max`
- `taskmanager.network.hybrid-shuffle.enable-new-mode`

The following configurations have been deprecated to simply the configuration of network buffers:
- `taskmanager.network.memory.buffers-per-channel`
- `taskmanager.network.memory.floating-buffers-per-gate`
- `taskmanager.network.memory.max-buffers-per-channel`
- `taskmanager.network.memory.max-overdraft-buffers-per-gate`
- `taskmanager.network.memory.exclusive-buffers-request-timeout-ms` (Please use `taskmanager.network.memory.buffers-request-timeout` instead.)

The configuration `taskmanager.network.batch-shuffle.compression.enabled` has been deprecated. Please set `taskmanager.network.compression.codec` to `NONE` to disable compression.

The following Netty-related configurations are no longer recommended for use and have been deprecated:
- `taskmanager.network.netty.num-arenas`
- `taskmanager.network.netty.server.numThreads`
- `taskmanager.network.netty.client.numThreads`
- `taskmanager.network.netty.server.backlog`
- `taskmanager.network.netty.sendReceiveBufferSize`
- `taskmanager.network.netty.transport`

The following configurations are unnecessary and have been deprecated:
- `taskmanager.network.max-num-tcp-connections`
- `fine-grained.shuffle-mode.all-blocking`

#### Improve Table/SQL Configuration for Flink 2.0

##### [FLINK-35473](https://issues.apache.org/jira/browse/FLINK-35473)

As Apache Flink progresses to version 2.0, several table configuration options are being deprecated
and replaced to improve user-friendliness and maintainability.

###### Deprecated Configuration Options

The following table configuration options are deprecated in this release and will be removed in Flink 2.0:

Deprecated Due to TPC Testing Irrelevance

These options were previously used for fine-tuning TPC testing but are no longer needed by the current Flink planner:

- `table.exec.range-sort.enabled`
- `table.optimizer.rows-per-local-agg`
- `table.optimizer.join.null-filter-threshold`
- `table.optimizer.semi-anti-join.build-distinct.ndv-ratio`
- `table.optimizer.shuffle-by-partial-key-enabled`
- `table.optimizer.smj.remove-sort-enabled`
- `table.optimizer.cnf-nodes-limit`

Deprecated Due to Legacy Interface

These options were introduced for the now-obsolete `FilterableTableSource` interface:

- `table.optimizer.source.aggregate-pushdown-enabled`
- `table.optimizer.source.predicate-pushdown-enabled`

###### New and Updated Configuration Options

SQL Client Option

- `sql-client.display.max-column-width` has been replaced with `table.display.max-column-width`.

Batch Execution Options

The following options have been moved from `org.apache.flink.table.planner.codegen.agg.batch.HashAggCodeGenerator`
to `org.apache.flink.table.api.config` and promoted to `@PublicEvolving`

- `table.exec.local-hash-agg.adaptive.enabled`
- `table.exec.local-hash-agg.adaptive.sampling-threshold`
- `table.exec.local-hash-agg.adaptive.distinct-value-rate-threshold`

Lookup Hint Options

The following options have been moved from `org.apache.flink.table.planner.hint.LookupJoinHintOptions`
to `org.apache.flink.table.api.config.LookupJoinHintOptions` and promoted to `@PublicEvolving`

- `table`
- `async`
- `output-mode`
- `capacity`
- `timeout`
- `retry-predicate`
- `retry-strategy`
- `fixed-delay`
- `max-attempts`

Optimizer Options

The following options have been moved from `org.apache.flink.table.planner.plan.optimize.RelNodeBlock`
to `org.apache.flink.table.api.config.OptimizerConfigOptions` and promoted to `@PublicEvolving`

- `table.optimizer.union-all-as-breakpoint-enabled`
- `table.optimizer.reuse-optimize-block-with-digest-enabled`

Aggregate Optimizer Option

The following option has been moved from `org.apache.flink.table.planner.plan.rules.physical.stream.IncrementalAggregateRule`
to `org.apache.flink.table.api.config.OptimizerConfigOptions` and promoted to `@PublicEvolving`

- `table.optimizer.incremental-agg-enabled`
