---
title: "Release Notes - Flink 1.13"
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

# Release Notes - Flink 1.13

These release notes discuss important aspects, such as configuration, behavior, or dependencies,
that changed between Flink 1.12 and Flink 1.13. Please read these notes carefully if you are
planning to upgrade your Flink version to 1.13.

### Failover

#### Remove state.backend.async option.

##### [FLINK-21935](https://issues.apache.org/jira/browse/FLINK-21935)

The `state.backend.async` option is deprecated. Snapshots are always asynchronous now (as they were
by default before) and there is no option to configure a synchronous snapshot any more.

The constructors of `FsStateBackend` and `MemoryStateBackend` that take a flag for sync/async
snapshots are kept for API compatibility, but the flags are ignored now.

#### Disentangle StateBackends from Checkpointing

##### [FLINK-19463](https://issues.apache.org/jira/browse/FLINK-19463)

Flink has always separated local state storage from fault tolerance.
Keyed state is maintained locally in state backends, either on the JVM heap or in embedded RocksDB instances.
Fault tolerance comes from checkpoints and savepoints - periodic snapshots of a job's internal state to some durable file system - such as Amazon S3 or HDFS. 

Historically, Flink's `StateBackend` interface intermixed these concepts in a way that confused many users. 
In 1.13, checkpointing configurations have been extracted into their own interface, `CheckpointStorage`. 

This change does not affect the runtime behavior and simply provides a better mental model to users. 
Pipelines can be updated to use the new the new abstractions without losing state, consistency, or change in semantics. 

Please follow the [migration guide](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/ops/state/state_backends/#migrating-from-legacy-backends) or the JavaDoc on the deprecated state backend classes - `MemoryStateBackend`, `FsStateBackend` and `RocksDBStateBackend` for migration details. 

#### Unify binary format for Keyed State savepoints

##### [FLINK-20976](https://issues.apache.org/jira/browse/FLINK-20976)

Flink’s savepoint binary format is unified across all state backends. That means you can take a
savepoint with one state backend and then restore it using another.

If you want to switch the state backend you should first upgrade your Flink version to 1.13, then
take a savepoint with the new version, and only after that, you can restore it with a different
state backend.

#### FailureRateRestartBackoffTimeStrategy allows one less restart than configured

##### [FLINK-20752](https://issues.apache.org/jira/browse/FLINK-20752)

The Failure Rate Restart Strategy was allowing 1 less restart per interval than configured. Users
wishing to keep the current behavior should reduce the maximum number of allowed failures per
interval by 1.

#### Support rescaling for Unaligned Checkpoints

##### [FLINK-17979](https://issues.apache.org/jira/browse/FLINK-17979)

While recovering from unaligned checkpoints, users can now change the parallelism of the job. This
change allows users to quickly upscale the job under backpressure.

### SQL

#### Officially deprecate the legacy planner

##### [FLINK-21709](https://issues.apache.org/jira/browse/FLINK-21709)

The old planner of the Table & SQL API is deprecated and will be dropped in Flink 1.14. This means
that both the BatchTableEnvironment and DataSet API interop are reaching end of life. Use the
unified TableEnvironment for batch and stream processing with the new planner, or the DataStream API
in batch execution mode.

#### Use TIMESTAMP_LTZ as return type for function PROCTIME()

##### [FLINK-21714](https://issues.apache.org/jira/browse/FLINK-21714)

Before Flink 1.13, the function return type of `PROCTIME()` is `TIMESTAMP`, and the return value is
the `TIMESTAMP` in UTC time zone, e.g. the wall-clock shows `2021-03-01 12:00:00` at Shanghai,
however the `PROCTIME()` displays `2021-03-01 04:00:00` which is wrong. Flink 1.13 fixes this issue
and uses `TIMESTAMP_LTZ` type as return type of `PROCTIME()`, users don't need to deal time zone
problems anymore.

#### Support defining event time attribute on TIMESTAMP_LTZ column

##### [FLINK-20387](https://issues.apache.org/jira/browse/FLINK-20387)

Support defining event time attribute on TIMESTAMP_LTZ column, base on this, Flink SQL gracefully
support the Daylight Saving Time.

#### Correct function CURRENT_TIMESTAMP/CURRENT_TIME/CURRENT_DATE/LOCALTIME/LOCALTIMESTAMP/NOW()

##### [FLINK-21713](https://issues.apache.org/jira/browse/FLINK-21713)

The value of time function CURRENT_TIMESTAMP and NOW() are corrected from UTC time with `TIMESTAMP`
type to epoch time with `TIMESTAMP_LTZ` type. Time function LOCALTIME, LOCALTIMESTAMP, CURRENT_DATE,
CURRENT_TIME, CURRENT_TIMESTAMP and NOW() are corrected from evaluates for per record in batch mode
to evaluate once at query-start for batch job.

#### Disable problematic cast conversion between NUMERIC type and TIMESTAMP type

##### [FLINK-21698](https://issues.apache.org/jira/browse/FLINK-21698)

The CAST operation between `NUMERIC` type and `TIMESTAMP` type is problematic and is disabled now,
e.g.
`CAST(numeric AS TIMESTAMP(3))` is disabled and should use `TO_TIMESTAMP(FROM_UNIXTIME(numeric))`
instead.

#### Support USE MODULES syntax

##### [FLINK-21298](https://issues.apache.org/jira/browse/FLINK-21298)

The term MODULES is a reserved keyword now. Use backticks to escape column names and other
identifiers with this name.

#### Update TableResult.collect()/TableResult.print() to the new type system

##### [FLINK-20613](https://issues.apache.org/jira/browse/FLINK-20613)

`Table.execute().collect()` might return slightly different results for column types and row kind.
The most important differences include:

* Structured types are represented as POJOs of the original class and not Row anymore.
* Raw types are serialized according to the configuration in TableConfig.

#### Add new StreamTableEnvironment.fromDataStream

##### [FLINK-19977](https://issues.apache.org/jira/browse/FLINK-19977)

`StreamTableEnvironment.fromDataStream` has slightly different semantics now because it has been
integrated into the new type system. Esp. row fields derived from composite type information
might be in a different order compared to 1.12. The old behavior is still available via the
overloaded method that takes expressions like `fromDataStream(ds, $("field1"), $("field2"))`.

#### Update the Row.toString method

##### [FLINK-18090](https://issues.apache.org/jira/browse/FLINK-18090)

The `Row.toSting()` method has been reworked. This is an incompatible change. If the legacy
representation is still required for tests, the old behavior can be restored via the flag
`RowUtils.USE_LEGACY_TO_STRING` for the local JVM. However, relying on the row's string
representation for tests is not a good idea in general as field data types are not verified.

#### Support start SQL Client with an initialization SQL file

##### [FLINK-20320](https://issues.apache.org/jira/browse/FLINK-20320)

The `sql-client-defaults.yaml` YAML file is deprecated and not provided in the release package. To
be compatible, it's still supported to initialize the SQL Client with the YAML file if manually
provided. But it's recommend to use the new introduced `-i` startup option to execute an
initialization SQL file to setup the SQL Client session. The so-called initialization SQL file can
use Flink DDLs to define available catalogs, table sources and sinks, user-defined functions, and
other properties required for execution and deployment. The support of legacy SQL Client YAML file
will be totally dropped in Flink 1.14.

#### Hive dialect no longer supports Flink syntax for DML and DQL

##### [FLINK-21808](https://issues.apache.org/jira/browse/FLINK-21808)

Hive dialect supports HiveQL for DML and DQL. Please switch to default dialect in order to write in
Flink syntax.

### Runtime

#### BoundedOneInput.endInput is called when taking synchronous savepoint

##### [FLINK-21132](https://issues.apache.org/jira/browse/FLINK-21132)

`endInput()` is not called anymore (on BoundedOneInput and BoundedMultiInput) when the job is
stopping with savepoint.

#### Remove JobManagerOptions.SCHEDULING_STRATEGY

##### [FLINK-20591](https://issues.apache.org/jira/browse/FLINK-20591)

The configuration parameter `jobmanager.scheduler.scheduling-strategy` has been removed, because
the `legacy` scheduler has been removed from Flink 1.13.0.

#### Warn user if System.exit() is called in user code

##### [FLINK-15156](https://issues.apache.org/jira/browse/FLINK-15156)

A new configuration value `cluster.intercept-user-system-exit` allows to log a warning, or throw an
exception if user code calls `System.exit()`.

This feature is not covering all locations in Flink where user code is executed. It just adds the
infrastructure for such an interception. We are tracking this improvement in
[FLINK-21307](https://issues.apache.org/jira/browse/FLINK-21307).

#### MiniClusterJobClient#getAccumulators was infinitely blocking in local environment for a streaming job
##### [FLINK-18685](https://issues.apache.org/jira/browse/FLINK-18685)

The semantics for accumulators have now changed in `MiniClusterJobClient` to fix this bug and comply with other JobClient implementations:
Previously `MiniClusterJobClient` assumed that `getAccumulator()` was called on a bounded pipeline and that the user wanted to acquire the final
accumulator values after the job is finished.
But now it returns the current value of accumulators immediately to be compatible with unbounded pipelines.

If it is run on a bounded pipeline, then to get the final accumulator values after the job is finished, one needs to call

`getJobExecutionResult().thenApply(JobExecutionResult::getAllAccumulatorResults)`

### Docker

#### Consider removing automatic configuration fo number of slots from docker

##### [FLINK-21036](https://issues.apache.org/jira/browse/FLINK-21036)

The docker images no longer set the default number of taskmanager slots to the number of CPU cores.
This behavior was inconsistent with all other deployment methods and ignored any limits on the CPU
usage set via docker.

#### Rework jemalloc switch to use an environment variable

##### [FLINK-21034](https://issues.apache.org/jira/browse/FLINK-21034)

The docker switch for disabling the jemalloc memory allocator has been reworked from a script
argument to an environment variable called DISABLE_JEMALLOC. If set to "true" jemalloc will not be
enabled.

### Connectors

#### Remove swift FS filesystem

##### [FLINK-21819](https://issues.apache.org/jira/browse/FLINK-21819)

The Swift filesystem is no longer being actively developed and has been removed from the project and
distribution.

##### [FLINK-22133](https://issues.apache.org/jira/browse/FLINK-22133)
The unified source API for connectors has a minor breaking change. The `SplitEnumerator.snapshotState()`
method was adjusted to accept the *Checkpoint ID* of the checkpoint for which the snapshot is created.

### Monitoring & debugging

#### Introduce latency tracking state

##### [FLINK-21736](https://issues.apache.org/jira/browse/FLINK-21736)

State access latency metrics are introduced to track all kinds of keyed state access to help debug
state performance. This feature is not enabled by default and can be turned on by
setting `state.backend.latency-track.keyed-state-enabled` to true.

#### Support for CPU flame graphs in web UI

##### [FLINK-13550](https://issues.apache.org/jira/browse/FLINK-13550)

Flink now offers flame graphs for each node in the job graph. Please enable this experimental feature
by setting the respective configuration flag `rest.flamegraph.enabled`.

#### Display last n exceptions/causes for job restarts in Web UI

##### [FLINK-6042](https://issues.apache.org/jira/browse/FLINK-6042)

Flink exposes the exception history now through the REST API and the UI. The amount of most-recently
handled exceptions that shall be tracked can be defined through `web.exception-history-size`.
Some values of the exception history's REST API Json response are deprecated as part of this effort.

#### Create backPressuredTimeMsPerSecond metric

##### [FLINK-20717](https://issues.apache.org/jira/browse/FLINK-20717)

Previously `idleTimeMsPerSecond` was defined as the time task spent waiting for either the input or
the back pressure. Now `idleTimeMsPerSecond` excludes back pressured time, so if the task is back
pressured it is not idle. The back pressured time is now measured separately
as `backPressuredTimeMsPerSecond`.

#### Enable log4j2 monitor interval by default

##### [FLINK-20510](https://issues.apache.org/jira/browse/FLINK-20510)

The Log4j support for updating the Log4j configuration at runtime has been enabled by default. The
configuration files are checked for changes every 30 seconds.

#### ZooKeeper quorum fails to start due to missing log4j library

##### [FLINK-20404](https://issues.apache.org/jira/browse/FLINK-20404)

The Zookeeper scripts in the Flink distribution have been modified to disable the Log4j JMX
integration due to an incompatibility between Zookeeper 3.4 and Log4j 2. To re-enable this feature,
remove the line in the `zookeeper.sh` file that sets `zookeeper.jmx.log4j.disable`.

#### Expose stage of task initialization

##### [FLINK-17012](https://issues.apache.org/jira/browse/FLINK-17012)

Task's RUNNING state was split into two states: INITIALIZING and RUNNING. Task is INITIALIZING while
state is initialising and in case of unaligned checkpoints, until all the in-flight data has been
recovered.

### Deployment

#### Officially deprecate Mesos support

##### [FLINK-22352](https://issues.apache.org/jira/browse/FLINK-22352)

The community decided to deprecate the Apache Mesos support for Apache Flink. It is subject to
removal in the future. Users are encouraged to switch to a different resource manager.
