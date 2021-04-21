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

#### Update the Row.toString method

##### [FLINK-18090](https://issues.apache.org/jira/browse/FLINK-18090)

The `Row.toSting()` method has been reworked. This is an incompatible change. If the legacy
representation is still required for tests, the old behavior can be restored via the flag
`RowUtils.USE_LEGACY_TO_STRING` for the local JVM. However, relying on the row's string
representation for tests is not a good idea in general as field data types are not verified.

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
infrastructure for such an interception.

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

