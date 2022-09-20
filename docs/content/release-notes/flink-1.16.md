---
title: "Release Notes - Flink 1.16"
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

# Release notes - Flink 1.16

These release notes discuss important aspects, such as configuration, behavior, or dependencies,
that changed between Flink 1.15 and Flink 1.16. Please read these notes carefully if you are
planning to upgrade your Flink version to 1.16.

### DataStream

#### Support Cache in DataStream for Batch Processing(FLIP-205)

##### [FLINK-27521](https://issues.apache.org/jira/browse/FLINK-27521)

Supports caching the result of a transformation via DataStream#cache(). The cached intermediate result
is generated lazily at the first time the intermediate result is computed so that the result can be
reused by later jobs. If the cache is lost, it will be recomputed using the original transformations.
Notes that currently only batch mode is supported.

### Table API & SQL

#### Remove string expression DSL

##### [FLINK-26704](https://issues.apache.org/jira/browse/FLINK-26704)

The deprecated String expression DSL has been removed from Java/Scala/Python Table API.

#### Add advanced function DDL syntax "USING JAR"

##### [FLINK-14055](https://issues.apache.org/jira/browse/FLINK-14055)

In 1.16, we introduced the `CREATE FUNCTION ... USING JAR` syntax to support the dynamic loading of 
the UDF jar in per job, which is convenient for platform developers to easily achieve UDF management.
In addition, we also port the `ADD JAR` syntax from SqlClient to `TableEnvironment` side, this allows
the syntax is more general to Table API users. However, due to inconsistent classloader in StreamExecutionEnvironment
and TableEnvironment, the `ADD JAR` syntax is not available for Table API program currently, it will
be resolved by https://issues.apache.org/jira/browse/FLINK-29240

More information about this feature could be found in https://nightlies.apache.org/flink/flink-docs-master/docs/dev/table/sql/create/#create-function.

#### Allow passing a ClassLoader to EnvironmentSettings

##### [FLINK-15635](https://issues.apache.org/jira/browse/FLINK-15635)

TableEnvironment introduces a user class loader to have a consistent class loading behavior in table
programs, SQL Client and SQL Gateway. The user classloader manages all user jars such as jar added
by `ADD JAR` or `CREATE FUNCTION .. USING JAR ..` statements. User-defined functions/connectors/catalogs
should replace `Thread.currentThread().getContextClassLoader()` with the user class loader to load classes.
Otherwise, ClassNotFoundException maybe thrown. The user class loader can be accessed via `FunctionContext#getUserCodeClassLoader`,
`DynamicTableFactory.Context#getClassLoader` and `CatalogFactory.Context#getClassLoader`.

#### Support Retryable Lookup Join To Solve Delayed Updates Issue In External Systems

##### [FLINK-28779](https://issues.apache.org/jira/browse/FLINK-28779)

Adds retryable lookup join to support both async and sync lookups in order to solve the delayed updates issue in external systems.

#### Make `AsyncDataStream.OutputMode` configurable for table module

##### [FLINK-27622](https://issues.apache.org/jira/browse/FLINK-27622)

It is recommend to set the new option 'table.exec.async-lookup.output-mode' to 'ALLOW_UNORDERED' when
no stritctly output order is needed, this will yield significant performance gains on append-only streams

#### Harden correctness for non-deterministic updates present in the changelog pipeline

##### [FLINK-27849](https://issues.apache.org/jira/browse/FLINK-27849)

For complex streaming jobs, now it's possible to detect and resolve potential correctness issues before running.

### Connectors

#### Move Elasticsearch connector to external connector repository

##### [FLINK-26884](https://issues.apache.org/jira/browse/FLINK-26884)

The Elasticsearch connector has been copied from the Flink repository to its own individual repository
at https://github.com/apache/flink-connector-elasticsearch

#### Drop support for Hive versions 1.*, 2.1.* and 2.2.*

##### [FLINK-27044](https://issues.apache.org/jira/browse/FLINK-27044)

Support for Hive 1.*, 2.1.* and 2.2.* has been dropped from Flink. These Hive version are no longer
supported by the Hive community and therefore are also no longer supported by Flink.

#### Hive sink report statistics to Hive metastore

##### [FLINK-28883](https://issues.apache.org/jira/browse/FLINK-28883)

In batch mode, Hive sink now will report statistics for written tables and partitions to Hive metastore by default.
This might be time-consuming when there are many written files. You can disable this feature by
setting `table.exec.hive.sink.statistic-auto-gather.enable` to `false`.

#### Remove a number of Pulsar cursor APIs

##### [FLINK-27399](https://issues.apache.org/jira/browse/FLINK-27399)

A number of breaking changes were made to the Pulsar Connector cursor APIs:

- CursorPosition#seekPosition() has been removed.
- StartCursor#seekPosition() has been removed.
- StopCursor#shouldStop now returns a StopCondition instead of a boolean.

#### Mark StreamingFileSink as deprecated

##### [FLINK-27188](https://issues.apache.org/jira/browse/FLINK-27188)

The StreamingFileSink has been deprecated in favor of the unified FileSink since Flink 1.12.
This changed is reflected in the docs, but not yet in the codebase.

#### Flink generated Avro schemas can't be parsed using Python

##### [FLINK-2596](https://issues.apache.org/jira/browse/FLINK-25962)

Avro schemas generated by Flink now use the "org.apache.flink.avro.generated" namespace for compatibility with the Avro Python SDK.

#### Introduce Protobuf format

##### [FLINK-18202](https://issues.apache.org/jira/browse/FLINK-18202)

Apache Flink now support the Protocol Buffers (Protobuf) format. This allows you to use this format
directly in your Table API or SQL applications.

#### Introduce configurable RateLimitingStrategy for Async Sink

##### [FLINK-28487](https://issues.apache.org/jira/browse/FLINK-28487)

Supports configurable RateLimitingStrategy for the AsyncSinkWriter. This change allows sink implementers
to change the behaviour of an AsyncSink when requests fail, for a specific sink. If no RateLimitingStrategy
is specified, it will default to current default of AIMDRateLimitingStrategy.

### Runtime & Coordination

##### Remove brackets around variables

###### [FLINK-24078](https://issues.apache.org/jira/browse/FLINK-24078)

The keys in the map returned my MetricGroup#getAllVariables are no longer surrounded by brackets,
e.g., <job_id> is now stored as job_id.

#### Deprecate reflection-based reporter instantiation

##### [FLINK-27206](https://issues.apache.org/jira/browse/FLINK-27206)

Configuring reporters by their class has been deprecated. Reporter implementations should provide a
MetricReporterFactory, and all configurations should be migrated to such a factory.

If the reporter is loaded from the plugins directory, setting metrics.reporter.reporter_name.class no longer works.

#### Deprecate Datadog reporter 'tags' option

##### [FLINK-29002](https://issues.apache.org/jira/browse/FLINK-29002)

The 'tags' option from the DatadogReporter has been deprecated in favor of the generic 'scope.variables.additional' option.

#### Return 503 Service Unavailable if endpoint is not ready yet

###### [FLINK-25269](https://issues.apache.org/jira/browse/FLINK-25269)

The REST API now returns a 503 Service Unavailable error when a request is made but the backing
component isn't ready yet. Previously this returned a 500 Internal Server error.

#### Completed Jobs Information Enhancement(FLIP-241)

##### [FLINK-28307](https://issues.apache.org/jira/browse/FLINK-28307)

We have enhanced the experiences of viewing completed jobs’ information in this release.
- JobManager / HistoryServer WebUI now provides detailed execution time metrics, including durations tasks spend in each execution state and the accumulated busy / idle / back-pressured time during running.
- JobManager / HistoryServer WebUI now provides aggregation of major SubTask metrics, grouped by Task or TaskManager.
- JobManager / HistoryServer WebUI now provides more environmental information, including environment variables, JVM options and classpath.
- HistoryServer now supports browsing logs from external log archiving services. For more details: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/deployment/advanced/historyserver#log-integration

#### Hybrid Shuffle Mode(FLIP-235)

##### [FLINK-27862](https://issues.apache.org/jira/browse/FLINK-27862)

We have introduced a new Hybrid Shuffle Mode for batch executions. It combines the advantages of blocking shuffle and pipelined shuffle (in streaming mode).
- Like blocking shuffle, it does not require upstream and downstream tasks to run simultaneously, which allows executing a job with little resources.
- Like pipelined shuffle, it does not require downstream tasks to be executed after upstream tasks finish, which reduces the overall execution time of the job when given sufficient resources.
- It adapts to custom preferences between persisting less data and restarting less tasks on failures, by providing different spilling strategies.

For more details: https://nightlies.apache.org/flink/flink-docs-release-1.16/docs/ops/batch/batch_shuffle.md

### Checkpoints

#### Add the overdraft buffer in BufferPool to reduce unaligned checkpoint being blocked

##### [FLINK-26762](https://issues.apache.org/jira/browse/FLINK-26762)

New concept of overdraft network buffers was introduced to mitigate effects of uninterruptible
blocking a subtask thread during back pressure. Starting from 1.16.0 Flink subtask can request
by default up to 5 extra (overdraft) buffers over the regular configured amount(you can read more
about this in the documentation: https://nightlies.apache.org/flink/flink-docs-master/docs/deployment/memory/network_mem_tuning/#overdraft-buffers). 
This change can slightly increase memory consumption of the Flink Job. To restore the older behaviour
you can set `taskmanager.network.memory.max-overdraft-buffers-per-gate` to zero.

#### Non-deterministic UID generation might cause issues during restore

##### [FLINK-28861](https://issues.apache.org/jira/browse/FLINK-28861)

1.15.0 and 1.15.1 generated non-deterministic UIDs for operators that make it difficult/impossible
to restore state or upgrade to next patch version. A new table.exec.uid.generation config option (with correct default behavior)
disables setting a UID for new pipelines from non-compiled plans. Existing pipelines can set table.exec.uid.generation=ALWAYS
if the 1.15.0/1 behavior was acceptable.

#### Support recovery (from checkpoints) after disabling changelog backend

##### [FLINK-23252](https://issues.apache.org/jira/browse/FLINK-23252)

Added support for disabling changelog when recovering from checkpoints

### Python

#### Update PyFlink to use the new type system

##### [FLINK-25231](https://issues.apache.org/jira/browse/FLINK-25231)

PyFlink now uses Flink's target data type system instead of the legacy data type system.

#### Annotate Python3.6 as deprecated in PyFlink 1.16

##### [FLINK-28195](https://issues.apache.org/jira/browse/FLINK-28195)

Python 3.6 extended support end on 23 December 2021. We plan that PyFlink 1.16 will be the last version support Python3.6.

### Dependency upgrades

#### Update the Hadoop implementation for filesystems to 3.3.2

##### [FLINK-27308](https://issues.apache.org/jira/browse/FLINK-27308)

The Hadoop implementation used for Flink's filesystem implementation has been updated. This provides
Flink users with the features that are listed under https://issues.apache.org/jira/browse/HADOOP-17566.
One of these features is to enable client-side encryption of Flink state via https://issues.apache.org/jira/browse/HADOOP-13887.

##### Upgrade Kafka Client to 3.1.1 

###### [FLINK-28060](https://issues.apache.org/jira/browse/FLINK-28060)

Kafka connector uses Kafka client 3.1.1 by default now.

#### Upgrade Hive 2.3 connector to version 2.3.9

##### [FLINK-27063](https://issues.apache.org/jira/browse/FLINK-27063)

Upgrade Hive 2.3 connector to version 2.3.9

#### Update dependency version for PyFlink

##### [FLINK-25188](https://issues.apache.org/jira/browse/FLINK-25188)

For support of Python3.9 and M1, PyFlink updates a series dependencies version:
    ```
    apache-beam==2.38.0
    arrow==5.0.0
    pemja==0.2.4
    ```
