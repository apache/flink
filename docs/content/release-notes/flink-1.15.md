---
title: "Release Notes - Flink 1.15"
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

# Release notes - Flink 1.15

These release notes discuss important aspects, such as configuration, behavior, 
or dependencies, that changed between Flink 1.14 and Flink 1.15. Please read these 
notes carefully if you are planning to upgrade your Flink version to 1.15.

## Summary of changed dependency names

There are Several changes in Flink 1.15 that require updating dependency names when 
upgrading from earlier versions, mainly including the effort to opting-out Scala dependencies
from non-scala modules and reorganize table modules. A quick checklist of the dependency changes
is as follows:

* Any dependency to one of the following modules needs to be updated to no longer include a suffix:
  
    ```
    flink-cep
    flink-clients
    flink-connector-elasticsearch-base
    flink-connector-elasticsearch6
    flink-connector-elasticsearch7
    flink-connector-gcp-pubsub
    flink-connector-hbase-1.4
    flink-connector-hbase-2.2
    flink-connector-hbase-base
    flink-connector-jdbc
    flink-connector-kafka
    flink-connector-kinesis
    flink-connector-nifi
    flink-connector-pulsar
    flink-connector-rabbitmq
    flink-container
    flink-dstl-dfs
    flink-gelly
    flink-hadoop-bulk
    flink-kubernetes
    flink-runtime-web
    flink-sql-connector-elasticsearch6
    flink-sql-connector-elasticsearch7
    flink-sql-connector-hbase-1.4
    flink-sql-connector-hbase-2.2
    flink-sql-connector-kafka
    flink-sql-connector-kinesis
    flink-sql-connector-rabbitmq
    flink-state-processor-api
    flink-statebackend-rocksdb
    flink-streaming-java
    flink-test-utils
    flink-yarn
    flink-table-api-java-bridge
    flink-table-runtime
    flink-sql-client
    flink-orc
    flink-orc-nohive
    flink-parquet
    ```
* For Table / SQL users, the new module `flink-table-planner-loader` replaces `flink-table-planner_2.12`
  and avoids the need for a Scala suffix. For backwards compatibility, users can still
  swap it with `flink-table-planner_2.12` located in `opt/`.
  `flink-table-uber` has been split into `flink-table-api-java-uber`,
  `flink-table-planner(-loader)`, and `flink-table-runtime`. Scala users need to explicitly add a dependency
  to `flink-table-api-scala` or `flink-table-api-scala-bridge`.

The detail of the involved issues are listed as follows.

#### Add support for opting-out of Scala 

##### [FLINK-20845](https://issues.apache.org/jira/browse/FLINK-20845)

The Java DataSet/-Stream APIs are now independent of Scala and no longer transitively depend on it.

The implications are the following:

* If you only intend to use the Java APIs, with Java types,
then you can opt-in to a Scala-free Flink by removing the `flink-scala` jar from the `lib/` directory of the distribution.
You are then free to use any Scala version and Scala libraries.
You can either bundle Scala itself in your user-jar; or put into the `lib/` directory of the distribution.

* If you relied on the Scala APIs, without an explicit dependency on them,
  then you may experience issues when building your projects. You can solve this by adding explicit dependencies to
  the APIs that you are using. This should primarily affect users of the Scala `DataStream/CEP` APIs.

* A lot of modules have lost their Scala suffix. 
  Further caution is advised when mixing dependencies from different Flink versions (e.g., an older connector), 
  as you may now end up pulling in multiple versions of a single module (that would previously be prevented by the name being equal).
  
#### Reorganize table modules and introduce flink-table-planner-loader 

##### [FLINK-25128](https://issues.apache.org/jira/browse/FLINK-25128)

The new module `flink-table-planner-loader` replaces `flink-table-planner_2.12` and avoids the need for a Scala suffix.
It is included in the Flink distribution under `lib/`. For backwards compatibility, users can still swap it with 
`flink-table-planner_2.12` located in `opt/`. As a consequence, `flink-table-uber` has been split into `flink-table-api-java-uber`, 
`flink-table-planner(-loader)`, and `flink-table-runtime`. `flink-sql-client` has no Scala suffix anymore.

It is recommended to let new projects depend on `flink-table-planner-loader` (without Scala suffix) in provided scope.

Note that the distribution does not include the Scala API by default.
Scala users need to explicitly add a dependency to `flink-table-api-scala` or `flink-table-api-scala-bridge`.

#### Remove flink-scala dependency from flink-table-runtime 

##### [FLINK-25114](https://issues.apache.org/jira/browse/FLINK-25114)

The `flink-table-runtime` has no Scala suffix anymore.
Make sure to include `flink-scala` if the legacy type system (based on TypeInformation) with case classes is still used within Table API.

#### flink-table uber jar should not include flink-connector-files dependency 

##### [FLINK-24687](https://issues.apache.org/jira/browse/FLINK-24687)

The table file system connector is not part of the `flink-table-uber` JAR anymore but is a dedicated (but removable)
`flink-connector-files` JAR in the `/lib` directory of a Flink distribution.

## JDK Upgrade

The support of Java 8 is now deprecated and will be removed in a future release 
([FLINK-25247](https://issues.apache.org/jira/browse/FLINK-25247)). We recommend 
all users to migrate to Java 11.

The default Java version in the Flink docker images is now Java 11 
([FLINK-25251](https://issues.apache.org/jira/browse/FLINK-25251)). 
There are images built with Java 8, tagged with “java8”. 

## Drop support for Scala 2.11

Support for Scala 2.11 has been removed in
[FLINK-20845](https://issues.apache.org/jira/browse/FLINK-20845). 
All Flink dependencies that (transitively)
depend on Scala are suffixed with the Scala version that they are built for, for
example `flink-streaming-scala_2.12`. Users should update all Flink dependecies,
changing "2.11" to "2.12".

Scala versions (2.11, 2.12, etc.) are not binary compatible with one another. That
also means that there's no guarantee that you can restore from a savepoint, made
with a Flink Scala 2.11 application, if you're upgrading to a Flink Scala 2.12
application. This depends on the data types that you have been using in your
application.

The Scala Shell/REPL has been removed in
[FLINK-24360](https://issues.apache.org/jira/browse/FLINK-24360).

## Table API & SQL

#### Disable the legacy casting behavior by default 

##### [FLINK-26551](https://issues.apache.org/jira/browse/FLINK-26551)

The legacy casting behavior has been disabled by default. This might have 
implications on corner cases (string parsing, numeric overflows, to string 
representation, varchar/binary precisions). Set 
`table.exec.legacy-cast-behaviour=ENABLED` to restore the old behavior.

#### Enforce CHAR/VARCHAR precision when outputting to a Sink 

##### [FLINK-24753](https://issues.apache.org/jira/browse/FLINK-24753)

`CHAR`/`VARCHAR` lengths are enforced (trimmed/padded) by default now before entering
the table sink.

#### Support the new type inference in Scala Table API table functions 

##### [FLINK-26518](https://issues.apache.org/jira/browse/FLINK-26518)

Table functions that are called using Scala implicit conversions have been updated 
to use the new type system and new type inference. Users are requested to update 
their UDFs or use the deprecated `TableEnvironment.registerFunction` to restore 
the old behavior temporarily by calling the function via name.

#### Propagate executor config to TableConfig 

##### [FLINK-26421](https://issues.apache.org/jira/browse/FLINK-26421)

`flink-conf.yaml` and other configurations from outer layers (e.g. CLI) are now 
propagated into `TableConfig`. Even though configuration set directly in `TableConfig` 
has still precedence, this change can have side effects if table configuration 
was accidentally set in other layers.

#### Remove pre FLIP-84 methods 

##### [FLINK-26090](https://issues.apache.org/jira/browse/FLINK-26090)

The previously deprecated methods `TableEnvironment.execute`, `Table.insertInto`,
`TableEnvironment.fromTableSource`, `TableEnvironment.sqlUpdate`, and 
`TableEnvironment.explain` have been removed. Please use 
`TableEnvironment.executeSql`, `TableEnvironment.explainSql`, 
`TableEnvironment.createStatementSet`, as well as `Table.executeInsert`, 
`Table.explain` and `Table.execute` and the newly introduces classes 
`TableResult`, `ResultKind`, `StatementSet` and `ExplainDetail`.

#### Fix parser generator warnings 

##### [FLINK-26053](https://issues.apache.org/jira/browse/FLINK-26053)

`STATEMENT` is a reserved keyword now. Use backticks to escape tables, fields and 
other references.

#### Expose uid generator for DataStream/Transformation providers 

##### [FLINK-25990](https://issues.apache.org/jira/browse/FLINK-25990)

`DataStreamScanProvider` and `DataStreamSinkProvider` for table connectors received 
an additional method that might break implementations that used lambdas before. 
We recommend static classes as a replacement and future robustness.

#### Add new STATEMENT SET syntax 

##### [FLINK-25392](https://issues.apache.org/jira/browse/FLINK-25392)

It is recommended to update statement sets to the new SQL syntax: 

```SQL
EXECUTE STATEMENT SET BEGIN ... END;
EXPLAIN STATEMENT SET BEGIN ... END;
```

#### Check & possible fix decimal precision and scale for all Aggregate functions 

##### [FLINK-24809](https://issues.apache.org/jira/browse/FLINK-24809)

This changes the result of a decimal `SUM()` with retraction and `AVG()`. Part of the behavior
is restored back to be the same with 1.13 so that the behavior as a whole could be consistent 
with Hive / Spark.

#### Clarify semantics of DecodingFormat and its data type 

##### [FLINK-24776](https://issues.apache.org/jira/browse/FLINK-24776)

The `DecodingFormat` interface was used for both projectable and non-projectable
formats which led to inconsistent implementations. The `FileSystemTableSource` 
has been updated to distinguish between those two interfaces now. Users that 
implement custom formats for `FileSystemTableSource` might need to verify the 
implementation and make sure to implement `ProjectableDecodingFormat` if necessary.

#### Push down partitions before filters 

##### [FLINK-24717](https://issues.apache.org/jira/browse/FLINK-24717)

This might have an impact on existing table source implementations as push down 
filters might not contain partition predicates anymore. However, the connector 
implementation for table sources that implement both partition and filter push 
down became easier with this change.

#### Flink SQL `SUM()` causes a precision error 

##### [FLINK-24691](https://issues.apache.org/jira/browse/FLINK-24691)

This changes the result of a decimal `SUM()` between 1.14.0 and 1.14.1. It restores 
the behavior of 1.13 to be consistent with Hive/Spark.

#### Use the new casting rules in TableResult#print 

##### [FLINK-24685](https://issues.apache.org/jira/browse/FLINK-24685)

The string representation of `BOOLEAN` columns from DDL results 
(`true/false -> TRUE/FALSE`), and row columns in DQL results 
(`+I[...] -> (...)`) has changed for printing.

#### Casting from a string to a DATE and TIME allows incomplete strings 

##### [FLINK-24421](https://issues.apache.org/jira/browse/FLINK-24421)

The defaults for casting incomplete strings like `"12"` to TIME have changed from `12:01:01` to `12:00:00`.

#### Casting from STRING to TIMESTAMP_LTZ looses fractional seconds 

##### [FLINK-24446](https://issues.apache.org/jira/browse/FLINK-24446)

`STRING` to `TIMESTAMP(_LTZ)` casting now considers fractional seconds.
Previously fractional seconds of any precision were ignored.

#### Sinks built with the unified sink framework do not receive timestamps when used in Table API 

##### [FLINK-24608](https://issues.apache.org/jira/browse/FLINK-Table)

This adds an additional operator to the topology if the new sink interfaces are used 
(e.g. for Kafka). It could cause issues in 1.14.1 when restoring from a 1.14 savepoint. 
A workaround is to cast the time attribute to a regular timestamp in the SQL statement
closely before the sink.

#### SQL functions should return `STRING` instead of `VARCHAR(2000)` 

##### [FLINK-24586](https://issues.apache.org/jira/browse/FLINK-24586)

Functions that returned `VARCHAR(2000)` in 1.14, return `VARCHAR` with maximum 
length now. In particular this includes:

```SQL
SON_VALUE
CHR
REVERSE
SPLIT_INDEX
REGEXP_EXTRACT
PARSE_URL
FROM_UNIXTIME
DECODE
DATE_FORMAT
CONVERT_TZ
```

#### Support IS JSON for Table API 

##### [FLINK-16501](https://issues.apache.org/jira/browse/FLINK-16501)

This issue added IS JSON for Table API. Notes that `IS JSON` does not return
`NULL` anymore but always `FALSE` (even if the argument is `NULL`).

#### Disable upsert into syntax in Flink SQL 

##### [FLINK-22942](https://issues.apache.org/jira/browse/FLINK-22942)

Disabled `UPSERT INTO` statement.
`UPSERT INTO` syntax was exposed by mistake in previous releases without detailed discussed.
From this release every `UPSERT INTO` is going to throw an exception.
Users of `UPSERT INTO` should use the documented `INSERT INTO` statement instead.

#### RuntimeException: while resolving method 'booleanValue' in class class java.math.BigDecimal 

##### [FLINK-23271](https://issues.apache.org/jira/browse/FLINK-23271)

Casting to `BOOLEAN` is not allowed from decimal numeric types anymore.

#### Upsert materializer is not inserted for all sink providers 

##### [FLINK-23895](https://issues.apache.org/jira/browse/FLINK-23895)

This issue aims to fix various primary key issues that effectively made it impossible to use this feature.
The change might affect savepoint backwards compatibility for those incorrect pipelines.
Also the resulting changelog stream might be different after these changes.
Pipelines that were correct before should be restorable from a savepoint.

#### Propagate unique keys for fromChangelogStream 

##### [FLINK-24033](https://issues.apache.org/jira/browse/FLINK-24033)

`StreamTableEnvironment.fromChangelogStream` might produce a different stream because
primary keys were not properly considered before.

#### TableResult#print() should use internal data types 

##### [FLINK-24461](https://issues.apache.org/jira/browse/FLINK-24461)

The results of `Table#print` have changed to be closer to actual SQL data types.
E.g. decimal is printing correctly with leading/trailing zeros.

## Connectors

#### Remove MapR filesystem 

##### [FLINK-25553](https://issues.apache.org/jira/browse/FLINK-25553)

Support for the MapR FileSystem has been dropped.

#### Merge flink-connector-testing into flink-connector-test-utils 

##### [FLINK-25712](https://issues.apache.org/jira/browse/FLINK-25712)

The `flink-connector-testing` module has been removed and users should use
`flink-connector-test-utils` module instead.

#### Support partition keys through metadata (for FileSystem connector) 

##### [FLINK-24617](https://issues.apache.org/jira/browse/FLINK-24617)

Now the formats implementing `BulkWriterFormatFactory` don't need to implement
partition keys reading anymore, as it's managed internally by `FileSystemTableSource`.

#### Port ElasticSearch Sink to new Unified Sink API (FLIP-143) 

##### [FLINK-24323](https://issues.apache.org/jira/browse/FLINK-24323)

`ElasticsearchXSinkBuilder` supersedes `ElasticsearchSink.Builder` and provides at-least-once writing with the
new unified sink interface supporting both batch and streaming mode of DataStream API.

For Elasticsearch 7 users that use the old ElasticsearchSink interface
(`org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink`)
and depend on their own elasticsearch-rest-high-level-client version,
updating the client dependency to a version >= 7.14.0 is required due to internal changes.

#### Reduce legacy in Table API connectors 

##### [FLINK-24397](https://issues.apache.org/jira/browse/FLINK-24397)

The old JDBC connector (indicated by `connector.type=jdbc` in DDL) has been removed.
If not done already, users need to upgrade to the newer stack (indicated by `connector=jdbc` in DDL).


#### Extensible unified Sink uses new metric to capture outgoing records

##### [FLINK-26126](https://issues.apache.org/jira/browse/FLINK-26126)

New metrics `numRecordsSend` and `numRecordsSendErrors` have been introduced for users to monitor the number of 
records sent to the external system. The `numRecordsOut` should be used to monitor the number of records 
transferred between sink tasks.

Connector developers should pay attention to the usage of these metrics numRecordsOut, 
numRecordsSend and numRecordsSendErrors while building sink connectors. 
Please refer to the new Kafka Sink for details. 
Additionally, since numRecordsOut now only counts the records sent between sink tasks 
and numRecordsOutErrors was designed for counting the records sent to the external system, 
we deprecated numRecordsOutErrors and recommend using numRecordsSendErrors instead.

## Runtime & Coordination

#### Integrate retry strategy for cleanup stage 

##### [FLINK-25433](https://issues.apache.org/jira/browse/FLINK-25433)

Adds retry logic to the cleanup steps of a finished job. This feature changes the
way Flink jobs are cleaned up. Instead of trying once to clean up the job, this
step will be repeated until it succeeds. Users are meant to fix the issue that
prevents Flink from finalizing the job cleanup. The retry functionality can be
configured and disabled. More details can be found 
[in the documentation](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/deployment/config/#retryable-cleanup).

#### Introduce explicit shutdown signaling between `TaskManager` and `JobManager` 

##### [FLINK-25277](https://issues.apache.org/jira/browse/FLINK-25277)

`TaskManagers` now explicitly send a signal to the `JobManager` when shutting down. This reduces the down-scaling delay in reactive mode (which was previously bound to the heartbeat timeout).

#### Release `TaskManagerJobMetricGroup` with the last slot rather than task 

##### [FLINK-24864](https://issues.apache.org/jira/browse/FLINK-24864)

Job metrics on the TaskManager are now removed when the last slot is released, rather than the last task.
This means they may be reported for a longer time than before and when no tasks are running on the TaskManager.

#### Make errors happened during JobMaster initialization accessible through the exception history 

##### [FLINK-25096](https://issues.apache.org/jira/browse/FLINK-25096)

Fixes issue where the failover is not listed in the exception history but as a root
cause. That could have happened if the failure occurred during `JobMaster` 
initialization.


#### DispatcherResourceManagerComponent fails to deregister application if no leading ResourceManager 

##### [FLINK-24038](https://issues.apache.org/jira/browse/FLINK-24038)

A new multiple component leader election service was implemented that only runs a single leader election per Flink process.
If this should cause any problems, then you can set `high-availability.use-old-ha-services: true` in the `flink-conf.yaml`
to use the old high availability services.

#### Allow idempotent job cancellation 

##### [FLINK-24275](https://issues.apache.org/jira/browse/FLINK-24275)

Attempting to cancel a `FINISHED/FAILED` job now returns 409 Conflict instead of 404 Not Found.

#### Move async savepoint operation cache into Dispatcher

#### [FLINK-18312](https://issues.apache.org/jira/browse/FLINK-18312)

All `JobManagers` can now be queried for the status of a savepoint operation, irrespective of which `JobManager` received the initial request.

#### Standby per job mode Dispatchers don't know job's JobSchedulingStatus 

##### [FLINK-11813](https://issues.apache.org/jira/browse/FLINK-11813)

The issue of re-submitting a job in Application Mode when the job finished but failed during
cleanup is fixed through the introduction of the new component JobResultStore which enables
Flink to persist the cleanup state of a job to the file system. (see [FLINK-25431](https://issues.apache.org/jira/browse/FLINK-25431))


#### Change some default config values of blocking shuffle for better usability

##### [FLINK-25636](https://issues.apache.org/jira/browse/FLINK-25636)

Since 1.15, sort-shuffle has become the default blocking shuffle implementation and shuffle data
compression is enabled by default. These changes influence batch jobs only, for more information,
please refer to the [official document](https://nightlies.apache.org/flink/flink-docs-release-1.15).


## Checkpoints

#### FLIP-193: Snapshots ownership 

##### [FLINK-25154](https://issues.apache.org/jira/browse/FLINK-25154)

When restoring from a savepoint or retained externalized checkpoint you can choose 
the mode in which you want to perform the operation. You can choose from `CLAIM`, 
`NO_CLAIM`, `LEGACY` (the old behavior).

In `CLAIM` mode Flink takes ownership of the snapshot and will potentially try to 
remove the snapshot at a certain point in time. On the other hand the `NO_CLAIM` 
mode will make sure Flink does not depend on the existence of any files belonging 
to the initial snapshot. 

For a more thorough description see [the documentation](https://nightlies.apache.org/flink/flink-docs-release-1.15/docs/ops/state/savepoints/#restore-mode).

#### Support native savepoints (w/o modifying the statebackend specific snapshot strategies) 

##### [FLINK-25744](https://issues.apache.org/jira/browse/FLINK-25744)

When taking a savepoint you can specify the binary format. You can choose from native 
(specific to a particular state backend) or canonical (unified across all state backends).

#### Prevent JM from discarding state on checkpoint abortion 

##### [FLINK-24611](https://issues.apache.org/jira/browse/FLINK-24611)

Shared state tracking changed to use checkpoint ID instead of reference counts. 
Shared state is not cleaned up on abortion anymore (but rather on subsumption or 
job termination).

This might result in delays in discarding the state of aborted checkpoints.

#### Introduce incremental/full checkpoint size stats 

##### [FLINK-25557](https://issues.apache.org/jira/browse/FLINK-25557)

Introduce metrics of persistent bytes within each checkpoint (via REST API and UI), 
which could help users to know how much data size had been persisted during the 
incremental or change-log based checkpoint.

#### Enables final checkpoint by default 

##### [FLINK-25105](https://issues.apache.org/jira/browse/FLINK-25105)

In 1.15 we enabled the support of checkpoints after part of tasks finished by default, 
and made tasks waiting for the final checkpoint before exit to ensure all data got 
committed. 

However, it's worth noting that this change forces tasks to wait for one more 
checkpoint before exiting. In other words, this change will block the tasks until 
the next checkpoint get triggered and completed. If the checkpoint interval is long, 
the tasks' execution time would also be extended largely. In the worst case if the 
checkpoint interval is `Long.MAX_VALUE`, the tasks would be in fact blocked forever. 

More information about this feature and how to disable it could be found in 
[the documentation](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/#checkpointing-with-parts-of-the-graph-finished-beta).

#### Migrate state processor API to DataStream API 

##### [FLINK-24912](https://issues.apache.org/jira/browse/FLINK-24912)

The State Processor API has been migrated from Flinks legacy DataSet API to now 
run over DataStreams run under `BATCH` execution.

#### Relocate RocksDB's log under flink log directory by default 

##### [FLINK-24785](https://issues.apache.org/jira/browse/FLINK-24785)

The internal log of RocksDB would stay under flink's log directory by default.

## Dependency upgrades

#### Upgrade the minimal supported hadoop version to 2.8.5 

##### [FLINK-25224](https://issues.apache.org/jira/browse/FLINK-25224)

Minimal supported Hadoop client version is now 2.8.5 (version of the Flink runtime 
dependency). The client can still talk to older server versions as the binary protocol 
should be backward compatible.

#### Update Elasticsearch Sinks to latest minor versions 

##### [FLINK-25189](https://issues.apache.org/jira/browse/FLINK-25189)

Elasticsearch libraries used by the connector are bumped to 7.15.2 and 6.8.20 
respectively. 

For Elasticsearch 7 users that use the old ElasticsearchSink interface 
(`org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink`) and 
depend on their own `elasticsearch-rest-high-level-client` version, will need 
to update the client dependency to a version >= 7.14.0 due to internal changes.


#### Drop support for Zookeeper 3.4 

##### [FLINK-25146](https://issues.apache.org/jira/browse/FLINK-25146)

Support for using Zookeeper 3.4 for HA has been dropped.
Users relying on Zookeeper need to upgrade to 3.5/3.6.
By default Flink now uses a Zookeeper 3.5 client.

#### Upgrade Kafka dependency 

##### [FLINK-24765](https://issues.apache.org/jira/browse/FLINK-24765)

Kafka connector uses Kafka client 2.8.1 by default now.

## Bind to localhost by default

For security purposes, standalone clusters now bind the REST API and RPC endpoints to 
localhost by default. The goal is to prevent cases where users unknowingly exposed the cluster to 
the outside, as they would previously bind to all interfaces.

This can be reverted by removing the:

* `rest.bind-address` 
* `jobmanager.bind-host`
* `taskmanager.bind-host`
settings from the flink-conf.yaml .

Note that within Docker containers, the REST API still binds to 0.0.0.0.
