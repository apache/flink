---
title: "Release Notes - Flink 1.9"
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

# Release Notes - Flink 1.9


These release notes discuss important aspects, such as configuration, behavior,
or dependencies, that changed between Flink 1.8 and Flink 1.9. It also provides an overview
on known shortcoming or limitations with new experimental features introduced in 1.9.

Please read these notes carefully if you are planning to upgrade your Flink version to 1.9.



## Known shortcomings or limitations for new features

### New Table / SQL Blink planner

Flink 1.9.0 provides support for two planners for the Table API, namely Flink's original planner and the new Blink
planner. The original planner maintains same behaviour as previous releases, while the new Blink planner is still
considered experimental and has the following limitations:

- The Blink planner can not be used with `BatchTableEnvironment`, and therefore Table programs ran with the planner can not 
be transformed to `DataSet` programs. This is by design and will also not be supported in the future. Therefore, if
you want to run a batch job with the Blink planner, please use the new `TableEnvironment`. For streaming jobs,
both `StreamTableEnvironment` and `TableEnvironment` works.
- Implementations of `StreamTableSink` should implement the `consumeDataStream` method instead of `emitDataStream`
if it is used with the Blink planner. Both methods work with the original planner.
This is by design to make the returned `DataStreamSink` accessible for the planner.
- Due to a bug with how transformations are not being cleared on execution, `TableEnvironment` instances should not
be reused across multiple SQL statements when using the Blink planner.
- `Table.flatAggregate` is not supported
- Session and count windows are not supported when running batch jobs.
- The Blink planner only supports the new `Catalog` API, and does not support `ExternalCatalog` which is now deprecated.

Related issues:
- [FLINK-13708: Transformations should be cleared because a table environment could execute multiple job](https://issues.apache.org/jira/browse/FLINK-13708)
- [FLINK-13473: Add GroupWindowed FlatAggregate support to stream Table API (Blink planner), i.e, align with Flink planner](https://issues.apache.org/jira/browse/FLINK-13473)
- [FLINK-13735: Support session window with Blink planner in batch mode](https://issues.apache.org/jira/browse/FLINK-13735)
- [FLINK-13736: Support count window with Blink planner in batch mode](https://issues.apache.org/jira/browse/FLINK-13736)

### SQL DDL

In Flink 1.9.0, the community also added a preview feature about SQL DDL, but only for batch style DDLs.
Therefore, all streaming related concepts are not supported yet, for example watermarks.

Related issues:
- [FLINK-13661: Add a stream specific CREATE TABLE SQL DDL](https://issues.apache.org/jira/browse/FLINK-13661)
- [FLINK-13568: DDL create table doesn't allow STRING data type](https://issues.apache.org/jira/browse/FLINK-13568)

### Java 9 support

Since Flink 1.9.0, Flink can now be compiled and run on Java 9. Note that certain components interacting
with external systems (connectors, filesystems, metric reporters, etc.) may not work since the respective projects may
have skipped Java 9 support.

Related issues:
- [FLINK-8033: JDK 9 support](https://issues.apache.org/jira/browse/FLINK-8033)

### Memory management

In Fink 1.9.0 and prior version, the managed memory fraction of taskmanager is controlled by `taskmanager.memory.fraction`,
and with 0.7 as the default value. However, sometimes this will cause OOMs due to the fact that the default value of JVM
parameter `NewRatio` is 2, which means the old generation occupied only 2/3 (0.66) of the heap memory. So if you run into
this case, please manually change this value to a lower value.

Related issues:
- [FLINK-14123: Lower the default value of taskmanager.memory.fraction](https://issues.apache.org/jira/browse/FLINK-14123)

## Deprecations and breaking changes

### Scala expression DSL for Table API moved to `flink-table-api-scala`

Since 1.9.0, the implicit conversions for the Scala expression DSL for the Table API has been moved to
`flink-table-api-scala`. This requires users to update the imports in their Table programs.

Users of pure Table programs should define their imports like:

```
import org.apache.flink.table.api._

TableEnvironment.create(...)
```

Users of the DataStream API should define their imports like:

```
import org.apache.flink.table.api._
import org.apache.flink.table.api.scala._

StreamTableEnvironment.create(...)
```

Related issues:
- [FLINK-13045: Move Scala expression DSL to flink-table-api-scala](https://issues.apache.org/jira/browse/FLINK-13045)

### Failover strategies

As a result of completing fine-grained recovery ([FLIP-1](https://cwiki.apache.org/confluence/display/FLINK/FLIP-1+%3A+Fine+Grained+Recovery+from+Task+Failures)),
Flink will now attempt to only restart tasks that are
connected to failed tasks through a pipelined connection. By default, the `region` failover strategy is used.

Users who were not using a restart strategy or have already configured a failover strategy should not be affected.
Moreover, users who already enabled the `region` failover strategy, along with a restart strategy that enforces a
certain number of restarts or introduces a restart delay, will see changes in behavior.
The `region` failover strategy now correctly respects constraints that are defined by the restart strategy.

Streaming users who were not using a failover strategy may be affected if their jobs are embarrassingly parallel or
contain multiple independent jobs. In this case, only the failed parallel pipeline or affected jobs will be restarted. 

Batch users may be affected if their job contains blocking exchanges (usually happens for shuffles) or the
`ExecutionMode` was set to `BATCH` or `BATCH_FORCED` via the `ExecutionConfig`. 

Overall, users should see an improvement in performance.

Related issues:
- [FLINK-13223: Set jobmanager.execution.failover-strategy to region in default flink-conf.yaml](https://issues.apache.org/jira/browse/FLINK-13223)
- [FLINK-13060: FailoverStrategies should respect restart constraints](https://issues.apache.org/jira/browse/FLINK-13060)

### Job termination via CLI

With the support of graceful job termination with savepoints for semantic correctness
([FLIP-34](https://cwiki.apache.org/confluence/pages/viewpage.action?pageId=103090212)), a few changes
related to job termination has been made to the CLI.

From now on, the `stop` command with no further arguments stops the job with a savepoint targeted at the
default savepoint location (as configured via the `state.savepoints.dir` property in the job configuration),
or a location explicitly specified using the `-p <savepoint-path>` option. Please make sure to configure the
savepoint path using either one of these options.

Since job terminations are now always accompanied with a savepoint, stopping jobs is expected to take longer now.

Related issues:
- [FLINK-13123: Align Stop/Cancel Commands in CLI and REST Interface and Improve Documentation](https://issues.apache.org/jira/browse/FLINK-13123)
- [FLINK-11458: Add TERMINATE/SUSPEND Job with Savepoint](https://issues.apache.org/jira/browse/FLINK-11458)

### Network stack

A few changes in the network stack related to changes in the threading model of `StreamTask` to a mailbox-based approach
requires close attention to some related configuration:

- Due to changes in the lifecycle management of result partitions, partition requests as well as re-triggers will now
happen sooner. Therefore, it is possible that some jobs with long deployment times and large state might start failing
more frequently with `PartitionNotFound` exceptions compared to previous versions. If that's the case, users should
increase the value of `taskmanager.network.request-backoff.max` in order to have the same effective partition
request timeout as it was prior to 1.9.0.

- To avoid a potential deadlock, a timeout has been added for how long a task will wait for assignment of exclusive
memory segments. The default timeout is 30 seconds, and is configurable via `taskmanager.network.memory.exclusive-buffers-request-timeout-ms`.
It is possible that for some previously working deployments this default timeout value is too low
and might have to be increased.

Please also notice that several network I/O metrics have had their scope changed. See the [1.9 metrics documentation](https://ci.apache.org/projects/flink/flink-docs-master/ops/metrics.html)
for which metrics are affected. In 1.9.0, these metrics will still be available under their previous scopes, but this
may no longer be the case in future versions.

Related issues:
- [FLINK-13013: Make sure that SingleInputGate can always request partitions](https://issues.apache.org/jira/browse/FLINK-13013)
- [FLINK-12852: Deadlock occurs when requiring exclusive buffer for RemoteInputChannel](https://issues.apache.org/jira/browse/FLINK-12852)
- [FLINK-12555: Introduce an encapsulated metric group layout for shuffle API and deprecate old one](https://issues.apache.org/jira/browse/FLINK-12555)

### AsyncIO

Due to a bug in the `AsyncWaitOperator`, in 1.9.0 the default chaining behaviour of the operator is now changed so
that it is never chained after another operator. This should not be problematic for migrating from older version
snapshots as long as an uid was assigned to the operator. If an uid was not assigned to the operator, please see
the instructions [here](https://ci.apache.org/projects/flink/flink-docs-release-1.9/ops/upgrading.html#matching-operator-state)
for a possible workaround.

Related issues:
- [FLINK-13063: AsyncWaitOperator shouldn't be releasing checkpointingLock](https://issues.apache.org/jira/browse/FLINK-13063)

### Connectors and Libraries

#### Introduced `KafkaSerializationSchema` to fully replace `KeyedSerializationSchema`

The universal `FlinkKafkaProducer` (in `flink-connector-kafka`) supports a new `KafkaSerializationSchema` that will
fully replace `KeyedSerializationSchema` in the long run. This new schema allows directly generating Kafka
`ProducerRecord`s for sending to Kafka, therefore enabling the user to use all available Kafka features (in the context
of Kafka records).

#### Dropped connectors and libraries

- The Elasticsearch 1 connector has been dropped and will no longer receive patches. Users may continue to use the
connector from a previous series (like 1.8) with newer versions of Flink. It is being dropped due to being used
significantly less than more recent versions (Elasticsearch versions 2.x and 5.x are downloaded 4 to 5 times more), and
hasn't seen any development for over a year.

- The older Python APIs for batch and streaming have been removed and will no longer receive new patches.
A new API is being developed based on the Table API as part of [FLINK-12308: Support python language in Flink Table API](https://issues.apache.org/jira/browse/FLINK-12308).
Existing users may continue to use these older APIs with future versions of Flink by copying both the `flink-streaming-python`
and `flink-python` jars into the `/lib` directory of the distribution and the corresponding start scripts `pyflink-stream.sh`
and `pyflink.sh` into the `/bin` directory of the distribution.

- The older machine learning libraries have been removed and will no longer receive new patches.
This is due to efforts towards a new Table-based machine learning library ([FLIP-39](https://docs.google.com/document/d/1StObo1DLp8iiy0rbukx8kwAJb0BwDZrQrMWub3DzsEo/edit)).
Users can still use the 1.8 version of the legacy library if their projects still rely on it.

Related issues:
- [FLINK-11693: Add KafkaSerializationSchema that directly uses ProducerRecord](https://issues.apache.org/jira/browse/FLINK-11693)
- [FLINK-12151: Drop Elasticsearch 1 connector](https://issues.apache.org/jira/browse/FLINK-12151)
- [FLINK-12903: Remove legacy flink-python APIs](https://issues.apache.org/jira/browse/FLINK-12903)
- [FLINK-12308: Support python language in Flink Table API](https://issues.apache.org/jira/browse/FLINK-12308)
- [FLINK-12597: Remove the legacy flink-libraries/flink-ml](https://issues.apache.org/jira/browse/FLINK-12597)

### MapR dependency removed

Dependency on MapR vendor-specific artifacts has been removed, by changing the MapR filesystem connector to work
purely based on reflection. This does not introduce any regession in the support for the MapR filesystem.
The decision to remove hard dependencies on the MapR artifacts was made due to very flaky access to the secure https
endpoint of the MapR artifact repository, and affected build stability of Flink.

Related issues:
- [FLINK-12578: Use secure URLs for Maven repositories](https://issues.apache.org/jira/browse/FLINK-12578)
- [FLINK-13499: Remove dependency on MapR artifact repository](https://issues.apache.org/jira/browse/FLINK-13499)

### StateDescriptor interface change

Access to the state serializer in `StateDescriptor` is now modified from protected to private access. Subclasses
should use the `StateDescriptor#getSerializer()` method as the only means to obtain the wrapped state serializer.

Related issues:
- [FLINK-12688: Make serializer lazy initialization thread safe in StateDescriptor](https://issues.apache.org/jira/browse/FLINK-12688)

### Web UI dashboard

The web frontend of Flink has been updated to use the latest Angular version (7.x). The old frontend remains
available in Flink 1.9.x, but will be removed in a later Flink release once the new frontend is considered stable.

Related issues:
- [FLINK-10705: Rework Flink Web Dashoard](https://issues.apache.org/jira/browse/FLINK-10705)
