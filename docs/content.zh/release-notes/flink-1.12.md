---
title: "Release Notes - Flink 1.12"
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

# Release Notes - Flink 1.12

These release notes discuss important aspects, such as configuration, behavior,
or dependencies, that changed between Flink 1.11 and Flink 1.12. Please read
these notes carefully if you are planning to upgrade your Flink version to 1.12.

### Known Issues

#### Unaligned checkpoint recovery may lead to corrupted data stream 
##### [FLINK-20654](https://issues.apache.org/jira/browse/FLINK-20654)

Using unaligned checkpoints in Flink 1.12.0 combined with two/multiple inputs tasks or with union inputs for single input tasks can result in corrupted state.

This can happen if a new checkpoint is triggered before recovery is fully completed. For state to be corrupted a task with two or more input gates must receive a checkpoint barrier exactly at the same time this tasks finishes recovering spilled in-flight data. In such case this new checkpoint can succeed, with corrupted/missing in-flight data, which will result in various deserialisation/corrupted data stream errors when someone attempts to recover from such corrupted checkpoint.

Using unaligned checkpoints in Flink 1.12.1, a corruption may occur in the checkpoint following a declined checkpoint.

A late barrier of a canceled checkpoint may lead to buffers being not written into the successive checkpoint, such that recovery is not possible. This happens, when the next checkpoint barrier arrives at a given operator before all previous barriers arrived, which can only happen after cancellation in unaligned checkpoints.  

### APIs

#### Remove deprecated methods in ExecutionConfig 
##### [FLINK-19084](https://issues.apache.org/jira/browse/FLINK-19084)

Deprecated method `ExecutionConfig#isLatencyTrackingEnabled` was removed, you can use `ExecutionConfig#getLatencyTrackingInterval` instead. 

Deprecated and methods without effect were removed: `ExecutionConfig#enable/disableSysoutLogging`, `ExecutionConfig#set/isFailTaskOnCheckpointError`.

Removed `-q` flag from cli. The option had no effect. 

#### Remove deprecated RuntimeContext#getAllAccumulators 
##### [FLINK-19032](https://issues.apache.org/jira/browse/FLINK-19032)

The deprecated method `RuntimeContext#getAllAccumulators` was removed. Please use `RuntimeContext#getAccumulator` instead. 

#### Deprecated CheckpointConfig#setPreferCheckpointForRecovery due to risk of data loss 
##### [FLINK-20441](https://issues.apache.org/jira/browse/FLINK-20441)

The `CheckpointConfig#setPreferCheckpointForRecovery` method has been deprecated, because preferring older checkpoints over newer savepoints for recovery can lead to data loss.

#### FLIP-134: Batch execution for the DataStream API

- Allow explicitly configuring time behaviour on `KeyedStream.intervalJoin()` [FLINK-19479](https://issues.apache.org/jira/browse/FLINK-19479)

  Before Flink 1.12 the `KeyedStream.intervalJoin()` operation was changing behavior based on the globally set Stream TimeCharacteristic. In Flink 1.12 we introduced explicit `inProcessingTime()` and `inEventTime()` methods on `IntervalJoin` and the join no longer changes behaviour based on the global characteristic. 

- Deprecate `timeWindow()` operations in DataStream API [FLINK-19318](https://issues.apache.org/jira/browse/FLINK-19318)

  In Flink 1.12 we deprecated the `timeWindow()` operations in the DataStream API. Please use `window(WindowAssigner)` with either a `TumblingEventTimeWindows`, `SlidingEventTimeWindows`, `TumblingProcessingTimeWindows`, or `SlidingProcessingTimeWindows`. For more information, see the deprecation description of `TimeCharacteristic`/`setStreamTimeCharacteristic`. 

- Deprecate `StreamExecutionEnvironment.setStreamTimeCharacteristic()` and `TimeCharacteristic` [FLINK-19319](https://issues.apache.org/jira/browse/FLINK-19319)

  In Flink 1.12 the default stream time characteristic has been changed to `EventTime`, thus you don't need to call this method for enabling event-time support anymore. Explicitly using processing-time windows and timers works in event-time mode. If you need to disable watermarks, please use `ExecutionConfig.setAutoWatermarkInterval(long)`. If you are using `IngestionTime`, please manually set an appropriate `WatermarkStrategy`. If you are using generic "time window" operations (for example `KeyedStream.timeWindow()`) that change behaviour based on the time characteristic, please use equivalent operations that explicitly specify processing time or event time. 

- Allow explicitly configuring time behaviour on CEP PatternStream [FLINK-19326](https://issues.apache.org/jira/browse/FLINK-19326)

  Before Flink 1.12 the CEP operations were changing their behavior based on the globally set Stream TimeCharacteristic. In Flink 1.12 we introduced explicit `inProcessingTime()` and `inEventTime()` methods on `PatternStream` and the CEP operations no longer change their behaviour based on the global characteristic. 

#### API cleanups

- Remove remaining UdfAnalyzer configurations [FLINK-13857](https://issues.apache.org/jira/browse/FLINK-13857)

  The `ExecutionConfig#get/setCodeAnalysisMode` method and `SkipCodeAnalysis` class were removed. They took no effect even before that change, therefore there is no need to use any of these. 

- Remove deprecated `DataStream#split` [FLINK-19083](https://issues.apache.org/jira/browse/FLINK-19083)

  The `DataStream#split()` operation has been removed after being marked as deprecated for a couple of versions. Please use [Side Outputs]({{< ref "docs/dev/datastream/side_output" >}})) instead.

- Remove deprecated `DataStream#fold()` method and all related classes [FLINK-19035](https://issues.apache.org/jira/browse/FLINK-19035)

  The long deprecated `(Windowed)DataStream#fold` was removed in 1.12. Please use other operations such as e.g. `(Windowed)DataStream#reduce` that perform better in distributed systems. 

#### Extend CompositeTypeSerializerSnapshot to allow composite serializers to signal migration based on outer configuration 
##### [FLINK-17520](https://issues.apache.org/jira/browse/FLINK-17520)

The `boolean isOuterSnapshotCompatible(TypeSerializer)` on the `CompositeTypeSerializerSnapshot` class has been deprecated, in favor of a new `OuterSchemaCompatibility#resolveOuterSchemaCompatibility(TypeSerializer)` method. Please implement that instead. Compared to the old method, the new method allows composite serializers to signal state schema migration based on outer schema and configuration. 

#### Bump Scala Macros Version to 2.1.1 
##### [FLINK-19278](https://issues.apache.org/jira/browse/FLINK-19278)

Flink now relies on Scala Macros 2.1.1. This means that we no longer support Scala < 2.11.11. 

### SQL

#### Use new type inference for SQL DDL of aggregate functions 
##### [FLINK-18901](https://issues.apache.org/jira/browse/FLINK-18901)

The `CREATE FUNCTION` DDL for aggregate functions uses the new type inference now. It might be necessary to update existing implementations to the new reflective type extraction logic. Use `StreamTableEnvironment.registerFunction` for the old stack. 

#### Update parser module for FLIP-107 
##### [FLINK-19273](https://issues.apache.org/jira/browse/FLINK-19273)

The term `METADATA` is a reserved keyword now. Use backticks to escape column names and other identifiers with this name. 

#### Update internal aggregate functions to new type system 
##### [FLINK-18809](https://issues.apache.org/jira/browse/FLINK-18809)

SQL queries that use the `COLLECT` function might need to be updated to the new type system. 

### Connectors and Formats

#### Remove Kafka 0.10.x and 0.11.x connectors 
##### [FLINK-19152](https://issues.apache.org/jira/browse/FLINK-19152)

In Flink 1.12 we removed the Kafka 0.10.x and 0.11.x connectors. Please use the universal Kafka connector which works with any Kafka cluster version after 0.10.2.x. 

Please refer to the [documentation]({{< ref "docs/connectors/datastream/kafka" >}}) to learn about how to upgrade the Flink Kafka Connector version. 

#### Csv Serialization schema contains line delimiter 
##### [FLINK-19868](https://issues.apache.org/jira/browse/FLINK-19868)

The `csv.line-delimiter` option has been removed from CSV format. Because the line delimiter should be defined by the connector instead of format. If users have been using this option in previous Flink version, they should alter such table to remove this option when upgrading to Flink 1.12. There should not much users using this option. 

#### Upgrade to Kafka Schema Registry Client 5.5.0 
##### [FLINK-18546](https://issues.apache.org/jira/browse/FLINK-18546)

The `flink-avro-confluent-schema-registry` module is no longer provided as a fat-jar. You should include its dependencies in your job's fat-jar. Sql-client users can use flink-sql-avro-confluent-schema-registry fat jar. 

#### Upgrade to Avro version 1.10.0 from 1.8.2 
##### [FLINK-18192](https://issues.apache.org/jira/browse/FLINK-18192)

The default version of Avro in flink-avro module was upgraded to 1.10. If for some reason you need an older version (you have Avro coming from Hadoop, or you use classes generated from an older Avro version), please explicitly downgrade the Avro version in your project. 

NOTE: We observed a decreased performance of the Avro 1.10 version compared to 1.8.2. If you are concerned with the performance and you are fine working with an older version of Avro, consider downgrading the Avro version. 

#### Create an uber jar when packaging flink-avro for SQL Client 
##### [FLINK-18802](https://issues.apache.org/jira/browse/FLINK-18802)

The SQL Client jar was renamed to flink-sql-avro-{{< version >}}.jar, previously flink-avro-{{< version >}}-sql-jar.jar. Moreover it is no longer needed to add Avro dependencies manually. 

### Deployment

#### Default log4j configuration rolls logs after reaching 100 megabytes 
##### [FLINK-8357](https://issues.apache.org/jira/browse/FLINK-8357)

The default log4j configuration has changed: Besides the existing rolling of log files on startup of Flink, they also roll once they've reached a size of 100MB. Flink keeps a total of 10 log files, effectively limiting the total size of the log directory to 1GB (per Flink service logging to that directory).

#### Use jemalloc by default in the Flink docker image 
##### [FLINK-19125](https://issues.apache.org/jira/browse/FLINK-19125)

jemalloc is adopted as the default memory allocator in Flink's docker image to reduce issues with memory fragmentation. Users can roll back to using glibc by passing the 'disable-jemalloc' flag to the `docker-entrypoint.sh` script. For more details, please refer to the [Flink on Docker documentation]({{< ref "docs/deployment/resource-providers/standalone/docker" >}}). 

#### Upgrade Mesos version to 1.7 
##### [FLINK-19783](https://issues.apache.org/jira/browse/FLINK-19783)

The Mesos dependency has been bumped from 1.0.1 to 1.7.0. 

#### Send SIGKILL if Flink process doesn't stop after a timeout 
##### [FLINK-17470](https://issues.apache.org/jira/browse/FLINK-17470)

In Flink 1.12 we changed the behavior of the standalone scripts to issue a SIGKILL if a SIGTERM did not succeed in shutting down a Flink process. 

#### Introduce non-blocking job submission 
##### [FLINK-16866](https://issues.apache.org/jira/browse/FLINK-16866)

The semantics of submitting a job have slightly changed. The submission call returns almost immediately, with the job being in a new `INITIALIZING` state. Operations such as triggering a savepoint or retrieving the full job details are not available while the job is in that state. 

Once the JobManager for that job has been created, the job is in `CREATED` state and all calls are available. 

### Runtime

#### FLIP-141: Intra-Slot Managed Memory Sharing

The configuration `python.fn-execution.buffer.memory.size` and `python.fn-execution.framework.memory.size` have been removed and so will not take effect any more. Besides, the default value of `python.fn-execution.memory.managed` has been changed to true and so managed memory will be used by default for Python workers. In cases where Python UDFs are used together with the RocksDB state backend in streaming or built-in batch algorithms in batch, the user can control how managed memory should be shared between data processing (RocksDB state backend or batch algorithms) and Python, by overwriting [managed memory consumer weights]({{< ref "docs/deployment/memory/mem_setup_tm" >}}#consumer-weights). 

#### FLIP-119 Pipelined Region Scheduling 
##### [FLINK-16430](https://issues.apache.org/jira/browse/FLINK-16430)

Beginning from Flink 1.12, jobs will be scheduled in the unit of pipelined regions. A pipelined region is a set of pipelined connected tasks. This means that, for streaming jobs which consist of multiple regions, it no longer waits for all tasks to acquire slots before starting to deploy tasks. Instead, any region can be deployed once it has acquired enough slots for within tasks. For batch jobs, tasks will not be assigned slots and get deployed individually. Instead, a task will be deployed together with all other tasks in the same region, once the region has acquired enough slots. 

The old scheduler can be enabled using the `jobmanager.scheduler.scheduling-strategy: legacy` setting.
