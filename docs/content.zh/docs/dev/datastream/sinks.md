---
title: "Data Sinks"
weight: 12
type: docs
aliases:
  - /dev/stream/sinks.html
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

# Data Sinks

This page describes Flink's Data Sink API and the concepts and architecture behind it.
**Read this, if you are interested in how data sinks in Flink work, or if you want to implement a new Data Sink.**

If you are looking for pre-defined sink connectors, please check the [Connector Docs]({{< ref "docs/connectors/datastream/overview" >}}).

## The Data Sink API
This section describes the major interfaces of the new Sink API introduced in [FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) and [FLIP-372](https://cwiki.apache.org/confluence/display/FLINK/FLIP-372%3A+Enhance+and+synchronize+Sink+API+to+match+the+Source+API), and provides tips to the developers on the Sink development.

### Sink
The {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/Sink.java" name="Sink" >}} API is a factory style interface to create the [SinkWriter](#sinkwriter) to write the data.

The Sink implementations should be serializable as the Sink instances are serialized and uploaded to the Flink cluster at runtime.

#### Use the Sink
We can add a `Sink` to `DataStream` by calling `DataStream.sinkTo(Sink)` method. For example,

{{< tabs "bde5ff60-4e61-4633-a6dc-50413cfd7b45" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Source mySource = new MySource(...);

DataStream<Integer> stream = env.fromSource(
        mySource,
        WatermarkStrategy.noWatermarks(),
        "MySourceName");

Sink mySink = new MySink(...);

        stream.sinkTo(mySink);
...
```
{{< /tab >}}
{{< /tabs >}}

----

### SinkWriter

The core {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/SinkWriter.java" name="SinkWriter" >}} API is responsible for writing data to downstream system.

The `SinkWriter` API only has three methods:
- write(InputT element, Context context): Adds an element to the writer.
- flush(boolean endOfInput): Called on checkpoint or end of the input, setting this flag causes the writer to flush all pending data for `at-least-once`. To archive `exactly-once` semantic, the writer should implement the [SupportsCommitter](#supportscommitter) interface.
- writeWatermark(Watermark watermark): Adds a watermark to the writer.

Please check the [Java doc](https://nightlies.apache.org/flink/flink-docs-release-2.0/api/java//org/apache/flink/api/connector/sink2/SinkWriter.html) of the class for more details.

## Advanced Sink API

### SupportsWriterState

The {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/SupportsWriterState.java" name="SupportsWriterState" >}} interface is used to indicate that the sink supports writer state, which means that the sink can be recovered from a failure.

The `SupportsWriterState` interface requires the `SinkWriter` to implement the {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/StatefulSinkWriter.java" name="StatefulSinkWriter" >}} interface.

### SupportsCommitter

The {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/SupportsCommitter.java" name="SupportsCommitter" >}} interface is used to indicate that the sink supports exactly-once semantics using a two-phase commit protocol.

The `Sink` consists of a `CommittingSinkWriter` that performs the precommits and a `Committer` that actually commits the data. To facilitate the separation, the `CommittingSinkWriter` creates `committables` on checkpoint or end of input and the sends it to the `Committer`.

The `Sink` needs to be serializable. All configuration should be validated eagerly. The respective sink writers and committers are transient and will only be created in the subtasks on the TaskManagers.

### Custom sink topology

For advanced developers, they may want to specify their own sink operator topology(A structure composed of a series of operators), such as collecting `committables` to one subtask and processing them together, or performing operations such as merging small files after `Committer`. Flink provides the following interfaces to allow expert users to customize the sink operator topology.

#### SupportsPreWriteTopology

`SupportsPreWriteTopology` interface Allows expert users to implement a custom operator topology before `SinkWriter`, which can be used to process or redistribute the input data. For example, sending data of the same partition to the same SinkWriter of Kafka or Iceberg.

The following figure shows the operator topology of using {{< gh_link file="flink-runtime/src/main/java/org/apache/flink/streaming/api/connector/sink2/SupportsPreWriteTopology.java" name="SupportsPreWriteTopology" >}}:

{{< img src="/fig/dev/datastream/SupportsPreWriteTopology.png" class="center" >}}

In the figure above, user add a `PrePartition` and `PostPartition` operator in the `SupportsPreWriteTopology` topology, and redistribute the input data to the `SinkWriter`.

#### SupportsPreCommitTopology

`SupportsPreCommitTopology` interface Allows expert users to implement a custom operator topology after `SinkWriter` and before `Committer`, which can be used to process or redistribute the commit messages.

The following figure shows the operator topology of using {{< gh_link file="flink-runtime/src/main/java/org/apache/flink/streaming/api/connector/sink2/SupportsPreCommitTopology.java" name="SupportsPreCommitTopology" >}}:

{{< img src="/fig/dev/datastream/SupportsPreCommitTopology.png" class="center" >}}

In the figure above, user add a `CollectCommit` operator in the `SupportsPreCommitTopology` topology, and collect all the commit messages from the `SinkWriter` to one subtask, then send to the `Committer` to process them centrally, this can reduce the number of interactions with the server. 

Please note that the parallelism has only been modified here for display purposes. In fact, the parallelism can be set by user.

#### SupportsPostCommitTopology

`SupportsPostCommitTopology` interface Allows expert users to implement a custom operator topology after `Committer`.

The following figure shows the operator topology of using {{< gh_link file="flink-runtime/src/main/java/org/apache/flink/streaming/api/connector/sink2/SupportsPostCommitTopology.java" name="SupportsPostCommitTopology" >}}:

{{< img src="/fig/dev/datastream/SupportsPostCommitTopology.png" class="center" >}}

In the figure above, users add a `MergeFile` operator in the `SupportsPostCommitTopology` topology, the `MergeFile` operator can merge small files into larger files to speed up the reading of file system. 
