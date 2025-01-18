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
This section describes the major interfaces of the new Sink API introduced in [FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction), and provides tips to the developers on the Sink development. 

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
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val mySource = new MySource(...)

val stream = env.fromSource(
      mySource,
      WatermarkStrategy.noWatermarks(),
      "MySourceName")
val mySink = new MySink(...)

val stream = stream.sinkTo(mySink)
...
```
{{< /tab >}}
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()

my_source = ...

env.from_source(
    my_source,
    WatermarkStrategy.no_watermarks(),
    "my_source_name")

my_sink = ...

env.sinkTo(my_sink)
```
{{< /tab >}}
{{< /tabs >}}

----

### SinkWriter

The core {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/SinkWriter.java" name="SinkWriter" >}} API is responsible for writing data to downstream system.

The `SinkWriter` API only has three methods:
- write(InputT element, Context context): Adds an element to the writer.
- flush(boolean endOfInput): Called on checkpoint or end of input so that the writer to flush all pending data for at-least-once.
- writeWatermark(Watermark watermark): Adds a watermark to the writer.

Please check the Java doc of the class for more details.

## Advanced Sink API

### SupportsWriterState

The {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/SupportsWriterState.java" name="SupportsWriterState" >}} interface is used to indicate that the sink supports writer state, which means that the sink can be recovered from a failure.

The `SupportsWriterState` interface would require the `SinkWriter` to implement the {{ gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/StatefulSinkWriter.java" name="StatefulSinkWriter" >}} interface.

### SupportsCommitter

The {{< gh_link file="flink-core/src/main/java/org/apache/flink/api/connector/sink2/SupportsCommitter.java" name="SupportsCommitter" >}} interface is used to indicate that the sink supports exactly-once semantics using a two-phase commit protocol.

The `Sink` consists of a `CommittingSinkWriter` that performs the precommits and a `Committer` that actually commits the data. To facilitate the separation, the `CommittingSinkWriter` creates `committables` on checkpoint or end of input and the sends it to the `Committer`.

The `Sink` needs to be serializable. All configuration should be validated eagerly. The respective sink writers and committers are transient and will only be created in the subtasks on the TaskManagers.

### SupportsPreWriteTopology

Allows expert users to implement a custom topology before `SinkWriter`.

### SupportsPreCommitTopology

Allows expert users to implement a custom topology after `SinkWriter` and before `Committer`.

### SupportsPostCommitTopology

Allows expert users to implement a custom topology after `Committer`.
