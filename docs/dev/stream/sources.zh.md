---
title: "Data Sources"
nav-title: "Data Sources"
nav-parent_id: streaming
nav-pos: 10
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


* This will be replaced by the TOC
{:toc}

<div class="alert alert-warning">
  <p><strong>Note:</strong> This describes the new Data Source API, introduced in Flink 1.11 as part of <a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface">FLIP-27</a>.
  This new API is currently in <strong>BETA</strong> status.</p>
  <p>Most of the existing source connectors are not yet (as of Flink 1.11) implemented using this new API,
  but using the previous API, based on <a href="https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source/SourceFunction.java">SourceFunction</a>.</p>
</div>


This page describes Flink's Data Source API and the concepts and architecture behind it.
**Read this, if you are interested in how data sources in Flink work, or if you want to implement a new Data Source.**

If you are looking for pre-defined source connectors, please check the [Connector Docs]({{ site.baseurl }}/dev/connectors/).


## Data Source Concepts

**Core Components**

A Data Source has three core components: *Splits*, the *SplitEnumerator*, and the *SourceReader*.

  - A **Split** is a portion of data consumed by the source, like a file or a log partition. Splits are granularity by which the source distributes the work and parallelizes the data reading.

  - The **SourceReader** requests *Splits* and processes them, for example by reading the file or log partition represented by the *Split*. The *SourceReader* run in parallel on the Task Managers in the `SourceOperators` and produces the parallel stream of events/records.

  - The **SplitEnumerator** generates the *Splits* and assignes them to the *SourceReaders*. It runs as a single instance on the Job Manager and is responsible for maintaining the backlog of pending *Splits* and assigning them to the readers in a balanced manner.
  
The [Source]() class is API entry point that ties the above three components together.

<div style="text-align: center">
  <img width="70%" src="{{ site.baseurl }}/fig/source_components.svg" alt="Illustration of SplitEnumerator and SourceReader interacting." />
</div>


**Unified Across Streaming and Batch**

The Data Source API supports both unbounded streaming sources and bounded batch sources, in a unified way.

The difference between both cases is minimal: In the bounded/batch case, the enumerator generates a fix set of splits, and each split is necessarily finite. In the unbounded streaming case, one of the two is not true (splits are not finite, or the enumerator keep generating new splits).

#### Examples

Here are some simplified conceptual examples to illustrate how the data source components interact, in streaming and batch cases.

*Note that this does not the accurately describe how the Kafka and File source implementations work; parts are simplified, for illustrative purposes.*

**Bounded File Source**

The source has the URI/Path of a directory to read, and a *Format* that defines how to parse the files.

  - A *Split* is a file, or a region of a file (if the data format supports splitting the file).
  - The *SplitEnumerator* lists all files under the given directory path. It assigns Splits to the next reader that requests a Split. Once all Splits are assigned, it responds to requests with *NoMoreSplits*.
  - The *SourceReader* requests a Split and reads the assigned Split (file or file region) and parses it using the given Format. If it does not get another Split, but a *NoMoreSplits* message, it finishes.

**Unbounded Streaming File Source**

This source works the same way as described above, except that the *SplitEnumerator* never responds with *NoMoreSplits* and periodically lists the contents under the given URI/Path to check for new files. Once it finds new files, it generates new Splits for them and can assign them to the available SourceReaders.

**Unbounded Streaming Kafka Source**

The source has a Kafka Topic (or list of Topics or Topic regex) and a *Deserializer* to parse the records.

  - A *Split* is a Kafka Topic Partition.
  - The *SplitEnumerator* connects to the brokers to list all topic partitions involved in the subscribed topics. The enumerator can optionally repeat this operation to discover newly added topics/partitions.
  - The *SourceReader* reads the assigned Splits (Topic Partitions) using the KafkaConsumer and deserializes the records using the provided Deserializer. The splits (Topic Partitions) do not have an end, so the reader never reaches the end of the data.

**Bounded Kafka Source**

Same as above, except that each Split (Topic Partition) has a defined end offset. Once the *SourceReader* reaches the end offset for a Split, it finishes that Split. Once all assigned Splits are finished, the SourceReader finishes.

----
----

## The Data Source API

Source, SourceEnumerator, SourceReader.

env.continuousSource(...).

----
----

## The Split Reader API


core SourceReader API is asynchronous and rather low level, leavers users to handle splits manually.
in practice, many sources have perform blocking poll() and I/O operations, needing separate threads.
Split Reader is base implementation for that.

----
----

## Event Time and Watermarks

*Event Time* assignment and *Watermark Generation* happen as part of the data sources. The event streams leaving the Source Readers have event timestamps and (during streaming execution) contain watermarks. See [Timely Stream Processing]({{ site.baseurl }}/concepts/timely-stream-processing.html) for an introduction to Event Time and Watermarks.

<span class="label label-danger">Important</span> Applications based on the legacy [SourceFunction](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source/SourceFunction.java) typically generate timestamps and watermarks in a separate later step via `stream.assignTimestampsAndWatermarks(WatermarkStrategy)`. This function should not be used with the new sources, because timestamps will be already assigned, and it will override the previous split-aware watermarks.

#### API

The `WatermarkStrategy` is passed to the Source during creation in the DataStream API and creates both the [TimestampAssigner](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/eventtime/TimestampAssigner.java) and [WatermarkGenerator](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/eventtime/WatermarkGenerator.java).

{% highlight java %}
environment.continuousSource(
    Source<OUT, ?, ?> source,
    WatermarkStrategy<OUT> timestampsAndWatermarks,
    String sourceName)
{% endhighlight %}

The `TimestampAssigner` and `WatermarkGenerator` run transparently as part of the `ReaderOutput`(or `SourceOutput`) so source implementors do not have to implement and timestamp extraction and watermark generation code.

#### Event Timestamps

Event timestamps are assigned in two steps:

  1. The SourceReader may attach the *source record timestamp* to the event, by calling `SourceOutput.collect(event, timestamp)`.
     This is relevant only for data sources that are record-based and have timestamps, such as Kafka, Kinesis, Pulsar, or Pravega.
     Sources that are not based on records with timestamps (like files) do not have a *source record timestamp*.
     This step is part of the source connector implementation and not parameterized by the application that uses the source.

  2. The `TimestampAssigner`, which is configured by the application, assigns the final timestamp.
     The `TimestampAssigner` sees the original *source record timestamp* and the event. The assigner can use the *source record timestamp* or access a field of the event obtain the final event timestamp.
  
This two-step approach allows users to reference both timestamps from the source systems and timestamps in the event's data as the event timestamp.

*Note:* When using a data source without *source record timestamps* (like files) and selecting the *source record timestamp* as the final event timestamp, events will get a default timestamp equal to `LONG_MIN` *(=-9,223,372,036,854,775,808)*.

#### Watermark Generation

Watermark Generators are only active during streaming execution. Batch execution deactivates Watermark Generators; all related operations described below become effectively no-ops.

The data source API supports running watermark generators individually *per split*. That allows Flink to observe the event time progress per split individually, which is important to handle *event time skew* properly and prevent *idle partitions* from holding back the event time progress of the entire application.

<div style="text-align: center">
  <img width="80%" src="{{ site.baseurl }}/fig/per_split_watermarks.svg" alt="Watermark Generation in a Source with two Splits." />
</div>

When implementing a source connector using the *Split Reader API*, this is automatically handled. All implementations based on the Split Reader API have split-aware watermarks out-of-the-box.

For an implementation of the lower level `SourceReader` API to use split-aware watermark generation, the implementation must ouput events from different splits to different outputs, the *split-local SourceOutputs*. Split-local outputs can be created and released on the main [ReaderOutput](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/ReaderOutput.java) via the `createOutputForSplit(splitId)` and `releaseOutputForSplit(splitId)` methods. Please refer to the JavaDocs of the class and methods for details.
