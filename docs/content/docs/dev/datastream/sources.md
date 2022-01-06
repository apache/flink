---
title: "Data Sources"
weight: 11
type: docs
aliases:
  - /dev/stream/sources.html
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

# Data Sources

This page describes Flink's Data Source API and the concepts and architecture behind it.
**Read this, if you are interested in how data sources in Flink work, or if you want to implement a new Data Source.**

If you are looking for pre-defined source connectors, please check the [Connector Docs]({{< ref "docs/connectors/datastream/overview" >}}).

## Data Source Concepts

**Core Components**

A Data Source has three core components: *Splits*, the *SplitEnumerator*, and the *SourceReader*.

  - A **Split** is a portion of data consumed by the source, like a file or a log partition. Splits are granularity by which the source distributes the work and parallelizes the data reading.

  - The **SourceReader** requests *Splits* and processes them, for example by reading the file or log partition represented by the *Split*. The *SourceReader* run in parallel on the Task Managers in the `SourceOperators` and produces the parallel stream of events/records.

  - The **SplitEnumerator** generates the *Splits* and assigns them to the *SourceReaders*. It runs as a single instance on the Job Manager and is responsible for maintaining the backlog of pending *Splits* and assigning them to the readers in a balanced manner.
  
The [Source](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java) class is API entry point that ties the above three components together.

{{< img src="/fig/source_components.svg" alt="Illustration of SplitEnumerator and SourceReader interacting" width="70%" >}}


**Unified Across Streaming and Batch**

The Data Source API supports both unbounded streaming sources and bounded batch sources, in a unified way.

The difference between both cases is minimal: In the bounded/batch case, the enumerator generates a fix set of splits, and each split is necessarily finite. In the unbounded streaming case, one of the two is not true (splits are not finite, or the enumerator keep generating new splits).

#### Examples

Here are some simplified conceptual examples to illustrate how the data source components interact, in streaming and batch cases.

*Note that this does not accurately describe how the Kafka and File source implementations work; parts are simplified, for illustrative purposes.*

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

## The Data Source API
This section describes the major interfaces of the new Source API introduced in FLIP-27, and provides tips to the developers on the Source development. 

### Source
The [Source](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/Source.java) API is a factory style interface to create the following components.

  - *Split Enumerator*
  - *Source Reader*
  - *Split Serializer*
  - *Enumerator Checkpoint Serializer*
  
In addition to that, the Source provides the [boundedness](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/Boundedness.java) attribute of the source, so that Flink can choose appropriate mode to run the Flink jobs.

The Source implementations should be serializable as the Source instances are serialized and uploaded to the Flink cluster at runtime.

### SplitEnumerator
The [SplitEnumerator](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumerator.java) is expected to be the "brain" of the Source. Typical implementations of the `SplitEnumerator` do the following:

  - `SourceReader` registration handling
  - `SourceReader` failure handling
    - The `addSplitsBack()` method will be invoked when a `SourceReader` fails. The SplitEnumerator should take back the split assignments that have not been acknowledged by the failed `SourceReader`.
  - `SourceEvent` handling
    - `SourceEvent`s are custom events sent between `SplitEnumerator` and `SourceReader`. The implementation can leverage this mechanism to perform sophisticated coordination.  
  - Split discovery and assignment
    - The `SplitEnumerator` can assign splits to the `SourceReader`s in response to various events, including discovery of new splits, new `SourceReader` registration, `SourceReader` failure, etc.

A `SplitEnumerator` can accomplish the above work with the help of the [SplitEnumeratorContext](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/SplitEnumeratorContext.java) which is provided to the `Source` on creation or restore of the `SplitEnumerator`. 
The `SplitEnumeratorContext` allows a `SplitEnumerator` to retrieve necessary information of the readers and perform coordination actions.
The `Source` implementation is expected to pass the `SplitEnumeratorContext` to the `SplitEnumerator` instance. 

While a `SplitEnumerator` implementation can work well in a reactive way by only taking coordination actions when its method is invoked, some `SplitEnumerator` implementations might want to take actions actively. For example, a `SplitEnumerator` may want to periodically run split discovery and assign the new splits to the `SourceReaders`. 
Such implementations may find that the `callAsync()` method `SplitEnumeratorContext` is handy. The code snippet below shows how the `SplitEnumerator` implementation can achieve that without maintaining its own threads.

```java
class MySplitEnumerator implements SplitEnumerator<MySplit> {
    private final long DISCOVER_INTERVAL = 60_000L;

    /**
     * A method to discover the splits.
     */
    private List<MySplit> discoverSplits() {...}
    
    @Override
    public void start() {
        ...
        enumContext.callAsync(this::discoverSplits, splits -> {
            Map<Integer, List<MockSourceSplit>> assignments = new HashMap<>();
            int parallelism = enumContext.currentParallelism();
            for (MockSourceSplit split : splits) {
                int owner = split.splitId().hashCode() % parallelism;
                assignments.computeIfAbsent(owner, new ArrayList<>()).add(split);
            }
            enumContext.assignSplits(new SplitsAssignment<>(assignments));
        }, 0L, DISCOVER_INTERVAL);
        ...
    }
    ...
}
```

### SourceReader

The [SourceReader](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/SourceReader.java) is a component running in the Task Managers to consume the records from the Splits. 

The `SourceReader` exposes a pull-based consumption interface. A Flink task keeps calling `pollNext(ReaderOutput)` in a loop to poll records from the `SourceReader`. The return value of the `pollNext(ReaderOutput)` method indicates the status of the source reader.

  - `MORE_AVAILABLE` - The SourceReader has more records available immediately.
  - `NOTHING_AVAILABLE` - The SourceReader does not have more records available at this point, but may have more records in the future.
  - `END_OF_INPUT` - The SourceReader has exhausted all the records and reached the end of data. This means the SourceReader can be closed.

In the interest of performance, a `ReaderOutput` is provided to the `pollNext(ReaderOutput)` method, so a `SourceReader` can emit multiple records in a single call of pollNext() if it has to. For example, sometimes the external system works at the granularity of blocks. A block may contain multiple records but the source can only checkpoint at the block boundaries. In this case the `SourceReader` can emit all the records in one block at a time to the `ReaderOutput`.
**However, the `SourceReader` implementation should avoid emitting multiple records in a single `pollNext(ReaderOutput)` invocation unless necessary.** This is because the task thread that is polling from the `SourceReader` works in an event-loop and cannot block.

All the state of a `SourceReader` should be maintained inside the `SourceSplit`s which are returned at the `snapshotState()` invocation. Doing this allows the `SourceSplit`s to be reassigned to other `SourceReaders` when needed.

A `SourceReaderContext` is provided to the `Source` upon a `SourceReader` creation. It is expected that the `Source` will pass the context to the `SourceReader` instance. The `SourceReader` can send `SourceEvent` to its `SplitEnumerator` through the `SourceReaderContext`. A typical design pattern of the `Source` is letting the `SourceReader`s report their local information to the `SplitEnumerator` who has a global view to make decisions.

The `SourceReader` API is a low level API that allows users to deal with the splits manually and have their own threading model to fetch and handover the records. To facilitate the `SourceReader` implementation, Flink has provided a [SourceReaderBase](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/SourceReaderBase.java) class which significantly reduces the amount the work needed to write a `SourceReader`.
**It is highly recommended for the connector developers to take advantage of the `SourceReaderBase` instead of writing the `SourceReader`s from scratch**. For more details please check the [Split Reader API](#the-split-reader-api) section.

### Use the Source
In order to create a `DataStream` from a `Source`, one needs to pass the `Source` to a `StreamExecutionEnvironment`. For example,

{{< tabs "bde5ff60-4e61-4633-a6dc-50413cfd7b45" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Source mySource = new MySource(...);

DataStream<Integer> stream = env.fromSource(
        mySource,
        WatermarkStrategy.noWatermarks(),
        "MySourceName");
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
...
```
{{< /tab >}}
{{< /tabs >}}

----

## The Split Reader API

The core SourceReader API is fully asynchronous and requires implementations to manage asynchronous split reading manually.
However, in practice, most sources perform blocking operations, like blocking *poll()* calls on clients (for example the `KafkaConsumer`), or blocking I/O operations on distributed file systems (HDFS, S3, ...). To make this compatible with the asynchronous Source API, these blocking (synchronous) operations need to happen in separate threads, which hand over the data to the asynchronous part of the reader.

The [SplitReader](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/splitreader/SplitReader.java) is the high-level API for simple synchronous reading/polling-based source implementations, like file reading, Kafka, etc.

The core is the `SourceReaderBase` class, which takes a `SplitReader` and creates fetcher threads running the SplitReader, supporting different consumption threading models.

### SplitReader

The `SplitReader` API only has three methods:
  - A blocking fetch method to return a [RecordsWithSplitIds](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/RecordsWithSplitIds.java)
  - A non-blocking method to handle split changes.
  - A non-blocking wake up method to wake up the blocking fetch operation.

The `SplitReader` only focuses on reading the records from the external system, therefore is much simpler compared with `SourceReader`.
Please check the Java doc of the class for more details.

### SourceReaderBase

It is quite common that a `SourceReader` implementation does the following:

  - Have a pool of threads fetching from splits of the external system in a blocking way.
  - Handle the synchronization between the internal fetching threads and other methods invocations such as `pollNext(ReaderOutput)`.
  - Maintain the per split watermark for watermark alignment.
  - Maintain the state of each split for checkpoint.
  
In order to reduce the work of writing a new `SourceReader`, Flink provides a [SourceReaderBase](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/SourceReaderBase.java) class to serve as a base implementation of the `SourceReader`. 
`SourceReaderBase` has all the above work done out of the box. To write a new `SourceReader`, one can just let the `SourceReader` implementation inherit from the `SourceReaderBase`, fill in a few methods and implement a high level [SplitReader](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/splitreader/SplitReader.java).

### SplitFetcherManager

The `SourceReaderBase` supports a few threading models out of the box, depending on the behavior of the [SplitFetcherManager](https://github.com/apache/flink/blob/master/flink-connectors/flink-connector-base/src/main/java/org/apache/flink/connector/base/source/reader/fetcher/SplitFetcherManager.java) it works with.
The `SplitFetcherManager` helps create and maintain a pool of `SplitFetcher`s each fetching with a `SplitReader`. It also determines how to assign splits to each split fetcher.

As an example, as illustrated below, a `SplitFetcherManager` may have a fixed number of threads, each fetching from some splits assigned to the `SourceReader`.

{{< img width="70%" src="/fig/source_reader.svg" alt="One fetcher per split threading model." >}}

The following code snippet implements this threading model.

```java
/**
 * A SplitFetcherManager that has a fixed size of split fetchers and assign splits 
 * to the split fetchers based on the hash code of split IDs.
 */
public class FixedSizeSplitFetcherManager<E, SplitT extends SourceSplit> 
        extends SplitFetcherManager<E, SplitT> {
    private final int numFetchers;

    public FixedSizeSplitFetcherManager(
            int numFetchers,
            FutureNotifier futureNotifier,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitReaderSupplier) {
        super(futureNotifier, elementsQueue, splitReaderSupplier);
        this.numFetchers = numFetchers;
        // Create numFetchers split fetchers.
        for (int i = 0; i < numFetchers; i++) {
            startFetcher(createSplitFetcher());
        }
    }

    @Override
    public void addSplits(List<SplitT> splitsToAdd) {
        // Group splits by their owner fetchers.
        Map<Integer, List<SplitT>> splitsByFetcherIndex = new HashMap<>();
        splitsToAdd.forEach(split -> {
            int ownerFetcherIndex = split.hashCode() % numFetchers;
            splitsByFetcherIndex
                    .computeIfAbsent(ownerFetcherIndex, s -> new ArrayList<>())
                    .add(split);
        });
        // Assign the splits to their owner fetcher.
        splitsByFetcherIndex.forEach((fetcherIndex, splitsForFetcher) -> {
            fetchers.get(fetcherIndex).addSplits(splitsForFetcher);
        });
    }
}
```

And a `SourceReader` using this threading model can be created like following:

```java
public class FixedFetcherSizeSourceReader<E, T, SplitT extends SourceSplit, SplitStateT>
        extends SourceReaderBase<E, T, SplitT, SplitStateT> {

    public FixedFetcherSizeSourceReader(
            FutureNotifier futureNotifier,
            FutureCompletingBlockingQueue<RecordsWithSplitIds<E>> elementsQueue,
            Supplier<SplitReader<E, SplitT>> splitFetcherSupplier,
            RecordEmitter<E, T, SplitStateT> recordEmitter,
            Configuration config,
            SourceReaderContext context) {
        super(
                futureNotifier,
                elementsQueue,
                new FixedSizeSplitFetcherManager<>(
                        config.getInteger(SourceConfig.NUM_FETCHERS),
                        futureNotifier,
                        elementsQueue,
                        splitFetcherSupplier),
                recordEmitter,
                config,
                context);
    }

    @Override
    protected void onSplitFinished(Collection<String> finishedSplitIds) {
        // Do something in the callback for the finished splits.
    }

    @Override
    protected SplitStateT initializedState(SplitT split) {
        ...
    }

    @Override
    protected SplitT toSplitType(String splitId, SplitStateT splitState) {
        ...
    }
}
```

The `SourceReader` implementations can also implement their own threading model easily on top of the `SplitFetcherManager` and `SourceReaderBase`.

## Event Time and Watermarks

*Event Time* assignment and *Watermark Generation* happen as part of the data sources. The event streams leaving the Source Readers have event timestamps and (during streaming execution) contain watermarks. See [Timely Stream Processing]({{< ref "docs/concepts/time" >}}) for an introduction to Event Time and Watermarks.

{{< hint warning >}}
Applications based on the legacy [SourceFunction](https://github.com/apache/flink/blob/master/flink-streaming-java/src/main/java/org/apache/flink/streaming/api/functions/source/SourceFunction.java) typically generate timestamps and watermarks in a separate later step via `stream.assignTimestampsAndWatermarks(WatermarkStrategy)`. This function should not be used with the new sources, because timestamps will be already assigned, and it will override the previous split-aware watermarks.
{{< /hint >}}

#### API

The `WatermarkStrategy` is passed to the Source during creation in the DataStream API and creates both the [TimestampAssigner](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/eventtime/TimestampAssigner.java) and [WatermarkGenerator](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/common/eventtime/WatermarkGenerator.java).

```java
environment.fromSource(
    Source<OUT, ?, ?> source,
    WatermarkStrategy<OUT> timestampsAndWatermarks,
    String sourceName)
```

The `TimestampAssigner` and `WatermarkGenerator` run transparently as part of the `ReaderOutput`(or `SourceOutput`) so source implementors do not have to implement any timestamp extraction and watermark generation code.

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

{{< img width="80%" src="/fig/per_split_watermarks.svg" alt="Watermark Generation in a Source with two Splits." >}}

When implementing a source connector using the *Split Reader API*, this is automatically handled. All implementations based on the Split Reader API have split-aware watermarks out-of-the-box.

For an implementation of the lower level `SourceReader` API to use split-aware watermark generation, the implementation must output events from different splits to different outputs: the *Split-local SourceOutputs*. Split-local outputs can be created and released on the main [ReaderOutput](https://github.com/apache/flink/blob/master/flink-core/src/main/java/org/apache/flink/api/connector/source/ReaderOutput.java) via the `createOutputForSplit(splitId)` and `releaseOutputForSplit(splitId)` methods. Please refer to the JavaDocs of the class and methods for details.
