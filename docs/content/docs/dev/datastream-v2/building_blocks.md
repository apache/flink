---
title: Building Blocks
weight: 3
type: docs
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

{{< hint warning >}}
**Note:** DataStream API V2 is a new set of APIs, to gradually replace the original DataStream API. It is currently in the experimental stage and is not fully available for production.
{{< /hint >}}

# Building Blocks

DataStream, Partitioning, ProcessFunction are the most fundamental elements of DataStream API and respectively represent:

- What are the types of data streams

- How data is partitioned

- How to perform operations / processing on data streams

They are also the core parts of the fundamental primitives provided by DataStream API.

## DataStream

Data flows on the stream may be divided into multiple partitions. According to how the data is partitioned on the stream, we divide it into the following categories:

- Global Stream: Force single partition/parallelism, and the correctness of data depends on this.

- Partition Stream:
  - Divide data into multiple partitions. State is only available within the partition. One partition can only be processed by one task, but one task can handle one or multiple partitions.
According to the partitioning approach, it can be further divided into the following two categories:

    - Keyed Partition Stream: Each key is a partition, and the partition to which the data belongs is deterministic.

    - Non-Keyed Partition Stream: Each parallelism is a partition, and the partition to which the data belongs is nondeterministic.

- Broadcast Stream: Each partition contains the same data.

## Partitioning

Above we defined the stream and how it is partitioned. The next topic to discuss is how to convert
between different partition types. We call these transformations partitioning.
For example non-keyed partition stream can be transformed to keyed partition stream via a `KeyBy` partitioning.

```java
NonKeyedPartitionStream<Tuple<Integer, String>> stream = ...;
KeyedPartitionStream<Integer, String> keyedStream = stream.keyBy(record -> record.f0);
```

Overall, we have the following four partitioning:
 
- KeyBy: Let all data be repartitioned according to specified key.

- Shuffle: Repartition and shuffle all data.

- Global: Merge all partitions into one. 

- Broadcast: Force partitions broadcast data to downstream.

The specific transformation relationship is shown in the following table:

{{< img src="/fig/datastream/one-input-partitioning.png" alt="one-input-partitioning" width="70%" >}}

(A crossed box indicates that it is not supported or not required)

One thing to note is: broadcast can only be used in conjunction with other inputs and cannot be directly converted to other streams.

## ProcessFunction
Once we have the data stream, we can apply operations on it. The operations that can be performed over
DataStream are collectively called Process Function. It is the only entrypoint for defining all kinds
of processing on the data streams.

### Classification of ProcessFunction
According to the number of input / output, they are classified as follows:

|               Partitioning                |    Number of Inputs    |        Number of Outputs        |
|:-----------------------------------------:|:----------------------:|:-------------------------------:|
|    OneInputStreamProcessFunction          |           1            |                1                |
| TwoInputNonBroadcastStreamProcessFunction |           2            |                1                |
|  TwoInputBroadcastStreamProcessFunction   |           2            |                1                |
|      TwoOutputStreamProcessFunction       |           1            |                2                |

(Processing for more inputs and outputs can be achieved by combining multiple process functions)

We have two types of two-input process function, depending on whether one of the input is broadcast stream.

DataStream has series of `process` and `connectAndProcess` methods to transform the input stream or connect and transform two input streams via ProcessFunction.

### Requirements for input and output streams

The following two tables list the input and output stream combinations supported by OneInputStreamProcessFunction and TwoOutputStreamProcessFunction respectively.

For OneInputStreamProcessFunction:

| Input Stream |  Output Stream   |  
|:------------:|:----------------:|
|    Global    |      Global      |
|    Keyed     | Keyed / NonKeyed |
|   NonKeyed   |     NonKeyed     |
|  Broadcast   |  Not Supported   |

For TwoOutputStreamProcessFunction:

| Input Stream |             Output Stream             |  
|:------------:|:-------------------------------------:|
|    Global    |            Global + Global            |
|    Keyed     | Keyed + Keyed / Non-Keyed + Non-Keyed |
|   NonKeyed   |          NonKeyed + NonKeyed          |
|  Broadcast   |             Not Supported             |

Things with two inputs is a little more complicated. The following table lists which streams are compatible with each other and the types of streams they output.

A cross(❎) indicates not supported.

|  Output   | Global |       Keyed        | NonKeyed |     Broadcast     |
|:---------:|:------:|:------------------:|:--------:|:-----------------:|
|  Global   | Global |         ❎          |    ❎     |         ❎      |
|   Keyed   |   ❎   | NonKeyed / Keyed   |    ❎     | NonKeyed / Keyed  |
| NonKeyed  |   ❎   |         ❎          | NonKeyed |     NonKeyed      |
| Broadcast |   ❎   |  NonKeyed / Keyed  | NonKeyed |         ❎         |

## Config Process

After defining the process functions, you may want to make some configurations for the properties of this processing.
For example, set the parallelism and name of the process operation, etc.

The return value of `process`/`connectAndProcess` is both a stream and a handle that allowing us to configure the previous processing.
It has a number of methods called `withXXX` to do the configuration. For example:

```java
inputStream
  .process(func1) // do process 1
  .withName("my-process-func") // configure name for process 1
  .withParallelism(2) //  configure parallelism for process 1
  .process(func2) //  do further process 2
```

## Example

Here is an example to show how to use those building blocks to write a flink job.

```java
// create environment
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
// create a stream from source
env.fromSource(someSource)
    // map every element x to x + 1.
    .process(new OneInputStreamProcessFunction<Integer, Integer>() {
                    @Override
                    public void processRecord(
                            Integer x,
                            Collector<Integer> output)
                            throws Exception {
                        output.collect(x + 1);
                    }
                })
    // If the sink does not support concurrent writes, we can force the stream to one partition
    .global()
    // sink the stream to some sink
    .toSink(someSink);
// execute the job
env.execute()
```

{{< top >}}
