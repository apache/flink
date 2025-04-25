---
title: Overview
weight: 2
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

# Flink DataStream API Programming Guide

DataStream programs in Flink are regular programs that implement transformations on data streams
(e.g., filtering, updating state, defining windows, aggregating). The data streams are initially created from various
sources (e.g., message queues, socket streams, files). Results are returned via sinks, which may for
example write the data to files, or to standard output (for example the command line
terminal). Flink programs run in a variety of contexts, standalone, or embedded in other programs.
The execution can happen in a local JVM, or on clusters of many machines.

## What is a DataStream?

The DataStream API gets its name from the special `DataStream` class that is
used to represent a collection of data in a Flink program. You can think of
them as immutable collections of data that can contain duplicates. This data
can either be finite or unbounded, the API that you use to work on them is the
same.

A `DataStream` is similar to a regular Java `Collection` in terms of usage but
is quite different in some key ways. They are immutable, meaning that once they
are created you cannot add or remove elements. You can also not simply inspect
the elements inside but only work on them using the `DataStream` API
operations, which are also called transformations.

You can create an initial `DataStream` by adding a source in a Flink program.
Then you can derive new streams from this and combine them by using API methods
such as `process`, `connectAndProcess`, and so on.

## Fundamental Primitives and Extensions

Based on whether its functionality must be provided by flink, we divide the relevant concepts in DataStream API 
into two categories: fundamental primitives and high-level extensions.

### Fundamental primitives

Fundamental primitives are the basic and necessary semantics that flink need to provide in order to
define a stateful stream processing application, which cannot be achieved by users if not provided by
the framework. It includes data stream, partitioning, process function, state, processing timer service,
watermark.

More details can be found in:
- [Building Blocks]({{< ref "docs/dev/datastream-v2/building_blocks" >}}): Given the most basic elements of DataStream API.
- [State Processing]({{< ref "docs/dev/datastream-v2/context_and_state_processing" >}}): Explanation of how to develop stateful applications.
- [Time Processing # Processing Timer Service]({{< ref "docs/dev/datastream-v2/time-processing/processing_timer_service" >}}): Explanation of how to handle processing time.
- [Watermark]({{< ref "docs/dev/datastream-v2/watermark" >}}): Explanation of how to define and handle user defined events.

### High-Level Extensions

High-Level extensions are like short-cuts / sugars, without which users can probably still achieve the same
behavior by working with the fundamental APIs, but would be a lot easier with the builtin supports. 
It includes event timer service, window and join.

More details can be found in:
- [Time Processing # Event Timer Service]({{< ref "docs/dev/datastream-v2/time-processing/event_timer_service" >}}): Explanation of how to handle event time via extension.
- [Builtin Functions]({{< ref "docs/dev/datastream-v2/builtin-funcs/windows" >}}): Explanation of how to do window aggregation and join via extension.

## Anatomy of a Flink DataStream Program

Flink programs look like regular programs that transform `DataStream`.  Each
program consists of the same basic parts:

1. Obtain an `Execution Environment`,
2. Load/create the initial data,
3. Specify transformations on this data,
4. Specify where to put the results of your computations,
5. Trigger the program execution

### Obtain an `Execution Environment`

We will now give an overview of each of those steps, please refer to the
respective sections for more details.

The `ExecutionEnvironment` is the basis for all Flink programs. You can
obtain one using these static methods on `ExecutionEnvironment`:

```java
ExecutionEnvironment env = ExecutionEnvironment.getInstance();
```

If you are executing your program inside an IDE or as a regular Java program it will create a local environment
that will execute your program on your local machine. If you created a JAR file
from your program, and invoke it through the command line, the Flink cluster manager will execute your main method and
`ExecutionEnvironment.getInstance()` will return an execution environment for executing
your program on a cluster.


### Load/create the Initial Data

Sources are where your program reads its input from. You can attach a source to your program by
using `ExecutionEnvironment.fromSource(source, sourceName)`. Flink comes with a number of pre-implemented
source, you can use [FLIP-27](https://cwiki.apache.org/confluence/x/co_zBQ) based source via `DataStreamV2SourceUtils.wrapSource(source)` or use 
`DataStreamV2SourceUtils.fromData(collection)` for testing/debugging purpose.

As an example, to just read data from a predefined collection, you can use:

```java
ExecutionEnvironment env = ExecutionEnvironment.getInstance();

NonKeyedPartitionStream<String> input = 
        env.fromSource(
                DataStreamV2SourceUtils.fromData(Arrays.asList("1", "2", "3")), 
                "source"
        );
```

Since data from source has no clear partitioning, this will give you a `NonKeyedPartitionStream` on which you can then apply transformations to create new
derived DataStreams. For other types of DataStream, see [Building Blocks # DataStream]({{< ref "docs/dev/datastream-v2/building_blocks" >}}#datastream).

### Specify Transformations on this Data

You apply transformations by calling methods on DataStream with a
`ProcssFunction`(using lambda expression here for simplicity). For example, a map transformation looks like this:

```java
NonKeyedPartitionStream<String> input = ...;

NonKeyedPartitionStream<Integer> parsed = source.process(
        (OneInputStreamProcessFunction<String, Integer>)
                (record, output, ctx) -> {
                    output.collect(Integer.parseInt(record));
                });
```

This will create a new DataStream by converting every String in the original
collection to an Integer. For more details of processing, see [Building Blocks # Process Function]({{< ref "docs/dev/datastream-v2/building_blocks" >}}#processfunction). 

### Specify Where to Put the Results of Your Computations

Once you have a DataStream containing your final results, you can write it to
an external system by creating a sink. 

Data sinks consume DataStreams and forward them to files, sockets, external systems, or print them.
Flink comes with a variety of built-in sink implementations, you can use SinkV2 based sink via `DataStreamV2SinkUtils.wrapSink(sink)`.

These are just an example for creating a sink to print the results:

```java
parsed.toSink(DataStreamV2SinkUtils.wrapSink(new PrintSink<>()));
```

### Trigger the Program Execution

Once you specified the complete program you need to **trigger the program
execution** by calling `execute()` on the `ExecutionEnvironment`.
Depending on the type of the `ExecutionEnvironment` the execution will be
triggered on your local machine or submit your program for execution on a
cluster. The `execute()` method will wait for the job to finish.

That last part about program execution is crucial to understanding when and how
Flink operations are executed. All Flink programs are executed lazily: When the
program's main method is executed, the data loading and transformations do not
happen directly. Rather, each operation is created and added to a dataflow
graph. The operations are actually executed when the execution is explicitly
triggered by an `execute()` call on the execution environment.  Whether the
program is executed locally or on a cluster depends on the type of execution
environment.

The lazy evaluation lets you construct sophisticated programs that Flink
executes as one holistically planned unit.

Where to go next?
-----------------

* [Building Blocks]({{< ref "docs/dev/datastream-v2/building_blocks" >}})
* [Context and State Processing]({{< ref "docs/dev/datastream-v2/context_and_state_processing" >}})
* [Time Processing]({{< ref "docs/dev/datastream-v2/time-processing/processing_timer_service" >}})
* [Builtin Functions]({{< ref "docs/dev/datastream-v2/builtin-funcs/windows" >}})
* [Watermark]({{< ref "docs/dev/datastream-v2/watermark" >}})

{{< top >}}
