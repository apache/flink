---
title: "Hadoop MapReduce compatibility with Flink"
weight: 8
type: docs
aliases:
  - /dev/batch/hadoop_map_reduce.html
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

# Flink and Map Reduce compatibility

Flink is compatible with Apache Hadoop MapReduce interfaces and therefore allows
reusing code that was implemented for Hadoop MapReduce.

You can:

- use Hadoop's `Writable` [data types]({{< ref "docs/dev/datastream/fault-tolerance/serialization/types_serialization" >}}#supported-data-types) in Flink programs.
- use any Hadoop `InputFormat` as a [DataSource]({{< ref "docs/connectors/dataset/formats/hadoop" >}}#using-hadoop-inputformats).
- use any Hadoop `OutputFormat` as a [DataSink]({{< ref "docs/connectors/dataset/formats/hadoop" >}}#using-hadoop-outputformats).
- use a Hadoop `Mapper` as [FlatMapFunction]({{< ref "docs/dev/dataset/transformations" >}}#flatmap).
- use a Hadoop `Reducer` as [GroupReduceFunction]({{< ref "docs/dev/dataset/transformations" >}}#groupreduce-on-grouped-dataset).

This document shows how to use existing Hadoop MapReduce code with Flink. Please refer to the
[Connecting to other systems]({{< ref "docs/deployment/filesystems/overview" >}}#hadoop-file-system-hdfs-and-its-other-implementations) guide for reading from Hadoop supported file systems.

## Project Configuration

Support for Hadoop is contained in the `flink-hadoop-compatibility`
Maven module.

Add the following dependency to your `pom.xml` to use hadoop

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility{{< scala_version >}}</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

If you want to run your Flink application locally (e.g. from your IDE), you also need to add 
a `hadoop-client` dependency such as:

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.8.3</version>
    <scope>provided</scope>
</dependency>
```

## Using Hadoop Mappers and Reducers

Hadoop Mappers are semantically equivalent to Flink's [FlatMapFunctions]({{< ref "docs/dev/dataset/transformations" >}}#flatmap) and Hadoop Reducers are equivalent to Flink's [GroupReduceFunctions]({{< ref "docs/dev/dataset/transformations" >}}#groupreduce-on-grouped-dataset). Flink provides wrappers for implementations of Hadoop MapReduce's `Mapper` and `Reducer` interfaces, i.e., you can reuse your Hadoop Mappers and Reducers in regular Flink programs. At the moment, only the Mapper and Reduce interfaces of Hadoop's mapred API (`org.apache.hadoop.mapred`) are supported.

The wrappers take a `DataSet<Tuple2<KEYIN,VALUEIN>>` as input and produce a `DataSet<Tuple2<KEYOUT,VALUEOUT>>` as output where `KEYIN` and `KEYOUT` are the keys and `VALUEIN` and `VALUEOUT` are the values of the Hadoop key-value pairs that are processed by the Hadoop functions. For Reducers, Flink offers a wrapper for a GroupReduceFunction with (`HadoopReduceCombineFunction`) and without a Combiner (`HadoopReduceFunction`). The wrappers accept an optional `JobConf` object to configure the Hadoop Mapper or Reducer.

Flink's function wrappers are

- `org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction`,
- `org.apache.flink.hadoopcompatibility.mapred.HadoopReduceFunction`, and
- `org.apache.flink.hadoopcompatibility.mapred.HadoopReduceCombineFunction`.

and can be used as regular Flink [FlatMapFunctions]({{< ref "docs/dev/dataset/transformations" >}}#flatmap) or [GroupReduceFunctions]({{< ref "docs/dev/dataset/transformations" >}}#groupreduce-on-grouped-dataset).

The following example shows how to use Hadoop `Mapper` and `Reducer` functions.

```java
// Obtain data to process somehow.
DataSet<Tuple2<LongWritable, Text>> text = [...]

DataSet<Tuple2<Text, LongWritable>> result = text
  // use Hadoop Mapper (Tokenizer) as MapFunction
  .flatMap(new HadoopMapFunction<LongWritable, Text, Text, LongWritable>(
    new Tokenizer()
  ))
  .groupBy(0)
  // use Hadoop Reducer (Counter) as Reduce- and CombineFunction
  .reduceGroup(new HadoopReduceCombineFunction<Text, LongWritable, Text, LongWritable>(
    new Counter(), new Counter()
  ));
```

**Please note:** The Reducer wrapper works on groups as defined by Flink's [groupBy()]({{< ref "docs/dev/dataset/transformations" >}}#groupreduce-on-grouped-dataset) operation. It does not consider any custom partitioners, sort or grouping comparators you might have set in the `JobConf`.

## Complete Hadoop WordCount Example

The following example shows a complete WordCount implementation using Hadoop data types, Input- and OutputFormats, and Mapper and Reducer implementations.

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

// Set up the Hadoop TextInputFormat.
Job job = Job.getInstance();
HadoopInputFormat<LongWritable, Text> hadoopIF =
  new HadoopInputFormat<LongWritable, Text>(
    new TextInputFormat(), LongWritable.class, Text.class, job
  );
TextInputFormat.addInputPath(job, new Path(inputPath));

// Read data using the Hadoop TextInputFormat.
DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopIF);

DataSet<Tuple2<Text, LongWritable>> result = text
  // use Hadoop Mapper (Tokenizer) as MapFunction
  .flatMap(new HadoopMapFunction<LongWritable, Text, Text, LongWritable>(
    new Tokenizer()
  ))
  .groupBy(0)
  // use Hadoop Reducer (Counter) as Reduce- and CombineFunction
  .reduceGroup(new HadoopReduceCombineFunction<Text, LongWritable, Text, LongWritable>(
    new Counter(), new Counter()
  ));

// Set up the Hadoop TextOutputFormat.
HadoopOutputFormat<Text, LongWritable> hadoopOF =
  new HadoopOutputFormat<Text, LongWritable>(
    new TextOutputFormat<Text, LongWritable>(), job
  );
hadoopOF.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
TextOutputFormat.setOutputPath(job, new Path(outputPath));

// Emit data using the Hadoop TextOutputFormat.
result.output(hadoopOF);

// Execute Program
env.execute("Hadoop WordCount");
```

{{< top >}}
