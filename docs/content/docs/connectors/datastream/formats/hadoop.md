---
title:  "Hadoop"
weight: 4
type: docs
aliases:
  - /dev/connectors/formats/hadoop.html
  - /apis/streaming/connectors/formats/hadoop.html

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

# Hadoop formats

## Project Configuration

Support for Hadoop input/output formats is part of the `flink-java` and
`flink-scala` Maven modules that are always required when writing Flink jobs.
The code is located in `org.apache.flink.api.java.hadoop` and
`org.apache.flink.api.scala.hadoop` in an additional sub-package for the
`mapred` and `mapreduce` API.

Support for Hadoop Mappers and Reducers is contained in the `flink-hadoop-compatibility`
Maven module.
This code resides in the `org.apache.flink.hadoopcompatibility`
package.

Add the following dependency to your `pom.xml` if you want to reuse Mappers
and Reducers.

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

## Using Hadoop InputFormats

To use Hadoop `InputFormats` with Flink the format must first be wrapped
using either `readHadoopFile` or `createHadoopInput` of the
`HadoopInputs` utility class.
The former is used for input formats derived
from `FileInputFormat` while the latter has to be used for general purpose
input formats.
The resulting `InputFormat` can be used to create a data source by using
`ExecutionEnvironmen#createInput`.

The resulting `DataSet` contains 2-tuples where the first field
is the key and the second field is the value retrieved from the Hadoop
InputFormat.

The following example shows how to use Hadoop's `TextInputFormat`.

{{< tabs "baa59ec9-046e-4fe3-a2db-db5ee09d0635" >}}
{{< tab "Java" >}}

```java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Tuple2<LongWritable, Text>> input =
    env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(),
                        LongWritable.class, Text.class, textPath));

// Do something with the data.
[...]
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val env = ExecutionEnvironment.getExecutionEnvironment

val input: DataSet[(LongWritable, Text)] =
  env.createInput(HadoopInputs.readHadoopFile(
                    new TextInputFormat, classOf[LongWritable], classOf[Text], textPath))

// Do something with the data.
[...]
```

{{< /tab >}}
{{< /tabs >}}

## Using Hadoop OutputFormats

Flink provides a compatibility wrapper for Hadoop `OutputFormats`. Any class
that implements `org.apache.hadoop.mapred.OutputFormat` or extends
`org.apache.hadoop.mapreduce.OutputFormat` is supported.
The OutputFormat wrapper expects its input data to be a DataSet containing
2-tuples of key and value. These are to be processed by the Hadoop OutputFormat.

The following example shows how to use Hadoop's `TextOutputFormat`.

{{< tabs "d4af1c52-0e4c-490c-8c35-e3d60b1b52ee" >}}
{{< tab "Java" >}}

```java
// Obtain the result we want to emit
DataSet<Tuple2<Text, IntWritable>> hadoopResult = [...]

// Set up the Hadoop TextOutputFormat.
HadoopOutputFormat<Text, IntWritable> hadoopOF =
  // create the Flink wrapper.
  new HadoopOutputFormat<Text, IntWritable>(
    // set the Hadoop OutputFormat and specify the job.
    new TextOutputFormat<Text, IntWritable>(), job
  );
hadoopOF.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
TextOutputFormat.setOutputPath(job, new Path(outputPath));

// Emit data using the Hadoop TextOutputFormat.
hadoopResult.output(hadoopOF);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// Obtain your result to emit.
val hadoopResult: DataSet[(Text, IntWritable)] = [...]

val hadoopOF = new HadoopOutputFormat[Text,IntWritable](
  new TextOutputFormat[Text, IntWritable],
  new JobConf)

hadoopOF.getJobConf.set("mapred.textoutputformat.separator", " ")
FileOutputFormat.setOutputPath(hadoopOF.getJobConf, new Path(resultPath))

hadoopResult.output(hadoopOF)


```

{{< /tab >}}
{{< /tabs >}}

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
