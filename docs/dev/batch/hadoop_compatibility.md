---
title: "Hadoop Compatibility"
is_beta: true
nav-parent_id: batch
nav-pos: 7
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

Flink is compatible with Apache Hadoop MapReduce interfaces and therefore allows
reusing code that was implemented for Hadoop MapReduce.

You can:

- use Hadoop's `Writable` [data types](index.html#data-types) in Flink programs.
- use any Hadoop `InputFormat` as a [DataSource](index.html#data-sources).
- use any Hadoop `OutputFormat` as a [DataSink](index.html#data-sinks).
- use a Hadoop `Mapper` as [FlatMapFunction](dataset_transformations.html#flatmap).
- use a Hadoop `Reducer` as [GroupReduceFunction](dataset_transformations.html#groupreduce-on-grouped-dataset).

This document shows how to use existing Hadoop MapReduce code with Flink. Please refer to the
[Connecting to other systems]({{ site.baseurl }}/dev/batch/connectors.html) guide for reading from Hadoop supported file systems.

* This will be replaced by the TOC
{:toc}

### Project Configuration

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

{% highlight xml %}
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility{{ site.scala_version_suffix }}</artifactId>
	<version>{{site.version}}</version>
</dependency>
{% endhighlight %}

### Using Hadoop Data Types

Flink supports all Hadoop `Writable` and `WritableComparable` data types
out-of-the-box. You do not need to include the Hadoop Compatibility dependency,
if you only want to use your Hadoop data types. See the
[Programming Guide](index.html#data-types) for more details.

### Using Hadoop InputFormats

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

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

DataSet<Tuple2<LongWritable, Text>> input =
    env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(),
                        LongWritable.class, Text.class, textPath));

// Do something with the data.
[...]
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
val env = ExecutionEnvironment.getExecutionEnvironment

val input: DataSet[(LongWritable, Text)] =
  env.createInput(HadoopInputs.readHadoopFile(
                    new TextInputFormat, classOf[LongWritable], classOf[Text], textPath))

// Do something with the data.
[...]
{% endhighlight %}

</div>

</div>

### Using Hadoop OutputFormats

Flink provides a compatibility wrapper for Hadoop `OutputFormats`. Any class
that implements `org.apache.hadoop.mapred.OutputFormat` or extends
`org.apache.hadoop.mapreduce.OutputFormat` is supported.
The OutputFormat wrapper expects its input data to be a DataSet containing
2-tuples of key and value. These are to be processed by the Hadoop OutputFormat.

The following example shows how to use Hadoop's `TextOutputFormat`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">

{% highlight java %}
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
{% endhighlight %}

</div>
<div data-lang="scala" markdown="1">

{% highlight scala %}
// Obtain your result to emit.
val hadoopResult: DataSet[(Text, IntWritable)] = [...]

val hadoopOF = new HadoopOutputFormat[Text,IntWritable](
  new TextOutputFormat[Text, IntWritable],
  new JobConf)

hadoopOF.getJobConf.set("mapred.textoutputformat.separator", " ")
FileOutputFormat.setOutputPath(hadoopOF.getJobConf, new Path(resultPath))

hadoopResult.output(hadoopOF)


{% endhighlight %}

</div>

</div>

### Using Hadoop Mappers and Reducers

Hadoop Mappers are semantically equivalent to Flink's [FlatMapFunctions](dataset_transformations.html#flatmap) and Hadoop Reducers are equivalent to Flink's [GroupReduceFunctions](dataset_transformations.html#groupreduce-on-grouped-dataset). Flink provides wrappers for implementations of Hadoop MapReduce's `Mapper` and `Reducer` interfaces, i.e., you can reuse your Hadoop Mappers and Reducers in regular Flink programs. At the moment, only the Mapper and Reduce interfaces of Hadoop's mapred API (`org.apache.hadoop.mapred`) are supported.

The wrappers take a `DataSet<Tuple2<KEYIN,VALUEIN>>` as input and produce a `DataSet<Tuple2<KEYOUT,VALUEOUT>>` as output where `KEYIN` and `KEYOUT` are the keys and `VALUEIN` and `VALUEOUT` are the values of the Hadoop key-value pairs that are processed by the Hadoop functions. For Reducers, Flink offers a wrapper for a GroupReduceFunction with (`HadoopReduceCombineFunction`) and without a Combiner (`HadoopReduceFunction`). The wrappers accept an optional `JobConf` object to configure the Hadoop Mapper or Reducer.

Flink's function wrappers are

- `org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction`,
- `org.apache.flink.hadoopcompatibility.mapred.HadoopReduceFunction`, and
- `org.apache.flink.hadoopcompatibility.mapred.HadoopReduceCombineFunction`.

and can be used as regular Flink [FlatMapFunctions](dataset_transformations.html#flatmap) or [GroupReduceFunctions](dataset_transformations.html#groupreduce-on-grouped-dataset).

The following example shows how to use Hadoop `Mapper` and `Reducer` functions.

{% highlight java %}
// Obtain data to process somehow.
DataSet<Tuple2<Text, LongWritable>> text = [...]

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
{% endhighlight %}

**Please note:** The Reducer wrapper works on groups as defined by Flink's [groupBy()](dataset_transformations.html#transformations-on-grouped-dataset) operation. It does not consider any custom partitioners, sort or grouping comparators you might have set in the `JobConf`.

### Complete Hadoop WordCount Example

The following example shows a complete WordCount implementation using Hadoop data types, Input- and OutputFormats, and Mapper and Reducer implementations.

{% highlight java %}
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
HadoopOutputFormat<Text, IntWritable> hadoopOF =
  new HadoopOutputFormat<Text, IntWritable>(
    new TextOutputFormat<Text, IntWritable>(), job
  );
hadoopOF.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
TextOutputFormat.setOutputPath(job, new Path(outputPath));

// Emit data using the Hadoop TextOutputFormat.
result.output(hadoopOF);

// Execute Program
env.execute("Hadoop WordCount");
{% endhighlight %}

{% top %}
