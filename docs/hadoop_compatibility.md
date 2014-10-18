---
title: "Hadoop Compatibility"
---

* This will be replaced by the TOC
{:toc}

Flink is compatible with many Apache Hadoop's MapReduce interfaces and allows to reuse a lot of code that was implemented for Hadoop MapReduce.

You can:

- use Hadoop's `Writable` [data types](programming_guide.html#data-types) in Flink programs.
- use any Hadoop `InputFormat` as a [DataSource](programming_guide.html#data-sources).
- use any Hadoop `OutputFormat` as a [DataSink](programming_guide.html#data-sinks).
- use a Hadoop `Mapper` as [FlatMapFunction](dataset_transformations.html#flatmap).
- use a Hadoop `Reducer` as [GroupReduceFunction](dataset_transformations.html#groupreduce-on-grouped-dataset).

This document shows how to use existing Hadoop MapReduce code with Flink.

### Project Configuration

The Hadoop Compatibility Layer is part of the `flink-addons` Maven module. All relevant classes are located in the `org.apache.flink.hadoopcompatibility` package. It includes separate packages and classes for the Hadoop `mapred` and `mapreduce` APIs.

Add the following dependency to your `pom.xml` to use the Hadoop Compatibility Layer.

~~~xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility</artifactId>
	<version>{{site.FLINK_VERSION_STABLE}}</version>
</dependency>
~~~

### Using Hadoop Data Types

Flink supports all Hadoop `Writable` and `WritableComparable` data types out-of-the-box. You do not need to include the Hadoop Compatibility dependency, if you only want to use your Hadoop data types. See the [Programming Guide](programming_guide.html#data-types) for more details.

### Using Hadoop InputFormats

Flink provides a compatibility wrapper for Hadoop `InputFormats`. Any class that implements `org.apache.hadoop.mapred.InputFormat` or extends `org.apache.hadoop.mapreduce.InputFormat` is supported. Thus, Flink can handle Hadoop built-in formats such as `TextInputFormat` as well as external formats such as Hive's `HCatInputFormat`. Data read from Hadoop InputFormats is converted into a `DataSet<Tuple2<KEY,VALUE>>` where `KEY` is the key and `VALUE` is the value of the original Hadoop key-value pair.

Flink's InputFormat wrappers are 

- `org.apache.flink.hadoopcompatibility.mapred.HadoopInputFormat` and 
- `org.apache.flink.hadoopcompatibility.mapreduce.HadoopInputFormat`

and can be used as regular Flink [InputFormats](programming_guide.html#data-sources).

The following example shows how to use Hadoop's `TextInputFormat`.

~~~java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
// Set up the Hadoop TextInputFormat.
Job job = Job.getInstance();
HadoopInputFormat<LongWritable, Text> hadoopIF = 
  // create the Flink wrapper.
  new HadoopInputFormat<LongWritable, Text>(
    // create the Hadoop InputFormat, specify key and value type, and job.
    new TextInputFormat(), LongWritable.class, Text.class, job
  );
TextInputFormat.addInputPath(job, new Path(inputPath));
		
// Read data using the Hadoop TextInputFormat.
DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopIF);

// Do something with the data.
[...]
~~~

### Using Hadoop OutputFormats

Flink provides a compatibility wrapper for Hadoop `OutputFormats`. Any class that implements `org.apache.hadoop.mapred.OutputFormat` or extends `org.apache.hadoop.mapreduce.OutputFormat` is supported. The OutputFormat wrapper expects its input data to be a `DataSet<Tuple2<KEY,VALUE>>` where `KEY` is the key and `VALUE` is the value of the Hadoop key-value pair that is processed by the Hadoop OutputFormat.

Flink's OUtputFormat wrappers are

- `org.apache.flink.hadoopcompatibility.mapred.HadoopOutputFormat` and 
- `org.apache.flink.hadoopcompatibility.mapreduce.HadoopOutputFormat`

and can be used as regular Flink [OutputFormats](programming_guide.html#data-sinks).

The following example shows how to use Hadoop's `TextOutputFormat`.

~~~java
// Obtain your result to emit.
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
result.output(hadoopOF)
      .setParallelism(1);
~~~

**Please note:** At the moment, Hadoop OutputFormats must be executed with a parallelism of 1 (DOP = 1). This limitation will be resolved soon.

### Using Hadoop Mappers and Reducers

Hadoop Mappers are semantically equivalent to Flink's [FlatMapFunctions](dataset_transformations.html#flatmap) and Hadoop Reducers are equivalent to Flink's [GroupReduceFunctions](dataset_transformations.html#groupreduce-on-grouped-dataset). Flink provides wrappers for implementations of Hadoop MapReduce's `Mapper` and `Reducer` interfaces, i.e., you can reuse your Hadoop Mappers and Reducers in regular Flink programs. At the moment, only the Mapper and Reduce interfaces of Hadoop's mapred API (`org.apache.hadoop.mapred`) are supported.

The wrappers take a `DataSet<Tuple2<KEYIN,VALUEIN>>` as input and produce a `DataSet<Tuple2<KEYOUT,VALUEOUT>>` as output where `KEYIN` and `KEYOUT` are the keys and `VALUEIN` and `VALUEOUT` are the values of the Hadoop key-value pairs that are processed by the Hadoop functions. For Reducers, Flink offers a wrapper for a GroupReduceFunction with (`HadoopReduceCombineFunction`) and without a Combiner (`HadoopReduceFunction`). The wrappers accept an optional `JobConf` object to configure the Hadoop Mapper or Reducer.

Flink's function wrappers are 

- `org.apache.flink.hadoopcompatibility.mapred.HadoopMapFunction`,
- `org.apache.flink.hadoopcompatibility.mapred.HadoopReduceFunction`, and
- `org.apache.flink.hadoopcompatibility.mapred.HadoopReduceCombineFunction`.

and can be used as regular Flink [FlatMapFunctions](dataset_transformations.html#flatmap) or [GroupReduceFunctions](dataset_transformations.html#groupreduce-on-grouped-dataset).

The following example shows how to use Hadoop `Mapper` and `Reducer` functions.

~~~java
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
~~~

**Please note:** The Reducer wrapper works on groups as defined by Flink's [groupBy()](dataset_transformations.html#transformations-on-grouped-dataset) operation. It does not consider any custom partitioners, sort or grouping comparators you might have set in the `JobConf`. 

### Complete Hadoop WordCount Example

The following example shows a complete WordCount implementation using Hadoop data types, Input- and OutputFormats, and Mapper and Reducer implementations.

~~~java
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
result.output(hadoopOF)
      .setParallelism(1);

// Execute Program
env.execute("Hadoop WordCount");
~~~
