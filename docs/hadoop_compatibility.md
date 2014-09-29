---
title: "Hadoop I/O Compatibility"
---

Flink not only supports types that implement Apache Hadoop's `Writable` interface by default, but also provides a compatibility 
layer that allows for using any class extending `org.apache.hadoop.mapred(uce).InputFormat` as a Flink `InputFormat` as well as any 
class extending `org.apache.hadoop.mapred(uce).OutputFormat` as a Flink `OutputFormat`. 

Thus, Flink can handle Hadoop-related formats from the common `TextInputFormat` up to third-party components such as Hive through HCatalog's `HCatInputFormat`. Flink supports both the formats which use the old `org.apache.hadoop.mapred` API as well as the new `org.apache.hadoop.mapreduce` API.

This document explains how to configure your Maven project correctly and shows an example.

### Project Configuration

The Hadoop Compatibility Layer is part of the *addons* Maven project. All relevant classes are located in the `org.apache.flink.hadoopcompatibility` package. The package includes separate packages and classes for the Hadoop `mapred` and `mapreduce` API.

Add the following dependency to your `pom.xml` to use the Hadoop Compatibility Layer.

~~~xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility</artifactId>
	<version>{{site.FLINK_VERSION_STABLE}}</version>
</dependency>
~~~

### Examples

The following example shows how to read a file from Hadoop's `TextInputFormat`, count the words with Flink and output the result with Hadoop's `TextOutputFormat`.

Flink's `HadoopInputFormat` is initiated in line 5 and acts as a wrapper for the Hadoop `TextInputFormat`. An instance of `TextInputFormat` and the corresponding return types for key and value together with a Hadoop `Job` definition must be passed to the constructor of the `HadoopInputFormat`.

Take care that you choose the right classes since `HadoopInputFormat` and `HadoopOutputFormat` exist in both packages for Hadoop's `mapred` and `mapreduce` API. For example, the `HadoopInputFormat` for the `mapred` API  takes a Hadoop `JobConf` instance as parameter instead of `Job` instance.

Flink's `HadoopOutputFormat` is initiated in a similar way with an instance of Hadoop's `TextOutputFormat` and the previously used Hadoop `Job` definition.

Additional Hadoop properties can be set by calling the formats `getConfiguration()` (for `mapreduce` API) or `getJobConf()` (for `mapred` API) method.

~~~java
ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
		
// Set up the Hadoop Input Format
Job job = Job.getInstance();
HadoopInputFormat<LongWritable, Text> hadoopInputFormat = 
    new HadoopInputFormat<LongWritable, Text>(
        new TextInputFormat(), LongWritable.class, Text.class, job
    );
TextInputFormat.addInputPath(job, new Path(inputPath));
		
// Create a Flink job with it
DataSet<Tuple2<LongWritable, Text>> text = env.createInput(hadoopInputFormat);
		
// Tokenize the line and convert from Writable "Text" to String for better handling
DataSet<Tuple2<String, Integer>> words = text.flatMap(new Tokenizer());
		
// Sum up the words
DataSet<Tuple2<String, Integer>> result = words.groupBy(0).aggregate(Aggregations.SUM, 1);
		
// Convert String back to Writable "Text" for use with Hadoop Output Format
DataSet<Tuple2<Text, IntWritable>> hadoopResult = result.map(new HadoopDatatypeMapper());
		
// Set up Hadoop Output Format
HadoopOutputFormat<Text, IntWritable> hadoopOutputFormat = 
    new HadoopOutputFormat<Text, IntWritable>(
        new TextOutputFormat<Text, IntWritable>(), job
    );
hadoopOutputFormat.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
TextOutputFormat.setOutputPath(job, new Path(outputPath));
		
// Output & Execute
hadoopResult.output(hadoopOutputFormat);
env.execute("Word Count");
~~~

A full running example can be found in `org.apache.flink.hadoopcompatibility.mapred(uce).example.WordCount`.
