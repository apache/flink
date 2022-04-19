---
title:  "Hadoop"
weight: 4
type: docs
aliases:
  - /zh/dev/connectors/formats/hadoop.html
  - /zh/apis/streaming/connectors/formats/hadoop.html

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

对 Hadoop 的支持位于 `flink-hadoop-compatibility` Maven 模块中。

将以下依赖添加到 `pom.xml` 中使用 hadoop

```xml
<dependency>
	<groupId>org.apache.flink</groupId>
	<artifactId>flink-hadoop-compatibility{{< scala_version >}}</artifactId>
	<version>{{< version >}}</version>
</dependency>
```

如果你想在本地运行你的 Flink 应用（例如在 IDE 中），你需要按照如下所示将 `hadoop-client` 依赖也添加到 `pom.xml`：

```xml
<dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-client</artifactId>
    <version>2.8.5</version>
    <scope>provided</scope>
</dependency>
```

## Using Hadoop InputFormats

在 Flink 中使用 Hadoop `InputFormats`，必须首先使用 `HadoopInputs` 工具类的 `readHadoopFile` 或 `createHadoopInput` 包装 Input Format。
前者用于从 `FileInputFormat` 派生的 Input Format，而后者必须用于通用的 Input Format。
生成的 `InputFormat` 可通过使用 `ExecutionEnvironmen#createInput` 创建数据源。

生成的 `DataStream` 包含 2 元组，其中第一个字段是键，第二个字段是从 Hadoop `InputFormat` 接收的值。

下面的示例展示了如何使用 Hadoop 的 `TextInputFormat`。

{{< tabs "baa59ec9-046e-4fe3-a2db-db5ee09d0635" >}}
{{< tab "Java" >}}

```java
StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<Tuple2<LongWritable, Text>> input =
    env.createInput(HadoopInputs.readHadoopFile(new TextInputFormat(),
                        LongWritable.class, Text.class, textPath));

// 对数据进行一些处理。
[...]
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment

val input: DataStream[(LongWritable, Text)] =
  env.createInput(HadoopInputs.readHadoopFile(
                    new TextInputFormat, classOf[LongWritable], classOf[Text], textPath))

// 对数据进行一些处理。
[...]
```

{{< /tab >}}
{{< /tabs >}}

## Using Hadoop OutputFormats

Flink 为 Hadoop `OutputFormats` 提供了一个兼容性包装器。支持任何实现 `org.apache.hadoop.mapred.OutputFormat` 或扩展 `org.apache.hadoop.mapreduce.OutputFormat` 的类。
`OutputFormat` 包装器期望其输入数据是包含键和值的 2-元组的 DataSet。这些将由 Hadoop `OutputFormat` 处理。

下面的示例展示了如何使用 Hadoop 的 `TextOutputFormat`。

{{< tabs "d4af1c52-0e4c-490c-8c35-e3d60b1b52ee" >}}
{{< tab "Java" >}}

```java
// 获取我们希望发送的结果
DataStream<Tuple2<Text, IntWritable>> hadoopResult = [...];

// 设置 the Hadoop TextOutputFormat。
HadoopOutputFormat<Text, IntWritable> hadoopOF =
  // 创建 Flink wrapper.
  new HadoopOutputFormat<Text, IntWritable>(
    // 设置 Hadoop OutputFormat 并指定 job。
    new TextOutputFormat<Text, IntWritable>(), job
  );
hadoopOF.getConfiguration().set("mapreduce.output.textoutputformat.separator", " ");
TextOutputFormat.setOutputPath(job, new Path(outputPath));

// 使用 Hadoop TextOutputFormat 发送数据。
hadoopResult.output(hadoopOF);
```

{{< /tab >}}
{{< tab "Scala" >}}

```scala
// 获取我们希望发送的结果
val hadoopResult: DataStream[(Text, IntWritable)] = [...]

val hadoopOF = new HadoopOutputFormat[Text,IntWritable](
  new TextOutputFormat[Text, IntWritable],
  new JobConf)

hadoopOF.getJobConf.set("mapred.textoutputformat.separator", " ")
FileOutputFormat.setOutputPath(hadoopOF.getJobConf, new Path(resultPath))

hadoopResult.output(hadoopOF)


```

{{< /tab >}}
{{< /tabs >}}

{{< top >}}
