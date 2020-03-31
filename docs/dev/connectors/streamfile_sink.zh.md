---
title: "Streaming File Sink"
nav-title: Streaming File Sink
nav-parent_id: connectors
nav-pos: 5
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

这个连接器提供了一个 Sink 来将分区文件写入到支持 [Flink `FileSystem`]({{ site.baseurl}}/zh/ops/filesystems/index.html) 接口的文件系统中。

为了处理无界的流数据，Streaming File Sink 会将数据写入到桶中。如何分桶是可以配置的，默认策略是基于时间的分桶，这种策略每个小时创建并写入一个新的桶，从而得到流数据在特定时间间隔内接收记录所对应的文件。

桶目录中包含多个实际输出数据的部分文件（part file），对于每一个接收桶数据的 Sink Subtask ，至少存在一个部分文件（part file）。额外的部分文件（part file）将根据滚动策略创建，滚动策略是可以配置的。默认的策略是根据文件大小和超时时间来滚动文件。超时时间指打开文件的最长持续时间，以及文件关闭前的最长非活动时间。

 <div class="alert alert-info">
     <b>重要:</b> 使用 StreamingFileSink 时需要启用 Checkpoint ，每次做 Checkpoint 时写入完成。如果 Checkpoint 被禁用，部分文件（part file）将永远处于 'in-progress' 或 'pending' 状态，下游系统无法安全地读取。
 </div>

 <img src="{{ site.baseurl }}/fig/streamfilesink_bucketing.png" class="center" style="width: 100%;" />

### 桶分配

桶分配逻辑定义了如何将数据结构化为基本输出目录中的子目录

行格式和批量格式都使用 [DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html) 作为默认的分配器。
默认情况下，DateTimeBucketAssigner 基于系统默认时区每小时创建一个桶，格式如下： `yyyy-MM-dd--HH` 。日期格式（即桶的大小）和时区都可以手动配置。

我们可以在格式构建器上调用 `.withBucketAssigner(assigner)` 来自定义 [BucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/BucketAssigner.html) 。

Flink 有两个内置的 BucketAssigners ：

 - [DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html) ：默认基于时间的分配器
 - [BasePathBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/BasePathBucketAssigner.html) ：将所有部分文件（part file）存储在基本路径中的分配器（单个全局桶）

### 滚动策略

滚动策略 [RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html) 定义了指定的文件在何时关闭（closed）并将其变为 Pending 状态，随后变为 Finished 状态。处于 Pending 状态的文件会在下一次 Checkpoint 时变为 Finished 状态，通过设置 Checkpoint 间隔时间，可以控制部分文件（part file）对下游读取者可用的速度、大小和数量。

Flink 有两个内置的滚动策略：

 - [DefaultRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/DefaultRollingPolicy.html)
 - [OnCheckpointRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/OnCheckpointRollingPolicy.html)

### 部分文件（part file） 生命周期

为了在下游系统中使用 StreamingFileSink 的输出，我们需要了解输出文件的命名规则和生命周期。

部分文件（part file）可以处于以下三种状态之一：
 1. **In-progress** ：当前文件正在写入中
 2. **Pending** ：当处于 In-progress 状态的文件关闭（closed）了，就变为 Pending 状态
 3. **Finished** ：在成功的 Checkpoint 后，Pending 状态将变为 Finished 状态

处于 Finished 状态的文件以后不会被修改，可以被下游系统安全地读取。Finished 状态的文件只能通过它们的命名来区分。

文件命名方案：
 - **In-progress / Pending**：`part-subtaskIndex-partFileIndex.inprogress.uid`
 - **Finished** ：`part-subtaskIndex-partFileIndex`

对于任何给定的 Subtask ，部分文件（part file）的索引按创建顺序严格递增。但是，这些索引并不总是连续的。当作业重启时，所有 Subtask 部分文件（part file）索引的值将是 `max part index + 1` 。

对于每个活动的桶，Writer 在任何时候都只有一个处于 In-progress 状态的部分文件（part file），但是可能有几个 Penging 和 Finished 状态的部分文件（part file）。

**部分文件（part file）例子**

为了更好地理解这些文件的生命周期，让我们来看一个包含 2 个 Sink Subtask 的简单例子：

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    └── part-1-0.inprogress.ea65a428-a1d0-4a0b-bbc5-7a436a75e575
```

当部分文件 `part-1-0` 被滚动（假设它变得太大了）时，它将成为 Pending 状态，但是它还没有被重命名。然后 Sink 会创建一个新的部分文件： `part-1-1`：

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-1-0.inprogress.ea65a428-a1d0-4a0b-bbc5-7a436a75e575
    └── part-1-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

 `part-1-0` 现在处于 Pending 状态等待完成，在下一次成功的 Checkpoint 后，它会变成 Finished 状态：

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-1-0
    └── part-1-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

根据分桶策略创建新的桶，但是这并不会影响当前处于 In-progress 状态的文件：

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-1-0
    └── part-1-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
└── 2019-08-25--13
    └── part-0-2.inprogress.2b475fec-1482-4dea-9946-eb4353b475f1
```

因为分桶策略基于每条记录进行评估，所以旧桶仍然可以接受新的记录。

## 文件格式

 `StreamingFileSink` 支持行编码格式和批量编码格式，比如 [Apache Parquet](http://parquet.apache.org) 。
这两种变体随附了各自的构建器，可以使用以下静态方法创建：

 - Row-encoded sink: `StreamingFileSink.forRowFormat(basePath, rowEncoder)`
 - Bulk-encoded sink: `StreamingFileSink.forBulkFormat(basePath, bulkWriterFactory)`

创建行或批量编码的 Sink 时，我们需要指定存储桶的基本路径和数据的编码逻辑。

更多配置操作以及不同数据格式的实现请参考 [StreamingFileSink]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.html) 

### 行编码格式

行编码格式需要指定一个 [Encoder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/Encoder.html) 。Encoder 负责为每个处于 In-progress 状态文件的`OutputStream` 序列化数据。

除了桶分配器之外，[RowFormatBuilder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.RowFormatBuilder.html)  还允许用户指定：

 - Custom [RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html) ：自定义滚动策略以覆盖默认的 DefaultRollingPolicy
 - bucketCheckInterval （默认为1分钟）：毫秒间隔，用于基于时间的滚动策略。

字符串元素写入示例：


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

DataStream<String> input = ...;

final StreamingFileSink<String> sink = StreamingFileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
    .withRollingPolicy(
        DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            .withMaxPartSize(1024 * 1024 * 1024)
            .build())
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy

val input: DataStream[String] = ...

val sink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
    .withRollingPolicy(
        DefaultRollingPolicy.builder()
            .withRolloverInterval(TimeUnit.MINUTES.toMillis(15))
            .withInactivityInterval(TimeUnit.MINUTES.toMillis(5))
            .withMaxPartSize(1024 * 1024 * 1024)
            .build())
    .build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

这个例子创建了一个简单的 Sink ，将记录分配给默认的一小时时间桶。它还指定了一个滚动策略，该策略在以下三种情况下滚动处于 In-progress 状态的部分文件（part file）：

 - 它至少包含 15 分钟的数据
 - 最近 5 分钟没有收到新的记录
 - 文件大小达到 1GB （写入最后一条记录后）

### 批量编码格式

批量编码 Sink 的创建与行编码 Sink 相似，不过在这里我们不是指定编码器  `Encoder` 而是指定 [BulkWriter.Factory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/BulkWriter.Factory.html) 。
`BulkWriter` 定义了如何添加、刷新元素，以及如何批量编码。

Flink 有两个内置的 BulkWriter Factory ：

 - [ParquetWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/ParquetWriterFactory.html)
 - [SequenceFileWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/sequencefile/SequenceFileWriterFactory.html)
 - [CompressWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/compress/CompressWriterFactory.html)

#### Parquet 格式

Flink 包含为不同 Avro 类型，创建 ParquetWriterFactory 的便捷方法，更多信息请参考 [ParquetAvroWriters]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/avro/ParquetAvroWriters.html) 。

要编写其他 Parquet 兼容的数据格式，用户需要创建 ParquetWriterFactory 并实现 [ParquetBuilder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/ParquetBuilder.html) 接口。

在应用中使用 Parquet 批量编码器，你需要添加以下依赖：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-parquet{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

这个例子使用 StreamingFileSink 将 Avro 数据写入 Parquet 格式：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters;
import org.apache.avro.Schema;


Schema schema = ...;
DataStream<GenericRecord> stream = ...;

final StreamingFileSink<GenericRecord> sink = StreamingFileSink
	.forBulkFormat(outputBasePath, ParquetAvroWriters.forGenericRecord(schema))
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.formats.parquet.avro.ParquetAvroWriters
import org.apache.avro.Schema

val schema: Schema = ...
val input: DataStream[GenericRecord] = ...

val sink: StreamingFileSink[GenericRecord] = StreamingFileSink
    .forBulkFormat(outputBasePath, ParquetAvroWriters.forGenericRecord(schema))
    .build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

#### Hadoop SequenceFile 格式

在应用中使用 SequenceFile 批量编码器，你需要添加以下依赖：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-sequence-file</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

简单的 SequenceFile 写入示例：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


DataStream<Tuple2<LongWritable, Text>> input = ...;
Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
final StreamingFileSink<Tuple2<LongWritable, Text>> sink = StreamingFileSink
  .forBulkFormat(
    outputBasePath,
    new SequenceFileWriterFactory<>(hadoopConf, LongWritable.class, Text.class))
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text;

val input: DataStream[(LongWritable, Text)] = ...
val hadoopConf: Configuration = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration())
val sink: StreamingFileSink[(LongWritable, Text)] = StreamingFileSink
  .forBulkFormat(
    outputBasePath,
    new SequenceFileWriterFactory(hadoopConf, LongWritable.class, Text.class))
	.build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

SequenceFileWriterFactory 支持附加构造函数参数指定压缩设置。

####  关于S3的重要内容

<span class="label label-danger">重要提示 1</span>: 对于 S3，`StreamingFileSink`  只支持基于 [Hadoop](https://hadoop.apache.org/) 
的文件系统实现，不支持基于 [Presto](https://prestodb.io/) 的实现。如果想使用 `StreamingFileSink` 向 S3 写入数据并且将 
checkpoint 放在基于 Presto 的文件系统，建议明确指定 *"s3a://"* （for Hadoop）作为sink的目标路径方案，并且为 checkpoint 路径明确指定 *"s3p://"* （for Presto）。
如果 Sink 和 checkpoint 都使用 *"s3://"* 路径的话，可能会导致不可预知的行为，因为双方的实现都在“监听”这个路径。

<span class="label label-danger">重要提示 2</span>: `StreamingFileSink` 使用 S3 的 [Multi-part Upload](https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html)
（后续使用MPU代替）特性可以保证精确一次的语义。这个特性支持以独立的块（因此被称为"multi-part"）模式上传文件，当 MPU 的所有部分文件
成功上传之后，可以合并成原始文件。对于失效的 MPUs，S3 提供了一个基于桶生命周期的规则，用户可以用这个规则来丢弃在指定时间内未完成的MPU。
如果在一些部分文件还未上传时触发 savepoint，并且这个规则设置的比较严格，这意味着相关的 MPU在作业重启之前可能会超时。后续的部分文件没
有写入到 savepoint, 那么在 Flink 作业从 savepoint 恢复时，会因为拿不到缺失的部分文件，导致任务失败并抛出异常。

{% top %}
