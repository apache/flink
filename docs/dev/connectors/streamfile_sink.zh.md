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

这个连接器提供了一个 Sink 来将分区文件写入到支持 [Flink `FileSystem`]({{ site.baseurl}}/zh/ops/filesystems/index.html) 接口的文件系统中。

由于在流处理中输入可能是无限的，所以流处理的文件 sink 会将数据写入到桶中。如何分桶是可以配置的，一种有效的默认
策略是基于时间的分桶，这种策略每个小时写入一个新的桶，这些桶各包含了无限输出流的一部分数据。

在一个桶内部，会进一步将输出基于滚动策略切分成更小的文件。这有助于防止桶文件变得过大。滚动策略也是可以配置的，默认
策略会根据文件大小和超时时间来滚动文件，超时时间是指没有新数据写入部分文件（part file）的时间。

`StreamingFileSink` 支持行编码格式和批量编码格式，比如 [Apache Parquet](http://parquet.apache.org)。

#### 使用行编码输出格式

只需要配置一个输出路径和一个 [Encoder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/Encoder.html)。
Encoder负责为每个文件的 `OutputStream` 序列化数据。

基本用法如下:


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

DataStream<String> input = ...;

final StreamingFileSink<String> sink = StreamingFileSink
	.forRowFormat(new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

val input: DataStream[String] = ...

val sink: StreamingFileSink[String] = StreamingFileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
    .build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

上面的代码创建了一个按小时分桶、按默认策略滚动的 sink。默认分桶器是
[DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html)
，默认滚动策略是
[DefaultRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/DefaultRollingPolicy.html)。
可以为 sink builder 自定义
[BucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/BucketAssigner.html)
和
[RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html)。
更多配置操作以及分桶器和滚动策略的工作机制和相互影响请参考：
[StreamingFileSink]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.html)。

#### 使用批量编码输出格式

上面的示例使用 `Encoder` 分别序列化每一个记录。除此之外，流式文件 sink 还支持批量编码的输出格式，比如 [Apache Parquet](http://parquet.apache.org)。
使用这种编码格式需要用 `StreamingFileSink.forBulkFormat()` 来代替 `StreamingFileSink.forRowFormat()` ，然后指定一个 `BulkWriter.Factory`。

[ParquetAvroWriters]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/avro/ParquetAvroWriters.html)
中包含了为各种类型创建 `BulkWriter.Factory` 的静态方法。

<div class="alert alert-info">
    <b>重要:</b> 批量编码格式只能和 `OnCheckpointRollingPolicy` 结合使用，每次做 checkpoint 时滚动文件。
</div>

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
