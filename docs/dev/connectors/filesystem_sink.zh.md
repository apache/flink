---
title: "Hadoop FileSystem 连接器"
nav-title: Hadoop FileSystem
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

<div class="alert alert-info" markdown="span">
`BucketingSink` 从 **Flink 1.9** 开始已经被废弃，并会在后续的版本中删除。请使用
[__StreamingFileSink__]({{site.baseurl}}/zh/dev/connectors/streamfile_sink.html)。
</div>

这个连接器可以向所有 [Hadoop FileSystem](http://hadoop.apache.org) 支持的文件系统写入分区文件。
使用前，需要在工程里添加下面的依赖：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-filesystem{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

注意连接器目前还不是二进制发行版的一部分，添加依赖、打包配置以及集群运行信息请参考 [这里]({{site.baseurl}}/zh/getting-started/project-setup/dependencies.html)。

#### 分桶文件 Sink

关于分桶的配置我们后面会有讲述，这里先创建一个分桶 sink，默认情况下这个 sink 会将数据写入到按照时间切分的滚动文件中：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

input.addSink(new BucketingSink<String>("/base/path"));

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

input.addSink(new BucketingSink[String]("/base/path"))

{% endhighlight %}
</div>
</div>

初始化时只需要一个参数，这个参数表示分桶文件存储的路径。分桶 sink 可以通过指定自定义的 bucketer、 writer 和 batch 值进一步配置。

默认情况下，当数据到来时，分桶 sink 会按照系统时间对数据进行切分，并以 `"yyyy-MM-dd--HH"` 的时间格式给每个桶命名。然后 
`DateTimeFormatter` 按照这个时间格式将当前系统时间以 JVM 默认时区转换成分桶的路径。用户可以自定义时区来生成
分桶的路径。每遇到一个新的日期都会产生一个新的桶。例如，如果时间的格式以分钟为粒度，那么每分钟都会产生一个桶。每个桶都是一个目录，
目录下包含了几个部分文件（part files）：每个 sink 的并发实例都会创建一个属于自己的部分文件，当这些文件太大的时候，sink 会产生新的部分文件。
当一个桶不再活跃时，打开的部分文件会刷盘并且关闭。如果一个桶最近一段时间都没有写入，那么这个桶被认为是不活跃的。sink 默认会每分钟
检查不活跃的桶、关闭那些超过一分钟没有写入的桶。这些行为可以通过 `BucketingSink` 的 `setInactiveBucketCheckInterval()` 
和 `setInactiveBucketThreshold()` 进行设置。

可以调用`BucketingSink` 的 `setBucketer()` 方法指定自定义的 bucketer，如果需要的话，也可以使用一个元素或者元组属性来决定桶的路径。

默认的 writer 是 `StringWriter`。数据到达时，通过 `toString()` 方法得到内容，内容以换行符分隔，`StringWriter` 将数据
内容写入部分文件。可以通过 `BucketingSink` 的 `setWriter()` 指定自定义的 writer。`SequenceFileWriter` 支持写入 Hadoop
SequenceFiles，并且可以配置是否开启压缩。

关闭部分文件和打开新部分文件的时机可以通过两个配置来确定：
 
* 设置文件大小（默认文件大小是384MB）
* 设置文件滚动周期，单位是毫秒（默认滚动周期是 `Long.MAX_VALUE`）

当上述两个条件中的任意一个被满足，都会生成一个新的部分文件。

示例:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<IntWritable,Text>> input = ...;

BucketingSink<Tuple2<IntWritable,Text>> sink = new BucketingSink<Tuple2<IntWritable,Text>>("/base/path");
sink.setBucketer(new DateTimeBucketer<>("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")));
sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,
sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
// the SequenceFileWriter only works with Flink Tuples
import org.apache.flink.api.java.tuple.Tuple2
val input: DataStream[Tuple2[A, B]] = ... 

val sink = new BucketingSink[Tuple2[IntWritable, Text]]("/base/path")
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm", ZoneId.of("America/Los_Angeles")))
sink.setWriter(new SequenceFileWriter[IntWritable, Text])
sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,
sink.setBatchRolloverInterval(20 * 60 * 1000); // this is 20 mins

input.addSink(sink)

{% endhighlight %}
</div>
</div>

上述代码会创建一个 sink，这个 sink 按下面的模式写入桶文件：

{% highlight plain %}
/base/path/{date-time}/part-{parallel-task}-{count}
{% endhighlight %}

`date-time` 是我们从日期/时间格式获得的字符串，`parallel-task` 是 sink 并发实例的索引，`count` 是因文件大小或者滚动周期而产生的
文件的编号。

更多信息，请参考 [BucketingSink](https://flink.apache.org/docs/latest/api/java/org/apache/flink/streaming/connectors/fs/bucketing/BucketingSink.html)。

{% top %}
