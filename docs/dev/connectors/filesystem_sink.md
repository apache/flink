---
title: "HDFS Connector"
nav-title: Rolling File Sink
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

This connector provides a Sink that writes partitioned files to any filesystem supported by
Hadoop FileSystem. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-filesystem{{ site.scala_version_suffix }}</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here]({{site.baseurl}}/dev/linking.html)
for information about how to package the program with the libraries for
cluster execution.

#### Bucketing File Sink

The bucketing behaviour as well as the writing can be configured but we will get to that later.
This is how you can create a bucketing sink which by default, sinks to rolling files that are split by time:

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

The only required parameter is the base path where the buckets will be
stored. The sink can be further configured by specifying a custom bucketer, writer and batch size.

By default the bucketing sink will split by the current system time when elements arrive and will
use the datetime pattern `"yyyy-MM-dd--HH"` to name the buckets. This pattern is passed to
`SimpleDateFormat` with the current system time to form a bucket path. A new bucket will be created
whenever a new date is encountered. For example, if you have a pattern that contains minutes as the
finest granularity you will get a new bucket every minute. Each bucket is itself a directory that
contains several part files: each parallel instance of the sink will create its own part file and
when part files get too big the sink will also create a new part file next to the others. When a
bucket becomes inactive, the open part file will be flushed and closed. A bucket is regarded as
inactive when it hasn't been written to recently. By default, the sink checks for inactive buckets
every minute, and closes any buckets which haven't been written to for over a minute. This
behaviour can be configured with `setInactiveBucketCheckInterval()` and
`setInactiveBucketThreshold()` on a `BucketingSink`.

You can also specify a custom bucketer by using `setBucketer()` on a `BucketingSink`. If desired,
the bucketer can use a property of the element or tuple to determine the bucket directory.

The default writer is `StringWriter`. This will call `toString()` on the incoming elements
and write them to part files, separated by newline. To specify a custom writer use `setWriter()`
on a `BucketingSink`. If you want to write Hadoop SequenceFiles you can use the provided
`SequenceFileWriter` which can also be configured to use compression.

The last configuration option is the batch size. This specifies when a part file should be closed
and a new one started. (The default part file size is 384 MB).

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<IntWritable,Text>> input = ...;

BucketingSink<String> sink = new BucketingSink<String>("/base/path");
sink.setBucketer(new DateTimeBucketer<String>("yyyy-MM-dd--HHmm"));
sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[Tuple2[IntWritable, Text]] = ...

val sink = new BucketingSink[String]("/base/path")
sink.setBucketer(new DateTimeBucketer[String]("yyyy-MM-dd--HHmm"))
sink.setWriter(new SequenceFileWriter[IntWritable, Text]())
sink.setBatchSize(1024 * 1024 * 400) // this is 400 MB,

input.addSink(sink)

{% endhighlight %}
</div>
</div>

This will create a sink that writes to bucket files that follow this schema:

```
/base/path/{date-time}/part-{parallel-task}-{count}
```

Where `date-time` is the string that we get from the date/time format, `parallel-task` is the index
of the parallel sink instance and `count` is the running number of part files that where created
because of the batch size.

For in-depth information, please refer to the JavaDoc for
[BucketingSink](http://flink.apache.org/docs/latest/api/java/org/apache/flink/streaming/connectors/fs/bucketing/BucketingSink.html).
