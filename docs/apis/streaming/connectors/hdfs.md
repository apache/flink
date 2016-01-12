---
title: "HDFS Connector"

# Sub-level navigation
sub-nav-group: streaming
sub-nav-parent: connectors
sub-nav-pos: 3
sub-nav-title: HDFS
---

This connector provides a Sink that writes rolling files to any filesystem supported by
Hadoop FileSystem. To use this connector, add the
following dependency to your project:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-filesystem</artifactId>
  <version>{{site.version}}</version>
</dependency>
{% endhighlight %}

Note that the streaming connectors are currently not part of the binary
distribution. See
[here](cluster_execution.html#linking-with-modules-not-contained-in-the-binary-distribution)
for information about how to package the program with the libraries for
cluster execution.

#### Rolling File Sink

The rolling behaviour as well as the writing can be configured but we will get to that later.
This is how you can create a default rolling sink:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> input = ...;

input.addSink(new RollingSink<String>("/base/path"));

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[String] = ...

input.addSink(new RollingSink("/base/path"))

{% endhighlight %}
</div>
</div>

The only required parameter is the base path where the rolling files (buckets) will be
stored. The sink can be configured by specifying a custom bucketer, writer and batch size.

By default the rolling sink will use the pattern `"yyyy-MM-dd--HH"` to name the rolling buckets.
This pattern is passed to `SimpleDateFormat` with the current system time to form a bucket path. A
new bucket will be created whenever the bucket path changes. For example, if you have a pattern
that contains minutes as the finest granularity you will get a new bucket every minute.
Each bucket is itself a directory that contains several part files: Each parallel instance
of the sink will create its own part file and when part files get too big the sink will also
create a new part file next to the others. To specify a custom bucketer use `setBucketer()`
on a `RollingSink`.

The default writer is `StringWriter`. This will call `toString()` on the incoming elements
and write them to part files, separated by newline. To specify a custom writer use `setWriter()`
on a `RollingSink`. If you want to write Hadoop SequenceFiles you can use the provided
`SequenceFileWriter` which can also be configured to use compression.

The last configuration option is the batch size. This specifies when a part file should be closed
and a new one started. (The default part file size is 384 MB).

Example:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<Tuple2<IntWritable,Text>> input = ...;

RollingSink sink = new RollingSink<String>("/base/path");
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"));
sink.setWriter(new SequenceFileWriter<IntWritable, Text>());
sink.setBatchSize(1024 * 1024 * 400); // this is 400 MB,

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val input: DataStream[Tuple2[IntWritable, Text]] = ...

val sink = new RollingSink[String]("/base/path")
sink.setBucketer(new DateTimeBucketer("yyyy-MM-dd--HHmm"))
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
[RollingSink](http://flink.apache.org/docs/latest/api/java/org/apache/flink/streaming/connectors/fs/RollingSink.html).
