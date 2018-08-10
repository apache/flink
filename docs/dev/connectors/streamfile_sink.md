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

This connector provides a Sink that writes partitioned files to filesystems
supported by the Flink `FileSystem` abstraction. Since in streaming the input
is potentially infinite, the streaming file sink writes data into buckets. The
bucketing behaviour is configurable but a useful default is time-based
bucketing where we start writing a new bucket every hour and thus get
individual files that each contain a part of the infinite output stream.

Within a bucket, we further split the output into smaller part files based on a
rolling policy. This is useful to prevent individual bucket files from getting
too big. This is also configurable but the default policy rolls files based on
file size and a timeout, i.e if no new data was written to a part file. 

#### Usage

The only required configuration are the base path were we want to output our
data and an
[Encoder]({{ site.baseurl }}/api/java/org/apache/flink/api/common/serialization/Encoder.html)
that is used for serializing records to the `OutputStream` for each file.

Basic usage thus looks like this:


<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;

DataStream<String> input = ...;

final StreamingFileSink<String> sink = StreamingFileSink
	.forRowFormat(new Path(outputPath), (Encoder<String>) (element, stream) -> {
		PrintStream out = new PrintStream(stream);
		out.println(element.f1);
	})
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.api.common.serialization.Encoder
import org.apache.flink.core.fs.Path
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink

val input: DataStream[String] = ...

final StreamingFileSink[String] sink = StreamingFileSink
	.forRowFormat(new Path(outputPath), (element, stream) => {
		val out = new PrintStream(stream)
		out.println(element.f1)
	})
	.build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

This will create a streaming sink that creates hourly buckets and uses a
default rolling policy. The default bucket assigner is
[DateTimeBucketAssigner]({{ site.baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html)
and the default rolling policy is
[DefaultRollingPolicy]({{ site.baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/DefaultRollingPolicy.html).
You can specify a custom
[BucketAssigner]({{ site.baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/BucketAssigner.html)
and
[RollingPolicy]({{ site.baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html)
on the sink builder. Please check out the JavaDoc for
[StreamingFileSink]({{ site.baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.html)
for more configuration options and more documentation about the workings and
interactions of bucket assigners and rolling policies.

#### Using Bulk-encoded Output Formats

In the above example we used an `Encoder` that can encode or serialize each
record individually. The streaming file sink also supports bulk-encoded output
formats such as [Apache Parquet](http://parquet.apache.org). To use these,
instead of `StreamingFileSink.forRowFormat()` you would use
`StreamingFileSink.forBulkFormat()` and specify a `BulkWriter.Factory`.

[ParquetAvroWriters]({{ site.baseurl }}/api/java/org/apache/flink/formats/parquet/avro/ParquetAvroWriters.html)
has static methods for creating a `BulkWriter.Factory` for various types.

<div class="alert alert-info">
  <strong>Note:</strong> With Bulk Writers, only the
  <code>OnCheckpointRollingPolicy</code>, which rolls the part file on every
  checkpoint, is supported.
</div>

{% top %}
