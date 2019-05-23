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
supported by the [Flink `FileSystem` abstraction]({{ site.baseurl}}/ops/filesystems.html).

Since in streaming the input is potentially infinite, the streaming file sink writes data
into buckets. The bucketing behaviour is configurable but a useful default is time-based
bucketing where we start writing a new bucket every hour and thus get
individual files that each contain a part of the infinite output stream.

Within a bucket, we further split the output into smaller part files based on a
rolling policy. This is useful to prevent individual bucket files from getting
too big. This is also configurable but the default policy rolls files based on
file size and a timeout, *i.e* if no new data was written to a part file. 

The `StreamingFileSink` supports both row-wise encoding formats and
bulk-encoding formats, such as [Apache Parquet](http://parquet.apache.org).

#### Using Row-encoded Output Formats

The only required configuration are the base path where we want to output our
data and an
[Encoder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/Encoder.html)
that is used for serializing records to the `OutputStream` for each file.

Basic usage thus looks like this:


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

This will create a streaming sink that creates hourly buckets and uses a
default rolling policy. The default bucket assigner is
[DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html)
and the default rolling policy is
[DefaultRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/DefaultRollingPolicy.html).
You can specify a custom
[BucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/BucketAssigner.html)
and
[RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html)
on the sink builder. Please check out the JavaDoc for
[StreamingFileSink]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.html)
for more configuration options and more documentation about the workings and
interactions of bucket assigners and rolling policies.

#### Using Bulk-encoded Output Formats

In the above example we used an `Encoder` that can encode or serialize each
record individually. The streaming file sink also supports bulk-encoded output
formats such as [Apache Parquet](http://parquet.apache.org). To use these,
instead of `StreamingFileSink.forRowFormat()` you would use
`StreamingFileSink.forBulkFormat()` and specify a `BulkWriter.Factory`.

[ParquetAvroWriters]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/avro/ParquetAvroWriters.html)
has static methods for creating a `BulkWriter.Factory` for various types.

<div class="alert alert-info">
    <b>IMPORTANT:</b> Bulk-encoding formats can only be combined with the
    `OnCheckpointRollingPolicy`, which rolls the in-progress part file on
    every checkpoint.
</div>

#### Important Considerations for S3

<span class="label label-danger">Important Note 1</span>: For S3, the `StreamingFileSink` 
supports only the [Hadoop-based](https://hadoop.apache.org/) FileSystem implementation, not
the implementation based on [Presto](https://prestodb.io/). In case your job uses the 
`StreamingFileSink` to write to S3 but you want to use the Presto-based one for checkpointing,
it is advised to use explicitly *"s3a://"* (for Hadoop) as the scheme for the target path of
the sink and *"s3p://"* for checkpointing (for Presto). Using *"s3://"* for both the sink
and checkpointing may lead to unpredictable behavior, as both implementations "listen" to that scheme.

<span class="label label-danger">Important Note 2</span>: To guarantee exactly-once semantics while
being efficient, the `StreamingFileSink` uses the [Multi-part Upload](https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html)
feature of S3 (MPU from now on). This feature allows to upload files in independent chunks (thus the "multi-part")
which can be combined into the original file when all the parts of the MPU are successfully uploaded. 
For inactive MPUs, S3 supports a bucket lifecycle rule that the user can use to abort multipart uploads 
that don't complete within a specified number of days after being initiated. This implies that if you set this rule 
aggressively and take a savepoint with some part-files being not fully uploaded, their associated MPUs may time-out 
before the job is restarted. This will result in your job not being able to restore from that savepoint as the
pending part-files are no longer there and Flink will fail with an exception as it tries to fetch them and fails.

{% top %}
