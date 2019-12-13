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

This connector provides a Sink that writes partitioned files to filesystems
supported by the [Flink `FileSystem` abstraction]({{ site.baseurl}}/ops/filesystems/index.html).

In order to handle unbounded data streams, the streaming file sink writes incoming data
into buckets. The bucketing behaviour is fully configurable with a default time-based
bucketing where we start writing a new bucket every hour and thus get files that correspond to
records received during certain time intervals from the stream.

The bucket directories themselves contain several part files with the actual output data, with at least
one for each subtask of the sink that has received data for the bucket. Additional part files will be created according to the configurable
rolling policy. The default policy rolls files based on size, a timeout that specifies the maximum duration for which a file can be open, and a maximum inactivity timeout after which the file is closed.

 <div class="alert alert-info">
     <b>IMPORTANT:</b> Checkpointing needs to be enabled when using the StreamingFileSink. Part files can only be finalized
     on successful checkpoints. If checkpointing is disabled part files will forever stay in `in-progress` or `pending` state
     and cannot be safely read by downstream systems.
 </div>

 <img src="{{ site.baseurl }}/fig/streamfilesink_bucketing.png" class="center" style="width: 100%;" />

### Bucket Assignment

The bucketing logic defines how the data will be structured into subdirectories inside the base output directory.

Both row and bulk formats use the [DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html) as the default assigner.
By default the DateTimeBucketAssigner creates hourly buckets based on the system default timezone
with the following format: `yyyy-MM-dd--HH`. Both the date format (i.e. bucket size) and timezone can be
configured manually.

We can specify a custom [BucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/BucketAssigner.html) by calling `.withBucketAssigner(assigner)` on the format builders.

Flink comes with two built in BucketAssigners:

 - [DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html) : Default time based assigner
 - [BasePathBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/BasePathBucketAssigner.html) : Assigner that stores all part files in the base path (single global bucket)

### Rolling Policy

The [RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html) defines when a given in-progress part file will be closed and moved to the pending and later to finished state.
In combination with the checkpointing interval (pending files become finished on the next checkpoint) this controls how quickly
part files become available for downstream readers and also the size and number of these parts.

Flink comes with two built-in RollingPolicies:

 - [DefaultRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/DefaultRollingPolicy.html)
 - [OnCheckpointRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/OnCheckpointRollingPolicy.html)

### Part file lifecycle

In order to use the output of the StreamingFileSink in downstream systems, we need to understand the naming and lifecycle of the output files produced.

Part files can be in one of three states:
 1. **In-progress** : The part file that is currently being written to is in-progress
 2. **Pending** : Once a part file is closed for writing it becomes pending
 3. **Finished** : On successful checkpoints pending files become finished

Only finished files are safe to read by downstream systems as those are guaranteed to not be modified later. Finished files can be distinguished by their naming scheme only.

File naming schemes:
 - **In-progress / Pending**: `part-subtaskIndex-partFileIndex.inprogress.uid`
 - **Finished:** `part-subtaskIndex-partFileIndex`

Part file indexes are strictly increasing for any given subtask (in the order they were created). However these indexes are not always sequential. When the job restarts, the next part index for all subtask will be the `max part index + 1`.

Each writer subtask will have a single in-progress part file at any given time for every active bucket, but there can be several pending and finished files.

**Part file example**

To better understand the lifecycle of these files let's look at a simple example with 2 sink subtasks:

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    └── part-1-0.inprogress.ea65a428-a1d0-4a0b-bbc5-7a436a75e575
```

When the part file `part-1-0` is rolled (let's say it becomes too large), it becomes pending but it is not renamed. The sink then opens a new part file: `part-1-1`:

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-1-0.inprogress.ea65a428-a1d0-4a0b-bbc5-7a436a75e575
    └── part-1-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

As `part-1-0` is now pending completion, after the next successful checkpoint, it is finalized:

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-1-0
    └── part-1-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

New buckets are created as dictated by the bucketing policy, and this doesn't affect currently in-progress files:

```
└── 2019-08-25--12
    ├── part-0-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-1-0
    └── part-1-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
└── 2019-08-25--13
    └── part-0-2.inprogress.2b475fec-1482-4dea-9946-eb4353b475f1
```

Old buckets can still receive new records as the bucketing policy is evaluated on a per-record basis.

### Part file configuration

The filenames of the part files could be defined using `OutputFileConfig`, this configuration contain a part prefix and part suffix, 
that will be used with the parallel subtask index of the sink and a rolling counter. 
For example for a prefix "prefix" and a suffix ".ext" the file create:

```
└── 2019-08-25--12
    ├── prefix-0-0.ext
    ├── prefix-0-1.ext.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── prefix-1-0.ext
    └── prefix-1-1.ext.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

## File Formats

The `StreamingFileSink` supports both row-wise and bulk encoding formats, such as [Apache Parquet](http://parquet.apache.org).
These two variants come with their respective builders that can be created with the following static methods:

 - Row-encoded sink: `StreamingFileSink.forRowFormat(basePath, rowEncoder)`
 - Bulk-encoded sink: `StreamingFileSink.forBulkFormat(basePath, bulkWriterFactory)`

When creating either a row or a bulk encoded sink we have to specify the base path where the buckets will be
stored and the encoding logic for our data.

Please check out the JavaDoc for [StreamingFileSink]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.html) for all the configuration options
and more documentation about the implementation of the different data formats.

### Row-encoded Formats

Row-encoded formats need to specify an [Encoder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/Encoder.html) that is used for serializing individual rows to the `OutputStream` of the in-progress part files.

In addition to the bucket assigner the [RowFormatBuilder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.RowFormatBuilder.html) allows the user to specify:

 - Custom [RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html) : Rolling polciy to override the DefaultRollingPolicy
 - bucketCheckInterval (default = 1 min) : Millisecond interval for checking time based rolling policies

Basic usage for writing String elements thus looks like this:


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

This example creates a simple sink that assigns records to the default one hour time buckets. It also specifies
a rolling policy that rolls the in-progress part file on either of the following 3 conditions:

 - It contains at least 15 minutes worth of data
 - It hasn't received new records for the last 5 minutes
 - The file size reached 1 GB (after writing the last record)

### Bulk-encoded Formats

Bulk-encoded sinks are created similarly to the row-encoded ones but here instead of
specifying an `Encoder` we have to specify [BulkWriter.Factory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/BulkWriter.Factory.html).
The `BulkWriter` logic defines how new elements added, flushed and how the bulk of records
are finalized for further encoding purposes.

Flink comes with three built-in BulkWriter factories:

 - [ParquetWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/ParquetWriterFactory.html)
 - [SequenceFileWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/sequencefile/SequenceFileWriterFactory.html)
 - [CompressWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/compress/CompressWriterFactory.html)

#### Parquet format

Flink contains built in convenience methods for creating Parquet writer factories for Avro data. These methods
and their associated documentation can be found in the [ParquetAvroWriters]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/avro/ParquetAvroWriters.html) class.

For writing to other Parquet compatible data formats, users need to create the ParquetWriterFactory with a custom implementation of the [ParquetBuilder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/ParquetBuilder.html) interface.

To use the Parquet bulk encoder in your application you need to add the following dependency:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-parquet{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

A StreamingFileSink that writes Avro data to Parquet format can be created like this:

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

#### Hadoop SequenceFile format

To use the SequenceFile bulk encoder in your application you need to add the following dependency:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-sequence-file</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

A simple SequenceFile writer can be created like this:

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

The SequenceFileWriterFactory supports additional constructor parameters to specify compression settings.

### Important Considerations for S3

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
