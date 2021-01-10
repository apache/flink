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
supported by the [Flink `FileSystem` abstraction]({% link deployment/filesystems/index.md %}).

The streaming file sink writes incoming data into buckets. Given that the incoming streams can be unbounded,
data in each bucket are organized into part files of finite size. The bucketing behaviour is fully configurable
with a default time-based bucketing where we start writing a new bucket every hour. This means that each resulting
bucket will contain files with records received during 1 hour intervals from the stream.

Data within the bucket directories are split into part files. Each bucket will contain at least one part file for
each subtask of the sink that has received data for that bucket. Additional part files will be created according to the configurable
rolling policy. The default policy rolls part files based on size, a timeout that specifies the maximum duration for which a file can be open, and a maximum inactivity timeout after which the file is closed.

 <div class="alert alert-info">
     <b>IMPORTANT:</b> Checkpointing needs to be enabled when using the StreamingFileSink. Part files can only be finalized
     on successful checkpoints. If checkpointing is disabled, part files will forever stay in the `in-progress` or the `pending` state,
     and cannot be safely read by downstream systems.
 </div>

 <img src="{% link /fig/streamfilesink_bucketing.png %}" class="center" style="width: 100%;" />


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

In addition to the bucket assigner, the [RowFormatBuilder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/StreamingFileSink.RowFormatBuilder.html) allows the user to specify:

 - Custom [RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html) : Rolling policy to override the DefaultRollingPolicy
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
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
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
a rolling policy that rolls the in-progress part file on any of the following 3 conditions:

 - It contains at least 15 minutes worth of data
 - It hasn't received new records for the last 5 minutes
 - The file size has reached 1 GB (after writing the last record)

### Bulk-encoded Formats

Bulk-encoded sinks are created similarly to the row-encoded ones, but instead of
specifying an `Encoder`, we have to specify a [BulkWriter.Factory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/api/common/serialization/BulkWriter.Factory.html).
The `BulkWriter` logic defines how new elements are added and flushed, and how a batch of records
is finalized for further encoding purposes.

Flink comes with four built-in BulkWriter factories:

 - [ParquetWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/parquet/ParquetWriterFactory.html)
 - [AvroWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/avro/AvroWriterFactory.html)
 - [SequenceFileWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/sequencefile/SequenceFileWriterFactory.html)
 - [CompressWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/compress/CompressWriterFactory.html)
 - [OrcBulkWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/orc/writer/OrcBulkWriterFactory.html)

<div class="alert alert-info">
     <b>IMPORTANT:</b> Bulk Formats can only have `OnCheckpointRollingPolicy`, which rolls (ONLY) on every checkpoint.
</div>

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
DataStream<GenericRecord> input = ...;

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

Similarly, a StreamingFileSink that writes Protobuf data to Parquet format can be created like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters;

// ProtoRecord is a generated protobuf Message class.
DataStream<ProtoRecord> input = ...;

final StreamingFileSink<ProtoRecord> sink = StreamingFileSink
	.forBulkFormat(outputBasePath, ParquetProtoWriters.forType(ProtoRecord.class))
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters

// ProtoRecord is a generated protobuf Message class.
val input: DataStream[ProtoRecord] = ...

val sink: StreamingFileSink[ProtoRecord] = StreamingFileSink
    .forBulkFormat(outputBasePath, ParquetProtoWriters.forType(classOf[ProtoRecord]))
    .build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

#### Avro format

Flink also provides built-in support for writing data into Avro files. A list of convenience methods to create
Avro writer factories and their associated documentation can be found in the 
[AvroWriters]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/avro/AvroWriters.html) class.

To use the Avro writers in your application you need to add the following dependency:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

A StreamingFileSink that writes data to Avro files can be created like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.avro.Schema;


Schema schema = ...;
DataStream<GenericRecord> input = ...;

final StreamingFileSink<GenericRecord> sink = StreamingFileSink
	.forBulkFormat(outputBasePath, AvroWriters.forGenericRecord(schema))
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.formats.avro.AvroWriters
import org.apache.avro.Schema

val schema: Schema = ...
val input: DataStream[GenericRecord] = ...

val sink: StreamingFileSink[GenericRecord] = StreamingFileSink
    .forBulkFormat(outputBasePath, AvroWriters.forGenericRecord(schema))
    .build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

For creating customized Avro writers, e.g. enabling compression, users need to create the `AvroWriterFactory`
with a custom implementation of the [AvroBuilder]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/avro/AvroBuilder.html) interface:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
AvroWriterFactory<?> factory = new AvroWriterFactory<>((AvroBuilder<Address>) out -> {
	Schema schema = ReflectData.get().getSchema(Address.class);
	DatumWriter<Address> datumWriter = new ReflectDatumWriter<>(schema);

	DataFileWriter<Address> dataFileWriter = new DataFileWriter<>(datumWriter);
	dataFileWriter.setCodec(CodecFactory.snappyCodec());
	dataFileWriter.create(schema, out);
	return dataFileWriter;
});

DataStream<Address> stream = ...
stream.addSink(StreamingFileSink.forBulkFormat(
	outputBasePath,
	factory).build());
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val factory = new AvroWriterFactory[Address](new AvroBuilder[Address]() {
    override def createWriter(out: OutputStream): DataFileWriter[Address] = {
        val schema = ReflectData.get.getSchema(classOf[Address])
        val datumWriter = new ReflectDatumWriter[Address](schema)

        val dataFileWriter = new DataFileWriter[Address](datumWriter)
        dataFileWriter.setCodec(CodecFactory.snappyCodec)
        dataFileWriter.create(schema, out)
        dataFileWriter
    }
})

val stream: DataStream[Address] = ...
stream.addSink(StreamingFileSink.forBulkFormat(
    outputBasePath,
    factory).build());
{% endhighlight %}
</div>
</div>

#### ORC Format
 
To enable the data to be bulk encoded in ORC format, Flink offers [OrcBulkWriterFactory]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/formats/orc/writers/OrcBulkWriterFactory.html) 
which takes a concrete implementation of [Vectorizer]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/orc/vector/Vectorizer.html).

Like any other columnar format that encodes data in bulk fashion, Flink's `OrcBulkWriter` writes the input elements in batches. It uses 
ORC's `VectorizedRowBatch` to achieve this. 

Since the input element has to be transformed to a `VectorizedRowBatch`, users have to extend the abstract `Vectorizer` 
class and override the `vectorize(T element, VectorizedRowBatch batch)` method. As you can see, the method provides an 
instance of `VectorizedRowBatch` to be used directly by the users so users just have to write the logic to transform the 
input `element` to `ColumnVectors` and set them in the provided `VectorizedRowBatch` instance.

For example, if the input element is of type `Person` which looks like: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

class Person {
    private final String name;
    private final int age;
    ...
}

{% endhighlight %}
</div>
</div>

Then a child implementation to convert the element of type `Person` and set them in the `VectorizedRowBatch` can be like: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;

public class PersonVectorizer extends Vectorizer<Person> implements Serializable {	
	public PersonVectorizer(String schema) {
		super(schema);
	}
	@Override
	public void vectorize(Person element, VectorizedRowBatch batch) throws IOException {
		BytesColumnVector nameColVector = (BytesColumnVector) batch.cols[0];
		LongColumnVector ageColVector = (LongColumnVector) batch.cols[1];
		int row = batch.size++;
		nameColVector.setVal(row, element.getName().getBytes(StandardCharsets.UTF_8));
		ageColVector.vector[row] = element.getAge();
	}
}

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import java.nio.charset.StandardCharsets
import org.apache.hadoop.hive.ql.exec.vector.{BytesColumnVector, LongColumnVector}

class PersonVectorizer(schema: String) extends Vectorizer[Person](schema) {

  override def vectorize(element: Person, batch: VectorizedRowBatch): Unit = {
    val nameColVector = batch.cols(0).asInstanceOf[BytesColumnVector]
    val ageColVector = batch.cols(1).asInstanceOf[LongColumnVector]
    nameColVector.setVal(batch.size + 1, element.getName.getBytes(StandardCharsets.UTF_8))
    ageColVector.vector(batch.size + 1) = element.getAge
  }

}

{% endhighlight %}
</div>
</div>

To use the ORC bulk encoder in an application, users need to add the following dependency:

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-orc{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

And then a `StreamingFileSink` that writes data in ORC format can be created like this:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;

String schema = "struct<_col0:string,_col1:int>";
DataStream<Person> input = ...;

final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(new PersonVectorizer(schema));

final StreamingFileSink<Person> sink = StreamingFileSink
	.forBulkFormat(outputBasePath, writerFactory)
	.build();

input.addSink(sink);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink
import org.apache.flink.orc.writer.OrcBulkWriterFactory

val schema: String = "struct<_col0:string,_col1:int>"
val input: DataStream[Person] = ...
val writerFactory = new OrcBulkWriterFactory(new PersonVectorizer(schema));

val sink: StreamingFileSink[Person] = StreamingFileSink
    .forBulkFormat(outputBasePath, writerFactory)
    .build()

input.addSink(sink)

{% endhighlight %}
</div>
</div>

OrcBulkWriterFactory can also take Hadoop `Configuration` and `Properties` so that a custom Hadoop configuration and ORC 
writer properties can be provided.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
String schema = ...;
Configuration conf = ...;
Properties writerProperties = new Properties();

writerProps.setProperty("orc.compress", "LZ4");
// Other ORC supported properties can also be set similarly.

final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(
    new PersonVectorizer(schema), writerProperties, conf);

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val schema: String = ...
val conf: Configuration = ...
val writerProperties: Properties = new Properties()

writerProps.setProperty("orc.compress", "LZ4")
// Other ORC supported properties can also be set similarly.

val writerFactory = new OrcBulkWriterFactory(
    new PersonVectorizer(schema), writerProperties, conf)
{% endhighlight %}
</div>
</div> 

The complete list of ORC writer properties can be found [here](https://orc.apache.org/docs/hive-config.html).

Users who want to add user metadata to the ORC files can do so by calling `addUserMetadata(...)` inside the overriding 
`vectorize(...)` method.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

public class PersonVectorizer extends Vectorizer<Person> implements Serializable {	
	@Override
	public void vectorize(Person element, VectorizedRowBatch batch) throws IOException {
		...
		String metadataKey = ...;
		ByteBuffer metadataValue = ...;
		this.addUserMetadata(metadataKey, metadataValue);
	}
}

{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

class PersonVectorizer(schema: String) extends Vectorizer[Person](schema) {

  override def vectorize(element: Person, batch: VectorizedRowBatch): Unit = {
    ...
    val metadataKey: String = ...
    val metadataValue: ByteBuffer = ...
    addUserMetadata(metadataKey, metadataValue)
  }

}

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

## Bucket Assignment

The bucketing logic defines how the data will be structured into subdirectories inside the base output directory.

Both row and bulk formats (see [File Formats](#file-formats)) use the [DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html) as the default assigner.
By default the `DateTimeBucketAssigner` creates hourly buckets based on the system default timezone
with the following format: `yyyy-MM-dd--HH`. Both the date format (*i.e.* bucket size) and timezone can be
configured manually.

We can specify a custom [BucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/BucketAssigner.html) by calling `.withBucketAssigner(assigner)` on the format builders.

Flink comes with two built in BucketAssigners:

 - [DateTimeBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/DateTimeBucketAssigner.html) : Default time based assigner
 - [BasePathBucketAssigner]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/bucketassigners/BasePathBucketAssigner.html) : Assigner that stores all part files in the base path (single global bucket)

## Rolling Policy

The [RollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/RollingPolicy.html) defines when a given in-progress part file will be closed and moved to the pending and later to finished state.
Part files in the "finished" state are the ones that are ready for viewing and are guaranteed to contain valid data that will not be reverted in case of failure.
The Rolling Policy in combination with the checkpointing interval (pending files become finished on the next checkpoint) control how quickly
part files become available for downstream readers and also the size and number of these parts.

Flink comes with two built-in RollingPolicies:

 - [DefaultRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/DefaultRollingPolicy.html)
 - [OnCheckpointRollingPolicy]({{ site.javadocs_baseurl }}/api/java/org/apache/flink/streaming/api/functions/sink/filesystem/rollingpolicies/OnCheckpointRollingPolicy.html)

## Part file lifecycle

In order to use the output of the `StreamingFileSink` in downstream systems, we need to understand the naming and lifecycle of the output files produced.

Part files can be in one of three states:
 1. **In-progress** : The part file that is currently being written to is in-progress
 2. **Pending** : Closed (due to the specified rolling policy) in-progress files that are waiting to be committed
 3. **Finished** : On successful checkpoints pending files transition to "Finished"

Only finished files are safe to read by downstream systems as those are guaranteed to not be modified later.

<div class="alert alert-info">
     <b>IMPORTANT:</b> Part file indexes are strictly increasing for any given subtask (in the order they were created). However these indexes are not always sequential. When the job restarts, the next part index for all subtask will be the `max part index + 1`
where `max` is computed across all subtasks.
</div>

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

Finished files can be distinguished from the in-progress ones by their naming scheme only.

By default, the file naming strategy is as follows:
 - **In-progress / Pending**: `part-<subtaskIndex>-<partFileIndex>.inprogress.uid`
 - **Finished:** `part-<subtaskIndex>-<partFileIndex>`

Flink allows the user to specify a prefix and/or a suffix for his/her part files. 
This can be done using an `OutputFileConfig`. 
For example for a prefix "prefix" and a suffix ".ext" the sink will create the following files:

```
└── 2019-08-25--12
    ├── prefix-0-0.ext
    ├── prefix-0-1.ext.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── prefix-1-0.ext
    └── prefix-1-1.ext.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

The user can specify an `OutputFileConfig` in the following way:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

OutputFileConfig config = OutputFileConfig
 .builder()
 .withPartPrefix("prefix")
 .withPartSuffix(".ext")
 .build();
            
StreamingFileSink<Tuple2<Integer, Integer>> sink = StreamingFileSink
 .forRowFormat((new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
 .withBucketAssigner(new KeyBucketAssigner())
 .withRollingPolicy(OnCheckpointRollingPolicy.build())
 .withOutputFileConfig(config)
 .build();
			
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}

val config = OutputFileConfig
 .builder()
 .withPartPrefix("prefix")
 .withPartSuffix(".ext")
 .build()
            
val sink = StreamingFileSink
 .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
 .withBucketAssigner(new KeyBucketAssigner())
 .withRollingPolicy(OnCheckpointRollingPolicy.build())
 .withOutputFileConfig(config)
 .build()
			
{% endhighlight %}
</div>
</div>

## Important Considerations

### General

<span class="label label-danger">Important Note 1</span>: When using Hadoop < 2.7, please use
the `OnCheckpointRollingPolicy` which rolls part files on every checkpoint. The reason is that if part files "traverse"
the checkpoint interval, then, upon recovery from a failure the `StreamingFileSink` may use the `truncate()` method of the 
filesystem to discard uncommitted data from the in-progress file. This method is not supported by pre-2.7 Hadoop versions 
and Flink will throw an exception.

<span class="label label-danger">Important Note 2</span>: Given that Flink sinks and UDFs in general do not differentiate between
normal job termination (*e.g.* finite input stream) and termination due to failure, upon normal termination of a job, the last 
in-progress files will not be transitioned to the "finished" state.

<span class="label label-danger">Important Note 3</span>: Flink and the `StreamingFileSink` never overwrites committed data.
Given this, when trying to restore from an old checkpoint/savepoint which assumes an in-progress file which was committed
by subsequent successful checkpoints, the `StreamingFileSink` will refuse to resume and it will throw an exception as it cannot locate the 
in-progress file.

<span class="label label-danger">Important Note 4</span>: Currently, the `StreamingFileSink` only supports three filesystems: 
HDFS, S3, and Local. Flink will throw an exception when using an unsupported filesystem at runtime.

### S3-specific

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
