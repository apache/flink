---
title: FileSystem
weight: 6
type: docs
aliases:
  - /dev/connectors/file_sink.html
  - /apis/streaming/connectors/filesystem_sink.html
  - /docs/connectors/datastream/streamfile_sink/
  - /docs/connectors/datastream/file_sink/
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

# FileSystem

This connector provides a unified Source and Sink for `BATCH` and `STREAMING` that reads or writes (partitioned) files to file systems
supported by the [Flink `FileSystem` abstraction]({{< ref "docs/deployment/filesystems/overview" >}}). This filesystem
connector provides the same guarantees for both `BATCH` and `STREAMING` and is designed to provide exactly-once semantics for `STREAMING` execution.

The connector supports reading and writing a set of files from any (distributed) file system (e.g. POSIX, S3, HDFS)
with a [format]({{< ref "docs/connectors/datastream/formats/overview" >}}) (e.g., Avro, CSV, Parquet),
and produces a stream or records.

## File Source

The `File Source` is based on the [Source API]({{< ref "docs/dev/datastream/sources" >}}#the-data-source-api), 
a unified data source that reads files - both in batch and in streaming mode. 
It is divided into the following two parts: `SplitEnumerator` and `SourceReader`. 

* `SplitEnumerator` is responsible for discovering and identifying the files to read and assigns them to the `SourceReader`.
* `SourceReader` requests the files it needs to process and reads the file from the filesystem. 

You will need to combine the File Source with a [format]({{< ref "docs/connectors/datastream/formats/overview" >}}), which allows you to
parse CSV, decode AVRO, or read Parquet columnar files.

#### Bounded and Unbounded Streams

A bounded `File Source` lists all files (via SplitEnumerator - a recursive directory list with filtered-out hidden files) and reads them all.

An unbounded `File Source` is created when configuring the enumerator for periodic file discovery.
In this case, the `SplitEnumerator` will enumerate like the bounded case but, after a certain interval, repeats the enumeration.
For any repeated enumeration, the `SplitEnumerator` filters out previously detected files and only sends new ones to the `SourceReader`.

### Usage

You can start building a File Source via one of the following API calls:

{{< tabs "FileSourceUsage" >}}
{{< tab "Java" >}}
```java
// reads the contents of a file from a file stream. 
FileSource.forRecordStreamFormat(StreamFormat,Path...)
        
// reads batches of records from a file at a time
FileSource.forBulkFileFormat(BulkFormat,Path...)
```
{{< /tab >}}
{{< /tabs >}}

This creates a `FileSource.FileSourceBuilder` on which you can configure all the properties of the File Source.

For the bounded/batch case, the File Source processes all files under the given path(s).
For the continuous/streaming case, the source periodically checks the paths for new files and will start reading those.

When you start creating a File Source (via the `FileSource.FileSourceBuilder` created through one of the above-mentioned methods),
the source is in bounded/batch mode by default. You can call `AbstractFileSource.AbstractFileSourceBuilder.monitorContinuously(Duration)`
to put the source into continuous streaming mode.

{{< tabs "FileSourceBuilder" >}}
{{< tab "Java" >}}
```java
final FileSource<String> source =
        FileSource.forRecordStreamFormat(...)
        .monitorContinuously(Duration.ofMillis(5))  
        .build();
```
{{< /tab >}}
{{< /tabs >}}

### Format Types

The reading of each file happens through file readers defined by file formats. 
These define the parsing logic for the contents of the file. There are multiple classes that the source supports.
The interfaces are a tradeoff between simplicity of implementation and flexibility/efficiency.

* A `StreamFormat` reads the contents of a file from a file stream. It is the simplest format to implement, 
and provides many features out-of-the-box (like checkpointing logic) but is limited in the optimizations it can apply 
(such as object reuse, batching, etc.).

* A `BulkFormat` reads batches of records from a file at a time. 
It is the most "low level" format to implement, but offers the greatest flexibility to optimize the implementation.

#### TextLine Format

A `StreamFormat` reader formats text lines from a file.
The reader uses Java's built-in `InputStreamReader` to decode the byte stream using
various supported charset encodings.
This format does not support optimized recovery from checkpoints. On recovery, it will re-read
and discard the number of lines that were processed before the last checkpoint. This is due to
the fact that the offsets of lines in the file cannot be tracked through the charset decoders
with their internal buffering of stream input and charset decoder state.

#### SimpleStreamFormat Abstract Class

This is a simple version of `StreamFormat` for formats that are not splittable.
Custom reads of Array or File can be done by implementing `SimpleStreamFormat`:

{{< tabs "SimpleStreamFormat" >}}
{{< tab "Java" >}}
```java
private static final class ArrayReaderFormat extends SimpleStreamFormat<byte[]> {
    private static final long serialVersionUID = 1L;

    @Override
    public Reader<byte[]> createReader(Configuration config, FSDataInputStream stream)
            throws IOException {
        return new ArrayReader(stream);
    }

    @Override
    public TypeInformation<byte[]> getProducedType() {
        return PrimitiveArrayTypeInfo.BYTE_PRIMITIVE_ARRAY_TYPE_INFO;
    }
}

final FileSource<byte[]> source =
                FileSource.forRecordStreamFormat(new ArrayReaderFormat(), path).build();
```
{{< /tab >}}
{{< /tabs >}}

An example of a `SimpleStreamFormat` is `CsvReaderFormat`. It can be initialized like this:
```java
CsvReaderFormat<SomePojo> csvFormat = CsvReaderFormat.forPojo(SomePojo.class);
FileSource<SomePojo> source = 
        FileSource.forRecordStreamFormat(csvFormat, Path.fromLocalFile(...)).build();
```

The schema for CSV parsing, in this case, is automatically derived based on the fields of the `SomePojo` class using the `Jackson` library. (Note: you might need to add `@JsonPropertyOrder({field1, field2, ...})` annotation to your class definition with the fields order exactly matching those of the CSV file columns).

If you need more fine-grained control over the CSV schema or the parsing options, use the more low-level `forSchema` static factory method of `CsvReaderFormat`:

```java
CsvReaderFormat<T> forSchema(CsvMapper mapper, 
                             CsvSchema schema, 
                             TypeInformation<T> typeInformation) 
```

#### Bulk Format

The BulkFormat reads and decodes batches of records at a time. Examples of bulk formats
are formats like ORC or Parquet.
The outer `BulkFormat` class acts mainly as a configuration holder and factory for the
reader. The actual reading is done by the `BulkFormat.Reader`, which is created in the
`BulkFormat#createReader(Configuration, FileSourceSplit)` method. If a bulk reader is
created based on a checkpoint during checkpointed streaming execution, then the reader is
re-created in the `BulkFormat#restoreReader(Configuration, FileSourceSplit)` method.

A `SimpleStreamFormat` can be turned into a `BulkFormat` by wrapping it in a `StreamFormatAdapter`:
```java
BulkFormat<SomePojo, FileSourceSplit> bulkFormat = 
        new StreamFormatAdapter<>(CsvReaderFormat.forPojo(SomePojo.class));
```

### Customizing File Enumeration

{{< tabs "CustomizingFileEnumeration" >}}
{{< tab "Java" >}}
```java
/**
 * A FileEnumerator implementation for hive source, which generates splits based on 
 * HiveTablePartition.
 */
public class HiveSourceFileEnumerator implements FileEnumerator {
    
    // reference constructor
    public HiveSourceFileEnumerator(...) {
        ...
    }

    /***
     * Generates all file splits for the relevant files under the given paths. The {@code
     * minDesiredSplits} is an optional hint indicating how many splits would be necessary to
     * exploit parallelism properly.
     */
    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        // createInputSplits:splitting files into fragmented collections
        return new ArrayList<>(createInputSplits(...));
    }

    ...

    /***
     * A factory to create HiveSourceFileEnumerator.
     */
    public static class Provider implements FileEnumerator.Provider {

        ...
        @Override
        public FileEnumerator create() {
            return new HiveSourceFileEnumerator(...);
        }
    }
}
// use the customizing file enumeration
new HiveSource<>(
        ...,
        new HiveSourceFileEnumerator.Provider(
        partitions != null ? partitions : Collections.emptyList(),
        new JobConfWrapper(jobConf)),
       ...);
```
{{< /tab >}}
{{< /tabs >}}

### Current Limitations

Watermarking does not work very well for large backlogs of files. This is because watermarks eagerly advance within a file, and the next file might contain data later than the watermark.

For Unbounded File Sources, the enumerator currently remembers paths of all already processed files, which is a state that can, in some cases, grow rather large.
There are plans to add a compressed form of tracking already processed files in the future (for example, by keeping modification timestamps below boundaries).

### Behind the Scenes
{{< hint info >}}
If you are interested in how File Source works through the new data source API design, you may
want to read this part as a reference. For details about the new data source API, check out the
[documentation on data sources]({{< ref "docs/dev/datastream/sources.md" >}}) and
<a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface">FLIP-27</a>
for more descriptive discussions.
{{< /hint >}}

## File Sink

The file sink writes incoming data into buckets. Given that the incoming streams can be unbounded,
data in each bucket is organized into part files of finite size. The bucketing behaviour is fully configurable
with a default time-based bucketing where we start writing a new bucket every hour. This means that each resulting
bucket will contain files with records received during 1 hour intervals from the stream.

Data within the bucket directories is split into part files. Each bucket will contain at least one part file for
each subtask of the sink that has received data for that bucket. Additional part files will be created according to the configurable
rolling policy. For `Row-encoded Formats` (see [File Formats](#file-formats)) the default policy rolls part files based
on size, a timeout that specifies the maximum duration for which a file can be open, and a maximum inactivity
timeout after which the file is closed. For `Bulk-encoded Formats` we roll on every checkpoint and the user can 
specify additional conditions based on size or time.

{{< hint info >}}

**IMPORTANT**: Checkpointing needs to be enabled when using the `FileSink` in `STREAMING` mode. Part files 
can only be finalized on successful checkpoints. If checkpointing is disabled, part files will forever stay 
in the `in-progress` or the `pending` state, and cannot be safely read by downstream systems.

{{< /hint >}}

 {{< img src="/fig/streamfilesink_bucketing.png"  width="100%" >}}

### Format Types

The `FileSink` supports both row-wise and bulk encoding formats, such as [Apache Parquet](http://parquet.apache.org).
These two variants come with their respective builders that can be created with the following static methods:

 - Row-encoded sink: `FileSink.forRowFormat(basePath, rowEncoder)`
 - Bulk-encoded sink: `FileSink.forBulkFormat(basePath, bulkWriterFactory)`

When creating either a row or a bulk encoded sink we have to specify the base path where the buckets will be
stored and the encoding logic for our data.

Please check out the JavaDoc for {{< javadoc file="org/apache/flink/connector/file/sink/FileSink.html" name="FileSink">}}
for all the configuration options and more documentation about the implementation of the different data formats.

#### Row-encoded Formats

Row-encoded formats need to specify an `Encoder`
that is used for serializing individual rows to the `OutputStream` of the in-progress part files.

In addition to the bucket assigner, the RowFormatBuilder allows the user to specify:

 - Custom RollingPolicy : Rolling policy to override the DefaultRollingPolicy
 - bucketCheckInterval (default = 1 min) : Interval for checking time based rolling policies

Basic usage for writing String elements thus looks like this:


{{< tabs "08046394-3912-497d-ab4b-e07a0ef1f519" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.core.fs.Path;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

DataStream<String> input = ...;

final FileSink<String> sink = FileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder<String>("UTF-8"))
    .withRollingPolicy(
        DefaultRollingPolicy.builder()
            .withRolloverInterval(Duration.ofSeconds(10))
            .withInactivityInterval(Duration.ofSeconds(10))
            .withMaxPartSize(MemorySize.ofMebiBytes(1))
            .build())
	.build();

input.sinkTo(sink);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.api.common.serialization.SimpleStringEncoder
import org.apache.flink.core.fs.Path
import org.apache.flink.configuration.MemorySize
import org.apache.flink.connector.file.sink.FileSink
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy

import java.time.Duration

val input: DataStream[String] = ...

val sink: FileSink[String] = FileSink
    .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
    .withRollingPolicy(
        DefaultRollingPolicy.builder()
            .withRolloverInterval(Duration.ofSeconds(10))
            .withInactivityInterval(Duration.ofSeconds(10))
            .withMaxPartSize(MemorySize.ofMebiBytes(1))
            .build())
    .build()

input.sinkTo(sink)

```
{{< /tab >}}
{{< /tabs >}}

This example creates a simple sink that assigns records to the default one hour time buckets. It also specifies
a rolling policy that rolls the in-progress part file on any of the following 3 conditions:

 - It contains at least 15 minutes worth of data
 - It hasn't received new records for the last 5 minutes
 - The file size has reached 1 GB (after writing the last record)

#### Bulk-encoded Formats

Bulk-encoded sinks are created similarly to the row-encoded ones, but instead of
specifying an `Encoder`, we have to specify a {{< javadoc file="org/apache/flink/api/common/serialization/BulkWriter.Factory.html" name="BulkWriter.Factory">}}.
The `BulkWriter` logic defines how new elements are added and flushed, and how a batch of records
is finalized for further encoding purposes.

Flink comes with four built-in BulkWriter factories:

* ParquetWriterFactory
* AvroWriterFactory
* SequenceFileWriterFactory
* CompressWriterFactory
* OrcBulkWriterFactory

{{< hint info >}}
**Important** Bulk Formats can only have a rolling policy that extends the `CheckpointRollingPolicy`. 
The latter rolls on every checkpoint. A policy can roll additionally based on size or processing time.
{{< /hint >}}

##### Parquet format

Flink contains built in convenience methods for creating Parquet writer factories for Avro data. These methods
and their associated documentation can be found in the AvroParquetWriters class.

For writing to other Parquet compatible data formats, users need to create the ParquetWriterFactory with a custom implementation of the ParquetBuilder interface.

To use the Parquet bulk encoder in your application you need to add the following dependency:

{{< artifact flink-parquet withScalaVersion >}}

A `FileSink` that writes Avro data to Parquet format can be created like this:

{{< tabs "4ff7b496-3a80-46f4-9b7d-7a9222672927" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters;
import org.apache.avro.Schema;


Schema schema = ...;
DataStream<GenericRecord> input = ...;

final FileSink<GenericRecord> sink = FileSink
	.forBulkFormat(outputBasePath, AvroParquetWriters.forGenericRecord(schema))
	.build();

input.sinkTo(sink);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.parquet.avro.AvroParquetWriters
import org.apache.avro.Schema

val schema: Schema = ...
val input: DataStream[GenericRecord] = ...

val sink: FileSink[GenericRecord] = FileSink
    .forBulkFormat(outputBasePath, AvroParquetWriters.forGenericRecord(schema))
    .build()

input.sinkTo(sink)

```
{{< /tab >}}
{{< /tabs >}}

Similarly, a `FileSink` that writes Protobuf data to Parquet format can be created like this:

{{< tabs "dd1e3e68-855e-4d93-8f86-74d039591745" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters;

// ProtoRecord is a generated protobuf Message class.
DataStream<ProtoRecord> input = ...;

final FileSink<ProtoRecord> sink = FileSink
	.forBulkFormat(outputBasePath, ParquetProtoWriters.forType(ProtoRecord.class))
	.build();

input.sinkTo(sink);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.parquet.protobuf.ParquetProtoWriters

// ProtoRecord is a generated protobuf Message class.
val input: DataStream[ProtoRecord] = ...

val sink: FileSink[ProtoRecord] = FileSink
    .forBulkFormat(outputBasePath, ParquetProtoWriters.forType(classOf[ProtoRecord]))
    .build()

input.sinkTo(sink)

```
{{< /tab >}}
{{< /tabs >}}

##### Avro format

Flink also provides built-in support for writing data into Avro files. A list of convenience methods to create
Avro writer factories and their associated documentation can be found in the 
AvroWriters class.

To use the Avro writers in your application you need to add the following dependency:

{{< artifact flink-avro >}}

A `FileSink` that writes data to Avro files can be created like this:

{{< tabs "ee5f25e0-180e-43b1-ae91-277bf73d3a6c" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.avro.AvroWriters;
import org.apache.avro.Schema;


Schema schema = ...;
DataStream<GenericRecord> input = ...;

final FileSink<GenericRecord> sink = FileSink
	.forBulkFormat(outputBasePath, AvroWriters.forGenericRecord(schema))
	.build();

input.sinkTo(sink);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.formats.avro.AvroWriters
import org.apache.avro.Schema

val schema: Schema = ...
val input: DataStream[GenericRecord] = ...

val sink: FileSink[GenericRecord] = FileSink
    .forBulkFormat(outputBasePath, AvroWriters.forGenericRecord(schema))
    .build()

input.sinkTo(sink)

```
{{< /tab >}}
{{< /tabs >}}

For creating customized Avro writers, e.g. enabling compression, users need to create the `AvroWriterFactory`
with a custom implementation of the `AvroBuilder` interface:

{{< tabs "3bfe80db-db61-4498-9ec0-8017c64eab5c" >}}
{{< tab "Java" >}}
```java
AvroWriterFactory<?> factory = new AvroWriterFactory<>((AvroBuilder<Address>) out -> {
	Schema schema = ReflectData.get().getSchema(Address.class);
	DatumWriter<Address> datumWriter = new ReflectDatumWriter<>(schema);

	DataFileWriter<Address> dataFileWriter = new DataFileWriter<>(datumWriter);
	dataFileWriter.setCodec(CodecFactory.snappyCodec());
	dataFileWriter.create(schema, out);
	return dataFileWriter;
});

DataStream<Address> stream = ...
stream.sinkTo(FileSink.forBulkFormat(
	outputBasePath,
	factory).build());
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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
stream.sinkTo(FileSink.forBulkFormat(
    outputBasePath,
    factory).build());
```
{{< /tab >}}
{{< /tabs >}}

##### ORC Format
 
To enable the data to be bulk encoded in ORC format, Flink offers `OrcBulkWriterFactory`
which takes a concrete implementation of Vectorizer.

Like any other columnar format that encodes data in bulk fashion, Flink's `OrcBulkWriter` writes the input elements in batches. It uses 
ORC's `VectorizedRowBatch` to achieve this. 

Since the input element has to be transformed to a `VectorizedRowBatch`, users have to extend the abstract `Vectorizer` 
class and override the `vectorize(T element, VectorizedRowBatch batch)` method. As you can see, the method provides an 
instance of `VectorizedRowBatch` to be used directly by the users so users just have to write the logic to transform the 
input `element` to `ColumnVectors` and set them in the provided `VectorizedRowBatch` instance.

For example, if the input element is of type `Person` which looks like: 

{{< tabs "5436ac4a-4834-4fb3-9872-2dd5c3145efa" >}}
{{< tab "Java" >}}
```java

class Person {
    private final String name;
    private final int age;
    ...
}

```
{{< /tab >}}
{{< /tabs >}}

Then a child implementation to convert the element of type `Person` and set them in the `VectorizedRowBatch` can be like: 

{{< tabs "6eb8e5e8-5177-4c8d-bb5a-96c6333b0b01" >}}
{{< tab "Java" >}}
```java
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

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
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

```
{{< /tab >}}
{{< /tabs >}}

To use the ORC bulk encoder in an application, users need to add the following dependency:

{{< artifact flink-orc withScalaVersion >}}


And then a `FileSink` that writes data in ORC format can be created like this:

{{< tabs "f948c85a-d236-451d-b24a-612f96507805" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.orc.writer.OrcBulkWriterFactory;

String schema = "struct<_col0:string,_col1:int>";
DataStream<Person> input = ...;

final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(new PersonVectorizer(schema));

final FileSink<Person> sink = FileSink
	.forBulkFormat(outputBasePath, writerFactory)
	.build();

input.sinkTo(sink);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.orc.writer.OrcBulkWriterFactory

val schema: String = "struct<_col0:string,_col1:int>"
val input: DataStream[Person] = ...
val writerFactory = new OrcBulkWriterFactory(new PersonVectorizer(schema));

val sink: FileSink[Person] = FileSink
    .forBulkFormat(outputBasePath, writerFactory)
    .build()

input.sinkTo(sink)

```
{{< /tab >}}
{{< /tabs >}}

OrcBulkWriterFactory can also take Hadoop `Configuration` and `Properties` so that a custom Hadoop configuration and ORC 
writer properties can be provided.

{{< tabs "eeef2ece-6e21-4bfd-b3ed-3329136f3486" >}}
{{< tab "Java" >}}
```java
String schema = ...;
Configuration conf = ...;
Properties writerProperties = new Properties();

writerProperties.setProperty("orc.compress", "LZ4");
// Other ORC supported properties can also be set similarly.

final OrcBulkWriterFactory<Person> writerFactory = new OrcBulkWriterFactory<>(
    new PersonVectorizer(schema), writerProperties, conf);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val schema: String = ...
val conf: Configuration = ...
val writerProperties: Properties = new Properties()

writerProperties.setProperty("orc.compress", "LZ4")
// Other ORC supported properties can also be set similarly.

val writerFactory = new OrcBulkWriterFactory(
    new PersonVectorizer(schema), writerProperties, conf)
```
{{< /tab >}}
{{< /tabs >}} 

The complete list of ORC writer properties can be found [here](https://orc.apache.org/docs/hive-config.html).

Users who want to add user metadata to the ORC files can do so by calling `addUserMetadata(...)` inside the overriding 
`vectorize(...)` method.

{{< tabs "9880fed1-b5d7-440e-a5ca-c9b20f10dac2" >}}
{{< tab "Java" >}}
```java

public class PersonVectorizer extends Vectorizer<Person> implements Serializable {	
	@Override
	public void vectorize(Person element, VectorizedRowBatch batch) throws IOException {
		...
		String metadataKey = ...;
		ByteBuffer metadataValue = ...;
		this.addUserMetadata(metadataKey, metadataValue);
	}
}

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

class PersonVectorizer(schema: String) extends Vectorizer[Person](schema) {

  override def vectorize(element: Person, batch: VectorizedRowBatch): Unit = {
    ...
    val metadataKey: String = ...
    val metadataValue: ByteBuffer = ...
    addUserMetadata(metadataKey, metadataValue)
  }

}

```
{{< /tab >}}
{{< /tabs >}}

##### Hadoop SequenceFile format

To use the `SequenceFile` bulk encoder in your application you need to add the following dependency:

{{< artifact flink-sequence-file >}}

A simple `SequenceFile` writer can be created like this:

{{< tabs "d707ffcf-7df3-4847-bb01-5eaa9f12de88" >}}
{{< tab "Java" >}}
```java
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;


DataStream<Tuple2<LongWritable, Text>> input = ...;
Configuration hadoopConf = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration());
final FileSink<Tuple2<LongWritable, Text>> sink = FileSink
  .forBulkFormat(
    outputBasePath,
    new SequenceFileWriterFactory<>(hadoopConf, LongWritable.class, Text.class))
	.build();

input.sinkTo(sink);

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.configuration.GlobalConfiguration
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.SequenceFile
import org.apache.hadoop.io.Text;

val input: DataStream[(LongWritable, Text)] = ...
val hadoopConf: Configuration = HadoopUtils.getHadoopConfiguration(GlobalConfiguration.loadConfiguration())
val sink: FileSink[(LongWritable, Text)] = FileSink
  .forBulkFormat(
    outputBasePath,
    new SequenceFileWriterFactory(hadoopConf, LongWritable.class, Text.class))
	.build()

input.sinkTo(sink)

```
{{< /tab >}}
{{< /tabs >}}

The `SequenceFileWriterFactory` supports additional constructor parameters to specify compression settings.

### Bucket Assignment

The bucketing logic defines how the data will be structured into subdirectories inside the base output directory.

Both row and bulk formats (see [File Formats](#file-formats)) use the `DateTimeBucketAssigner` as the default assigner.
By default the `DateTimeBucketAssigner` creates hourly buckets based on the system default timezone
with the following format: `yyyy-MM-dd--HH`. Both the date format (*i.e.* bucket size) and timezone can be
configured manually.

We can specify a custom `BucketAssigner` by calling `.withBucketAssigner(assigner)` on the format builders.

Flink comes with two built-in BucketAssigners:

 - `DateTimeBucketAssigner` : Default time based assigner
 - `BasePathBucketAssigner` : Assigner that stores all part files in the base path (single global bucket)

### Rolling Policy

The `RollingPolicy` defines when a given in-progress part file will be closed and moved to the pending and later to finished state.
Part files in the "finished" state are the ones that are ready for viewing and are guaranteed to contain valid data that will not be reverted in case of failure.
In `STREAMING` mode, the Rolling Policy in combination with the checkpointing interval (pending files become finished on the next checkpoint) control how quickly
part files become available for downstream readers and also the size and number of these parts. In `BATCH` mode, part-files become visible at the end of the job but 
the rolling policy can control their maximum size. 

Flink comes with two built-in RollingPolicies:

 - `DefaultRollingPolicy`
 - `OnCheckpointRollingPolicy`

### Part file lifecycle

In order to use the output of the `FileSink` in downstream systems, we need to understand the naming and lifecycle of the output files produced.

Part files can be in one of three states:
 1. **In-progress** : The part file that is currently being written to is in-progress
 2. **Pending** : Closed (due to the specified rolling policy) in-progress files that are waiting to be committed
 3. **Finished** : On successful checkpoints (`STREAMING`) or at the end of input (`BATCH`) pending files transition to "Finished"

Only finished files are safe to read by downstream systems as those are guaranteed to not be modified later.

Each writer subtask will have a single in-progress part file at any given time for every active bucket, but there can be several pending and finished files.

**Part file example**

To better understand the lifecycle of these files let's look at a simple example with 2 sink subtasks:

```
└── 2019-08-25--12
    ├── part-4005733d-a830-4323-8291-8866de98b582-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    └── part-81fc4980-a6af-41c8-9937-9939408a734b-0.inprogress.ea65a428-a1d0-4a0b-bbc5-7a436a75e575
```

When the part file `part-81fc4980-a6af-41c8-9937-9939408a734b-0` is rolled (let's say it becomes too large), it becomes pending but it is not renamed. The sink then opens a new part file: `part-81fc4980-a6af-41c8-9937-9939408a734b-1`:

```
└── 2019-08-25--12
    ├── part-4005733d-a830-4323-8291-8866de98b582-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-81fc4980-a6af-41c8-9937-9939408a734b-0.inprogress.ea65a428-a1d0-4a0b-bbc5-7a436a75e575
    └── part-81fc4980-a6af-41c8-9937-9939408a734b-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

As `part-81fc4980-a6af-41c8-9937-9939408a734b-0` is now pending completion, after the next successful checkpoint, it is finalized:

```
└── 2019-08-25--12
    ├── part-4005733d-a830-4323-8291-8866de98b582-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-81fc4980-a6af-41c8-9937-9939408a734b-0
    └── part-81fc4980-a6af-41c8-9937-9939408a734b-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

New buckets are created as dictated by the bucketing policy, and this doesn't affect currently in-progress files:

```
└── 2019-08-25--12
    ├── part-4005733d-a830-4323-8291-8866de98b582-0.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── part-81fc4980-a6af-41c8-9937-9939408a734b-0
    └── part-81fc4980-a6af-41c8-9937-9939408a734b-1.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
└── 2019-08-25--13
    └── part-4005733d-a830-4323-8291-8866de98b582-0.inprogress.2b475fec-1482-4dea-9946-eb4353b475f1
```

Old buckets can still receive new records as the bucketing policy is evaluated on a per-record basis.

#### Part file configuration

Finished files can be distinguished from the in-progress ones by their naming scheme only.

By default, the file naming strategy is as follows:
 - **In-progress / Pending**: `part-<uid>-<partFileIndex>.inprogress.uid`
 - **Finished:** `part-<uid>-<partFileIndex>`
where `uid` is a random id assigned to a subtask of the sink when the subtask is instantiated. This `uid` is not fault-tolerant 
so it is regenerated when the subtask recovers from a failure.

Flink allows the user to specify a prefix and/or a suffix for his/her part files. 
This can be done using an `OutputFileConfig`. 
For example for a prefix "prefix" and a suffix ".ext" the sink will create the following files:

```
└── 2019-08-25--12
    ├── prefix-4005733d-a830-4323-8291-8866de98b582-0.ext
    ├── prefix-4005733d-a830-4323-8291-8866de98b582-1.ext.inprogress.bd053eb0-5ecf-4c85-8433-9eff486ac334
    ├── prefix-81fc4980-a6af-41c8-9937-9939408a734b-0.ext
    └── prefix-81fc4980-a6af-41c8-9937-9939408a734b-1.ext.inprogress.bc279efe-b16f-47d8-b828-00ef6e2fbd11
```

The user can specify an `OutputFileConfig` in the following way:

{{< tabs "3b8a397e-58d1-4a04-acae-b2dcace9f080" >}}
{{< tab "Java" >}}
```java

OutputFileConfig config = OutputFileConfig
 .builder()
 .withPartPrefix("prefix")
 .withPartSuffix(".ext")
 .build();
            
FileSink<Tuple2<Integer, Integer>> sink = FileSink
 .forRowFormat((new Path(outputPath), new SimpleStringEncoder<>("UTF-8"))
 .withBucketAssigner(new KeyBucketAssigner())
 .withRollingPolicy(OnCheckpointRollingPolicy.build())
 .withOutputFileConfig(config)
 .build();
			
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

val config = OutputFileConfig
 .builder()
 .withPartPrefix("prefix")
 .withPartSuffix(".ext")
 .build()
            
val sink = FileSink
 .forRowFormat(new Path(outputPath), new SimpleStringEncoder[String]("UTF-8"))
 .withBucketAssigner(new KeyBucketAssigner())
 .withRollingPolicy(OnCheckpointRollingPolicy.build())
 .withOutputFileConfig(config)
 .build()
			
```
{{< /tab >}}
{{< /tabs >}}

### Compaction

Since version 1.15 `FileSink` supports compaction of the `pending` files,
which allows the application to have smaller checkpoint interval without generating a lot of small files,
especially when using the [bulk encoded formats]({{< ref "docs/connectors/datastream/filesystem#bulk-encoded-formats" >}})
that have to rolling on taking checkpoints.

Compaction could be enabled with

{{< tabs "enablecompaction" >}}
{{< tab "Java" >}}
```java

FileSink<Integer> fileSink=
	FileSink.forRowFormat(new Path(path),new SimpleStringEncoder<Integer>())
	    .enableCompact(
	        FileCompactStrategy.Builder.newBuilder()
	            .setSizeThreshold(1024)
	            .enableCompactionOnCheckpoint(5)
	            .build(),
	        new RecordWiseFileCompactor<>(
	            new DecoderBasedReader.Factory<>(SimpleStringDecoder::new)))
	    .build();

```
{{< /tab >}}
{{< tab "Scala" >}}
```scala

val fileSink: FileSink[Integer] =
  FileSink.forRowFormat(new Path(path), new SimpleStringEncoder[Integer]())
    .enableCompact(
      FileCompactStrategy.Builder.newBuilder()
        .setSizeThreshold(1024)
        .enableCompactionOnCheckpoint(5)
        .build(),
      new RecordWiseFileCompactor(
        new DecoderBasedReader.Factory(() => new SimpleStringDecoder)))
    .build()

```
{{< /tab >}}
{{< /tabs >}}

Once enabled, the compaction happens between the files become `pending` and get committed. The pending files will
be first committed to temporary files whose path starts with `.`. Then these files will be compacted according to
the strategy by the compactor specified by the users, and the new compacted pending files will be generated.
Then these pending files will be emitted to the committer to be committed to the formal files. After that, the source files will be removed.

When enabling compaction, you need to specify the {{< javadoc file="org/apache/flink/connector/file/sink/compactor/FileCompactStrategy.html" name="FileCompactStrategy">}}
and the {{< javadoc file="org/apache/flink/connector/file/sink/compactor/FileCompactor.html" name="FileCompactor">}}.

The {{< javadoc file="org/apache/flink/connector/file/sink/compactor/FileCompactStrategy.html" name="FileCompactStrategy">}} specifies
when and which files get compacted. Currently, there are two parallel conditions: the target file size and the number of checkpoints get passed.
Once the total size of the cached files has reached the size threshold or the number of checkpoints since the last compaction has reached the specified number, 
the cached files will be scheduled to compact.

The {{< javadoc file="org/apache/flink/connector/file/sink/compactor/FileCompactor.html" name="FileCompactor">}} specifies how to compact
the give list of `Path` and write the result file. It could be classified into two types according to how to write the file:

- **{{< javadoc file="org/apache/flink/connector/file/sink/compactor/OutputStreamBasedFileCompactor.html" name="OutputStreamBasedFileCompactor">}}**: 
  The users can write the compacted results into an output stream. This is useful when the users don't want to or can't read records from the input files. 
  An example is the {{< javadoc file="org/apache/flink/connector/file/sink/compactor/ConcatFileCompactor.html" name="ConcatFileCompactor">}} that concats the list of files directly.
- **{{< javadoc file="org/apache/flink/connector/file/sink/compactor/RecordWiseFileCompactor.html" name="RecordWiseFileCompactor">}}**: 
  The compactor can read records one-by-one from the input files and write into the result file similar to the `FileWriter`. 
  An example is the {{< javadoc file="org/apache/flink/connector/file/sink/compactor/RecordWiseFileCompactor.html" name="RecordWiseFileCompactor">}} that reads records from the source files and then writes them with the `CompactingFileWriter`. Users need to specify how to read records from the source files.

{{< hint info >}}
**Important Note 1** Once the compaction is enabled, you must explicitly call `disableCompact` when building the `FileSink` if you want to disable compaction.

**Important Note 2** When the compaction is enabled, the written files need to wait for longer time before they get visible.
{{< /hint >}}

### Important Considerations

#### General

<span class="label label-danger">Important Note 1</span>: When using Hadoop < 2.7, please use
the `OnCheckpointRollingPolicy` which rolls part files on every checkpoint. The reason is that if part files "traverse"
the checkpoint interval, then, upon recovery from a failure the `FileSink` may use the `truncate()` method of the 
filesystem to discard uncommitted data from the in-progress file. This method is not supported by pre-2.7 Hadoop versions 
and Flink will throw an exception.

<span class="label label-danger">Important Note 2</span>: Given that Flink sinks and UDFs in general do not differentiate between
normal job termination (*e.g.* finite input stream) and termination due to failure, upon normal termination of a job, the last 
in-progress files will not be transitioned to the "finished" state.

<span class="label label-danger">Important Note 3</span>: Flink and the `FileSink` never overwrites committed data.
Given this, when trying to restore from an old checkpoint/savepoint which assumes an in-progress file which was committed
by subsequent successful checkpoints, the `FileSink` will refuse to resume and will throw an exception as it cannot locate the 
in-progress file.

<span class="label label-danger">Important Note 4</span>: Currently, the `FileSink` only supports three filesystems: 
HDFS, S3, and Local. Flink will throw an exception when using an unsupported filesystem at runtime.

#### BATCH-specific

<span class="label label-danger">Important Note 1</span>: Although the `Writer` is executed with the user-specified
parallelism, the `Committer` is executed with parallelism equal to 1.

<span class="label label-danger">Important Note 2</span>: Pending files are committed, i.e. transition to `Finished` 
state, after the whole input has been processed.

<span class="label label-danger">Important Note 3</span>: When High-Availability is activated, if a `JobManager` 
failure happens while the `Committers` are committing, then we may have duplicates. This is going to be fixed in  
future Flink versions 
(see progress in [FLIP-147](https://cwiki.apache.org/confluence/display/FLINK/FLIP-147%3A+Support+Checkpoints+After+Tasks+Finished)).

#### S3-specific

<span class="label label-danger">Important Note 1</span>: For S3, the `FileSink`
supports only the [Hadoop-based](https://hadoop.apache.org/) FileSystem implementation, not
the implementation based on [Presto](https://prestodb.io/). In case your job uses the
`FileSink` to write to S3 but you want to use the Presto-based one for checkpointing,
it is advised to use explicitly *"s3a://"* (for Hadoop) as the scheme for the target path of
the sink and *"s3p://"* for checkpointing (for Presto). Using *"s3://"* for both the sink
and checkpointing may lead to unpredictable behavior, as both implementations "listen" to that scheme.

<span class="label label-danger">Important Note 2</span>: To guarantee exactly-once semantics while
being efficient, the `FileSink` uses the [Multi-part Upload](https://docs.aws.amazon.com/AmazonS3/latest/dev/mpuoverview.html)
feature of S3 (MPU from now on). This feature allows to upload files in independent chunks (thus the "multi-part")
which can be combined into the original file when all the parts of the MPU are successfully uploaded.
For inactive MPUs, S3 supports a bucket lifecycle rule that the user can use to abort multipart uploads
that don't complete within a specified number of days after being initiated. This implies that if you set this rule
aggressively and take a savepoint with some part-files being not fully uploaded, their associated MPUs may time-out
before the job is restarted. This will result in your job not being able to restore from that savepoint as the
pending part-files are no longer there and Flink will fail with an exception as it tries to fetch them and fails.

{{< top >}}

