---
title: "Table Sources & Sinks"
nav-parent_id: tableapi
nav-pos: 40
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

A `TableSource` provides access to data which is stored in external systems (database, key-value store, message queue) or files. After a [TableSource is registered in a TableEnvironment](common.html#register-a-tablesource) it can accessed by [Table API](tableApi.html) or [SQL](sql.html) queries.

A TableSink [emits a Table](common.html#emit-a-table) to an external storage system, such as a database, key-value store, message queue, or file system (in different encodings, e.g., CSV, Parquet, or ORC). 

Have a look at the [common concepts and API](common.html) page for details how to [register a TableSource](common.html#register-a-tablesource) and how to [emit a Table through a TableSink](common.html#emit-a-table).

* This will be replaced by the TOC
{:toc}

Provided TableSources
---------------------

Currently, Flink provides the `CsvTableSource` to read CSV files and a few table sources to read JSON or Avro data from Kafka.
A custom `TableSource` can be defined by implementing the `BatchTableSource` or `StreamTableSource` interface. See section on [defining a custom TableSource](#define-a-tablesource) for details.

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `Kafka011AvroTableSource` | `flink-connector-kafka-0.11` | N | Y | A `TableSource` for Avro-encoded Kafka 0.11 topics.
| `Kafka011JsonTableSource` | `flink-connector-kafka-0.11` | N | Y | A `TableSource` for flat Json-encoded Kafka 0.11 topics.
| `Kafka010AvroTableSource` | `flink-connector-kafka-0.10` | N | Y | A `TableSource` for Avro-encoded Kafka 0.10 topics.
| `Kafka010JsonTableSource` | `flink-connector-kafka-0.10` | N | Y | A `TableSource` for flat Json-encoded Kafka 0.10 topics.
| `Kafka09AvroTableSource` | `flink-connector-kafka-0.9` | N | Y | A `TableSource` for Avro-encoded Kafka 0.9 topics.
| `Kafka09JsonTableSource` | `flink-connector-kafka-0.9` | N | Y | A `TableSource` for flat Json-encoded Kafka 0.9 topics.
| `Kafka08AvroTableSource` | `flink-connector-kafka-0.8` | N | Y | A `TableSource` for Avro-encoded Kafka 0.8 topics.
| `Kafka08JsonTableSource` | `flink-connector-kafka-0.8` | N | Y | A `TableSource` for flat Json-encoded Kafka 0.8 topics.
| `CsvTableSource` | `flink-table` | Y | Y | A simple `TableSource` for CSV files.
| `OrcTableSource` | `flink-orc` | Y | N | A `TableSource` for ORC files.

All sources that come with the `flink-table` dependency are directly available for Table API or SQL programs. For all other table sources, you have to add the respective dependency in addition to the `flink-table` dependency.

{% top %}

### KafkaJsonTableSource

A `KafkaJsonTableSource` ingests JSON-encoded messages from a Kafka topic. Currently, only JSON records with flat (non-nested) schema are supported.

A `KafkaJsonTableSource` is created and configured using a builder. The following example shows how to create a `KafkaJsonTableSource` with basic properties:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// create builder
KafkaTableSource source = Kafka010JsonTableSource.builder()
  // set Kafka topic
  .forTopic("sensors")
  // set Kafka consumer properties
  .withKafkaProperties(kafkaProps)
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())  
    .field("temp", Types.DOUBLE())
    .field("time", Types.SQL_TIMESTAMP()).build())
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// create builder
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // set Kafka topic
  .forTopic("sensors")
  // set Kafka consumer properties
  .withKafkaProperties(kafkaProps)
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temp", Types.DOUBLE)
    .field("time", Types.SQL_TIMESTAMP).build())
  .build()
{% endhighlight %}
</div>
</div>

#### Optional Configuration

* **Time Attributes:** Please see the sections on [configuring a rowtime attribute](#configure-a-rowtime-attribute) and [configuring a processing time attribute](#configure-a-processing-time-attribute).

* **Explicit JSON parse schema:** By default, the JSON records are parsed with the table schema. You can configure an explicit JSON schema and provide a mapping from table schema fields to JSON fields as shown in the following example.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Map<String, String> mapping = new HashMap<>();
mapping.put("sensorId", "id");
mapping.put("temperature", "temp");

KafkaTableSource source = Kafka010JsonTableSource.builder()
  // ...
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())
    .field("temperature", Types.DOUBLE()).build())
  // set JSON parsing schema
  .forJsonSchema(TableSchema.builder()
    .field("id", Types.LONG())
    .field("temp", Types.DOUBLE()).build())
  // set mapping from table fields to JSON fields
  .withTableToJsonMapping(mapping)
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // ...
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temperature", Types.DOUBLE).build())
  // set JSON parsing schema
  .forJsonSchema(TableSchema.builder()
    .field("id", Types.LONG)
    .field("temp", Types.DOUBLE).build())
  // set mapping from table fields to JSON fields
  .withTableToJsonMapping(Map(
    "sensorId" -> "id", 
    "temperature" -> "temp").asJava)
  .build()
{% endhighlight %}
</div>
</div>

* **Missing Field Handling:** By default, a missing JSON field is set to `null`. You can enable strict JSON parsing that will cancel the source (and query) if a field is missing.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
KafkaTableSource source = Kafka010JsonTableSource.builder()
  // ...
  // configure missing field behavior
  .failOnMissingField(true)
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // ...
  // configure missing field behavior
  .failOnMissingField(true)
  .build()
{% endhighlight %}
</div>
</div>

* **Specify the start reading position:** By default, the table source will start reading data from the committed group offsets in Zookeeper or Kafka brokers. You can specify other start positions via the builder's methods, which correspond to the configurations in section [Kafka Consumers Start Position Configuration](../connectors/kafka.html#kafka-consumers-start-position-configuration).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
KafkaTableSource source = Kafka010JsonTableSource.builder()
  // ...
  // start reading from the earliest offset
  .fromEarliest()
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // ...
  // start reading from the earliest offset
  .fromEarliest()
  .build()
{% endhighlight %}
</div>
</div>

{% top %}

### KafkaAvroTableSource

A `KafkaAvroTableSource` ingests Avro-encoded records from a Kafka topic.

A `KafkaAvroTableSource` is created and configured using a builder. The following example shows how to create a `KafkaAvroTableSource` with basic properties:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
// create builder
KafkaTableSource source = Kafka010AvroTableSource.builder()
  // set Kafka topic
  .forTopic("sensors")
  // set Kafka consumer properties
  .withKafkaProperties(kafkaProps)
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())
    .field("temp", Types.DOUBLE())
    .field("time", Types.SQL_TIMESTAMP()).build())
  // set class of Avro record
  .forAvroRecordClass(SensorReading.class)
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
// create builder
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // set Kafka topic
  .forTopic("sensors")
  // set Kafka consumer properties
  .withKafkaProperties(kafkaProps)
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temp", Types.DOUBLE)
    .field("time", Types.SQL_TIMESTAMP).build())
  // set class of Avro record
  .forAvroRecordClass(classOf[SensorReading])
  .build()
{% endhighlight %}
</div>
</div>

**NOTE:** The specified Avro record class must provide all fields of the table schema with corresponding type.

#### Optional Configuration

* **Time Attributes:** Please see the sections on [configuring a rowtime attribute](#configure-a-rowtime-attribute) and [configuring a processing time attribute](#configure-a-processing-time-attribute).

* **Explicit Schema Field to Avro Mapping:** By default, all fields of the table schema are mapped by name to fields of the Avro records. If the fields in the Avro records have different names, a mapping from table schema fields to Avro fields can be specified.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Map<String, String> mapping = new HashMap<>();
mapping.put("sensorId", "id");
mapping.put("temperature", "temp");

KafkaTableSource source = Kafka010AvroTableSource.builder()
  // ...
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())
    .field("temperature", Types.DOUBLE()).build())
  // set class of Avro record with fields [id, temp]
  .forAvroRecordClass(SensorReading.class)
  // set mapping from table fields to Avro fields
  .withTableToAvroMapping(mapping)
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010AvroTableSource.builder()
  // ...
  // set Table schema
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temperature", Types.DOUBLE).build())
  // set class of Avro record with fields [id, temp]
  .forAvroRecordClass(classOf[SensorReading])
  // set mapping from table fields to Avro fields
  .withTableToAvroMapping(Map(
    "sensorId" -> "id", 
    "temperature" -> "temp").asJava)
  .build()
{% endhighlight %}
</div>
</div>

* **Specify the start reading position:** By default, the table source will start reading data from the committed group offsets in Zookeeper or Kafka brokers. You can specify other start positions via the builder's methods, which correspond to the configurations in section [Kafka Consumers Start Position Configuration](../connectors/kafka.html#kafka-consumers-start-position-configuration).

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
KafkaTableSource source = Kafka010AvroTableSource.builder()
  // ...
  // start reading from the earliest offset
  .fromEarliest()
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010AvroTableSource.builder()
  // ...
  // start reading from the earliest offset
  .fromEarliest()
  .build()
{% endhighlight %}
</div>
</div>

{% top %}

### Configuring a Processing Time Attribute

[Processing time attributes](streaming.html#processing-time) are commonly used in streaming queries. A processing time attribute returns the current wall-clock time of the operator that accesses it. 

Batch queries support processing time attributes as well. However, processing time attributes are initialized with the wall-clock time of the table scan operator and keep this value throughout the query evaluation. 

A table schema field of type `SQL_TIMESTAMP` can be declared as a processing time attribute as shown in the following example.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
KafkaTableSource source = Kafka010JsonTableSource.builder()
  // ... 
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())  
    .field("temp", Types.DOUBLE())
    // field "ptime" is of type SQL_TIMESTAMP
    .field("ptime", Types.SQL_TIMESTAMP()).build())
  // declare "ptime" as processing time attribute
  .withProctimeAttribute("ptime")
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // ...
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temp", Types.DOUBLE)
    // field "ptime" is of type SQL_TIMESTAMP
    .field("ptime", Types.SQL_TIMESTAMP).build())
  // declare "ptime" as processing time attribute
  .withProctimeAttribute("ptime")
  .build()
{% endhighlight %}
</div>
</div>

{% top %}

### Configuring a Rowtime Attribute

[Rowtime attributes](streaming.html#event-time) are attributes of type `TIMESTAMP` and handled in a unified way in stream and batch queries.

A table schema field of type `SQL_TIMESTAMP` can be declared as rowtime attribute by specifying 

* the name of the field, 
* a `TimestampExtractor` that computes the actual value for the attribute (usually from one or more other attributes), and
* a `WatermarkStrategy` that specifies how watermarks are generated for the the rowtime attribute.

The following example shows how to configure a rowtime attribute.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
KafkaTableSource source = Kafka010JsonTableSource.builder()
  // ...
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())
    .field("temp", Types.DOUBLE())
    // field "rtime" is of type SQL_TIMESTAMP
    .field("rtime", Types.SQL_TIMESTAMP()).build())
  .withRowtimeAttribute(
    // "rtime" is rowtime attribute
    "rtime",
    // value of "rtime" is extracted from existing field with same name
    new ExistingField("rtime"),
    // values of "rtime" are at most out-of-order by 30 seconds
    new BoundedOutOfOrderWatermarks(30000L))
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // ...
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temp", Types.DOUBLE)
    // field "rtime" is of type SQL_TIMESTAMP
    .field("rtime", Types.SQL_TIMESTAMP).build())
  .withRowtimeAttribute(
    // "rtime" is rowtime attribute
    "rtime",
    // value of "rtime" is extracted from existing field with same name
    new ExistingField("rtime"),
    // values of "rtime" are at most out-of-order by 30 seconds
    new BoundedOutOfOrderTimestamps(30000L))
  .build()
{% endhighlight %}
</div>
</div>

#### Extracting Kafka 0.10+ Timestamps into Rowtime Attribute

Since Kafka 0.10, Kafka messages have a timestamp as metadata that specifies when the record was written into the Kafka topic. `KafkaTableSources` can assign Kafka's message timestamp as rowtime attribute as follows: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
KafkaTableSource source = Kafka010JsonTableSource.builder()
  // ...
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG())
    .field("temp", Types.DOUBLE())
    // field "rtime" is of type SQL_TIMESTAMP
    .field("rtime", Types.SQL_TIMESTAMP()).build())
  // use Kafka timestamp as rowtime attribute
  .withKafkaTimestampAsRowtimeAttribute()(
    // "rtime" is rowtime attribute
    "rtime",
    // values of "rtime" are ascending
    new AscendingTimestamps())
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val source: KafkaTableSource = Kafka010JsonTableSource.builder()
  // ...
  .withSchema(TableSchema.builder()
    .field("sensorId", Types.LONG)
    .field("temp", Types.DOUBLE)
    // field "rtime" is of type SQL_TIMESTAMP
    .field("rtime", Types.SQL_TIMESTAMP).build())
  // use Kafka timestamp as rowtime attribute
  .withKafkaTimestampAsRowtimeAttribute()(
    // "rtime" is rowtime attribute
    "rtime",
    // values of "rtime" are ascending
    new AscendingTimestamps())
  .build()
{% endhighlight %}
</div>
</div>

#### Provided TimestampExtractors

Flink provides `TimestampExtractor` implementations for common use cases.
The following `TimestampExtractor` implementations are currently available:

* `ExistingField(fieldName)`: Extracts the value of a rowtime attribute from an existing `LONG` or `SQL_TIMESTAMP` field.
* `StreamRecordTimestamp()`: Extracts the value of a rowtime attribute from the timestamp of the `DataStream` `StreamRecord`. Note, this `TimestampExtractor` is not available for batch table sources.

A custom `TimestampExtractor` can be defined by implementing the corresponding interface.

#### Provided WatermarkStrategies

Flink provides `WatermarkStrategy` implementations for common use cases.
The following `WatermarkStrategy` implementations are currently available:

* `AscendingTimestamps`: A watermark strategy for ascending timestamps. Records with timestamps that are out-of-order will be considered late.
* `BoundedOutOfOrderTimestamps(delay)`: A watermark strategy for timestamps that are at most out-of-order by the specified delay.

A custom `WatermarkStrategy` can be defined by implementing the corresponding interface.

{% top %}

### CsvTableSource

The `CsvTableSource` is already included in `flink-table` without additional dependencies.

The easiest way to create a `CsvTableSource` is by using the enclosed builder `CsvTableSource.builder()`, the builder has the following methods to configure properties:

 - `path(String path)` Sets the path to the CSV file, required.
 - `field(String fieldName, TypeInformation<?> fieldType)` Adds a field with the field name and field type information, can be called multiple times, required. The call order of this method defines also the order of the fields in a row.
 - `fieldDelimiter(String delim)` Sets the field delimiter, `","` by default.
 - `lineDelimiter(String delim)` Sets the line delimiter, `"\n"` by default.
 - `quoteCharacter(Character quote)` Sets the quote character for String values, `null` by default.
 - `commentPrefix(String prefix)` Sets a prefix to indicate comments, `null` by default.
 - `ignoreFirstLine()` Ignore the first line. Disabled by default.
 - `ignoreParseErrors()` Skip records with parse error instead to fail. Throwing an exception by default.

You can create the source as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
CsvTableSource csvTableSource = CsvTableSource
    .builder()
    .path("/path/to/your/file.csv")
    .field("name", Types.STRING())
    .field("id", Types.INT())
    .field("score", Types.DOUBLE())
    .field("comments", Types.STRING())
    .fieldDelimiter("#")
    .lineDelimiter("$")
    .ignoreFirstLine()
    .ignoreParseErrors()
    .commentPrefix("%")
    .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val csvTableSource = CsvTableSource
    .builder
    .path("/path/to/your/file.csv")
    .field("name", Types.STRING)
    .field("id", Types.INT)
    .field("score", Types.DOUBLE)
    .field("comments", Types.STRING)
    .fieldDelimiter("#")
    .lineDelimiter("$")
    .ignoreFirstLine
    .ignoreParseErrors
    .commentPrefix("%")
    .build
{% endhighlight %}
</div>
</div>

{% top %}

### OrcTableSource

The `OrcTableSource` reads [ORC files](https://orc.apache.org). ORC is a file format for structured data and stores the data in a compressed, columnar representation. ORC is very storage efficient and supports projection and filter push-down.

An `OrcTableSource` is created as shown below:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

// create Hadoop Configuration
Configuration config = new Configuration();

OrcTableSource orcTableSource = OrcTableSource.builder()
  // path to ORC file(s). NOTE: By default, directories are recursively scanned.
  .path("file:///path/to/data")
  // schema of ORC files
  .forOrcSchema("struct<name:string,addresses:array<struct<street:string,zip:smallint>>>")
  // Hadoop configuration
  .withConfiguration(config)
  // build OrcTableSource
  .build();
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

// create Hadoop Configuration
val config = new Configuration()

val orcTableSource = OrcTableSource.builder()
  // path to ORC file(s). NOTE: By default, directories are recursively scanned.
  .path("file:///path/to/data")
  // schema of ORC files
  .forOrcSchema("struct<name:string,addresses:array<struct<street:string,zip:smallint>>>")
  // Hadoop configuration
  .withConfiguration(config)
  // build OrcTableSource
  .build()
{% endhighlight %}
</div>
</div>

**Note:** The `OrcTableSource` does not support ORC's `Union` type yet.

{% top %}

Provided TableSinks
-------------------

The following table lists the `TableSink`s which are provided with Flink.

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `CsvTableSink` | `flink-table` | Y | Append | A simple sink for CSV files.
| `JDBCAppendTableSink` | `flink-jdbc` | Y | Append | Writes a Table to a JDBC table.
| `CassandraAppendTableSink` | `flink-connector-cassandra` | N | Append | Writes a Table to a Cassandra table. 
| `Kafka08JsonTableSink` | `flink-connector-kafka-0.8` | N | Append | A Kafka 0.8 sink with JSON encoding.
| `Kafka09JsonTableSink` | `flink-connector-kafka-0.9` | N | Append | A Kafka 0.9 sink with JSON encoding.
| `Kafka010JsonTableSink` | `flink-connector-kafka-0.10` | N | Append | A Kafka 0.10 sink with JSON encoding.

All sinks that come with the `flink-table` dependency can be directly used by your Table programs. For all other table sinks, you have to add the respective dependency in addition to the `flink-table` dependency.

A custom `TableSink` can be defined by implementing the `BatchTableSink`, `AppendStreamTableSink`, `RetractStreamTableSink`, or `UpsertStreamTableSink` interface. See section on [defining a custom TableSink](#define-a-tablesink) for details.

{% top %}

### KafkaJsonTableSink

A `KafkaJsonTableSink` emits a [streaming append `Table`](./streaming.html#table-to-stream-conversion) to an Apache Kafka topic. The rows of the table are encoded as JSON records. Currently, only tables with flat schema, i.e., non-nested fields, are supported. 

A `KafkaJsonTableSink` produces with at-least-once guarantees into a Kafka topic if the query is executed with [checkpointing enabled]({{ site.baseurl }}/dev/stream/state/checkpointing.html#enabling-and-configuring-checkpointing). 

By default, a `KafkaJsonTableSink` writes to at most as many partitions as its own parallelism (each parallel instance of the sink writes to exactly one partition). In order to distribute the writes to more partitions or control the routing of rows into partitions, a custom `FlinkKafkaPartitioner` can be provided.

The following example shows how to create a `KafkaJsonTableSink` for Kafka 0.10. Sinks for Kafka 0.8 and 0.9 are instantiated analogously. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

Table table = ...

Properties props = new Properties();
props.setProperty("bootstrap.servers", "localhost:9092");

table.writeToSink(
  new Kafka010JsonTableSink(
    "myTopic",                // Kafka topic to write to
    props));                  // Properties to configure the producer

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

val table: Table = ???

val props = new Properties()
props.setProperty("bootstrap.servers", "localhost:9092")

table.writeToSink(
  new Kafka010JsonTableSink(
    "myTopic",                // Kafka topic to write to
    props))                   // Properties to configure the producer
  
{% endhighlight %}
</div>
</div>

### CsvTableSink

The `CsvTableSink` emits a `Table` to one or more CSV files. 

The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming.html#table-to-stream-conversion) for details. When emitting a streaming table, rows are written at least once (if checkpointing is enabled) and the `CsvTableSink` does not split output files into bucket files but continuously writes to the same files. 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

Table table = ...

table.writeToSink(
  new CsvTableSink(
    path,                  // output path 
    "|",                   // optional: delimit files by '|'
    1,                     // optional: write to a single file
    WriteMode.OVERWRITE)); // optional: override existing files

{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

val table: Table = ???

table.writeToSink(
  new CsvTableSink(
    path,                             // output path 
    fieldDelim = "|",                 // optional: delimit files by '|'
    numFiles = 1,                     // optional: write to a single file
    writeMode = WriteMode.OVERWRITE)) // optional: override existing files

{% endhighlight %}
</div>
</div>

### JDBCAppendTableSink

The `JDBCAppendTableSink` emits a `Table` to a JDBC connection. The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming.html#table-to-stream-conversion) for details. 

The `JDBCAppendTableSink` inserts each `Table` row at least once into the database table (if checkpointing is enabled). However, you can specify the insertion query using <code>REPLACE</code> or <code>INSERT OVERWRITE</code> to perform upsert writes to the database.

To use the JDBC sink, you have to add the JDBC connector dependency (<code>flink-jdbc</code>) to your project. Then you can create the sink using <code>JDBCAppendSinkBuilder</code>:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

JDBCAppendTableSink sink = JDBCAppendTableSink.builder()
  .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
  .setDBUrl("jdbc:derby:memory:ebookshop")
  .setQuery("INSERT INTO books (id) VALUES (?)")
  .setParameterTypes(INT_TYPE_INFO)
  .build();

Table table = ...
table.writeToSink(sink);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val sink: JDBCAppendTableSink = JDBCAppendTableSink.builder()
  .setDrivername("org.apache.derby.jdbc.EmbeddedDriver")
  .setDBUrl("jdbc:derby:memory:ebookshop")
  .setQuery("INSERT INTO books (id) VALUES (?)")
  .setParameterTypes(INT_TYPE_INFO)
  .build()

val table: Table = ???
table.writeToSink(sink)
{% endhighlight %}
</div>
</div>

Similar to using <code>JDBCOutputFormat</code>, you have to explicitly specify the name of the JDBC driver, the JDBC URL, the query to be executed, and the field types of the JDBC table. 

{% top %}

### CassandraAppendTableSink

The `CassandraAppendTableSink` emits a `Table` to a Cassandra table. The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming.html#table-to-stream-conversion) for details. 

The `CassandraAppendTableSink` inserts all rows at least once into the Cassandra table if checkpointing is enabled. However, you can specify the query as upsert query.

To use the `CassandraAppendTableSink`, you have to add the Cassandra connector dependency (<code>flink-connector-cassandra</code>) to your project. The example below shows how to use the `CassandraAppendTableSink`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

ClusterBuilder builder = ... // configure Cassandra cluster connection

CassandraAppendTableSink sink = new CassandraAppendTableSink(
  builder, 
  // the query must match the schema of the table
  INSERT INTO flink.myTable (id, name, value) VALUES (?, ?, ?));

Table table = ...
table.writeToSink(sink);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val builder: ClusterBuilder = ... // configure Cassandra cluster connection

val sink: CassandraAppendTableSink = new CassandraAppendTableSink(
  builder, 
  // the query must match the schema of the table
  INSERT INTO flink.myTable (id, name, value) VALUES (?, ?, ?))

val table: Table = ???
table.writeToSink(sink)
{% endhighlight %}
</div>
</div>

{% top %}

Define a TableSource
--------------------

A `TableSource` is a generic interface that gives Table API and SQL queries access to data stored in an external system. It provides the schema of the table and the records that are mapped to rows with the table's schema. Depending on whether the `TableSource` is used in a streaming or batch query, the records are produced as a `DataSet` or `DataStream`. 

If a `TableSource` is used in a streaming query it must implement the `StreamTableSource` interface, if it is used in a batch query it must implement the `BatchTableSource` interface. A `TableSource` can also implement both interfaces and be used in streaming and batch queries. 

`StreamTableSource` and `BatchTableSource` extend the base interface `TableSource` that defines the following methods:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableSource<T> {

  public TableSchema getTableSchema();

  public TypeInformation<T> getReturnType();

  public String explainSource();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
TableSource[T] {

  def getTableSchema: TableSchema

  def getReturnType: TypeInformation[T]

  def explainSource: String

}
{% endhighlight %}
</div>
</div>

* `getTableSchema()`: Returns the schema of the table, i.e., the names and types of the fields of the table. The field types are defined using Flink's `TypeInformation` (see [Table API types](tableApi.html#data-types) and [SQL types](sql.html#data-types)).

* `getReturnType()`: Returns the physical type of the `DataStream` (`StreamTableSource`) or `DataSet` (`BatchTableSource`) and the records that are produced by the `TableSource`.

* `explainSource()`: Returns a String that describes the `TableSource`. This method is optional and used for display purposes only.

The `TableSource` interface separates the logical table schema from the physical type of the returned `DataStream` or `DataSet`. As a consequence, all fields of the table schema (`getTableSchema()`) must be mapped to a field with corresponding type of the physical return type (`getReturnType()`). By default, this mapping is done based on field names. For example, a `TableSource` that defines a table schema with two fields `[name: String, size: Integer]` requires a `TypeInformation` with at least two fields called `name` and `size` of type `String` and `Integer`, respectively. This could be a `PojoTypeInfo` or a `RowTypeInfo` that have two fields named `name` and `size` with matching types. 

However, some types, such as Tuple or CaseClass types, do support custom field names. If a `TableSource` returns a `DataStream` or `DataSet` of a type with fixed field names, it can implement the `DefinedFieldMapping` interface to map field names from the table schema to field names of the physical return type.

### Defining a BatchTableSource

The `BatchTableSource` interface extends the `TableSource` interface and defines one additional method:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableSource<T> extends TableSource<T> {

  public DataSet<T> getDataSet(ExecutionEnvironment execEnv);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
BatchTableSource[T] extends TableSource[T] {

  def getDataSet(execEnv: ExecutionEnvironment): DataSet[T]
}
{% endhighlight %}
</div>
</div>

* `getDataSet(execEnv)`: Returns a `DataSet` with the data of the table. The type of the `DataSet` must be identical to the return type defined by the `TableSource.getReturnType()` method. The `DataSet` can by created using a regular [data source]({{ site.baseurl }}/dev/batch/#data-sources) of the DataSet API. Commonly, a `BatchTableSource` is implemented by wrapping a `InputFormat` or [batch connector]({{ site.baseurl }}/dev/batch/connectors.html).

{% top %}

### Defining a StreamTableSource

The `StreamTableSource` interface extends the `TableSource` interface and defines one additional method: 

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
StreamTableSource<T> extends TableSource<T> {

  public DataStream<T> getDataStream(StreamExecutionEnvironment execEnv);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
StreamTableSource[T] extends TableSource[T] {

  def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[T]
}
{% endhighlight %}
</div>
</div>

* `getDataStream(execEnv)`: Returns a `DataStream` with the data of the table. The type of the `DataStream` must be identical to the return type defined by the `TableSource.getReturnType()` method. The `DataStream` can by created using a regular [data source]({{ site.baseurl }}/dev/datastream_api.html#data-sources) of the DataStream API. Commonly, a `StreamTableSource` is implemented by wrapping a `SourceFunction` or a [stream connector]({{ site.baseurl }}/dev/connectors/).

### Defining a TableSource with Time Attributes

Time-based operations of streaming [Table API](tableApi.html#group-windows) and [SQL](sql.html#group-windows) queries, such as windowed aggregations or joins, require explicitly specified [time attributes]({{ site.baseurl }}/dev/table/streaming.html#time-attributes). 

A `TableSource` defines a time attribute as a field of type `Types.SQL_TIMESTAMP` in its table schema. In contrast to all regular fields in the schema, a time attribute must not be matched to a physical field in the return type of the table source. Instead, a `TableSource` defines a time attribute by implementing a certain interface.

#### Defining a Processing Time Attribute

A `TableSource` defines a [processing time attribute](streaming.html#processing-time) by implementing the `DefinedProctimeAttribute` interface. The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DefinedProctimeAttribute {

  public String getProctimeAttribute();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
DefinedProctimeAttribute {

  def getProctimeAttribute: String
}
{% endhighlight %}
</div>
</div>

* `getProctimeAttribute()`: Returns the name of the processing time attribute. The specified attribute must be defined of type `Types.SQL_TIMESTAMP` in the table schema and can be used in time-based operations. A `DefinedProctimeAttribute` table source can define no processing time attribute by returning `null`.

**Note** Both `StreamTableSource` and `BatchTableSource` can implement `DefinedProctimeAttribute` and define a processing time attribute. In case of a `BatchTableSource` the processing time field is initialized with the current timestamp during the table scan.

#### Defining a Rowtime Attribute

A `TableSource` defines a [rowtime attribute](streaming.html#event-time) by implementing the `DefinedRowtimeAttributes` interface. The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DefinedRowtimeAttribute {

  public List<RowtimeAttributeDescriptor> getRowtimeAttributeDescriptors();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
DefinedRowtimeAttributes {

  def getRowtimeAttributeDescriptors: util.List[RowtimeAttributeDescriptor]
}
{% endhighlight %}
</div>
</div>

* `getRowtimeAttributeDescriptors()`: Returns a list of `RowtimeAttributeDescriptor`. A `RowtimeAttributeDescriptor` describes a rowtime attribute with the following properties:
  * `attributeName`: The name of the rowtime attribute in the table schema. The field must be defined with type `Types.SQL_TIMESTAMP`.
  * `timestampExtractor`: The timestamp extractor extracts the timestamp from a record with the return type. For example, it can convert convert a Long field into a timestamp or parse a String-encoded timestamp. Flink comes with a set of built-in `TimestampExtractor` implementation for common use cases. It is also possible to provide a custom implementation.
  * `watermarkStrategy`: The watermark strategy defines how watermarks are generated for the rowtime attribute. Flink comes with a set of built-in `WatermarkStrategy` implementations for common use cases. It is also possible to provide a custom implementation.
* **Note** Although the `getRowtimeAttributeDescriptors()` method returns a list of descriptors, only a single rowtime attribute is support at the moment. We plan to remove this restriction in the future and support tables with more than one rowtime attribute.

**IMPORTANT** Both, `StreamTableSource` and `BatchTableSource`, can implement `DefinedRowtimeAttributes` and define a rowtime attribute. In either case, the rowtime field is extracted using the `TimestampExtractor`. Hence, a `TableSource` that implements `StreamTableSource` and `BatchTableSource` and defines a rowtime attribute provides exactly the same data to streaming and batch queries.

{% top %}

### Defining a TableSource with Projection Push-Down

A `TableSource` supports projection push-down by implementing the `ProjectableTableSource` interface. The interface defines a single method:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
ProjectableTableSource<T> {

  public TableSource<T> projectFields(int[] fields);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
ProjectableTableSource[T] {

  def projectFields(fields: Array[Int]): TableSource[T]
}
{% endhighlight %}
</div>
</div>

* `projectFields(fields)`: Returns a *copy* of the `TableSource` with adjusted physical return type. The `fields` parameter provides the indexes of the fields that must be provided by the `TableSource`. The indexes relate to the `TypeInformation` of the physical return type, *not* to the logical table schema. The copied `TableSource` must adjust its return type and the returned `DataStream` or `DataSet`. The `TableSchema` of the copied `TableSource` must not be changed, i.e, it must be the same as the original `TableSource`. If the `TableSource` implements the `DefinedFieldMapping` interface, the field mapping must be adjusted to the new return type.

The `ProjectableTableSource` adds support to project flat fields. If the `TableSource` defines a table with nested schema, it can implement the `NestedFieldsProjectableTableSource` to extend the projection to nested fields. The `NestedFieldsProjectableTableSource` is defined as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
NestedFieldsProjectableTableSource<T> {

  public TableSource<T> projectNestedFields(int[] fields, String[][] nestedFields);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
NestedFieldsProjectableTableSource[T] {

  def projectNestedFields(fields: Array[Int], nestedFields: Array[Array[String]]): TableSource[T]
}
{% endhighlight %}
</div>
</div>

* `projectNestedField(fields, nestedFields)`: Returns a *copy* of the `TableSource` with adjusted physical return type. Fields of the physical return type may be removed or reordered but their type must not be changed. The contract of this method is essentially the same as for the `ProjectableTableSource.projectFields()` method. In addition, the `nestedFields` parameter contains for each field index in the `fields` list, a list of paths to all nested fields that are accessed by the query. All other nested fields do not need to be read, parsed, and set in the records that are produced by the `TableSource`. **IMPORTANT** the types of the projected fields must not be changed but unused fields may be set to null or to a default value.

{% top %}

### Defining a TableSource with Filter Push-Down

The `FilterableTableSource` interface adds support for filter push-down to a `TableSource`. A `TableSource` extending this interface is able to filter records such that the returned `DataStream` or `DataSet` returns fewer records.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
FilterableTableSource<T> {

  public TableSource<T> applyPredicate(List<Expression> predicates);

  public boolean isFilterPushedDown();
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
FilterableTableSource[T] {

  def applyPredicate(predicates: java.util.List[Expression]): TableSource[T]

  def isFilterPushedDown: Boolean
}
{% endhighlight %}
</div>
</div>

* `applyPredicate(predicates)`: Returns a *copy* of the `TableSource` with added predicates. The `predicates` parameter is a mutable list of conjunctive predicates that are "offered" to the `TableSource`. The `TableSource` accepts to evaluate a predicate by removing it from the list. Predicates that are left in the list will be evaluated by a subsequent filter operator. 
* `isFilterPushedDown()`: Returns true if the `applyPredicate()` method was called before. Hence, `isFilterPushedDown()` must return true for all `TableSource` instances returned from a `applyPredicate()` call.

{% top %}

Define a TableSink
------------------

A `TableSink` specifies how to emit a `Table` to an external system or location. The interface is generic such that it can support different storage locations and formats. There are different table sinks for batch tables and streaming tables.

The general interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
TableSink<T> {

  public TypeInformation<T> getOutputType();

  public String[] getFieldNames();

  public TypeInformation[] getFieldTypes();

  public TableSink<T> configure(String[] fieldNames, TypeInformation[] fieldTypes);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
TableSink[T] {

  def getOutputType: TypeInformation<T>

  def getFieldNames: Array[String]

  def getFieldTypes: Array[TypeInformation]

  def configure(fieldNames: Array[String], fieldTypes: Array[TypeInformation]): TableSink[T]
}
{% endhighlight %}
</div>
</div>

The `TableSink#configure` method is called to pass the schema of the Table (field names and types) to emit to the `TableSink`. The method must return a new instance of the TableSink which is configured to emit the provided Table schema.

### BatchTableSink

Defines an external `TableSink` to emit a batch table.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
BatchTableSink<T> extends TableSink<T> {

  public void emitDataSet(DataSet<T> dataSet);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
BatchTableSink[T] extends TableSink[T] {

  def emitDataSet(dataSet: DataSet[T]): Unit
}
{% endhighlight %}
</div>
</div>

{% top %}

### AppendStreamTableSink

Defines an external `TableSink` to emit a streaming table with only insert changes.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
AppendStreamTableSink<T> extends TableSink<T> {

  public void emitDataStream(DataStream<T> dataStream);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
AppendStreamTableSink[T] extends TableSink[T] {

  def emitDataStream(dataStream: DataStream<T>): Unit
}
{% endhighlight %}
</div>
</div>

If the table is also modified by update or delete changes, a `TableException` will be thrown.

{% top %}

### RetractStreamTableSink

Defines an external `TableSink` to emit a streaming table with insert, update, and delete changes.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
RetractStreamTableSink<T> extends TableSink<Tuple2<Boolean, T>> {

  public TypeInformation<T> getRecordType();

  public void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
RetractStreamTableSink[T] extends TableSink[Tuple2[Boolean, T]] {

  def getRecordType: TypeInformation[T]

  def emitDataStream(dataStream: DataStream[Tuple2[Boolean, T]]): Unit
}
{% endhighlight %}
</div>
</div>

The table will be converted into a stream of accumulate and retraction messages which are encoded as Java `Tuple2`. The first field is a boolean flag to indicate the message type (`true` indicates insert, `false` indicates delete). The second field holds the record of the requested type `T`.

{% top %}

### UpsertStreamTableSink

Defines an external `TableSink` to emit a streaming table with insert, update, and delete changes.

The interface looks as follows:

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
UpsertStreamTableSink<T> extends TableSink<Tuple2<Boolean, T>> {

  public void setKeyFields(String[] keys);

  public void setIsAppendOnly(boolean isAppendOnly);

  public TypeInformation<T> getRecordType();

  public void emitDataStream(DataStream<Tuple2<Boolean, T>> dataStream);
}
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
UpsertStreamTableSink[T] extends TableSink[Tuple2[Boolean, T]] {

  def setKeyFields(keys: Array[String]): Unit

  def setIsAppendOnly(isAppendOnly: Boolean): Unit

  def getRecordType: TypeInformation[T]

  def emitDataStream(dataStream: DataStream[Tuple2[Boolean, T]]): Unit
}
{% endhighlight %}
</div>
</div>

The table must be have unique key fields (atomic or composite) or be append-only. If the table does not have a unique key and is not append-only, a `TableException` will be thrown. The unique key of the table is configured by the `UpsertStreamTableSink#setKeyFields()` method.

The table will be converted into a stream of upsert and delete messages which are encoded as a Java `Tuple2`. The first field is a boolean flag to indicate the message type. The second field holds the record of the requested type `T`.

A message with true boolean field is an upsert message for the configured key. A message with false flag is a delete message for the configured key. If the table is append-only, all messages will have a true flag and must be interpreted as insertions.

{% top %}

