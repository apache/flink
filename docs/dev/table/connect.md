---
title: "Connect to External Systems"
nav-parent_id: tableapi
nav-pos: 19
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

Flink's Table API & SQL programs can be connected to other external systems for reading and writing both batch and streaming tables. A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system). A table sink emits a table to an external storage system. Depending on the type of source and sink, they support different formats such as CSV, Parquet, or ORC.

This page describes how to declare built-in table sources and/or table sinks and register them in Flink. After a source or sink has been registered, it can be accessed by Table API & SQL statements.

<span class="label label-danger">Attention</span> If you want to implement your own *custom* table source or sink, have a look at the [user-defined sources & sinks page](sourceSinks.html).

* This will be replaced by the TOC
{:toc}

Dependencies
------------

The following table list all available connectors and formats. Their mutual compatibility is tagged in the corresponding sections for [table connectors](connect.html#table-connectors) and [table formats](connect.html#table-formats). The following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{% if site.is_stable %}

### Connectors

| Name              | Version       | Maven dependency             | SQL Client JAR         |
| :---------------- | :------------ | :--------------------------- | :----------------------|
| Filesystem        |               | Built-in                     | Built-in               |
| Apache Kafka      | 0.8           | `flink-connector-kafka-0.8`  | Not available          |
| Apache Kafka      | 0.9           | `flink-connector-kafka-0.9`  | [Download](http://central.maven.org/maven2/org/apache/flink/flink-connector-kafka-0.9{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka-0.9{{site.scala_version_suffix}}-{{site.version}}-sql-jar.jar) |
| Apache Kafka      | 0.10          | `flink-connector-kafka-0.10` | [Download](http://central.maven.org/maven2/org/apache/flink/flink-connector-kafka-0.10{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka-0.10{{site.scala_version_suffix}}-{{site.version}}-sql-jar.jar) |
| Apache Kafka      | 0.11          | `flink-connector-kafka-0.11` | [Download](http://central.maven.org/maven2/org/apache/flink/flink-connector-kafka-0.11{{site.scala_version_suffix}}/{{site.version}}/flink-connector-kafka-0.11{{site.scala_version_suffix}}-{{site.version}}-sql-jar.jar) |

### Formats

| Name              | Maven dependency             | SQL Client JAR         |
| :---------------- | :--------------------------- | :--------------------- |
| CSV               | Built-in                     | Built-in               |
| JSON              | `flink-json`                 | [Download](http://central.maven.org/maven2/org/apache/flink/flink-json/{{site.version}}/flink-json-{{site.version}}-sql-jar.jar) |
| Apache Avro       | `flink-avro`                 | [Download](http://central.maven.org/maven2/org/apache/flink/flink-avro/{{site.version}}/flink-avro-{{site.version}}-sql-jar.jar) |

{% else %}

This table is only available for stable releases.

{% endif %}

{% top %}

Overview
--------

Beginning from Flink 1.6, the declaration of a connection to an external system is separated from the actual implementation.

Connections can be specified either

- **programmatically** using a `Descriptor` under `org.apache.flink.table.descriptors` for Table & SQL API
- or **declaratively** via [YAML configuration files](http://yaml.org/) for the SQL Client.

This allows not only for better unification of APIs and SQL Client but also for better extensibility in case of [custom implementations](sourceSinks.html) without changing the actual declaration.

Every declaration is similar to a SQL `CREATE TABLE` statement. One can define the name of the table, the schema of the table, a connector, and a data format upfront for connecting to an external system.

The **connector** describes the external system that stores the data of a table. Storage systems such as [Apacha Kafka](http://kafka.apache.org/) or a regular file system can be declared here. The connector might already provide a fixed format with fields and schema.

Some systems support different **data formats**. For example, a table that is stored in Kafka or in files can encode its rows with CSV, JSON, or Avro. A database connector might need the table schema here. Whether or not a storage system requires the definition of a format, is documented for every [connector](connect.html#table-connectors). Different systems also require different [types of formats](connect.html#table-formats) (e.g., column-oriented formats vs. row-oriented formats). The documentation states which format types and connectors are compatible.

The **table schema** defines the schema of a table that is exposed to SQL queries. It describes how a source maps the data format to the table schema and a sink vice versa. The schema has access to fields defined by the connector or format. It can use one or more fields for extracting or inserting [time attributes](streaming.html#time-attributes). If input fields have no determinstic field order, the schema clearly defines column names, their order, and origin.

The subsequent sections will cover each definition part ([connector](connect.html#table-connectors), [format](connect.html#table-formats), and [schema](connect.html#table-schema)) in more detail. The following example shows how to pass them:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnvironment
  .connect(...)
  .withFormat(...)
  .withSchema(...)
  .inAppendMode()
  .registerTableSource("MyTable")
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
name: MyTable
type: source
update-mode: append
connector: ...
format: ...
schema: ...
{% endhighlight %}
</div>
</div>

The table's type (`source`, `sink`, or `both`) determines how a table is registered. In case of table type `both`, both a table source and table sink are registered under the same name. Logically, this means that we can both read and write to such a table similarly to a table in a regular DBMS.

For streaming queries, an [update mode](connect.html#update-mode) declares how to communicate between a dynamic table and the storage system for continous queries.

The following code shows a full example of how to connect to Kafka for reading Avro records.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
tableEnvironment
  // declare the external system to connect to
  .connect(
    new Kafka()
      .version("0.10")
      .topic("test-input")
      .startFromEarliest()
      .property("zookeeper.connect", "localhost:2181")
      .property("bootstrap.servers", "localhost:9092")
  )

  // declare a format for this system
  .withFormat(
    new Avro()
      .avroSchema(
        "{" +
        "  \"namespace\": \"org.myorganization\"," +
        "  \"type\": \"record\"," +
        "  \"name\": \"UserMessage\"," +
        "    \"fields\": [" +
        "      {\"name\": \"timestamp\", \"type\": \"string\"}," +
        "      {\"name\": \"user\", \"type\": \"long\"}," +
        "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}" +
        "    ]" +
        "}" +
      )
  )

  // declare the schema of the table
  .withSchema(
    new Schema()
      .field("rowtime", Types.SQL_TIMESTAMP)
        .rowtime(new Rowtime()
          .timestampsFromField("ts")
          .watermarksPeriodicBounded(60000)
        )
      .field("user", Types.LONG)
      .field("message", Types.STRING)
  )

  // specify the update-mode for streaming tables
  .inAppendMode()

  // register as source, sink, or both and under a name
  .registerTableSource("MyUserTable");
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
tables:
  - name: MyUserTable      # name the new table
    type: source           # declare if the table should be "source", "sink", or "both"
    update-mode: append    # specify the update-mode for streaming tables

    # declare the external system to connect to
    connector:
      type: kafka
      version: "0.10"
      topic: test-input
      startup-mode: earliest-offset
      properties:
        - key: zookeeper.connect
          value: localhost:2181
        - key: bootstrap.servers
          value: localhost:9092

    # declare a format for this system
    format:
      type: avro
      avro-schema: >
        {
          "namespace": "org.myorganization",
          "type": "record",
          "name": "UserMessage",
            "fields": [
              {"name": "ts", "type": "string"},
              {"name": "user", "type": "long"},
              {"name": "message", "type": ["string", "null"]}
            ]
        }

    # declare the schema of the table
    schema:
      - name: rowtime
        type: TIMESTAMP
        rowtime:
          timestamps:
            type: from-field
            from: ts
          watermarks:
            type: periodic-bounded
            delay: "60000"
      - name: user
        type: BIGINT
      - name: message
        type: VARCHAR
{% endhighlight %}
</div>
</div>

In both ways the desired connection properties are converted into normalized, string-based key-value pairs. So-called [table factories](sourceSinks.html#define-a-tablefactory) create configured table sources, table sinks, and corresponding formats from the key-value pairs. All table factories that can be found via Java's [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) are taken into account when searching for exactly-one matching table factory.

If no factory can be found or multiple factories match for the given properties, an exception will be thrown with additional information about considered factories and supported properties.

{% top %}

Table Schema
------------

The table schema defines the names and types of columns similar to the column definitions of a SQL `CREATE TABLE` statement. In addition, one can specify how columns are mapped from and to fields of the format in which the table data is encoded. The origin of a field might be important if the name of the column should differ from the input/output format. For instance, a column `user_name` should reference the field `$$-user-name` from a JSON format. Additionally, the schema is needed to map types from an external system to Flink's representation. In case of a table sink, it ensures that only data with valid schema is written to an external system.

The following example shows a simple schema without time attributes and one-to-one field mapping of input/output to table columns.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withSchema(
  new Schema()
    .field("MyField1", Types.INT)     // required: specify the fields of the table (in this order)
    .field("MyField2", Types.STRING)
    .field("MyField3", Types.BOOLEAN)
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
schema:
  - name: MyField1    # required: specify the fields of the table (in this order)
    type: INT
  - name: MyField2
    type: VARCHAR
  - name: MyField3
    type: BOOLEAN
{% endhighlight %}
</div>
</div>

For *each field*, the following properties can be declared in addition to the column's name and type:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withSchema(
  new Schema()
    .field("MyField1", Types.SQL_TIMESTAMP)
      .proctime()      // optional: declares this field as a processing-time attribute
    .field("MyField2", Types.SQL_TIMESTAMP)
      .rowtime(...)    // optional: declares this field as a event-time attribute
    .field("MyField3", Types.BOOLEAN)
      .from("mf3")     // optional: original field in the input that is referenced/aliased by this field
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
schema:
  - name: MyField1
    type: TIMESTAMP
    proctime: true    # optional: boolean flag whether this field should be a processing-time attribute
  - name: MyField2
    type: TIMESTAMP
    rowtime: ...      # optional: wether this field should be a event-time attribute
  - name: MyField3
    type: BOOLEAN
    from: mf3         # optional: original field in the input that is referenced/aliased by this field
{% endhighlight %}
</div>
</div>

Time attributes are essential when working with unbounded streaming tables. Therefore both processing-time and event-time (also known as "rowtime") attributes can be defined as part of the schema.

For more information about time handling in Flink and especially event-time, we recommend the general [event-time section](streaming.html#time-attributes).

### Rowtime Attributes

In order to control the event-time behavior for tables, Flink provides predefined timestamp extractors and watermark strategies.

The following timestamp extractors are supported:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
// Converts an existing LONG or SQL_TIMESTAMP field in the input into the rowtime attribute.
.rowtime(
  new Rowtime()
    .timestampsFromField("ts_field")    // required: original field name in the input
)

// Converts the assigned timestamps from a DataStream API record into the rowtime attribute 
// and thus preserves the assigned timestamps from the source.
// This requires a source that assigns timestamps (e.g., Kafka 0.10+).
.rowtime(
  new Rowtime()
    .timestampsFromSource()
)

// Sets a custom timestamp extractor to be used for the rowtime attribute.
// The extractor must extend `org.apache.flink.table.sources.tsextractors.TimestampExtractor`.
.rowtime(
  new Rowtime()
    .timestampsFromExtractor(...)
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
# Converts an existing BIGINT or TIMESTAMP field in the input into the rowtime attribute.
rowtime:
  timestamps:
    type: from-field
    from: "ts_field"                 # required: original field name in the input

# Converts the assigned timestamps from a DataStream API record into the rowtime attribute 
# and thus preserves the assigned timestamps from the source.
rowtime:
  timestamps:
    type: from-source
{% endhighlight %}
</div>
</div>

The following watermark strategies are supported:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
// Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum 
// observed timestamp so far minus 1. Rows that have a timestamp equal to the max timestamp
// are not late.
.rowtime(
  new Rowtime()
    .watermarksPeriodicAscending()
)

// Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
// Emits watermarks which are the maximum observed timestamp minus the specified delay.
.rowtime(
  new Rowtime()
    .watermarksPeriodicBounded(2000)    // delay in milliseconds
)

// Sets a built-in watermark strategy which indicates the watermarks should be preserved from the
// underlying DataStream API and thus preserves the assigned watermarks from the source.
.rowtime(
  new Rowtime()
    .watermarksFromSource()
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
# Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum 
# observed timestamp so far minus 1. Rows that have a timestamp equal to the max timestamp
# are not late.
rowtime:
  watermarks:
    type: periodic-ascending

# Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
# Emits watermarks which are the maximum observed timestamp minus the specified delay.
rowtime:
  watermarks:
    type: periodic-bounded
    delay: ...                # required: delay in milliseconds

# Sets a built-in watermark strategy which indicates the watermarks should be preserved from the
# underlying DataStream API and thus preserves the assigned watermarks from the source.
rowtime:
  watermarks:
    type: from-source
{% endhighlight %}
</div>
</div>

Make sure to always declare both timestamps and watermarks. Watermarks are required for triggering time-based operations.

### Type Strings

Because type information is only available in a programming language, the following type strings are supported for being defined in a YAML file:

{% highlight yaml %}
VARCHAR
BOOLEAN
TINYINT
SMALLINT
INT
BIGINT
FLOAT
DOUBLE
DECIMAL
DATE
TIME
TIMESTAMP
ROW(fieldtype, ...)              # unnamed row; e.g. ROW(VARCHAR, INT) that is mapped to Flink's RowTypeInfo
                                 # with indexed fields names f0, f1, ...
ROW(fieldname fieldtype, ...)    # named row; e.g., ROW(myField VARCHAR, myOtherField INT) that
                                 # is mapped to Flink's RowTypeInfo
POJO(class)                      # e.g., POJO(org.mycompany.MyPojoClass) that is mapped to Flink's PojoTypeInfo
ANY(class)                       # e.g., ANY(org.mycompany.MyClass) that is mapped to Flink's GenericTypeInfo
ANY(class, serialized)           # used for type information that is not supported by Flink's Table & SQL API
{% endhighlight %}

{% top %}

Update Modes
------------

For streaming queries, it is required to declare how to perform the [conversion between a dynamic table and an external connector](streaming.html#dynamic-tables--continuous-queries). The *update mode* specifies which kind of messages should be exchanged with the external system:

**Append Mode:** In append mode, a dynamic table and an external connector only exchange INSERT messages.

**Retract Mode:** In retract mode, a dynamic table and an external connector exchange ADD and RETRACT messages. An INSERT change is encoded as an ADD message, a DELETE change as a RETRACT message, and an UPDATE change as a RETRACT message for the updated (previous) row and an ADD message for the updating (new) row. In this mode, a key must not be defined as opposed to upsert mode. However, every update consists of two messages which is less efficient.

**Upsert Mode:** In upsert mode, a dynamic table and an external connector exchange UPSERT and DELETE messages. This mode requires a (possibly composite) unique key by which updates can be propagated. The external connector needs to be aware of the unique key attribute in order to apply messages correctly. INSERT and UPDATE changes are encoded as UPSERT messages. DELETE changes as DELETE messages. The main difference to a retract stream is that UPDATE changes are encoded with a single message and are therefore more efficient.

<span class="label label-danger">Attention</span> The documentation of each connector states which update modes are supported.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.connect(...)
  .inAppendMode()    // otherwise: inUpsertMode() or inRetractMode()
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
tables:
  - name: ...
    update-mode: append    # otherwise: "retract" or "upsert"
{% endhighlight %}
</div>
</div>

See also the [general streaming concepts documentation](streaming.html#dynamic-tables--continuous-queries) for more information.

{% top %}

Table Connectors
----------------

Flink provides a set of connectors for connecting to external systems.

Please note that not all connectors are available in both batch and streaming yet. Furthermore, not every streaming connector supports every streaming mode. Therefore, each connector is tagged accordingly. A format tag indicates that the connector requires a certain type of format.

### File System Connector

<span class="label label-primary">Source: Batch</span>
<span class="label label-primary">Source: Streaming Append Mode</span>
<span class="label label-primary">Sink: Batch</span>
<span class="label label-primary">Sink: Streaming Append Mode</span>
<span class="label label-info">Format: CSV-only</span>

The file system connector allows for reading and writing from a local or distributed filesystem. A filesystem can be defined as:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.connect(
  new FileSystem()
    .path("file:///path/to/whatever")    // required: path to a file or directory
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
connector:
  type: filesystem
  path: "file:///path/to/whatever"    # required: path to a file or directory
{% endhighlight %}
</div>
</div>

The file system connector itself is included in Flink and does not require an additional dependency. A corresponding format needs to be specified for reading and writing rows from and to a file system.

<span class="label label-danger">Attention</span> Make sure to include [Flink File System specific dependencies]({{ site.baseurl }}/ops/filesystems.html).

<span class="label label-danger">Attention</span> File system sources and sinks for streaming are only experimental. In the future, we will support actual streaming use cases, i.e., directory monitoring and bucket output.

### Kafka Connector

<span class="label label-primary">Source: Streaming Append Mode</span>
<span class="label label-primary">Sink: Streaming Append Mode</span>
<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

The Kafka connector allows for reading and writing from and to an Apache Kafka topic. It can be defined as follows:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.connect(
  new Kafka()
    .version("0.11")    // required: valid connector versions are "0.8", "0.9", "0.10", and "0.11"
    .topic("...")       // required: topic name from which the table is read

    // optional: connector specific properties
    .property("zookeeper.connect", "localhost:2181")
    .property("bootstrap.servers", "localhost:9092")
    .property("group.id", "testGroup")

    // optional: select a startup mode for Kafka offsets
    .startFromEarliest()
    .startFromLatest()
    .startFromSpecificOffsets(...)

    // optional: output partitioning from Flink's partitions into Kafka's partitions
    .sinkPartitionerFixed()         // each Flink partition ends up in at-most one Kafka partition (default)
    .sinkPartitionerRoundRobin()    // a Flink partition is distributed to Kafka partitions round-robin
    .sinkPartitionerCustom(MyCustom.class)    // use a custom FlinkKafkaPartitioner subclass
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
connector:
  type: kafka
  version: 0.11       # required: valid connector versions are "0.8", "0.9", "0.10", and "0.11"
  topic: ...          # required: topic name from which the table is read

  properties:         # optional: connector specific properties
    - key: zookeeper.connect
      value: localhost:2181
    - key: bootstrap.servers
      value: localhost:9092
    - key: group.id
      value: testGroup

  startup-mode: ...   # optional: valid modes are "earliest-offset", "latest-offset",
                      # "group-offsets", or "specific-offsets"
  specific-offsets:   # optional: used in case of startup mode with specific offsets
    - partition: 0
      offset: 42
    - partition: 1
      offset: 300

  sink-partitioner: ...    # optional: output partitioning from Flink's partitions into Kafka's partitions
                           # valid are "fixed" (each Flink partition ends up in at most one Kafka partition),
                           # "round-robin" (a Flink partition is distributed to Kafka partitions round-robin)
                           # "custom" (use a custom FlinkKafkaPartitioner subclass)
  sink-partitioner-class: org.mycompany.MyPartitioner  # optional: used in case of sink partitioner custom
{% endhighlight %}
</div>
</div>

**Specify the start reading position:** By default, the Kafka source will start reading data from the committed group offsets in Zookeeper or Kafka brokers. You can specify other start positions, which correspond to the configurations in section [Kafka Consumers Start Position Configuration]({{ site.baseurl }}/dev/connectors/kafka.html#kafka-consumers-start-position-configuration).

**Flink-Kafka Sink Partitioning:** By default, a Kafka sink writes to at most as many partitions as its own parallelism (each parallel instance of the sink writes to exactly one partition). In order to distribute the writes to more partitions or control the routing of rows into partitions, a custom sink partitioner can be provided. The round-robin partitioner is useful to avoid an unbalanced partitioning. However, it will cause a lot of network connections between all the Flink instances and all the Kafka brokers.

**Consistency guarantees:** By default, a Kafka sink ingests data with at-least-once guarantees into a Kafka topic if the query is executed with [checkpointing enabled]({{ site.baseurl }}/dev/stream/state/checkpointing.html#enabling-and-configuring-checkpointing).

**Kafka 0.10+ Timestamps:** Since Kafka 0.10, Kafka messages have a timestamp as metadata that specifies when the record was written into the Kafka topic. These timestamps can be used for a [rowtime attribute](connect.html#defining-the-schema) by selecting `timestamps: from-source` in YAML and `timestampsFromSource()` in Java/Scala respectively. 

Make sure to add the version-specific Kafka dependency. In addition, a corresponding format needs to be specified for reading and writing rows from and to Kafka.

{% top %}

Table Formats
-------------

Flink provides a set of table formats that can be used with table connectors.

A format tag indicates the format type for matching with a connector.

### CSV Format

The CSV format allows to read and write comma-separated rows.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withFormat(
  new Csv()
    .field("field1", Types.STRING)    // required: ordered format fields
    .field("field2", Types.TIMESTAMP)
    .fieldDelimiter(",")              // optional: string delimiter "," by default 
    .lineDelimiter("\n")              // optional: string delimiter "\n" by default 
    .quoteCharacter('"')              // optional: single character for string values, empty by default
    .commentPrefix('#')               // optional: string to indicate comments, empty by default
    .ignoreFirstLine()                // optional: ignore the first line, by default it is not skipped
    .ignoreParseErrors()              // optional: skip records with parse error instead of failing by default
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
format:
  type: csv
  fields:                    # required: ordered format fields
    - name: field1
      type: VARCHAR
    - name: field2
      type: TIMESTAMP
  field-delimiter: ","       # optional: string delimiter "," by default 
  line-delimiter: "\n"       # optional: string delimiter "\n" by default 
  quote-character: '"'       # optional: single character for string values, empty by default
  comment-prefix: '#'        # optional: string to indicate comments, empty by default
  ignore-first-line: false   # optional: boolean flag to ignore the first line, by default it is not skipped
  ignore-parse-errors: true  # optional: skip records with parse error instead of failing by default
{% endhighlight %}
</div>
</div>

The CSV format is included in Flink and does not require additional dependencies.

<span class="label label-danger">Attention</span> The CSV format for writing rows is limited at the moment. Only a custom field delimiter is supported as optional parameter.

### JSON Format

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

The JSON format allows to read and write JSON data that corresponds to a given format schema. The format schema can be defined either as a Flink type, as a JSON schema, or derived from the desired table schema. A Flink type enables a more SQL-like definition and mapping to the corresponding SQL data types. The JSON schema allows for more complex and nested structures.

If the format schema is equal to the table schema, the schema can also be automatically derived. This allows for defining schema information only once. The names, types, and field order of the format are determined by the table's schema. Time attributes are ignored if their origin is not a field. A `from` definition in the table schema is interpreted as a field renaming in the format.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withFormat(
  new Json()
    .failOnMissingField(true)   // optional: flag whether to fail if a field is missing or not, false by default

    // required: define the schema either by using type information which parses numbers to corresponding types
    .schema(Type.ROW(...))

    // or by using a JSON schema which parses to DECIMAL and TIMESTAMP
    .jsonSchema(
      "{" +
      "  type: 'object'," +
      "  properties: {" +
      "    lon: {" +
      "      type: 'number'" +
      "    }," +
      "    rideTime: {" +
      "      type: 'string'," +
      "      format: 'date-time'" +
      "    }" +
      "  }" +
      "}"
    )

    // or use the table's schema
    .deriveSchema()
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
format:
  type: json
  fail-on-missing-field: true   # optional: flag whether to fail if a field is missing or not, false by default

  # required: define the schema either by using a type string which parses numbers to corresponding types
  schema: "ROW(lon FLOAT, rideTime TIMESTAMP)"

  # or by using a JSON schema which parses to DECIMAL and TIMESTAMP
  json-schema: >
    {
      type: 'object',
      properties: {
        lon: {
          type: 'number'
        },
        rideTime: {
          type: 'string',
          format: 'date-time'
        }
      }
    }

  # or use the table's schema
  derive-schema: true
{% endhighlight %}
</div>
</div>

The following table shows the mapping of JSON schema types to Flink SQL types:

| JSON schema                       | Flink SQL               |
| :-------------------------------- | :---------------------- |
| `object`                          | `ROW`                   |
| `boolean`                         | `BOOLEAN`               |
| `array`                           | `ARRAY[_]`              |
| `number`                          | `DECIMAL`               |
| `integer`                         | `DECIMAL`               |
| `string`                          | `VARCHAR`               |
| `string` with `format: date-time` | `TIMESTAMP`             |
| `string` with `format: date`      | `DATE`                  |
| `string` with `format: time`      | `TIME`                  |
| `string` with `encoding: base64`  | `ARRAY[TINYINT]`        |
| `null`                            | `NULL` (unsupported yet)|


Currently, Flink supports only a subset of the [JSON schema specification](http://json-schema.org/) `draft-07`. Union types (as well as `allOf`, `anyOf`, `not`) are not supported yet. `oneOf` and arrays of types are only supported for specifying nullability.

Simple references that link to a common definition in the document are supported as shown in the more complex example below:

{% highlight json %}
{
  "definitions": {
    "address": {
      "type": "object",
      "properties": {
        "street_address": {
          "type": "string"
        },
        "city": {
          "type": "string"
        },
        "state": {
          "type": "string"
        }
      },
      "required": [
        "street_address",
        "city",
        "state"
      ]
    }
  },
  "type": "object",
  "properties": {
    "billing_address": {
      "$ref": "#/definitions/address"
    },
    "shipping_address": {
      "$ref": "#/definitions/address"
    },
    "optional_address": {
      "oneOf": [
        {
          "type": "null"
        },
        {
          "$ref": "#/definitions/address"
        }
      ]
    }
  }
}
{% endhighlight %}

**Missing Field Handling:** By default, a missing JSON field is set to `null`. You can enable strict JSON parsing that will cancel the source (and query) if a field is missing.

Make sure to add the JSON format as a dependency.


### Apache Avro Format

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

The [Apache Avro](https://avro.apache.org/) format allows to read and write Avro data that corresponds to a given format schema. The format schema can be defined either as a fully qualified class name of an Avro specific record or as an Avro schema string. If a class name is used, the class must be available in the classpath during runtime.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withFormat(
  new Avro()

    // required: define the schema either by using an Avro specific record class
    .recordClass(User.class)

    // or by using an Avro schema
    .avroSchema(
      "{" +
      "  \"type\": \"record\"," +
      "  \"name\": \"test\"," +
      "  \"fields\" : [" +
      "    {\"name\": \"a\", \"type\": \"long\"}," +
      "    {\"name\": \"b\", \"type\": \"string\"}" +
      "  ]" +
      "}"
    )
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
format:
  type: avro

  # required: define the schema either by using an Avro specific record class
  record-class: "org.organization.types.User"

  # or by using an Avro schema
  avro-schema: >
    {
      "type": "record",
      "name": "test",
      "fields" : [
        {"name": "a", "type": "long"},
        {"name": "b", "type": "string"}
      ]
    }
{% endhighlight %}
</div>
</div>

Avro types are mapped to the corresponding SQL data types. Union types are only supported for specifying nullability otherwise they are converted to an `ANY` type. The following table shows the mapping:

| Avro schema                                 | Flink SQL               |
| :------------------------------------------ | :---------------------- |
| `record`                                    | `ROW`                   |
| `enum`                                      | `VARCHAR`               |
| `array`                                     | `ARRAY[_]`              |
| `map`                                       | `MAP[VARCHAR, _]`       |
| `union`                                     | non-null type or `ANY`  |
| `fixed`                                     | `ARRAY[TINYINT]`        |
| `string`                                    | `VARCHAR`               |
| `bytes`                                     | `ARRAY[TINYINT]`        |
| `int`                                       | `INT`                   |
| `long`                                      | `BIGINT`                |
| `float`                                     | `FLOAT`                 |
| `double`                                    | `DOUBLE`                |
| `boolean`                                   | `BOOLEAN`               |
| `int` with `logicalType: date`              | `DATE`                  |
| `int` with `logicalType: time-millis`       | `TIME`                  |
| `int` with `logicalType: time-micros`       | `INT`                   |
| `long` with `logicalType: timestamp-millis` | `TIMESTAMP`             |
| `long` with `logicalType: timestamp-micros` | `BIGINT`                |
| `bytes` with `logicalType: decimal`         | `DECIMAL`               |
| `fixed` with `logicalType: decimal`         | `DECIMAL`               |
| `null`                                      | `NULL` (unsupported yet)|

Avro uses [Joda-Time](http://www.joda.org/joda-time/) for representing logical date and time types in specific record classes. The Joda-Time dependency is not part of Flink's distribution. Therefore, make sure that Joda-Time is in your classpath together with your specific record class during runtime. Avro formats specified via a schema string do not require Joda-Time to be present.

Make sure to add the Apache Avro dependency.

{% top %}

Further TableSources and TableSinks
-----------------------------------

The following table sources and sinks have not yet been migrated (or have not been migrated entirely) to the new unified interfaces.

These are the additional `TableSource`s which are provided with Flink:

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `OrcTableSource` | `flink-orc` | Y | N | A `TableSource` for ORC files.

These are the additional `TableSink`s which are provided with Flink:

| **Class name** | **Maven dependency** | **Batch?** | **Streaming?** | **Description**
| `CsvTableSink` | `flink-table` | Y | Append | A simple sink for CSV files.
| `JDBCAppendTableSink` | `flink-jdbc` | Y | Append | Writes a Table to a JDBC table.
| `CassandraAppendTableSink` | `flink-connector-cassandra` | N | Append | Writes a Table to a Cassandra table. 

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