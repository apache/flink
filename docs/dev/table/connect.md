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

The following tables list all available connectors and formats. Their mutual compatibility is tagged in the corresponding sections for [table connectors](connect.html#table-connectors) and [table formats](connect.html#table-formats). The following tables provide dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{% if site.is_stable %}

### Connectors

| Name              | Version             | Maven dependency             | SQL Client JAR         |
| :---------------- | :------------------ | :--------------------------- | :----------------------|
| Filesystem        |                     | Built-in                     | Built-in               |
| Elasticsearch     | 6                   | `flink-connector-elasticsearch6` | [Download](http://central.maven.org/maven2/org/apache/flink/flink-sql-connector-elasticsearch6{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-elasticsearch6{{site.scala_version_suffix}}-{{site.version}}.jar) |
| Apache Kafka      | 0.8                 | `flink-connector-kafka-0.8`  | Not available          |
| Apache Kafka      | 0.9                 | `flink-connector-kafka-0.9`  | [Download](http://central.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka-0.9{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-kafka-0.9{{site.scala_version_suffix}}-{{site.version}}.jar) |
| Apache Kafka      | 0.10                | `flink-connector-kafka-0.10` | [Download](http://central.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka-0.10{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-kafka-0.10{{site.scala_version_suffix}}-{{site.version}}.jar) |
| Apache Kafka      | 0.11                | `flink-connector-kafka-0.11` | [Download](http://central.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka-0.11{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-kafka-0.11{{site.scala_version_suffix}}-{{site.version}}.jar) |
| Apache Kafka      | 0.11+ (`universal`) | `flink-connector-kafka`      | [Download](http://central.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka{{site.scala_version_suffix}}/{{site.version}}/flink-sql-connector-kafka{{site.scala_version_suffix}}-{{site.version}}.jar) |

### Formats

| Name                       | Maven dependency             | SQL Client JAR         |
| :------------------------- | :--------------------------- | :--------------------- |
| Old CSV (for files)        | Built-in                     | Built-in               |
| CSV (for Kafka)            | `flink-csv`                  | [Download](http://central.maven.org/maven2/org/apache/flink/flink-csv/{{site.version}}/flink-csv-{{site.version}}-sql-jar.jar) |
| JSON                       | `flink-json`                 | [Download](http://central.maven.org/maven2/org/apache/flink/flink-json/{{site.version}}/flink-json-{{site.version}}-sql-jar.jar) |
| Apache Avro                | `flink-avro`                 | [Download](http://central.maven.org/maven2/org/apache/flink/flink-avro/{{site.version}}/flink-avro-{{site.version}}-sql-jar.jar) |

{% else %}

These tables are only available for stable releases.

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

The **table schema** defines the schema of a table that is exposed to SQL queries. It describes how a source maps the data format to the table schema and a sink vice versa. The schema has access to fields defined by the connector or format. It can use one or more fields for extracting or inserting [time attributes](streaming/time_attributes.html). If input fields have no deterministic field order, the schema clearly defines column names, their order, and origin.

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

<div data-lang="python" markdown="1">
{% highlight python %}
table_environment \
    .connect(...) \
    .with_format(...) \
    .with_schema(...) \
    .in_append_mode() \
    .register_table_source("MyTable")
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

For streaming queries, an [update mode](connect.html#update-mode) declares how to communicate between a dynamic table and the storage system for continuous queries.

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
        "}"
      )
  )

  // declare the schema of the table
  .withSchema(
    new Schema()
      .field("rowtime", Types.SQL_TIMESTAMP)
        .rowtime(new Rowtime()
          .timestampsFromField("timestamp")
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

<div data-lang="python" markdown="1">
{% highlight python %}
table_environment \
    .connect(  # declare the external system to connect to
        Kafka()
        .version("0.10")
        .topic("test-input")
        .start_from_earliest()
        .property("zookeeper.connect", "localhost:2181")
        .property("bootstrap.servers", "localhost:9092")
    ) \
    .with_format(  # declare a format for this system
        Avro()
        .avro_schema(
            "{"
            "  \"namespace\": \"org.myorganization\","
            "  \"type\": \"record\","
            "  \"name\": \"UserMessage\","
            "    \"fields\": ["
            "      {\"name\": \"timestamp\", \"type\": \"string\"},"
            "      {\"name\": \"user\", \"type\": \"long\"},"
            "      {\"name\": \"message\", \"type\": [\"string\", \"null\"]}"
            "    ]"
            "}"
        )
    ) \
    .with_schema(  # declare the schema of the table
        Schema()
        .field("rowtime", DataTypes.TIMESTAMP())
        .rowtime(
            Rowtime()
            .timestamps_from_field("timestamp")
            .watermarks_periodic_bounded(60000)
        )
        .field("user", DataTypes.BIGINT())
        .field("message", DataTypes.STRING())
    ) \
    .in_append_mode() \
    .register_table_source("MyUserTable")  
    # specify the update-mode for streaming tables and
    # register as source, sink, or both and under a name
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

<div data-lang="python" markdown="1">
{% highlight python %}
.with_schema(
    Schema()
    .field("MyField1", DataTypes.INT())  # required: specify the fields of the table (in this order)
    .field("MyField2", DataTypes.STRING())
    .field("MyField3", DataTypes.BOOLEAN())
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

<div data-lang="python" markdown="1">
{% highlight python %}
.with_schema(
    Schema()
    .field("MyField1", DataTypes.TIMESTAMP())
    .proctime()  # optional: declares this field as a processing-time attribute
    .field("MyField2", DataTypes.TIMESTAMP())
    .rowtime(...)  # optional: declares this field as a event-time attribute
    .field("MyField3", DataTypes.BOOLEAN())
    .from_origin_field("mf3")  # optional: original field in the input that is referenced/aliased by this field
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

For more information about time handling in Flink and especially event-time, we recommend the general [event-time section](streaming/time_attributes.html).

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

<div data-lang="python" markdown="1">
{% highlight python %}
# Converts an existing BIGINT or TIMESTAMP field in the input into the rowtime attribute.
.rowtime(
    Rowtime()
    .timestamps_from_field("ts_field")  # required: original field name in the input
)

# Converts the assigned timestamps into the rowtime attribute
# and thus preserves the assigned timestamps from the source.
# This requires a source that assigns timestamps (e.g., Kafka 0.10+).
.rowtime(
    Rowtime()
    .timestamps_from_source()
)

# Sets a custom timestamp extractor to be used for the rowtime attribute.
# The extractor must extend `org.apache.flink.table.sources.tsextractors.TimestampExtractor`.
# Due to python can not accept java object, so it requires a full-qualified class name of the extractor.
.rowtime(
    Rowtime()
    .timestamps_from_extractor(...)
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

<div data-lang="python" markdown="1">
{% highlight python %}
# Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum
# observed timestamp so far minus 1. Rows that have a timestamp equal to the max timestamp
# are not late.
.rowtime(
    Rowtime()
    .watermarks_periodic_ascending()
)

# Sets a built-in watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
# Emits watermarks which are the maximum observed timestamp minus the specified delay.
.rowtime(
    Rowtime()
    .watermarks_periodic_bounded(2000)  # delay in milliseconds
)

# Sets a built-in watermark strategy which indicates the watermarks should be preserved from the
# underlying DataStream API and thus preserves the assigned watermarks from the source.
.rowtime(
    Rowtime()
    .watermarks_from_source()
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
MAP<fieldtype, fieldtype>        # generic map; e.g. MAP<VARCHAR, INT> that is mapped to Flink's MapTypeInfo
MULTISET<fieldtype>              # multiset; e.g. MULTISET<VARCHAR> that is mapped to Flink's MultisetTypeInfo
PRIMITIVE_ARRAY<fieldtype>       # primitive array; e.g. PRIMITIVE_ARRAY<INT> that is mapped to Flink's PrimitiveArrayTypeInfo
OBJECT_ARRAY<fieldtype>          # object array; e.g. OBJECT_ARRAY<POJO(org.mycompany.MyPojoClass)> that is mapped to
                                 #   Flink's ObjectArrayTypeInfo
ROW<fieldtype, ...>              # unnamed row; e.g. ROW<VARCHAR, INT> that is mapped to Flink's RowTypeInfo
                                 #   with indexed fields names f0, f1, ...
ROW<fieldname fieldtype, ...>    # named row; e.g., ROW<myField VARCHAR, myOtherField INT> that
                                 #   is mapped to Flink's RowTypeInfo
POJO<class>                      # e.g., POJO<org.mycompany.MyPojoClass> that is mapped to Flink's PojoTypeInfo
ANY<class>                       # e.g., ANY<org.mycompany.MyClass> that is mapped to Flink's GenericTypeInfo
ANY<class, serialized>           # used for type information that is not supported by Flink's Table & SQL API
{% endhighlight %}

{% top %}

Update Modes
------------

For streaming queries, it is required to declare how to perform the [conversion between a dynamic table and an external connector](streaming/dynamic_tables.html#continuous-queries). The *update mode* specifies which kind of messages should be exchanged with the external system:

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

<div data-lang="python" markdown="1">
{% highlight python %}
.connect(...) \
    .in_append_mode()  # otherwise: in_upsert_mode() or in_retract_mode()
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

See also the [general streaming concepts documentation](streaming/dynamic_tables.html#continuous-queries) for more information.

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

<div data-lang="python" markdown="1">
{% highlight python %}
.connect(
    FileSystem()
    .path("file:///path/to/whatever")  # required: path to a file or directory
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

<span class="label label-danger">Attention</span> Make sure to include [Flink File System specific dependencies]({{ site.baseurl }}/internals/filesystems.html).

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
    .version("0.11")    // required: valid connector versions are
                        //   "0.8", "0.9", "0.10", "0.11", and "universal"
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

<div data-lang="python" markdown="1">
{% highlight python %}
.connect(
    Kafka()
    .version("0.11")  # required: valid connector versions are
                      # "0.8", "0.9", "0.10", "0.11", and "universal"
    .topic("...")     # required: topic name from which the table is read
    
    # optional: connector specific properties
    .property("zookeeper.connect", "localhost:2181")
    .property("bootstrap.servers", "localhost:9092")
    .property("group.id", "testGroup")

    # optional: select a startup mode for Kafka offsets
    .start_from_earliest()
    .start_from_latest()
    .start_from_specific_offsets(...)

    # optional: output partitioning from Flink's partitions into Kafka's partitions
    .sink_partitioner_fixed()        # each Flink partition ends up in at-most one Kafka partition (default)
    .sink_partitioner_round_robin()  # a Flink partition is distributed to Kafka partitions round-robin
    .sink_partitioner_custom("full.qualified.custom.class.name")  # use a custom FlinkKafkaPartitioner subclass
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
connector:
  type: kafka
  version: "0.11"     # required: valid connector versions are
                      #   "0.8", "0.9", "0.10", "0.11", and "universal"
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

**Kafka 0.11+ Versioning:** Since Flink 1.7, the Kafka connector definition should be independent of a hard-coded Kafka version. Use the connector version `universal` as a wildcard for Flink's Kafka connector that is compatible with all Kafka versions starting from 0.11.

Make sure to add the version-specific Kafka dependency. In addition, a corresponding format needs to be specified for reading and writing rows from and to Kafka.

{% top %}

### Elasticsearch Connector

<span class="label label-primary">Sink: Streaming Append Mode</span>
<span class="label label-primary">Sink: Streaming Upsert Mode</span>
<span class="label label-info">Format: JSON-only</span>

The Elasticsearch connector allows for writing into an index of the Elasticsearch search engine.

The connector can operate in [upsert mode](#update-modes) for exchanging UPSERT/DELETE messages with the external system using a [key defined by the query](./streaming/dynamic_tables.html#table-to-stream-conversion).

For append-only queries, the connector can also operate in [append mode](#update-modes) for exchanging only INSERT messages with the external system. If no key is defined by the query, a key is automatically generated by Elasticsearch.

The connector can be defined as follows:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.connect(
  new Elasticsearch()
    .version("6")                      // required: valid connector versions are "6"
    .host("localhost", 9200, "http")   // required: one or more Elasticsearch hosts to connect to
    .index("MyUsers")                  // required: Elasticsearch index
    .documentType("user")              // required: Elasticsearch document type

    .keyDelimiter("$")        // optional: delimiter for composite keys ("_" by default)
                              //   e.g., "$" would result in IDs "KEY1$KEY2$KEY3"
    .keyNullLiteral("n/a")    // optional: representation for null fields in keys ("null" by default)

    // optional: failure handling strategy in case a request to Elasticsearch fails (fail by default)
    .failureHandlerFail()          // optional: throws an exception if a request fails and causes a job failure
    .failureHandlerIgnore()        //   or ignores failures and drops the request
    .failureHandlerRetryRejected() //   or re-adds requests that have failed due to queue capacity saturation
    .failureHandlerCustom(...)     //   or custom failure handling with a ActionRequestFailureHandler subclass

    // optional: configure how to buffer elements before sending them in bulk to the cluster for efficiency
    .disableFlushOnCheckpoint()    // optional: disables flushing on checkpoint (see notes below!)
    .bulkFlushMaxActions(42)       // optional: maximum number of actions to buffer for each bulk request
    .bulkFlushMaxSize("42 mb")     // optional: maximum size of buffered actions in bytes per bulk request
                                   //   (only MB granularity is supported)
    .bulkFlushInterval(60000L)     // optional: bulk flush interval (in milliseconds)

    .bulkFlushBackoffConstant()    // optional: use a constant backoff type
    .bulkFlushBackoffExponential() //   or use an exponential backoff type
    .bulkFlushBackoffMaxRetries(3) // optional: maximum number of retries
    .bulkFlushBackoffDelay(30000L) // optional: delay between each backoff attempt (in milliseconds)

    // optional: connection properties to be used during REST communication to Elasticsearch
    .connectionMaxRetryTimeout(3)  // optional: maximum timeout (in milliseconds) between retries
    .connectionPathPrefix("/v1")   // optional: prefix string to be added to every REST communication
)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
.connect(
    Elasticsearch()
    .version("6")                      # required: valid connector versions are "6"
    .host("localhost", 9200, "http")   # required: one or more Elasticsearch hosts to connect to
    .index("MyUsers")                  # required: Elasticsearch index
    .document_type("user")             # required: Elasticsearch document type

    .key_delimiter("$")       # optional: delimiter for composite keys ("_" by default)
                              #   e.g., "$" would result in IDs "KEY1$KEY2$KEY3"
    .key_null_literal("n/a")  # optional: representation for null fields in keys ("null" by default)

    # optional: failure handling strategy in case a request to Elasticsearch fails (fail by default)
    .failure_handler_fail()             # optional: throws an exception if a request fails and causes a job failure
    .failure_handler_ignore()           #   or ignores failures and drops the request
    .failure_handler_retry_rejected()   #   or re-adds requests that have failed due to queue capacity saturation
    .failure_handler_custom(...)        #   or custom failure handling with a ActionRequestFailureHandler subclass

    # optional: configure how to buffer elements before sending them in bulk to the cluster for efficiency
    .disable_flush_on_checkpoint()      # optional: disables flushing on checkpoint (see notes below!)
    .bulk_flush_max_actions(42)         # optional: maximum number of actions to buffer for each bulk request
    .bulk_flush_max_size("42 mb")       # optional: maximum size of buffered actions in bytes per bulk request
                                        #   (only MB granularity is supported)
    .bulk_flush_interval(60000)         # optional: bulk flush interval (in milliseconds)

    .bulk_flush_backoff_constant()      # optional: use a constant backoff type
    .bulk_flush_backoff_exponential()   #   or use an exponential backoff type
    .bulk_flush_backoff_max_retries(3)  # optional: maximum number of retries
    .bulk_flush_backoff_delay(30000)    # optional: delay between each backoff attempt (in milliseconds)

    # optional: connection properties to be used during REST communication to Elasticsearch
    .connection_max_retry_timeout(3)    # optional: maximum timeout (in milliseconds) between retries
    .connection_path_prefix("/v1")      # optional: prefix string to be added to every REST communication
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
connector:
  type: elasticsearch
  version: 6                # required: valid connector versions are "6"
    hosts:                  # required: one or more Elasticsearch hosts to connect to
      - hostname: "localhost"
        port: 9200
        protocol: "http"
    index: "MyUsers"        # required: Elasticsearch index
    document-type: "user"   # required: Elasticsearch document type

    key-delimiter: "$"      # optional: delimiter for composite keys ("_" by default)
                            #   e.g., "$" would result in IDs "KEY1$KEY2$KEY3"
    key-null-literal: "n/a" # optional: representation for null fields in keys ("null" by default)

    # optional: failure handling strategy in case a request to Elasticsearch fails ("fail" by default)
    failure-handler: ...    # valid strategies are "fail" (throws an exception if a request fails and
                            #   thus causes a job failure), "ignore" (ignores failures and drops the request),
                            #   "retry-rejected" (re-adds requests that have failed due to queue capacity
                            #   saturation), or "custom" for failure handling with a
                            #   ActionRequestFailureHandler subclass

    # optional: configure how to buffer elements before sending them in bulk to the cluster for efficiency
    flush-on-checkpoint: true   # optional: disables flushing on checkpoint (see notes below!) ("true" by default)
    bulk-flush:
      max-actions: 42           # optional: maximum number of actions to buffer for each bulk request
      max-size: 42 mb           # optional: maximum size of buffered actions in bytes per bulk request
                                #   (only MB granularity is supported)
      interval: 60000           # optional: bulk flush interval (in milliseconds)
      back-off:                 # optional: backoff strategy ("disabled" by default)
        type: ...               #   valid strategies are "disabled", "constant", or "exponential"
        max-retries: 3          # optional: maximum number of retries
        delay: 30000            # optional: delay between each backoff attempt (in milliseconds)

    # optional: connection properties to be used during REST communication to Elasticsearch
    connection-max-retry-timeout: 3   # optional: maximum timeout (in milliseconds) between retries
    connection-path-prefix: "/v1"     # optional: prefix string to be added to every REST communication
{% endhighlight %}
</div>
</div>

**Bulk flushing:** For more information about characteristics of the optional flushing parameters see the [corresponding low-level documentation]({{ site.baseurl }}/dev/connectors/elasticsearch.html).

**Disabling flushing on checkpoint:** When disabled, a sink will not wait for all pending action requests to be acknowledged by Elasticsearch on checkpoints. Thus, a sink does NOT provide any strong guarantees for at-least-once delivery of action requests.

**Key extraction:** Flink automatically extracts valid keys from a query. For example, a query `SELECT a, b, c FROM t GROUP BY a, b` defines a composite key of the fields `a` and `b`. The Elasticsearch connector generates a document ID string for every row by concatenating all key fields in the order defined in the query using a key delimiter. A custom representation of null literals for key fields can be defined.

<span class="label label-danger">Attention</span> A JSON format defines how to encode documents for the external system, therefore, it must be added as a [dependency](connect.html#formats).

{% top %}

Table Formats
-------------

Flink provides a set of table formats that can be used with table connectors.

A format tag indicates the format type for matching with a connector.

### CSV Format

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

The CSV format aims to comply with [RFC-4180](https://tools.ietf.org/html/rfc4180) ("Common Format and
MIME Type for Comma-Separated Values (CSV) Files") proposed by the Internet Engineering Task Force (IETF).

The format allows to read and write CSV data that corresponds to a given format schema. The format schema can be
defined either as a Flink type or derived from the desired table schema.

If the format schema is equal to the table schema, the schema can also be automatically derived. This allows for
defining schema information only once. The names, types, and fields' order of the format are determined by the
table's schema. Time attributes are ignored if their origin is not a field. A `from` definition in the table
schema is interpreted as a field renaming in the format.

The CSV format can be used as follows:

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withFormat(
  new Csv()

    // required: define the schema either by using type information
    .schema(Type.ROW(...))

    // or use the table's schema
    .deriveSchema()

    .fieldDelimiter(';')         // optional: field delimiter character (',' by default)
    .lineDelimiter("\r\n")       // optional: line delimiter ("\n" by default;
                                 //   otherwise "\r" or "\r\n" are allowed)
    .quoteCharacter('\'')        // optional: quote character for enclosing field values ('"' by default)
    .allowComments()             // optional: ignores comment lines that start with '#' (disabled by default);
                                 //   if enabled, make sure to also ignore parse errors to allow empty rows
    .ignoreParseErrors()         // optional: skip fields and rows with parse errors instead of failing;
                                 //   fields are set to null in case of errors
    .arrayElementDelimiter("|")  // optional: the array element delimiter string for separating
                                 //   array and row element values (";" by default)
    .escapeCharacter('\\')       // optional: escape character for escaping values (disabled by default)
    .nullLiteral("n/a")          // optional: null literal string that is interpreted as a
                                 //   null value (disabled by default)
)
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}
.with_format(
    Csv()

    # required: define the schema either by using type information
    .schema(DataTypes.ROW(...))

    # or use the table's schema
    .derive_schema()

    .field_delimiter(';')          # optional: field delimiter character (',' by default)
    .line_delimiter("\r\n")        # optional: line delimiter ("\n" by default;
                                   #   otherwise "\r" or "\r\n" are allowed)
    .quote_character('\'')         # optional: quote character for enclosing field values ('"' by default)
    .allow_comments()              # optional: ignores comment lines that start with '#' (disabled by default);
                                   #   if enabled, make sure to also ignore parse errors to allow empty rows
    .ignore_parse_errors()         # optional: skip fields and rows with parse errors instead of failing;
                                   #   fields are set to null in case of errors
    .array_element_delimiter("|")  # optional: the array element delimiter string for separating
                                   #   array and row element values (";" by default)
    .escape_character('\\')        # optional: escape character for escaping values (disabled by default)
    .null_literal("n/a")           # optional: null literal string that is interpreted as a
                                   #   null value (disabled by default)
)
{% endhighlight %}
</div>

<div data-lang="YAML" markdown="1">
{% highlight yaml %}
format:
  type: csv

  # required: define the schema either by using type information
  schema: "ROW(lon FLOAT, rideTime TIMESTAMP)"

  # or use the table's schema
  derive-schema: true

  field-delimiter: ";"         # optional: field delimiter character (',' by default)
  line-delimiter: "\r\n"       # optional: line delimiter ("\n" by default; otherwise "\r" or "\r\n" are allowed)
  quote-character: "'"         # optional: quote character for enclosing field values ('"' by default)
  allow-comments: true         # optional: ignores comment lines that start with "#" (disabled by default);
                               #   if enabled, make sure to also ignore parse errors to allow empty rows
  ignore-parse-errors: true    # optional: skip fields and rows with parse errors instead of failing;
                               #   fields are set to null in case of errors
  array-element-delimiter: "|" # optional: the array element delimiter string for separating
                               #   array and row element values (";" by default)
  escape-character: "\\"       # optional: escape character for escaping values (disabled by default)
  null-literal: "n/a"          # optional: null literal string that is interpreted as a
                               #   null value (disabled by default)
{% endhighlight %}
</div>
</div>

The following table lists supported types that can be read and written:

| Supported Flink SQL Types |
| :------------------------ |
| `ROW`                    |
| `VARCHAR`                |
| `ARRAY[_]`               |
| `INT`                    |
| `BIGINT`                 |
| `FLOAT`                  |
| `DOUBLE`                 |
| `BOOLEAN`                |
| `DATE`                   |
| `TIME`                   |
| `TIMESTAMP`              |
| `DECIMAL`                |
| `NULL` (unsupported yet) |

**Numeric types:** Value should be a number but the literal `"null"` can also be understood. An empty string is
considered `null`. Values are also trimmed (leading/trailing white space). Numbers are parsed using
Java's `valueOf` semantics. Other non-numeric strings may cause a parsing exception.

**String and time types:** Value is not trimmed. The literal `"null"` can also be understood. Time types
must be formatted according to the Java SQL time format with millisecond precision. For example:
`2018-01-01` for date, `20:43:59` for time, and `2018-01-01 20:43:59.999` for timestamp.

**Boolean type:** Value is expected to be a boolean (`"true"`, `"false"`) string or `"null"`. Empty strings are
interpreted as `false`. Values are trimmed (leading/trailing white space). Other values result in an exception.

**Nested types:** Array and row types are supported for one level of nesting using the array element delimiter.

**Primitive byte arrays:** Primitive byte arrays are handled in Base64-encoded representation.

**Line endings:** Line endings need to be considered even for row-based connectors (such as Kafka)
to be ignored for unquoted string fields at the end of a row.

**Escaping and quoting:** The following table shows examples of how escaping and quoting affect the parsing
of a string using `*` for escaping and `'` for quoting:

| CSV Field         | Parsed String        |
| :---------------- | :------------------- |
| `123*'4**`        | `123'4*`             |
| `'123''4**'`      | `123'4*`             |
| `'a;b*'c'`        | `a;b'c`              |
| `'a;b''c'`        | `a;b'c`              |

Make sure to add the CSV format as a dependency.

### JSON Format

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

The JSON format allows to read and write JSON data that corresponds to a given format schema. The format schema can be defined either as a Flink type, as a JSON schema, or derived from the desired table schema. A Flink type enables a more SQL-like definition and mapping to the corresponding SQL data types. The JSON schema allows for more complex and nested structures.

If the format schema is equal to the table schema, the schema can also be automatically derived. This allows for defining schema information only once. The names, types, and fields' order of the format are determined by the table's schema. Time attributes are ignored if their origin is not a field. A `from` definition in the table schema is interpreted as a field renaming in the format.

The JSON format can be used as follows:

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

<div data-lang="python" markdown="1">
{% highlight python %}
.with_format(
    Json()
    .fail_on_missing_field(True)   # optional: flag whether to fail if a field is missing or not, False by default

    # required: define the schema either by using type information which parses numbers to corresponding types
    .schema(DataTypes.ROW(...))

    # or by using a JSON schema which parses to DECIMAL and TIMESTAMP
    .json_schema(
        "{"
        "  type: 'object',"
        "  properties: {"
        "    lon: {"
        "      type: 'number'"
        "    },"
        "    rideTime: {"
        "      type: 'string',"
        "      format: 'date-time'"
        "    }"
        "  }"
        "}"
    )

    # or use the table's schema
    .derive_schema()
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

The Avro format can be used as follows:

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

<div data-lang="python" markdown="1">
{% highlight python %}
.with_format(
    Avro()

    # required: define the schema either by using an Avro specific record class
    .record_class("full.qualified.user.class.name")

    # or by using an Avro schema
    .avro_schema(
        "{"
        "  \"type\": \"record\","
        "  \"name\": \"test\","
        "  \"fields\" : ["
        "    {\"name\": \"a\", \"type\": \"long\"},"
        "    {\"name\": \"b\", \"type\": \"string\"}"
        "  ]"
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

### Old CSV Format

<span class="label label-danger">Attention</span> For prototyping purposes only!

The old CSV format allows to read and write comma-separated rows using the filesystem connector.

This format describes Flink's non-standard CSV table source/sink. In the future, the format will be
replaced by a proper RFC-compliant version. Use the RFC-compliant CSV format when writing to Kafka.
Use the old one for stream/batch filesystem operations for now.

<div class="codetabs" markdown="1">
<div data-lang="Java/Scala" markdown="1">
{% highlight java %}
.withFormat(
  new OldCsv()
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

<div data-lang="python" markdown="1">
{% highlight python %}
.with_format(
    OldCsv()
    .field("field1", DataTypes.STRING())    # required: ordered format fields
    .field("field2", DataTypes.TIMESTAMP())
    .field_delimiter(",")                   # optional: string delimiter "," by default
    .line_delimiter("\n")                   # optional: string delimiter "\n" by default
    .quote_character('"')                   # optional: single character for string values, empty by default
    .comment_prefix('#')                    # optional: string to indicate comments, empty by default
    .ignore_first_line()                    # optional: ignore the first line, by default it is not skipped
    .ignore_parse_errors()                  # optional: skip records with parse error instead of failing by default
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

The old CSV format is included in Flink and does not require additional dependencies.

<span class="label label-danger">Attention</span> The old CSV format for writing rows is limited at the moment. Only a custom field delimiter is supported as optional parameter.

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

The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming/dynamic_tables.html#table-to-stream-conversion) for details. When emitting a streaming table, rows are written at least once (if checkpointing is enabled) and the `CsvTableSink` does not split output files into bucket files but continuously writes to the same files.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

CsvTableSink sink = new CsvTableSink(
    path,                  // output path
    "|",                   // optional: delimit files by '|'
    1,                     // optional: write to a single file
    WriteMode.OVERWRITE);  // optional: override existing files

tableEnv.registerTableSink(
  "csvOutputTable",
  // specify table schema
  new String[]{"f0", "f1"},
  new TypeInformation[]{Types.STRING, Types.INT},
  sink);

Table table = ...
table.insertInto("csvOutputTable");
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}

val sink: CsvTableSink = new CsvTableSink(
    path,                             // output path
    fieldDelim = "|",                 // optional: delimit files by '|'
    numFiles = 1,                     // optional: write to a single file
    writeMode = WriteMode.OVERWRITE)  // optional: override existing files

tableEnv.registerTableSink(
  "csvOutputTable",
  // specify table schema
  Array[String]("f0", "f1"),
  Array[TypeInformation[_]](Types.STRING, Types.INT),
  sink)

val table: Table = ???
table.insertInto("csvOutputTable")
{% endhighlight %}
</div>

<div data-lang="python" markdown="1">
{% highlight python %}

field_names = ["f0", "f1"]
field_types = [DataTypes.STRING(), DataTypes.INT()]

sink = CsvTableSink(
    field_names,
    field_types,
    path,                 # output path
    "|",                  # optional: delimit files by '|'
    1,                    # optional: write to a single file
    WriteMode.OVERWRITE  # optional: override existing files
)

table_env.register_table_sink(
    "csvOutputTable",
    sink
)

table = ...
table.insert_into("csvOutputTable")
{% endhighlight %}
</div>
</div>

### JDBCAppendTableSink

The `JDBCAppendTableSink` emits a `Table` to a JDBC connection. The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming/dynamic_tables.html#table-to-stream-conversion) for details.

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

tableEnv.registerTableSink(
  "jdbcOutputTable",
  // specify table schema
  new String[]{"id"},
  new TypeInformation[]{Types.INT},
  sink);

Table table = ...
table.insertInto("jdbcOutputTable");
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

tableEnv.registerTableSink(
  "jdbcOutputTable",
  // specify table schema
  Array[String]("id"),
  Array[TypeInformation[_]](Types.INT),
  sink)

val table: Table = ???
table.insertInto("jdbcOutputTable")
{% endhighlight %}
</div>
</div>

Similar to using <code>JDBCOutputFormat</code>, you have to explicitly specify the name of the JDBC driver, the JDBC URL, the query to be executed, and the field types of the JDBC table.

{% top %}

### CassandraAppendTableSink

The `CassandraAppendTableSink` emits a `Table` to a Cassandra table. The sink only supports append-only streaming tables. It cannot be used to emit a `Table` that is continuously updated. See the [documentation on Table to Stream conversions](./streaming/dynamic_tables.html#table-to-stream-conversion) for details.

The `CassandraAppendTableSink` inserts all rows at least once into the Cassandra table if checkpointing is enabled. However, you can specify the query as upsert query.

To use the `CassandraAppendTableSink`, you have to add the Cassandra connector dependency (<code>flink-connector-cassandra</code>) to your project. The example below shows how to use the `CassandraAppendTableSink`.

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}

ClusterBuilder builder = ... // configure Cassandra cluster connection

CassandraAppendTableSink sink = new CassandraAppendTableSink(
  builder,
  // the query must match the schema of the table
  "INSERT INTO flink.myTable (id, name, value) VALUES (?, ?, ?)");

tableEnv.registerTableSink(
  "cassandraOutputTable",
  // specify table schema
  new String[]{"id", "name", "value"},
  new TypeInformation[]{Types.INT, Types.STRING, Types.DOUBLE},
  sink);

Table table = ...
table.insertInto(cassandraOutputTable);
{% endhighlight %}
</div>

<div data-lang="scala" markdown="1">
{% highlight scala %}
val builder: ClusterBuilder = ... // configure Cassandra cluster connection

val sink: CassandraAppendTableSink = new CassandraAppendTableSink(
  builder,
  // the query must match the schema of the table
  "INSERT INTO flink.myTable (id, name, value) VALUES (?, ?, ?)")

tableEnv.registerTableSink(
  "cassandraOutputTable",
  // specify table schema
  Array[String]("id", "name", "value"),
  Array[TypeInformation[_]](Types.INT, Types.STRING, Types.DOUBLE),
  sink)

val table: Table = ???
table.insertInto(cassandraOutputTable)
{% endhighlight %}
</div>
</div>

{% top %}
