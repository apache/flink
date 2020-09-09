---
title: "Table & SQL Connectors"
nav-id: sql-connectors
nav-parent_id: connectors-root
nav-pos: 2
nav-show_overview: true
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


Flink's Table API & SQL programs can be connected to other external systems for reading and writing both batch and streaming tables. A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system). A table sink emits a table to an external storage system. Depending on the type of source and sink, they support different formats such as CSV, Avro, Parquet, or ORC.

This page describes how to register table sources and table sinks in Flink using the natively supported connectors. After a source or sink has been registered, it can be accessed by Table API & SQL statements.

<span class="label label-info">NOTE</span> If you want to implement your own *custom* table source or sink, have a look at the [user-defined sources & sinks page]({% link dev/table/sourceSinks.md %}).

<span class="label label-danger">Attention</span> Flink Table & SQL introduces a new set of connector options since 1.11.0, if you are using the legacy connector options, please refer to the [legacy documentation]({% link dev/table/connect.md %}).

* This will be replaced by the TOC
{:toc}

Supported Connectors
------------

Flink natively support various connectors. The following tables list all available connectors.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Name</th>
        <th class="text-center">Version</th>
        <th class="text-center">Source</th>
        <th class="text-center">Sink</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><a href="{% link dev/table/connectors/filesystem.md %}">Filesystem</a></td>
      <td></td>
      <td>Bounded and Unbounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/elasticsearch.md %}">Elasticsearch</a></td>
      <td>6.x & 7.x</td>
      <td>Not supported</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/kafka.md %}">Apache Kafka</a></td>
      <td>0.10+</td>
      <td>Unbounded Scan</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/jdbc.md %}">JDBC</a></td>
      <td></td>
      <td>Bounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{% link dev/table/connectors/hbase.md %}">Apache HBase</a></td>
      <td>1.4.x</td>
      <td>Bounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    </tbody>
</table>

{% top %}

How to use connectors
--------

Flink supports to use SQL CREATE TABLE statement to register a table. One can define the table name, the table schema, and the table options for connecting to an external system.

The following code shows a full example of how to connect to Kafka for reading Json records.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyUserTable (
  -- declare the schema of the table
  `user` BIGINT,
  message STRING,
  ts TIMESTAMP,
  proctime AS PROCTIME(), -- use computed column to define proctime attribute
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND  -- use WATERMARK statement to define rowtime attribute
) WITH (
  -- declare the external system to connect to
  'connector' = 'kafka',
  'topic' = 'topic_name',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'   -- declare a format for this system
)
{% endhighlight %}
</div>
</div>

In this way the desired connection properties are converted into string-based key-value pairs. So-called [table factories]({% link dev/table/sourceSinks.md %}#define-a-tablefactory) create configured table sources, table sinks, and corresponding formats from the key-value pairs. All table factories that can be found via Java's [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) are taken into account when searching for exactly-one matching table factory.

If no factory can be found or multiple factories match for the given properties, an exception will be thrown with additional information about considered factories and supported properties.

{% top %}

Schema Mapping
------------

The body clause of a SQL `CREATE TABLE` statement defines the names and types of columns, constraints and watermarks. Flink doesn't hold the data, thus the schema definition only declares how to map types from an external system to Flink’s representation. The mapping may not be mapped by names, it depends on the implementation of formats and connectors. For example, a MySQL database table is mapped by field names (not case sensitive), and a CSV filesystem is mapped by field order (field names can be arbitrary). This will be explained in every connectors.

The following example shows a simple schema without time attributes and one-to-one field mapping of input/output to table columns.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

### Primary Key

Primary key constraints tell that a column or a set of columns of a table are unique and they do not contain nulls. Primary key uniquely identifies a row in a table.

The primary key of a source table is a metadata information for optimization. The primary key of a sink table is usually used by the sink implementation for upserting.

SQL standard specifies that a constraint can either be ENFORCED or NOT ENFORCED. This controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data the only mode we want to support is the NOT ENFORCED mode. Its up to the user to ensure that the query enforces key integrity.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN,
  PRIMARY KEY (MyField1, MyField2) NOT ENFORCED  -- defines a primary key on columns
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

### Time Attributes

Time attributes are essential when working with unbounded streaming tables. Therefore both proctime and rowtime attributes can be defined as part of the schema.

For more information about time handling in Flink and especially event-time, we recommend the general [event-time section]({% link dev/table/streaming/time_attributes.md %}).

#### Proctime Attributes

In order to declare a proctime attribute in the schema, you can use [Computed Column syntax]({% link dev/table/sql/create.md %}#create-table) to declare a computed column which is generated from `PROCTIME()` builtin function.
The computed column is a virtual column which is not stored in the physical data.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
  MyField4 AS PROCTIME() -- declares a proctime attribute
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

#### Rowtime Attributes

In order to control the event-time behavior for tables, Flink provides predefined timestamp extractors and watermark strategies.

Please refer to [CREATE TABLE statements]({% link dev/table/sql/create.md %}#create-table) for more information about defining time attributes in DDL.

The following timestamp extractors are supported:

<div class="codetabs" markdown="1">
<div data-lang="DDL" markdown="1">
{% highlight sql %}
-- use the existing TIMESTAMP(3) field in schema as the rowtime attribute
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ...
) WITH (
  ...
)

-- use system functions or UDFs or expressions to extract the expected TIMESTAMP(3) rowtime field
CREATE TABLE MyTable (
  log_ts STRING,
  ts_field AS TO_TIMESTAMP(log_ts),
  WATERMARK FOR ts_field AS ...
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

The following watermark strategies are supported:

<div class="codetabs" markdown="1">
<div data-lang="DDL" markdown="1">
{% highlight sql %}
-- Sets a watermark strategy for strictly ascending rowtime attributes. Emits a watermark of the
-- maximum observed timestamp so far. Rows that have a timestamp bigger to the max timestamp
-- are not late.
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field
) WITH (
  ...
)

-- Sets a watermark strategy for ascending rowtime attributes. Emits a watermark of the maximum
-- observed timestamp so far minus 1. Rows that have a timestamp bigger or equal to the max timestamp
-- are not late.
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field - INTERVAL '0.001' SECOND
) WITH (
  ...
)

-- Sets a watermark strategy for rowtime attributes which are out-of-order by a bounded time interval.
-- Emits watermarks which are the maximum observed timestamp minus the specified delay, e.g. 2 seconds.
CREATE TABLE MyTable (
  ts_field TIMESTAMP(3),
  WATERMARK FOR ts_field AS ts_field - INTERVAL '2' SECOND
) WITH (
  ...
)
{% endhighlight %}
</div>
</div>

Make sure to always declare both timestamps and watermarks. Watermarks are required for triggering time-based operations.

### SQL Types

Please see the [Data Types]({% link dev/table/types.md %}) page about how to declare a type in SQL.

{% top %}