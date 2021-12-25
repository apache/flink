---
title: "Overview"
weight: 1
type: docs
aliases:
  - /dev/table/connectors/
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

# Table & SQL Connectors


Flink's Table API & SQL programs can be connected to other external systems for reading and writing both batch and streaming tables. A table source provides access to data which is stored in external systems (such as a database, key-value store, message queue, or file system). A table sink emits a table to an external storage system. Depending on the type of source and sink, they support different formats such as CSV, Avro, Parquet, or ORC.

This page describes how to register table sources and table sinks in Flink using the natively supported connectors. After a source or sink has been registered, it can be accessed by Table API & SQL statements.

If you want to implement your own *custom* table source or sink, have a look at the [user-defined sources & sinks page]({{< ref "docs/dev/table/sourcessinks" >}}).

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
      <td><a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a></td>
      <td></td>
      <td>Bounded and Unbounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/elasticsearch" >}}">Elasticsearch</a></td>
      <td>6.x & 7.x</td>
      <td>Not supported</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/kafka" >}}">Apache Kafka</a></td>
      <td>0.10+</td>
      <td>Unbounded Scan</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/kinesis" >}}">Amazon Kinesis Data Streams</a></td>
      <td></td>
      <td>Unbounded Scan</td>
      <td>Streaming Sink</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/jdbc" >}}">JDBC</a></td>
      <td></td>
      <td>Bounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/hbase" >}}">Apache HBase</a></td>
      <td>1.4.x & 2.2.x</td>
      <td>Bounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    <tr>
      <td><a href="{{< ref "docs/connectors/table/hive/overview" >}}">Apache Hive</a></td>
      <td><a href="{{< ref "docs/connectors/table/hive/overview" >}}#supported-hive-versions">Supported Versions</a></td>
      <td>Unbounded Scan, Bounded Scan, Lookup</td>
      <td>Streaming Sink, Batch Sink</td>
    </tr>
    </tbody>
</table>

{{< top >}}

How to use connectors
--------

Flink supports using SQL `CREATE TABLE` statements to register tables. One can define the table name,
the table schema, and the table options for connecting to an external system.

See the [SQL section for more information about creating a table]({{< ref "docs/dev/table/sql/create" >}}#create-table).

The following code shows a full example of how to connect to Kafka for reading and writing JSON records.

{{< tabs "6d4f00e3-0a94-4ebd-b6b5-c5171851b500" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyUserTable (
  -- declare the schema of the table
  `user` BIGINT,
  `message` STRING,
  `rowtime` TIMESTAMP(3) METADATA FROM 'timestamp',    -- use a metadata column to access Kafka's record timestamp
  `proctime` AS PROCTIME(),    -- use a computed column to define a proctime attribute
  WATERMARK FOR `rowtime` AS `rowtime` - INTERVAL '5' SECOND    -- use a WATERMARK statement to define a rowtime attribute
) WITH (
  -- declare the external system to connect to
  'connector' = 'kafka',
  'topic' = 'topic_name',
  'scan.startup.mode' = 'earliest-offset',
  'properties.bootstrap.servers' = 'localhost:9092',
  'format' = 'json'   -- declare a format for this system
)
```
{{< /tab >}}
{{< /tabs >}}

The desired connection properties are converted into string-based key-value pairs. [Factories]({{< ref "docs/dev/table/sourcessinks" >}})
will create configured table sources, table sinks, and corresponding formats from the key-value pairs
based on factory identifiers (`kafka` and `json` in this example). All factories that can be found via
Java's [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html)
are taken into account when searching for exactly one matching factory for each component.

If no factory can be found or multiple factories match for the given properties, an exception will be
thrown with additional information about considered factories and supported properties.


Transform table connector/format resources
--------

Flink uses Java's [Service Provider Interfaces (SPI)](https://docs.oracle.com/javase/tutorial/sound/SPI-intro.html) to load the table connector/format factories by their identifiers. Since the SPI resource file named `org.apache.flink.table.factories.Factory` for every table connector/format is under the same directory `META-INF/services`, these resource files will override each other when build the uber-jar of the project which uses more than one table connector/format, which will cause Flink to fail to load table connector/format factories.

In this situation, the recommended way is transforming these resource files under the directory `META-INF/services` by [ServicesResourceTransformer](https://maven.apache.org/plugins/maven-shade-plugin/examples/resource-transformers.html) of maven shade plugin. Given the pom.xml file content of example that contains connector `flink-sql-connector-hive-3.1.2` and format `flink-parquet` in a project.

```xml

    <modelVersion>4.0.0</modelVersion>
    <groupId>org.example</groupId>
    <artifactId>myProject</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!--  other project dependencies  ...-->
        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-sql-connector-hive-3.1.2{{< scala_version >}}</artifactId>
            <version>{{< version >}}</version>
        </dependency>

        <dependency>
            <groupId>org.apache.flink</groupId>
            <artifactId>flink-parquet{{< scala_version >}}</artifactId>
            <version>{{< version >}}</version>
        </dependency>

    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-shade-plugin</artifactId>
                <executions>
                    <execution>
                        <id>shade</id>
                        <phase>package</phase>
                        <goals>
                            <goal>shade</goal>
                        </goals>
                        <configuration>
                            <transformers combine.children="append">
                                <!-- The service transformer is needed to merge META-INF/services files -->
                                <transformer implementation="org.apache.maven.plugins.shade.resource.ServicesResourceTransformer"/>
                                <!-- ... -->
                            </transformers>
                        </configuration>
                    </execution>
                </executions>
            </plugin>
        </plugins>
    </build>
```

After configured the `ServicesResourceTransformer`, the table connector/format resource files under the directory `META-INF/services` would be merged rather than overwritten each other when build the uber-jar of above project.

{{< top >}}

Schema Mapping
------------

The body clause of a SQL `CREATE TABLE` statement defines the names and types of physical columns,
constraints and watermarks. Flink doesn't hold the data, thus the schema definition only declares how
to map physical columns from an external system to Flink’s representation. The mapping may not be
mapped by names, it depends on the implementation of formats and connectors. For example, a MySQL database
table is mapped by field names (not case sensitive), and a CSV filesystem is mapped by field order
(field names can be arbitrary). This will be explained in every connector.

The following example shows a simple schema without time attributes and one-to-one field mapping
of input/output to table columns.

{{< tabs "0c267c40-32ef-4a00-b4eb-fa39bfe3f14d" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

### Metadata

Some connectors and formats expose additional metadata fields that can be accessed in metadata columns
next to the physical payload columns. See the [`CREATE TABLE` section]({{< ref "docs/dev/table/sql/create" >}}#columns)
for more information about metadata columns.

### Primary Key

Primary key constraints tell that a column or a set of columns of a table are unique and they do not contain nulls. Primary key uniquely identifies a row in a table.

The primary key of a source table is a metadata information for optimization. The primary key of a sink table is usually used by the sink implementation for upserting.

SQL standard specifies that a constraint can either be ENFORCED or NOT ENFORCED. This controls if the constraint checks are performed on the incoming/outgoing data. Flink does not own the data the only mode we want to support is the NOT ENFORCED mode. Its up to the user to ensure that the query enforces key integrity.

{{< tabs "9e32660c-868b-4b6a-9632-3b3ea482fe7d" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN,
  PRIMARY KEY (MyField1, MyField2) NOT ENFORCED  -- defines a primary key on columns
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

### Time Attributes

Time attributes are essential when working with unbounded streaming tables. Therefore both proctime and rowtime attributes can be defined as part of the schema.

For more information about time handling in Flink and especially event-time, we recommend the general [event-time section]({{< ref "docs/dev/table/concepts/time_attributes" >}}).

#### Proctime Attributes

In order to declare a proctime attribute in the schema, you can use [Computed Column syntax]({{< ref "docs/dev/table/sql/create" >}}#create-table) to declare a computed column which is generated from `PROCTIME()` builtin function.
The computed column is a virtual column which is not stored in the physical data.

{{< tabs "5d1f475b-a002-4e85-84f4-00ab0a55a548" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE MyTable (
  MyField1 INT,
  MyField2 STRING,
  MyField3 BOOLEAN
  MyField4 AS PROCTIME() -- declares a proctime attribute
) WITH (
  ...
)
```
{{< /tab >}}
{{< /tabs >}}

#### Rowtime Attributes

In order to control the event-time behavior for tables, Flink provides predefined timestamp extractors and watermark strategies.

Please refer to [CREATE TABLE statements]({{< ref "docs/dev/table/sql/create" >}}#create-table) for more information about defining time attributes in DDL.

The following timestamp extractors are supported:

{{< tabs "b40272ba-b259-4a26-9651-815006b283e7" >}}
{{< tab "DDL" >}}
```sql
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
```
{{< /tab >}}
{{< /tabs >}}

The following watermark strategies are supported:

{{< tabs "e004ebfb-75b1-4d81-80ff-ac5420744b75" >}}
{{< tab "DDL" >}}
```sql
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
```
{{< /tab >}}
{{< /tabs >}}

Make sure to always declare both timestamps and watermarks. Watermarks are required for triggering time-based operations.

### SQL Types

Please see the [Data Types]({{< ref "docs/dev/table/types" >}}) page about how to declare a type in SQL.

{{< top >}}
