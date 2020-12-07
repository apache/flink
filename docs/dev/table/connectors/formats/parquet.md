---
title: "Parquet Format"
nav-title: Parquet
nav-parent_id: sql-formats
nav-pos: 7
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

The [Apache Parquet](https://parquet.apache.org/) format allows to read and write Parquet data.

Dependencies
------------

{% assign connector = site.data.sql-connectors['parquet'] %}
{% include sql-connector-download-table.html
    connector=connector
%}

How to create a table with Parquet format
----------------

Here is an example to create a table using Filesystem connector and Parquet format.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3),
  dt STRING
) PARTITIONED BY (dt) WITH (
 'connector' = 'filesystem',
 'path' = '/tmp/user_behavior',
 'format' = 'parquet'
)
{% endhighlight %}
</div>
</div>

Format Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 50%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what format to use, here should be 'parquet'.</td>
    </tr>
    <tr>
      <td><h5>parquet.utc-timezone</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Use UTC timezone or local timezone to the conversion between epoch time and LocalDateTime. Hive 0.x/1.x/2.x use local timezone. But Hive 3.x use UTC timezone.</td>
    </tr>
    </tbody>
</table>

Parquet format also supports configuration from [ParquetOutputFormat](https://www.javadoc.io/doc/org.apache.parquet/parquet-hadoop/1.10.0/org/apache/parquet/hadoop/ParquetOutputFormat.html).
For example, you can configure `parquet.compression=GZIP` to enable gzip compression.

Data Type Mapping
----------------

Currently, Parquet format type mapping is compatible with Apache Hive, but different with Apache Spark:

- Timestamp: mapping timestamp type to int96 whatever the precision is.
- Decimal: mapping decimal type to fixed length byte array according to the precision.

The following table lists the type mapping from Flink type to Parquet type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink Data Type</th>
        <th class="text-center">Parquet type</th>
        <th class="text-center">Parquet logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>BINARY</td>
      <td>UTF8</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>BOOLEAN</td>
      <td></td>
    </tr>
    <tr>
      <td>BINARY / VARBINARY</td>
      <td>BINARY</td>
      <td></td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>FIXED_LEN_BYTE_ARRAY</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>INT32</td>
      <td>INT_8</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>INT32</td>
      <td>INT_16</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>INT32</td>
      <td></td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>INT64</td>
      <td></td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>FLOAT</td>
      <td></td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>DOUBLE</td>
      <td></td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>INT32</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIME</td>
      <td>INT32</td>
      <td>TIME_MILLIS</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>INT96</td>
      <td></td>
    </tr>
    </tbody>
</table>

<span class="label label-danger">Attention</span> Composite data type: Array, Map and Row are not supported.
