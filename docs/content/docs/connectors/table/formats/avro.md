---
title: Avro
weight: 4
type: docs
aliases:
  - /dev/table/connectors/formats/avro.html
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

# Avro Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The [Apache Avro](https://avro.apache.org/) format allows to read and write Avro data based on an Avro schema. Currently, the Avro schema is derived from table schema.

Dependencies
------------

{{< sql_download_table "avro" >}}

How to create a table with Avro format
----------------

Here is an example to create a table using Kafka connector and Avro format.

```sql
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3)
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'avro'
)
```

Format Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Option</th>
        <th class="text-center" style="width: 8%">Required</th>
        <th class="text-center" style="width: 8%">Forwarded</th>
        <th class="text-center" style="width: 7%">Default</th>
        <th class="text-center" style="width: 10%">Type</th>
        <th class="text-center" style="width: 42%">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify what format to use, here should be <code>'avro'</code>.</td>
    </tr>
    <tr>
      <td><h5>avro.encoding</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td>binary</td>
      <td>String</td>
      <td>Serialization encoding to use. The valid enumerations are: <code>binary</code>, <code>json</code>. <a href="https://avro.apache.org/docs/++version++/specification/#encodings">(reference)</a><br>
      Most applications will use the binary encoding, as it results in smaller and more efficient messages, reducing the usage of disk and network resources, and improving performance for high throughput data. <br>
      JSON encoding results in human-readable messages which can be useful during development and debugging, and is useful for compatibility when interacting with systems that cannot process binary encoded data.</td>
    </tr>
    <tr>
      <td><h5>avro.codec</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>For <a href="{{< ref "docs/connectors/table/filesystem" >}}">Filesystem</a> only, the compression codec for avro. Snappy compression as default. The valid enumerations are: null, deflate, snappy, bzip2, xz.</td>
    </tr>
    <tr>
      <td><h5>avro.timestamp_mapping.legacy</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Use the legacy mapping of timestamp in avro. Before 1.19, The default behavior of Flink wrongly mapped both SQL TIMESTAMP and TIMESTAMP_LTZ type to AVRO TIMESTAMP. <br>
      The correct behavior is Flink SQL TIMESTAMP maps Avro LOCAL TIMESTAMP and Flink SQL TIMESTAMP_LTZ maps Avro TIMESTAMP, you can obtain the correct mapping by disable using this legacy mapping. <br>
      Use legacy behavior by default for compatibility consideration. </td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, the Avro schema is always derived from table schema. Explicitly defining an Avro schema is not supported yet.
So the following table lists the type mapping from Flink type to Avro type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Avro type</th>
        <th class="text-left">Avro logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR / VARCHAR / STRING</td>
      <td>string</td>
      <td></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>bytes</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>fixed</code></td>
      <td><code>decimal</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>long</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>float</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>int</code></td>
      <td><code>date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>int</code></td>
      <td><code>time-millis</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>long</code></td>
      <td><code>timestamp-millis</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MAP</code><br>
      (key must be string/char/varchar type)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>MULTISET</code><br>
      (element must be string/char/varchar type)</td>
      <td><code>map</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>record</code></td>
      <td></td>
    </tr>
    </tbody>
</table>

In addition to the types listed above, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/++version++/specification/) for more information about Avro types.
