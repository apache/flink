---
title: Thrift
weight: 11
type: docs
aliases:
  - /dev/table/connectors/formats/thrift.html
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

# Thrift Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The [Apache Thrift](https://thrift.apache.org/) format allows to read and write Thrift data, based on table schema and thrift class.
When define table schema, the type of field in table schema must match with the type in thrift class.

Dependencies
------------

{{< sql_download_table "thrift" >}}

How to create a table with Thrift format
----------------

Here is an example to create a table using Kafka connector and Thrift format.

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
 'format' = 'thrift',
 'thrift.class' = 'org.apache.format.generated.thrift.Event',
 'thrift.protocol' = 'org.apache.thrift.protocol.TCompactProtocol'
)
```

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
      <td>Specify what format to use, here should be <code>'thrift'</code>.</td>
    </tr>
    <tr>
      <td><h5>thrift.class</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify the thrift class for deserialize or serialize.</td>
    </tr>
    <tr>
      <td><h5>thrift.protocol</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify the thrift protocol type for deserialize or serialize. </td>
    </tr>
    <tr>
      <td><h5>thrift.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>    
    <tr>
      <td><h5>thrift.ignore-field-mismatch</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Set the mismatch field to null instead of failing.</td>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, the Thrift schema (de)serialize messages based on both thrift class and table schema, 
So the following table lists the type mapping from Flink type to Thrift type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Thrift type</th>
        <th class="text-left">Thrift logical type</th>
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
      <td><code>bool</code></td>
      <td><code>boolean</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>binary</code></td>
      <td><code>ByteBuffer</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>binary</code></td>
      <td><code>ByteBuffer</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>byte</code></td>
      <td><code>byte</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>i16</code></td>
      <td><code>short</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>i32</code></td>
      <td><code>int</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>i64</code></td>
      <td><code>long</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>double</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>double</code></td>
      <td><code>double</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>i32</code></td>
      <td><code>int</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>i32</code></td>
      <td><code>time-millis</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>i64</code></td>
      <td><code>timestamp-millis</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>list/set</code></td>
      <td><code>list/set</code></td>
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
      <td><code>struct</code></td>
      <td></td>
    </tr>
    </tbody>
</table>

In addition to the types listed above, Flink supports reading/writing nullable types. Flink maps nullable types to Thrift `union(something, null)`, where `something` is the Thrift type converted from Flink type.

You can refer to [Apache Thrift Documentation](https://thrift.apache.org/docs/) for more information about Thrift types.
