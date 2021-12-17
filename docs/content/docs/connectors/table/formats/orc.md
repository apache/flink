---
title: Orc
weight: 9
type: docs
aliases:
  - /dev/table/connectors/formats/orc.html
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

# Orc Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The [Apache Orc](https://orc.apache.org/) format allows to read and write Orc data.

Dependencies
------------

{{< sql_download_table "orc" >}}

How to create a table with Orc format
----------------

Here is an example to create a table using Filesystem connector and Orc format.

```sql
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
 'format' = 'orc'
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
      <td>Specify what format to use, here should be 'orc'.</td>
    </tr>
    </tbody>
</table>

Orc format also supports table properties from [Table properties](https://orc.apache.org/docs/hive-config.html#table-properties).
For example, you can configure `orc.compress=SNAPPY` to enable snappy compression.

Data Type Mapping
----------------

Orc format type mapping is compatible with Apache Hive.
The following table lists the type mapping from Flink type to Orc type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink Data Type</th>
        <th class="text-center">Orc physical type</th>
        <th class="text-center">Orc logical type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>CHAR</td>
      <td>bytes</td>
      <td>CHAR</td>
    </tr>
    <tr>
      <td>VARCHAR</td>
      <td>bytes</td>
      <td>VARCHAR</td>
    </tr>
    <tr>
      <td>STRING</td>
      <td>bytes</td>
      <td>STRING</td>
    </tr>
    <tr>
      <td>BOOLEAN</td>
      <td>long</td>
      <td>BOOLEAN</td>
    </tr>
    <tr>
      <td>BYTES</td>
      <td>bytes</td>
      <td>BINARY</td>
    </tr>
    <tr>
      <td>DECIMAL</td>
      <td>decimal</td>
      <td>DECIMAL</td>
    </tr>
    <tr>
      <td>TINYINT</td>
      <td>long</td>
      <td>BYTE</td>
    </tr>
    <tr>
      <td>SMALLINT</td>
      <td>long</td>
      <td>SHORT</td>
    </tr>
    <tr>
      <td>INT</td>
      <td>long</td>
      <td>INT</td>
    </tr>
    <tr>
      <td>BIGINT</td>
      <td>long</td>
      <td>LONG</td>
    </tr>
    <tr>
      <td>FLOAT</td>
      <td>double</td>
      <td>FLOAT</td>
    </tr>
    <tr>
      <td>DOUBLE</td>
      <td>double</td>
      <td>DOUBLE</td>
    </tr>
    <tr>
      <td>DATE</td>
      <td>long</td>
      <td>DATE</td>
    </tr>
    <tr>
      <td>TIMESTAMP</td>
      <td>timestamp</td>
      <td>TIMESTAMP</td>
    </tr>
    <tr>
      <td>ARRAY</td>
      <td>-</td>
      <td>LIST</td>
    </tr>
    <tr>
      <td>MAP</td>
      <td>-</td>
      <td>MAP</td>
    </tr>
    <tr>
      <td>ROW</td>
      <td>-</td>
      <td>STRUCT</td>
    </tr>
    </tbody>
</table>
