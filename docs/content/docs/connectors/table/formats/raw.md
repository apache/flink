---
title: Raw
weight: 10
type: docs
aliases:
  - /dev/table/connectors/formats/raw.html
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

# Raw Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The Raw format allows to read and write raw (byte based) values as a single column.

Note: this format encodes `null` values as `null` of `byte[]` type. This may have limitation when used in `upsert-kafka`, because `upsert-kafka` treats `null` values as a tombstone message (DELETE on the key). Therefore, we recommend avoiding using `upsert-kafka` connector and the `raw` format as a `value.format` if the field can have a `null` value.

The Raw connector is built-in into the Blink planner, no additional dependencies are required.

Example
----------------

For example, you may have following raw log data in Kafka and want to read and analyse such data using Flink SQL.

```
47.29.201.179 - - [28/Feb/2019:13:17:10 +0000] "GET /?p=1 HTTP/2.0" 200 5316 "https://domain.com/?p=1" "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/72.0.3626.119 Safari/537.36" "2.75"
```

The following creates a table where it reads from (and can writes to) the underlying Kafka topic as an anonymous string value in UTF-8 encoding by using `raw` format:

```sql
CREATE TABLE nginx_log (
  log STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'nginx_log',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'format' = 'raw'
)
```

Then you can read out the raw data as a pure string, and split it into multiple fields using an user-defined-function for further analysing, e.g. `my_split` in the example.

```sql
SELECT t.hostname, t.datetime, t.url, t.browser, ...
FROM(
  SELECT my_split(log) as t FROM nginx_log
);
```

In contrast, you can also write a single column of STRING type into this Kafka topic as an anonymous string value in UTF-8 encoding.

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
      <td>Specify what format to use, here should be 'raw'.</td>
    </tr>
    <tr>
      <td><h5>raw.charset</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">UTF-8</td>
      <td>String</td>
      <td>Specify the charset to encode the text string.</td>
    </tr>
    <tr>
      <td><h5>raw.endianness</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">big-endian</td>
      <td>String</td>
      <td>Specify the endianness to encode the bytes of numeric value. Valid values are 'big-endian' and 'little-endian'.
      See more details of <a href="https://en.wikipedia.org/wiki/Endianness">endianness</a>.</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

The table below details the SQL types the format supports, including details of the serializer and deserializer class for encoding and decoding.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Value</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td>A UTF-8 (by default) encoded text string.<br>
       The encoding charset can be configured by 'raw.charset'.</td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY / BYTES</code></td>
      <td>The sequence of bytes itself.</td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td>A single byte to indicate boolean value, 0 means false, 1 means true.</td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td>A single byte of the singed number value.</td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td>Two bytes with big-endian (by default) encoding.<br>
       The endianness can be configured by 'raw.endianness'.</td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td>Four bytes with big-endian (by default) encoding.<br>
       The endianness can be configured by 'raw.endianness'.</td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td>Eight bytes with big-endian (by default) encoding.<br>
       The endianness can be configured by 'raw.endianness'.</td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td>Four bytes with IEEE 754 format and big-endian (by default) encoding.<br>
       The endianness can be configured by 'raw.endianness'.</td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td>Eight bytes with IEEE 754 format and big-endian (by default) encoding.<br>
       The endianness can be configured by 'raw.endianness'.</td>
    </tr>
    <tr>
      <td><code>RAW</code></td>
      <td>The sequence of bytes serialized by the underlying TypeSerializer of the RAW type.</td>
    </tr>
    </tbody>
</table>

