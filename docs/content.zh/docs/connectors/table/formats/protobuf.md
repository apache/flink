---
title: Protobuf
weight: 4
type: docs
aliases:
- /dev/table/connectors/formats/protobuf.html
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

# Protobuf Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The Protocol Buffers [Protobuf](https://developers.google.com/protocol-buffers) format allows you to read and write Protobuf data, based on Protobuf generated classes.

Dependencies
------------

{{< sql_download_table "protobuf" >}}

How to create a table with Protobuf format
----------------

Here is an example to create a table using the Kafka connector and Protobuf format.

Below is the proto definition file.

```
syntax = "proto2";
package com.example;
option java_package = "com.example";
option java_multiple_files = true;

message SimpleTest {
    optional int64 uid = 1;
    optional string name = 2;
    optional int32 category_type = 3;
    optional bytes content = 4;
    optional double price = 5;
    map<int64, InnerMessageTest> value_map = 6;
    repeated  InnerMessageTest value_arr = 7;
    optional Corpus corpus_int = 8; 
    optional Corpus corpus_str = 9; 
    
    message InnerMessageTest{
          optional int64 v1 =1;
          optional int32 v2 =2;
    }
    
    enum Corpus {
        UNIVERSAL = 0;
        WEB = 1;
        IMAGES = 2;
        LOCAL = 3;
        NEWS = 4;
        PRODUCTS = 5;
        VIDEO = 7;
      }
}
```

1. Use [`protoc`](https://developers.google.com/protocol-buffers/docs/javatutorial#compiling-your-protocol-buffers) command to compile the `.proto` file to java classes
2. Then compile and package the classes (there is no need to package proto-java into the jar)
3. Finally you should provide the `jar` in your classpath, e.g. pass it using `-j` in <a href="{{< ref "docs/dev/table/sqlClient" >}}">sql-client</a>

```sql
CREATE TABLE simple_test (
  uid BIGINT,
  name STRING,
  category_type INT,
  content BINARY,
  price DOUBLE,
  value_map map<BIGINT, row<v1 BIGINT, v2 INT>>,
  value_arr array<row<v1 BIGINT, v2 INT>>,
  corpus_int INT,
  corpus_str STRING
) WITH (
 'connector' = 'kafka',
 'topic' = 'user_behavior',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'protobuf',
 'protobuf.message-class-name' = 'com.example.SimpleTest',
 'protobuf.ignore-parse-errors' = 'true'
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
      <td>Specify what format to use, here should be <code>'protobuf'</code>.</td>
    </tr>
    <tr>
      <td><h5>protobuf.message-class-name</h5></td>
      <td>required</td>
      <td>no</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The full name of a Protobuf generated class. The name must match the message name in the proto definition file. <code>$</code> is supported for inner class names, like 'com.exmample.OuterClass$MessageClass'</td>
    </tr>
    <tr>
      <td><h5>protobuf.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Optional flag to skip rows with parse errors instead of failing.</td>
    </tr>
    <tr>
      <td><h5>protobuf.read-default-values</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>
          This option only works if the generated class's version is proto2. If this value is set to true, the format will read empty values as the default values defined in the proto file.
          If the value is set to false, the format will generate null values if the data element does not exist in the binary protobuf message.
          If the proto syntax is proto3, this value will forcibly be set to true, because proto3's standard is to use default values.
      </td>
    </tr>
    <tr>
      <td><h5>protobuf.write-null-string-literal</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">""</td>
      <td>String</td>
      <td>
          When serializing to protobuf data, this is the optional config to specify the string literal in Protobuf's array/map in case of null values.
      </td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

The following table lists the type mapping from Flink type to Protobuf type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">Protobuf type</th>
        <th class="text-left">Description</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>bool</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>bytes</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>int32</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>int64</code></td>
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
      <td><code>ARRAY</code></td>
      <td><code>repeated</code></td>
      <td>Elements cannot be null, the string default value can be specified by <code>write-null-string-literal</code></td>
    </tr>
    <tr>
      <td><code>MAP</code></td>
      <td><code>map</code></td>
      <td>Keys or values cannot be null, the string default value can be specified by <code>write-null-string-literal</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>message</code></td>
      <td></td>
    </tr>
    <tr>
      <td><code>VARCHAR / CHAR / TINYINT / SMALLINT / INTEGER / BIGINT</code></td>
      <td><code>enum</code></td>
      <td>The enum value of protobuf can be mapped to string or number of flink row accordingly.</td>
    </tr>
    <tr>
      <td><code>ROW&lt;seconds BIGINT, nanos INT&gt;</code></td>
      <td><code>google.protobuf.timestamp</code></td>
      <td>The google.protobuf.timestamp type can be mapped to seconds and fractions of seconds at nanosecond resolution in UTC epoch time using the row type as well as the protobuf definition.</td>
    </tr>
    </tbody>
</table>

Null Values
----------------
As protobuf does not permit null values in maps and array, we need to auto-generate default values when converting from Flink Rows to Protobuf.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Protobuf Data Type</th>
        <th class="text-left">Default Value</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td>int32 / int64 / float / double</td>
      <td>0</td>
    </tr>
    <tr>
      <td>string</td>
      <td>""</td>
    </tr>
    <tr>
      <td>bool</td>
      <td>false</td>
    </tr>
    <tr>
      <td>enum</td>
      <td>first element of enum</td>
    </tr>
    <tr>
      <td>binary</td>
      <td>ByteString.EMPTY</td>
    </tr>
    <tr>
      <td>message</td>
      <td>MESSAGE.getDefaultInstance()</td>
    </tr>
    </tbody>
</table>

OneOf field
----------------
In the serialization process, there's no guarantee that the Flink fields of the same one-of group only contain at most one valid value.
When serializing, each field is set in the order of Flink schema, so the field in the higher position will override the field in lower position in the same one-of group.

You can refer to [Language Guide (proto2)](https://developers.google.com/protocol-buffers/docs/proto) or [Language Guide (proto3)](https://developers.google.com/protocol-buffers/docs/proto3) for more information about Protobuf types.
