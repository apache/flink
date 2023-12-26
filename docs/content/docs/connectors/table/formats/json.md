---
title: JSON
weight: 3
type: docs
aliases:
  - /dev/table/connectors/formats/json.html
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

# JSON Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The [JSON](https://www.json.org/json-en.html) format allows to read and write JSON data based on an JSON schema. Currently, the JSON schema is derived from table schema.

The JSON format supports append-only streams, unless you're using a connector that explicitly support retract streams and/or upsert streams like the [Upsert Kafka]({{< ref "docs/connectors/table/upsert-kafka" >}}) connector. If you need to write retract streams and/or upsert streams, we suggest you to look at CDC JSON formats like [Debezium JSON]({{< ref "docs/connectors/table/formats/debezium" >}}) and [Canal JSON]({{< ref "docs/connectors/table/formats/canal" >}}).

Dependencies
------------

{{< sql_download_table "json" >}}

How to create a table with JSON format
----------------

Here is an example to create a table using Kafka connector and JSON format.

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
 'format' = 'json',
 'json.fail-on-missing-field' = 'false',
 'json.ignore-parse-errors' = 'true'
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
      <td>Specify what format to use, here should be <code>'json'</code>.</td>
    </tr>
    <tr>
      <td><h5>json.fail-on-missing-field</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to fail if a field is missing or not.</td>
    </tr>
    <tr>
      <td><h5>json.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td>no</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>
    <tr>
      <td><h5>json.timestamp-format.standard</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;"><code>'SQL'</code></td>
      <td>String</td>
      <td>Specify the input and output timestamp format for <code>TIMESTAMP</code> and <code>TIMESTAMP_LTZ</code> type. Currently supported values are <code>'SQL'</code> and <code>'ISO-8601'</code>:
      <ul>
        <li>Option <code>'SQL'</code> will parse input TIMESTAMP values in "yyyy-MM-dd HH:mm:ss.s{precision}" format, e.g "2020-12-30 12:13:14.123", 
        parse input TIMESTAMP_LTZ values in "yyyy-MM-dd HH:mm:ss.s{precision}'Z'" format, e.g "2020-12-30 12:13:14.123Z" and output timestamp in the same format.</li>
        <li>Option <code>'ISO-8601'</code>will parse input TIMESTAMP in "yyyy-MM-ddTHH:mm:ss.s{precision}" format, e.g "2020-12-30T12:13:14.123" 
        parse input TIMESTAMP_LTZ in "yyyy-MM-ddTHH:mm:ss.s{precision}'Z'" format, e.g "2020-12-30T12:13:14.123Z" and output timestamp in the same format.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>json.map-null-key.mode</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;"><code>'FAIL'</code></td>
      <td>String</td>
      <td>Specify the handling mode when serializing null keys for map data. Currently supported values are <code>'FAIL'</code>, <code>'DROP'</code> and <code>'LITERAL'</code>:
      <ul>
        <li>Option <code>'FAIL'</code> will throw exception when encountering map with null key.</li>
        <li>Option <code>'DROP'</code> will drop null key entries for map data.</li>
        <li>Option <code>'LITERAL'</code> will replace null key with string literal. The string literal is defined by <code>json.map-null-key.literal</code> option.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>json.map-null-key.literal</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">'null'</td>
      <td>String</td>
      <td>Specify string literal to replace null key when <code>'json.map-null-key.mode'</code> is LITERAL.</td>
    </tr>     
    <tr>
      <td><h5>json.encode.decimal-as-plain-number</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Encode all decimals as plain numbers instead of possible scientific notations. By default, decimals may be written using scientific notation. For example, <code>0.000000027</code> is encoded as <code>2.7E-8</code> by default, and will be written as <code>0.000000027</code> if set this option to true.</td>
    </tr>
    <tr>
      <td><h5>decode.json-parser.enabled</h5></td>
      <td>optional</td>
      <td></td>
      <td style="word-wrap: break-word;">true</td>
      <td>Boolean</td>
      <td>Whether to use the Jackson <code>JsonParser</code> to decode json. <code>JsonParser</code> is the Jackson JSON streaming API to read JSON data. This is much faster and consumes less memory compared to the previous <code>JsonNode</code> approach. Meanwhile, <code>JsonParser</code> also supports nested projection pushdown when reading data. This option is enabled by default. You can disable and fallback to the previous <code>JsonNode</code> approach when encountering any incompatibility issues.</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, the JSON schema is always derived from table schema. Explicitly defining an JSON schema is not supported yet.

Flink JSON format uses [jackson databind API](https://github.com/FasterXML/jackson-databind) to parse and generate JSON string.

The following table lists the type mapping from Flink type to JSON type.

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left">Flink SQL type</th>
        <th class="text-left">JSON type</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>CHAR / VARCHAR / STRING</code></td>
      <td><code>string</code></td>
    </tr>
    <tr>
      <td><code>BOOLEAN</code></td>
      <td><code>boolean</code></td>
    </tr>
    <tr>
      <td><code>BINARY / VARBINARY</code></td>
      <td><code>string with encoding: base64</code></td>
    </tr>
    <tr>
      <td><code>DECIMAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>TINYINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>SMALLINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>INT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>BIGINT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>FLOAT</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DOUBLE</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>DATE</code></td>
      <td><code>string with format: date</code></td>
    </tr>
    <tr>
      <td><code>TIME</code></td>
      <td><code>string with format: time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP</code></td>
      <td><code>string with format: date-time</code></td>
    </tr>
    <tr>
      <td><code>TIMESTAMP_WITH_LOCAL_TIME_ZONE</code></td>
      <td><code>string with format: date-time (with UTC time zone)</code></td>
    </tr>
    <tr>
      <td><code>INTERVAL</code></td>
      <td><code>number</code></td>
    </tr>
    <tr>
      <td><code>ARRAY</code></td>
      <td><code>array</code></td>
    </tr>
    <tr>
      <td><code>MAP / MULTISET</code></td>
      <td><code>object</code></td>
    </tr>
    <tr>
      <td><code>ROW</code></td>
      <td><code>object</code></td>
    </tr>
    </tbody>
</table>




