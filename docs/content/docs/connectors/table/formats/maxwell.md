---
title: Maxwell
weight: 7
type: docs
aliases:
  - /dev/table/connectors/formats/maxwell.html
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

# Maxwell Format

{{< label "Changelog-Data-Capture Format" >}}
{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

[Maxwell](https://maxwells-daemon.io/) is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL into Kafka, Kinesis and other streaming connectors. Maxwell provides a unified format schema for changelog and supports to serialize messages using JSON.

Flink supports to interpret Maxwell JSON messages as INSERT/UPDATE/DELETE messages into Flink SQL system. This is useful in many cases to leverage this feature, such as
 - synchronizing incremental data from databases to other systems
 - auditing logs
 - real-time materialized views on databases
 - temporal join changing history of a database table and so on.

Flink also supports to encode the INSERT/UPDATE/DELETE messages in Flink SQL as Maxwell JSON messages, and emit to external systems like Kafka.
However, currently Flink can't combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Flink encodes UPDATE_BEFORE and UDPATE_AFTER as DELETE and INSERT Maxwell messages.

Dependencies
------------

{{< sql_download_table "maxwell" >}}

*Note: please refer to [Maxwell documentation](http://maxwells-daemon.io/quickstart/) about how to synchronize changelog to Kafka topics with Maxwell JSON.*


How to use Maxwell format
----------------

Maxwell provides a unified format for changelog, here is a simple example for an update operation captured from a MySQL `products` table in JSON format:

```json
{
   "database":"test",
   "table":"e",
   "type":"insert",
   "ts":1477053217,
   "xid":23396,
   "commit":true,
   "position":"master.000006:800911",
   "server_id":23042,
   "thread_id":108,
   "primary_key": [1, "2016-10-21 05:33:37.523000"],
   "primary_key_columns": ["id", "c"],
   "data":{
     "id":111,
     "name":"scooter",
     "description":"Big 2-wheel scooter",
     "weight":5.15
   },
   "old":{
     "weight":5.18,
   }
}
```

*Note: please refer to [Maxwell documentation](http://maxwells-daemon.io/dataformat/) about the meaning of each fields.*

The MySQL `products` table has 4 columns (`id`, `name`, `description` and `weight`). The above JSON message is an update change event on the `products` table where the `weight` value of the row with `id = 111` is changed from `5.18` to `5.15`.
Assuming this messages is synchronized to Kafka topic `products_binlog`, then we can use the following DDL to consume this topic and interpret the change events.

```sql
CREATE TABLE topic_products (
  -- schema is totally the same to the MySQL "products" table
  id BIGINT,
  name STRING,
  description STRING,
  weight DECIMAL(10, 2)
) WITH (
 'connector' = 'kafka',
 'topic' = 'products_binlog',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'maxwell-json'
)
```

After registering the topic as a Flink table, then you can consume the Maxwell messages as a changelog source.

```sql
-- a real-time materialized view on the MySQL "products"
-- which calculate the latest average of weight for the same products
SELECT name, AVG(weight) FROM topic_products GROUP BY name;

-- synchronize all the data and incremental changes of MySQL "products" table to
-- Elasticsearch "products" index for future searching
INSERT INTO elasticsearch_products
SELECT * FROM topic_products;
```

Available Metadata
------------------

The following format metadata can be exposed as read-only (`VIRTUAL`) columns in a table definition.

{{< hint info >}}
Format metadata fields are only available if the
corresponding connector forwards format metadata. Currently, only the Kafka connector is able to expose
metadata fields for its value format.
{{< /hint >}}

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 40%">Data Type</th>
      <th class="text-center" style="width: 40%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>database</code></td>
      <td><code>STRING NULL</code></td>
      <td>The originating database. Corresponds to the <code>database</code> field in the
      Maxwell record if available.</td>
    </tr>
    <tr>
      <td><code>table</code></td>
      <td><code>STRING NULL</code></td>
      <td>The originating database table. Corresponds to the <code>table</code> field in the
      Maxwell record if available.</td>
    </tr>
    <tr>
      <td><code>primary-key-columns</code></td>
      <td><code>ARRAY&lt;STRING&gt; NULL</code></td>
      <td>Array of primary key names. Corresponds to the <code>primary_key_columns</code> field in the 
      Maxwell record if available.</td>
    </tr>
    <tr>
      <td><code>ingestion-timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(3) NULL</code></td>
      <td>The timestamp at which the connector processed the event. Corresponds to the <code>ts</code>
      field in the Maxwell record.</td>
    </tr>
    </tbody>
</table>

The following example shows how to access Maxwell metadata fields in Kafka:

```sql
CREATE TABLE KafkaTable (
  origin_database STRING METADATA FROM 'value.database' VIRTUAL,
  origin_table STRING METADATA FROM 'value.table' VIRTUAL,
  origin_primary_key_columns ARRAY<STRING> METADATA FROM 'value.primary-key-columns' VIRTUAL,
  origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'maxwell-json'
);
```

Format Options
----------------

<div data-lang="Maxwell Json" markdown="1">

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
      <td>Specify what format to use, here should be <code>'maxwell-json'</code>.</td>
    </tr>
    <tr>
      <td><h5>maxwell-json.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>
    <tr>
       <td><h5>maxwell-json.timestamp-format.standard</h5></td>
       <td>optional</td>
       <td style="word-wrap: break-word;"><code>'SQL'</code></td>
       <td>String</td>
       <td>Specify the input and output timestamp format. Currently supported values are <code>'SQL'</code> and <code>'ISO-8601'</code>:
       <ul>
         <li>Option <code>'SQL'</code> will parse input timestamp in "yyyy-MM-dd HH:mm:ss.s{precision}" format, e.g '2020-12-30 12:13:14.123' and output timestamp in the same format.</li>
         <li>Option <code>'ISO-8601'</code>will parse input timestamp in "yyyy-MM-ddTHH:mm:ss.s{precision}" format, e.g '2020-12-30T12:13:14.123' and output timestamp in the same format.</li>
       </ul>
       </td>
    </tr>
    <tr>
      <td><h5>maxwell-json.map-null-key.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>'FAIL'</code></td>
      <td>String</td>
      <td>Specify the handling mode when serializing null keys for map data. Currently supported values are <code>'FAIL'</code>, <code>'DROP'</code> and <code>'LITERAL'</code>:
      <ul>
        <li>Option <code>'FAIL'</code> will throw exception when encountering map with null key.</li>
        <li>Option <code>'DROP'</code> will drop null key entries for map data.</li>
        <li>Option <code>'LITERAL'</code> will replace null key with string literal. The string literal is defined by <code>maxwell-json.map-null-key.literal</code> option.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>maxwell-json.map-null-key.literal</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">'null'</td>
      <td>String</td>
      <td>Specify string literal to replace null key when <code>'maxwell-json.map-null-key.mode'</code> is LITERAL.</td>
    </tr>
    <tr>
      <td><h5>maxwell-json.encode.decimal-as-plain-number</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Encode all decimals as plain numbers instead of possible scientific notations. By default, decimals may be written using scientific notation. For example, <code>0.000000027</code> is encoded as <code>2.7E-8</code> by default, and will be written as <code>0.000000027</code> if set this option to true.</td>
    </tr>
    <tr>
      <td><h5>maxwell-json.allow-non-numeric-numbers</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Whether to fail if a field is non-numeric number or not. For example, <code>NaN</code></td>
    </tr>
    </tbody>
</table>

</div>

Caveats
----------------

### Duplicate change events

The Maxwell application allows to deliver every change event **exactly-once**. Flink works pretty well when consuming Maxwell produced events in this situation.
If Maxwell application works in **at-least-once** delivery, it may deliver duplicate change events to Kafka and Flink will get the duplicate events.
This may cause Flink query to get wrong results or unexpected exceptions. Thus, it is recommended to set job configuration [`table.exec.source.cdc-events-duplicate`]({{< ref "docs/dev/table/config" >}}#table-exec-source-cdc-events-duplicate) to `true` and define PRIMARY KEY on the source in this situation.
Framework will generate an additional stateful operator, and use the primary key to deduplicate the change events and produce a normalized changelog stream.

Data Type Mapping
----------------

Currently, the Maxwell format uses JSON for serialization and deserialization. Please refer to [JSON Format documentation]({{< ref "docs/connectors/table/formats/json" >}}#data-type-mapping) for more details about the data type mapping.
