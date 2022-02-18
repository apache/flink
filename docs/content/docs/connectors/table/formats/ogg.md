---
title: Ogg
weight: 8
type: docs
aliases:
  - /dev/table/connectors/formats/ogg.html
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

# Ogg Format

{{< label "Changelog-Data-Capture Format" >}} {{< label "Format: Serialization Schema" >}} {{<
label "Format: Deserialization Schema" >}}

[Oracle GoldenGate](https://www.oracle.com/integration/goldengate/) (a.k.a ogg) is a managed service
providing a real-time data mesh platform, which uses replication to keep data highly available, and
enabling real-time analysis. Customers can design, execute, and monitor their data replication and
stream data processing solutions without the need to allocate or manage compute environments. Ogg
provides a format schema for changelog and supports to serialize messages using JSON.

Flink supports to interpret Ogg JSON as INSERT/UPDATE/DELETE messages into Flink SQL system. This is
useful in many cases to leverage this feature, such as

- synchronizing incremental data from databases to other systems
- auditing logs
- real-time materialized views on databases
- temporal join changing history of a database table and so on.

Flink also supports to encode the INSERT/UPDATE/DELETE messages in Flink SQL as Ogg JSON, and emit
to external systems like Kafka. However, currently Flink can't combine UPDATE_BEFORE and
UPDATE_AFTER into a single UPDATE message. Therefore, Flink encodes UPDATE_BEFORE and UPDATE_AFTER
as DELETE and INSERT Ogg messages.

Dependencies
------------

#### Ogg Json

{{< sql_download_table "ogg-json" >}}

*Note: please refer
to [Ogg Kafka Handler documentation](https://docs.oracle.com/en/middleware/goldengate/big-data/19.1/gadbd/using-kafka-handler.html)
about how to set up an Ogg Kafka handler to synchronize changelog to Kafka topics.*


How to use Ogg format
----------------

Ogg provides a unified format for changelog, here is a simple example for an update operation
captured from an Oracle `PRODUCTS` table in JSON format:

```json
{
  "before": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.18
  },
  "after": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.15
  },
  "op_type": "U",
  "op_ts": "2020-05-13 15:40:06.000000",
  "current_ts": "2020-05-13 15:40:07.000000",
  "primary_keys": [
    "id"
  ],
  "pos": "00000000000000000000143",
  "table": "PRODUCTS"
}
```

*Note: please refer
to [Debezium documentation](https://debezium.io/documentation/reference/1.3/connectors/oracle.html#oracle-events)
about the meaning of each field.*

The Oracle `PRODUCTS` table has 4 columns (`id`, `name`, `description` and `weight`). The above JSON
message is an update change event on the `PRODUCTS` table where the `weight` value of the row
with `id = 111` is changed from `5.18` to `5.15`. Assuming this messages is synchronized to Kafka
topic `products_ogg`, then we can use the following DDL to consume this topic and interpret the
change events.

```sql
CREATE TABLE topic_products (
  -- schema is totally the same to the Oracle "products" table
  id BIGINT,
  name STRING,
  description STRING,
  weight DECIMAL(10, 2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'products_ogg',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'format' = 'ogg-json'
)
```

After registering the topic as a Flink table, then you can consume the Ogg messages as a changelog
source.

```sql
-- a real-time materialized view on the Oracle "PRODUCTS"
-- which calculate the latest average of weight for the same products
SELECT name, AVG(weight)
FROM topic_products
GROUP BY name;

-- synchronize all the data and incremental changes of Oracle "PRODUCTS" table to
-- Elasticsearch "products" index for future searching
INSERT INTO elasticsearch_products
SELECT *
FROM topic_products;
```

Available Metadata
------------------

The following format metadata can be exposed as read-only (`VIRTUAL`) columns in a table definition.

<span class="label label-danger">Attention</span> Format metadata fields are only available if the
corresponding connector forwards format metadata. Currently, only the Kafka connector is able to
expose metadata fields for its value format.

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
      <td><code>table</code></td>
      <td><code>STRING NULL </code></td>
      <td>Contains fully qualified table name. The format of the fully qualified table name is: 
        CATALOG NAME.SCHEMA NAME.TABLE NAME</td>
    </tr>
    <tr>
      <td><code>primary-keys</code></td>
      <td><code>ARRAY&lt;STRING&gt; NULL</code></td>
      <td>An array variable holding the column names of the primary keys of the source table. 
        The primary-keys field is only include in the JSON output if the includePrimaryKeys 
        configuration property is set to true.</td>
    </tr>
    <tr>
      <td><code>ingestion-timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(6) NULL</code></td>
      <td>The timestamp at which the connector processed the event. Corresponds to the current_ts field in the Ogg record.</td>
    </tr>
    <tr>
      <td><code>event-timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(6) NULL</code></td>
      <td>The timestamp at which the source system created the event. Corresponds to the op_ts field in the Ogg record.</td>
    </tr>
    </tbody>
</table>

The following example shows how to access Ogg metadata fields in Kafka:

```sql
CREATE TABLE KafkaTable (
  origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
  event_time TIMESTAMP(3) METADATA FROM 'value.event-timestamp' VIRTUAL,
  origin_table STRING METADATA FROM 'value.table' VIRTUAL,
  primary_keys ARRAY<STRING> METADATA FROM 'value.primary-keys' VIRTUAL,
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'ogg-json'
);
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
      <td>Specify what format to use, here should be <code>'ogg-json'</code>.</td>
    </tr>
    <tr>
      <td><h5>ogg-json.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>
    <tr>
      <td><h5>ogg-json.timestamp-format.standard</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">'SQL'</td>
      <td>String</td>
      <td>Specify the input and output timestamp format. Currently supported values are <code>'SQL'</code> and <code>'ISO-8601'</code>:
       <ul>
         <li>Option <code>'SQL'</code> will parse input timestamp in "yyyy-MM-dd HH:mm:ss.s{precision}" format, e.g '2020-12-30 12:13:14.123' and output timestamp in the same format.</li>
         <li>Option <code>'ISO-8601'</code>will parse input timestamp in "yyyy-MM-ddTHH:mm:ss.s{precision}" format, e.g '2020-12-30T12:13:14.123' and output timestamp in the same format.</li>
       </ul>
       </td>
    </tr>
    <tr>
      <td><h5>ogg-json.map-null-key.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>'FAIL'</code></td>
      <td>String</td>
      <td>Specify the handling mode when serializing null keys for map data. Currently supported values are <code>'FAIL'</code>, <code>'DROP'</code> and <code>'LITERAL'</code>:
      <ul>
        <li>Option <code>'FAIL'</code> will throw exception when encountering map with null key.</li>
        <li>Option <code>'DROP'</code> will drop null key entries for map data.</li>
        <li>Option <code>'LITERAL'</code> will replace null key with string literal. The string literal is defined by <code>ogg-json.map-null-key.literal</code> option.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>ogg-json.map-null-key.literal</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">'null'</td>
      <td>String</td>
      <td>Specify string literal to replace null key when <code>'ogg-json.map-null-key.mode'</code> is LITERAL.</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, the Ogg format uses JSON format for serialization and deserialization. Please refer
to [JSON Format documentation]({{< ref "docs/connectors/table/formats/json" >}}#data-type-mapping) for more details about the data type mapping.
