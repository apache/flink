---
title: "Debezium Format"
nav-title: Debezium
nav-parent_id: sql-formats
nav-pos: 4
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

<span class="label label-info">Changelog-Data-Capture Format</span>
<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

[Debezium](https://debezium.io/) is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL, PostgreSQL, Oracle, Microsoft SQL Server and many other databases into Kafka. Debezium provides a unified format schema for changelog and supports to serialize messages using JSON and [Apache Avro](https://avro.apache.org/).

Flink supports to interpret Debezium JSON and Avro messages as INSERT/UPDATE/DELETE messages into Flink SQL system. This is useful in many cases to leverage this feature, such as
 - synchronizing incremental data from databases to other systems
 - auditing logs
 - real-time materialized views on databases
 - temporal join changing history of a database table and so on.

Flink also supports to encode the INSERT/UPDATE/DELETE messages in Flink SQL as Debezium JSON or Avro messages, and emit to external systems like Kafka.
However, currently Flink can't combine UPDATE_BEFORE and UPDATE_AFTER into a single UPDATE message. Therefore, Flink encodes UPDATE_BEFORE and UDPATE_AFTER as DELETE and INSERT Debezium messages.

Dependencies
------------

<div class="codetabs" markdown="1">
<div data-lang="Debezium Avro" markdown="1">
{% assign connector = site.data.sql-connectors['debezium-avro-confluent'] %}
{% include sql-connector-download-table.html 
    connector=connector
%}
</div>
<div data-lang="Debezium Json" markdown="1">
{% assign connector = site.data.sql-connectors['debezium-json'] %}
{% include sql-connector-download-table.html
    connector=connector
%}
</div>
</div>

*Note: please refer to [Debezium documentation](https://debezium.io/documentation/reference/1.3/index.html) about how to setup a Debezium Kafka Connect to synchronize changelog to Kafka topics.*


How to use Debezium format
----------------

Debezium provides a unified format for changelog, here is a simple example for an update operation captured from a MySQL `products` table in JSON format:

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
  "source": {...},
  "op": "u",
  "ts_ms": 1589362330904,
  "transaction": null
}
```

*Note: please refer to [Debezium documentation](https://debezium.io/documentation/reference/1.3/connectors/mysql.html#mysql-connector-events_debezium) about the meaning of each fields.*

The MySQL `products` table has 4 columns (`id`, `name`, `description` and `weight`). The above JSON message is an update change event on the `products` table where the `weight` value of the row with `id = 111` is changed from `5.18` to `5.15`.
Assuming this messages is synchronized to Kafka topic `products_binlog`, then we can use the following DDL to consume this topic and interpret the change events.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
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
 -- using 'debezium-json' as the format to interpret Debezium JSON messages
 -- please use 'debezium-avro-confluent' if Debezium encodes messages in Avro format
 'format' = 'debezium-json'
)
{% endhighlight %}
</div>
</div>

In some cases, users may setup the Debezium Kafka Connect with the Kafka configuration `'value.converter.schemas.enable'` enabled to include schema in the message. Then the Debezium JSON message may look like this:

```json
{
  "schema": {...},
  "payload": {
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
    "source": {...},
    "op": "u",
    "ts_ms": 1589362330904,
    "transaction": null
  }
}
```

In order to interpret such messages, you need to add the option `'debezium-json.schema-include' = 'true'` into above DDL WITH clause (`false` by default). Usually, this is not recommended to include schema because this makes the messages very verbose and reduces parsing performance.

After registering the topic as a Flink table, then you can consume the Debezium messages as a changelog source.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
-- a real-time materialized view on the MySQL "products"
-- which calculate the latest average of weight for the same products
SELECT name, AVG(weight) FROM topic_products GROUP BY name;

-- synchronize all the data and incremental changes of MySQL "products" table to
-- Elasticsearch "products" index for future searching
INSERT INTO elasticsearch_products
SELECT * FROM topic_products;
{% endhighlight %}
</div>
</div>

Available Metadata
------------------

The following format metadata can be exposed as read-only (`VIRTUAL`) columns in a table definition.

<span class="label label-danger">Attention</span> Format metadata fields are only available if the
corresponding connector forwards format metadata. Currently, only the Kafka connector is able to expose
metadata fields for its value format.

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
      <td><code>schema</code></td>
      <td><code>STRING NULL</code></td>
      <td>JSON string describing the schema of the payload. Null if the schema is not included in
      the Debezium record.</td>
    </tr>
    <tr>
      <td><code>ingestion-timestamp</code></td>
      <td><code>TIMESTAMP(3) WITH LOCAL TIME ZONE NULL</code></td>
      <td>The timestamp at which the connector processed the event. Corresponds to the <code>ts_ms</code>
      field in the Debezium record.</td>
    </tr>
    <tr>
      <td><code>source.timestamp</code></td>
      <td><code>TIMESTAMP(3) WITH LOCAL TIME ZONE NULL</code></td>
      <td>The timestamp at which the source system created the event. Corresponds to the <code>source.ts_ms</code>
      field in the Debezium record.</td>
    </tr>
    <tr>
      <td><code>source.database</code></td>
      <td><code>STRING NULL</code></td>
      <td>The originating database. Corresponds to the <code>source.db</code> field in the
      Debezium record if available.</td>
    </tr>
    <tr>
      <td><code>source.schema</code></td>
      <td><code>STRING NULL</code></td>
      <td>The originating database schema. Corresponds to the <code>source.schema</code> field in the
      Debezium record if available.</td>
    </tr>
    <tr>
      <td><code>source.table</code></td>
      <td><code>STRING NULL</code></td>
      <td>The originating database table. Corresponds to the <code>source.table</code> or <code>source.collection</code>
      field in the Debezium record if available.</td>
    </tr>
    <tr>
      <td><code>source.properties</code></td>
      <td><code>MAP&lt;STRING, STRING&gt; NULL</code></td>
      <td>Map of various source properties. Corresponds to the <code>source</code> field in the Debezium record.</td>
    </tr>
    </tbody>
</table>

The following example shows how to access Debezium metadata fields in Kafka:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE KafkaTable (
  origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
  event_time TIMESTAMP(3) METADATA FROM 'value.source.timestamp' VIRTUAL,
  origin_database STRING METADATA FROM 'value.source.database' VIRTUAL,
  origin_schema STRING METADATA FROM 'value.source.schema' VIRTUAL,
  origin_table STRING METADATA FROM 'value.source.table' VIRTUAL,
  origin_properties MAP<STRING, STRING> METADATA FROM 'value.source.properties' VIRTUAL,
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);
{% endhighlight %}
</div>
</div>

Format Options
----------------

Flink provides `debezium-avro-confluent` and `debezium-json` formats to interpret Avro or Json messages produced by Debezium.
Use format `debezium-avro-confluent` to interpret Debezium Avro messages and format `debezium-json` to interpret Debezium Json messages.

<div class="codetabs" markdown="1">
<div data-lang="Debezium Avro" markdown="1">

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
      <td>Specify what format to use, here should be <code>'debezium-avro-confluent'</code>.</td>
    </tr>
    <tr>
      <td><h5>debezium-avro-confluent.schema-registry.url</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The URL of the Confluent Schema Registry to fetch/register schemas.</td>
    </tr>
    <tr>
      <td><h5>debezium-avro-confluent.schema-registry.subject</h5></td>
      <td>required by sink</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The Confluent Schema Registry subject under which to register the schema used by this format during serialization.</td>
    </tr>
    </tbody>
</table>

</div>
<div data-lang="Debezium Json" markdown="1">

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
      <td>Specify what format to use, here should be <code>'debezium-json'</code>.</td>
    </tr>
    <tr>
      <td><h5>debezium-json.schema-include</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>When setting up a Debezium Kafka Connect, users may enable a Kafka configuration <code>'value.converter.schemas.enable'</code> to include schema in the message.
          This option indicates whether the Debezium JSON message includes the schema or not. </td>
    </tr>
    <tr>
      <td><h5>debezium-json.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>
    <tr>
       <td><h5>debezium-json.timestamp-format.standard</h5></td>
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
      <td><h5>debezium-json.map-null-key.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>'FAIL'</code></td>
      <td>String</td>
      <td>Specify the handling mode when serializing null keys for map data. Currently supported values are <code>'FAIL'</code>, <code>'DROP'</code> and <code>'LITERAL'</code>:
      <ul>
        <li>Option <code>'FAIL'</code> will throw exception when encountering map with null key.</li>
        <li>Option <code>'DROP'</code> will drop null key entries for map data.</li>
        <li>Option <code>'LITERAL'</code> will replace null key with string literal. The string literal is defined by <code>debezium-json.map-null-key.literal</code> option.</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>debezium-json.map-null-key.literal</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">'null'</td>
      <td>String</td>
      <td>Specify string literal to replace null key when <code>'debezium-json.map-null-key.mode'</code> is LITERAL.</td>
    </tr>        
    </tbody>
</table>

</div>

Caveats
----------------

### Duplicate change events

Under normal operating scenarios, the Debezium application delivers every change event **exactly-once**. Flink works pretty well when consuming Debezium produced events in this situation.
However, Debezium application works in **at-least-once** delivery if any failover happens. See more details about delivery guarantee from [Debezium documentation](https://debezium.io/documentation/faq/#what_happens_when_an_application_stops_or_crashes).
That means, in the abnormal situations, Debezium may deliver duplicate change events to Kafka and Flink will get the duplicate events.
This may cause Flink query to get wrong results or unexpected exceptions. Thus, it is recommended to set job configuration [`table.exec.source.cdc-events-duplicate`]({% link dev/table/config.md %}#table-exec-source-cdc-events-duplicate) to `true` and define PRIMARY KEY on the source in this situation.
Framework will generate an additional stateful operator, and use the primary key to deduplicate the change events and produce a normalized changelog stream.

### Consuming data produced by Debezium Postgres Connector

If you are using [Debezium Connector for PostgreSQL](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html) to capture the changes to Kafka, please make sure the [REPLICA IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) configuration of the monitored PostgreSQL table has been set to `FULL` which is by default `DEFAULT`.
Otherwise, Flink SQL currently will fail to interpret the Debezium data.

In `FULL` strategy, the UPDATE and DELETE events will contain the previous values of all the table’s columns. In other strategies, the "before" field of UPDATE and DELETE events will only contain primary key columns or null if no primary key.
You can change the `REPLICA IDENTITY` by running `ALTER TABLE <your-table-name> REPLICA IDENTITY FULL`.
See more details in [Debezium Documentation for PostgreSQL REPLICA IDENTITY](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity).

Data Type Mapping
----------------

Currently, the Debezium format uses JSON and Avro format for serialization and deserialization. Please refer to [JSON Format documentation]({% link dev/table/connectors/formats/json.md %}#data-type-mapping) and [Confluent Avro Format documentation]({% link dev/table/connectors/formats/avro-confluent.md %}#data-type-mapping) for more details about the data type mapping.

