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
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

[Debezium](https://debezium.io/) is a CDC (Changelog Data Capture) tool that can stream changes in real-time from MySQL, PostgreSQL, Oracle, Microsoft SQL Server and many other databases into Kafka. Debezium provides a unified format schema for changelog and supports to serialize messages using JSON and [Apache Avro](https://avro.apache.org/).

Flink supports to interpret Debezium JSON messages as INSERT/UPDATE/DELETE messages into Flink SQL system. This is useful in many cases to leverage this feature, such as
 - synchronizing incremental data from databases to other systems
 - auditing logs
 - real-time materialized views on databases
 - temporal join changing history of a database table and so on.

*Note: Support for interpreting Debezium Avro messages and emitting Debezium messages is on the roadmap.*

Dependencies
------------

In order to setup the Debezium format, the following table provides dependency information for both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

| Maven dependency   | SQL Client JAR         |
| :----------------- | :----------------------|
| `flink-json`       | Built-in               |

*Note: please refer to [Debezium documentation](https://debezium.io/documentation/reference/1.1/index.html) about how to setup a Debezium Kafka Connect to synchronize changelog to Kafka topics.*


How to use Debezium format
----------------

Debezium provides a unified format for changelog, here is a simple example for an update operation captured from a MySQL `products` table:

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

*Note: please refer to [Debezium documentation](https://debezium.io/documentation/reference/1.1/connectors/mysql.html#mysql-connector-events_debezium) about the meaning of each fields.*

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
 'format' = 'debezium-json'  -- using debezium-json as the format
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
    </tbody>
</table>

Caveats
----------------

### Consuming data produced by Debezium Postgres Connector

If you are using [Debezium Connector for PostgreSQL](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html) to capture the changes to Kafka, please make sure the [REPLICA IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) configuration of the monitored PostgreSQL table has been set to `FULL` which is by default `DEFAULT`.
Otherwise, Flink SQL currently will fail to interpret the Debezium data.

In `FULL` strategy, the UPDATE and DELETE events will contain the previous values of all the table’s columns. In other strategies, the "before" field of UPDATE and DELETE events will only contain primary key columns or null if no primary key.
You can change the `REPLICA IDENTITY` by running `ALTER TABLE <your-table-name> REPLICA IDENTITY FULL`.
See more details in [Debezium Documentation for PostgreSQL REPLICA IDENTITY](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity).

Data Type Mapping
----------------

Currently, the Debezium format uses JSON format for deserialization. Please refer to [JSON format documentation]({% link dev/table/connectors/formats/json.md %}#data-type-mapping) for more details about the data type mapping.

