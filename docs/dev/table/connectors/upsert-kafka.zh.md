---
title: "Upsert Kafka SQL Connector"
nav-title: Upsert Kafka
nav-parent_id: sql-connectors
nav-pos: 3
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

<span class="label label-primary">Scan Source: Unbounded</span>
<span class="label label-primary">Sink: Streaming Upsert Mode</span>

* This will be replaced by the TOC
{:toc}

The Upsert Kafka connector allows for reading data from and writing data into Kafka topics in the upsert fashion.

As a source, the upsert-kafka connector produces a changelog stream, where each data record represents
an update or delete event. More precisely, the value in a data record is interpreted as an UPDATE of
the last value for the same key, if any (if a corresponding key doesn’t exist yet, the update will
be considered an INSERT). Using the table analogy, a data record in a changelog stream is interpreted
as an UPSERT aka INSERT/UPDATE because any existing row with the same key is overwritten. Also, null
values are interpreted in a special way: a record with a null value represents a “DELETE”.

As a sink, the upsert-kafka connector can consume a changelog stream. It will write INSERT/UPDATE_AFTER
data as normal Kafka messages value, and write DELETE data as Kafka messages with null values
(indicate tombstone for the key). Flink will guarantee the message ordering on the primary key by
partition data on the values of the primary key columns, so the update/deletion messages on the same
key will fall into the same partition.

Dependencies
------------

In order to set up the upsert-kafka connector, the following table provide dependency information for
both projects using a build automation tool (such as Maven or SBT) and SQL Client with SQL JAR bundles.

{% assign connector = site.data.sql-connectors['upsert-kafka'] %}
{% include sql-connector-download-table.html
    connector=connector
%}

Full Example
----------------

The example below shows how to create and use an Upsert Kafka table:

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE pageviews_per_region (
  region STRING,
  pv BIGINT,
  uv BIGINT,
  PRIMARY KEY (region) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  'topic' = 'pageviews_per_region',
  'properties.bootstrap.servers' = '...',
  'key.format' = 'avro',
  'value.format' = 'avro'
);

CREATE TABLE pageviews (
  user_id BIGINT,
  page_id BIGINT,
  viewtime TIMESTAMP,
  user_region STRING,
  WATERMARK FOR viewtime AS viewtime - INTERVAL '2' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'pageviews',
  'properties.bootstrap.servers' = '...',
  'format' = 'json'
);

-- calculate the pv, uv and insert into the upsert-kafka sink
INSERT INTO pageviews_per_region
SELECT
  region,
  COUNT(*),
  COUNT(DISTINCT user_id)
FROM pageviews
GROUP BY region;

{% endhighlight %}
</div>
</div>
<span class="label label-danger">Attention</span> Make sure to define the primary key in the DDL.

Connector Options
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
      <td><h5>connector</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify which connector to use, for the Upsert Kafka use: <code>'upsert-kafka'</code>.</td>
    </tr>
    <tr>
      <td><h5>topic</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The Kafka topic name to read from and write to.</td>
    </tr>
    <tr>
      <td><h5>properties.bootstrap.servers</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Comma separated list of Kafka brokers.</td>
    </tr>
    <tr>
      <td><h5>key.format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the key part of the Kafka messages. The key part
      fields are specified by the PRIMARY KEY syntax. The supported formats include <code>'csv'</code>,
      <code>'json'</code>, <code>'avro'</code>. Please refer to <a href="{% link dev/table/connectors/formats/index.zh.md %}">Formats</a>
      page for more details and more format options.
      </td>
    </tr>
    <tr>
      <td><h5>value.format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the value part of the Kafka messages.
      The supported formats include <code>'csv'</code>, <code>'json'</code>, <code>'avro'</code>.
      Please refer to <a href="{% link dev/table/connectors/formats/index.zh.md %}">Formats</a> page for more details and more format options.
      </td>
    </tr>
    <tr>
       <td><h5>value.fields-include</h5></td>
       <td>required</td>
       <td style="word-wrap: break-word;"><code>'ALL'</code></td>
       <td>String</td>
       <td>Controls which fields should end up in the value as well. Available values:
       <ul>
         <li><code>ALL</code>: the value part of the record contains all fields of the schema, even if they are part of the key.</li>
         <li><code>EXCEPT_KEY</code>: the value part of the record contains all fields of the schema except the key fields.</li>
       </ul>
       </td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the upsert-kafka sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    </tbody>
</table>

Features
----------------

### Primary Key Constraints

The Upsert Kafka always works in the upsert fashion and requires to define the primary key in the DDL.
With the assumption that records with the same key should be ordered in the same partition, the
primary key semantic on the changelog source means the materialized changelog is unique on the primary
keys. The primary key definition will also control which fields should end up in Kafka’s key.

### Consistency Guarantees

By default, an Upsert Kafka sink ingests data with at-least-once guarantees into a Kafka topic if
the query is executed with [checkpointing enabled]({% link dev/stream/state/checkpointing.zh.md %}#enabling-and-configuring-checkpointing).

This means, Flink may write duplicate records with the same key into the Kafka topic. But as the
connector is working in the upsert mode, the last record on the same key will take effect when
reading back as a source. Therefore, the upsert-kafka connector achieves idempotent writes just like
the [HBase sink]({{ site.baseurl }}/dev/table/connectors/hbase.html).

Data Type Mapping
----------------

Upsert Kafka stores message keys and values as bytes, so Upsert Kafka doesn't have schema or data types.
The messages are deserialized and serialized by formats, e.g. csv, json, avro. Thus, the data type mapping
is determined by specific formats. Please refer to [Formats]({% link dev/table/connectors/formats/index.zh.md %})
pages for more details.

{% top %}
