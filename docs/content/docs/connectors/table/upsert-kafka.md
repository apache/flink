---
title: Upsert Kafka
weight: 4
type: docs
aliases:
  - /dev/table/connectors/upsert-kafka.html
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

# Upsert Kafka SQL Connector

{{< label "Scan Source: Unbounded" >}}
{{< label "Sink: Streaming Upsert Mode" >}}

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

{{< sql_download_table "upsert-kafka" >}}

Full Example
----------------

The example below shows how to create and use an Upsert Kafka table:

```sql
CREATE TABLE pageviews_per_region (
  user_region STRING,
  pv BIGINT,
  uv BIGINT,
  PRIMARY KEY (user_region) NOT ENFORCED
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
  user_region,
  COUNT(*),
  COUNT(DISTINCT user_id)
FROM pageviews
GROUP BY user_region;

```

<span class="label label-danger">Attention</span> Make sure to define the primary key in the DDL.

Available Metadata
------------------

See the [regular Kafka connector]({{< ref "docs/connectors/table/kafka" >}}#available-metadata) for a list
of all available metadata fields.

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
      <td><h5>properties.*</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>
         This can set and pass arbitrary Kafka configurations. Suffix names must match the configuration key defined in <a href="https://kafka.apache.org/documentation/#configuration">Kafka Configuration documentation</a>. Flink will remove the "properties." key prefix and pass the transformed key and values to the underlying KafkaClient. For example, you can disable automatic topic creation via <code>'properties.allow.auto.create.topics' = 'false'</code>. But there are some configurations that do not support to set, because Flink will override them, e.g. <code>'key.deserializer'</code> and <code>'value.deserializer'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>key.format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td><p>The format used to deserialize and serialize the key part of Kafka messages.
      Please refer to the <a href="{{< ref "docs/connectors/table/formats/overview" >}}">formats</a> page
      for more details and more format options.</p>
      <span class="label label-danger">Attention</span> Compared to the regular Kafka connector, the
      key fields are specified by the <code>PRIMARY KEY</code> syntax.
      </td>
    </tr>
    <tr>
      <td><h5>key.fields-prefix</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Defines a custom prefix for all fields of the key format to avoid name clashes with fields
        of the value format. By default, the prefix is empty. If a custom prefix is defined, both the
        table schema and <code>'key.fields'</code> will work with prefixed names. When constructing the
        data type of the key format, the prefix will be removed and the non-prefixed names will be used
        within the key format. Please note that this option requires that <code>'value.fields-include'</code>
        must be set to <code>'EXCEPT_KEY'</code>.
      </td>
    </tr>
    <tr>
      <td><h5>value.format</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The format used to deserialize and serialize the value part of Kafka messages.
      Please refer to the <a href="{{< ref "docs/connectors/table/formats/overview" >}}">formats</a> page
      for more details and more format options.
      </td>
    </tr>
    <tr>
      <td><h5>value.fields-include</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">ALL</td>
      <td><p>Enum</p>Possible values: [ALL, EXCEPT_KEY]</td>
      <td>Defines a strategy how to deal with key columns in the data type of the value format. By
        default, <code>'ALL'</code> physical columns of the table schema will be included in the value
        format which means that key columns appear in the data type for both the key and value format.
      </td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Integer</td>
      <td>Defines the parallelism of the upsert-kafka sink operator. By default, the parallelism is determined by the framework using the same parallelism of the upstream chained operator.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.max-rows</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Integer</td>
      <td>The max size of buffered records before flush.
       When the sink receives many updates on the same key, the buffer will retain the last record of the same key.
       This can help to reduce data shuffling and avoid possible tombstone messages to Kafka topic. Can be set to '0' to disable it.
       By default, this is disabled. Note both <code>'sink.buffer-flush.max-rows'</code> and
       <code>'sink.buffer-flush.interval'</code> must be set to be greater than zero to enable sink buffer flushing.</td>
    </tr>
    <tr>
      <td><h5>sink.buffer-flush.interval</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">0</td>
      <td>Duration</td>
      <td>The flush interval mills, over this time, asynchronous threads will flush data.
       When the sink receives many updates on the same key, the buffer will retain the last record of the same key.
       This can help to reduce data shuffling and avoid possible tombstone messages to Kafka topic. Can be set to '0' to disable it.
       By default, this is disabled. Note both <code>'sink.buffer-flush.max-rows'</code> and
       <code>'sink.buffer-flush.interval'</code> must be set to be greater than zero to enable sink buffer flushing.</td>
    </tr>
    </tbody>
</table>

Features
----------------

### Key and Value Formats

See the [regular Kafka connector]({{< ref "docs/connectors/datastream/kafka" >}}#key-and-value-formats) for more
explanation around key and value formats. However, note that this connector requires both a key and
value format where the key fields are derived from the `PRIMARY KEY` constraint.

The following example shows how to specify and configure key and value formats. The format options are
prefixed with either the `'key'` or `'value'` plus format identifier.

{{< tabs "4868be95-f716-4828-a2ac-7899bf1df1ba" >}}
{{< tab "SQL" >}}
```sql
CREATE TABLE KafkaTable (
  `ts` TIMESTAMP(3) METADATA FROM 'timestamp',
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  PRIMARY KEY (`user_id`) NOT ENFORCED
) WITH (
  'connector' = 'upsert-kafka',
  ...

  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',

  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'EXCEPT_KEY'
)
```
{{< /tab >}}
{{< /tabs >}}

### Primary Key Constraints

The Upsert Kafka always works in the upsert fashion and requires to define the primary key in the DDL.
With the assumption that records with the same key should be ordered in the same partition, the
primary key semantic on the changelog source means the materialized changelog is unique on the primary
keys. The primary key definition will also control which fields should end up in Kafka’s key.

### Consistency Guarantees

By default, an Upsert Kafka sink ingests data with at-least-once guarantees into a Kafka topic if
the query is executed with [checkpointing enabled]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}#enabling-and-configuring-checkpointing).

This means, Flink may write duplicate records with the same key into the Kafka topic. But as the
connector is working in the upsert mode, the last record on the same key will take effect when
reading back as a source. Therefore, the upsert-kafka connector achieves idempotent writes just like
the [HBase sink]({{< ref "docs/connectors/table/hbase" >}}).

### Source Per-Partition Watermarks

Flink supports to emit per-partition watermarks for Upsert Kafka. Watermarks are generated inside the Kafka
consumer. The per-partition watermarks are merged in the same way as watermarks are merged during streaming
shuffles. The output watermark of the source is determined by the minimum watermark among the partitions
it reads. If some partitions in the topics are idle, the watermark generator will not advance. You can
alleviate this problem by setting the [`'table.exec.source.idle-timeout'`]({{< ref "docs/dev/table/config" >}}#table-exec-source-idle-timeout)
option in the table configuration.

Please refer to [Kafka watermark strategies]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#watermark-strategies-and-the-kafka-connector)
for more details.

Data Type Mapping
----------------

Upsert Kafka stores message keys and values as bytes, so Upsert Kafka doesn't have schema or data types.
The messages are serialized and deserialized by formats, e.g. csv, json, avro. Thus, the data type mapping
is determined by specific formats. Please refer to [Formats]({{< ref "docs/connectors/table/formats/overview" >}})
pages for more details.

{{< top >}}
