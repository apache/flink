---
title: Confluent Avro Variant
weight: 5
type: docs
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

# Confluent Avro Variant Format

{{< label "Format: Deserialization Schema" >}}

The Avro Variant Schema Registry (``avro-variant-confluent``) format allows you to read records that were serialized by the ``io.confluent.kafka.serializers.KafkaAvroSerializer`` and deserialize them into Flink's ``VARIANT`` type.

Unlike the [Confluent Avro]({{< ref "docs/connectors/table/formats/avro-confluent" >}}) format which requires the table schema to match the Avro schema, this format reads the entire Avro record into a single ``VARIANT`` column. The writer schema is resolved dynamically per record from the configured Confluent Schema Registry based on the schema version id encoded in the record. The Avro schema does not need to be known at table creation time and can evolve across records.

This format is deserialization-only and does not support writing.

Dependencies
------------

{{< sql_download_table "avro-confluent" >}}

For Maven, SBT, Gradle, or other build automation tools, please also ensure that Confluent's maven repository at `https://packages.confluent.io/maven/` is configured in your project's build files.

How to read with Avro-Variant-Confluent format
--------------

This format is particularly useful when a single Kafka topic carries multiple event types with different Avro schemas. With the standard ``avro-confluent`` format, you would need a separate table for each event type, each with a fixed schema. With ``avro-variant-confluent``, a single table can ingest all event types and you can route or filter them at query time. Also, can be useful when reading from multiple Kafka topics with topic pattern.

Example of a table that reads Avro records from Kafka into a VARIANT column:

```sql
CREATE TABLE kafka_source (
    data VARIANT
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'my-group',
    'format' = 'avro-variant-confluent',
    'avro-variant-confluent.url' = 'http://localhost:8081'
);

-- Access variant fields using bracket notation
SELECT
    data['id'] AS id,
    data['name'] AS name,
    data['email'] AS email
FROM kafka_source;
```

---

Example of reading from multiple topics with different Avro schemas using a topic pattern:

```sql
-- Read from multiple topics with different Avro schemas using a single table
CREATE TABLE all_orders (
    data VARIANT
) WITH (
    'connector' = 'kafka',
    'topic-pattern' = 'orders-*',
     ..
    'format' = 'avro-variant-confluent',
    'avro-variant-confluent.url' = 'http://localhost:8081'
);

-- Query across all order topics regardless of schema differences
SELECT
    data['order_id'] AS order_id,
    data['customer_id'] AS customer_id,
    data['total'] AS total,
    data['region'] AS region
FROM all_orders;
```

---

The format also supports an optional ``schema`` metadata column that exposes the Avro writer schema string for each record:

```sql
CREATE TABLE kafka_source (
    data VARIANT,
    avro_schema STRING METADATA FROM 'schema'
) WITH (
    'connector' = 'kafka',
    'topic' = 'user_events',
    'properties.bootstrap.servers' = 'localhost:9092',
    'properties.group.id' = 'my-group',
    'format' = 'avro-variant-confluent',
    'avro-variant-confluent.url' = 'http://localhost:8081'
);

-- Query fields and inspect the writer schema
SELECT
    data['id'] AS id,
    data['name'] AS name,
    avro_schema
FROM kafka_source;
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
            <td>Specify what format to use, here should be <code>'avro-variant-confluent'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.url</h5></td>
            <td>required</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The URL of the Confluent Schema Registry to fetch schemas.</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.properties</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Map</td>
            <td>Properties map that is forwarded to the underlying Schema Registry. This is useful for options that are not officially exposed via Flink config options. However, note that Flink options have higher precedence.</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.ssl.keystore.location</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Location / File of SSL keystore</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.ssl.keystore.password</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Password for SSL keystore</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.ssl.truststore.location</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Location / File of SSL truststore</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.ssl.truststore.password</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Password for SSL truststore</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.basic-auth.credentials-source</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Basic auth credentials source for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.basic-auth.user-info</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Basic auth user info for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.bearer-auth.credentials-source</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Bearer auth credentials source for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-variant-confluent.bearer-auth.token</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Bearer auth token for Schema Registry</td>
        </tr>
    </tbody>
</table>

Metadata
--------

The format supports the following metadata column:

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">Key</th>
        <th class="text-center" style="width: 20%">Data Type</th>
        <th class="text-center" style="width: 55%">Description</th>
      </tr>
    </thead>
    <tbody>
        <tr>
            <td><code>schema</code></td>
            <td><code>STRING NOT NULL</code></td>
            <td>The Avro writer schema string (JSON format) used to serialize the record.</td>
        </tr>
    </tbody>
</table>

Data Type Mapping
----------------

All Avro types are converted to Flink's VARIANT type. The following Avro logical types receive special handling:

| Avro Type | Avro Logical Type | Variant Representation |
|-----------|-------------------|----------------------|
| INT | date | DATE |
| INT | time-millis | LONG (microseconds) |
| LONG | timestamp-millis | TIMESTAMP |
| LONG | timestamp-micros | TIMESTAMP |
| BYTES/FIXED | decimal | DECIMAL |
| RECORD | - | OBJECT |
| ARRAY | - | ARRAY |
| MAP | - | OBJECT (string keys) |
| UNION (null + type) | - | nullable inner type |
| STRING, ENUM | - | STRING |
| BOOLEAN | - | BOOLEAN |
| INT | - | INT |
| LONG | - | LONG |
| FLOAT | - | FLOAT |
| DOUBLE | - | DOUBLE |
| BYTES, FIXED | - | BYTES |
| NULL | - | NULL |

