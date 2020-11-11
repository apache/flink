---
title: "Confluent Avro Format"
nav-title: Confluent Avro
nav-parent_id: sql-formats
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

The Avro Schema Registry (``avro-confluent``) format allows you to read records that were serialized by the ``io.confluent.kafka.serializers.KafkaAvroSerializer`` and to write records that can in turn be read by the ``io.confluent.kafka.serializers.KafkaAvroDeserializer``. 

When reading (deserializing) a record with this format the Avro writer schema is fetched from the configured Confluent Schema Registry based on the schema version id encoded in the record while the reader schema is inferred from table schema. 

When writing (serializing) a record with this format the Avro schema is inferred from the table schema and used to retrieve a schema id to be encoded with the data. The lookup is performed with in the configured Confluent Schema Registry under the [subject](https://docs.confluent.io/current/schema-registry/index.html#schemas-subjects-and-topics) given in `avro-confluent.schema-registry.subject`.

The Avro Schema Registry format can only be used in conjunction with [Apache Kafka SQL connector]({% link dev/table/connectors/kafka.md %}). 

Dependencies
------------

{% assign connector = site.data.sql-connectors['avro-confluent'] %} 
{% include sql-connector-download-table.html 
    connector=connector
%}

How to create a table with Avro-Confluent format
----------------

Here is an example to create a table using Kafka connector and Confluent Avro format.

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE user_behavior (
  user_id BIGINT,
  item_id BIGINT,
  category_id BIGINT,
  behavior STRING,
  ts TIMESTAMP(3)
) WITH (
  'connector' = 'kafka',
  'properties.bootstrap.servers' = 'localhost:9092',
  'topic' = 'user_behavior',
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://localhost:8081',
  'avro-confluent.schema-registry.subject' = 'user_behavior'
)
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
      <td>Specify what format to use, here should be <code>'avro-confluent'</code>.</td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.url</h5></td>
      <td>required</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The URL of the Confluent Schema Registry to fetch/register schemas</td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.subject</h5></td>
      <td>required by sink</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The Confluent Schema Registry subject under which to register the schema used by this format during serialization</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, Apache Flink always uses the table schema to derive the Avro reader schema during deserialization and Avro writer schema during serialization. Explicitly defining an Avro schema is not supported yet.
See the [Apache Avro Format]({% link dev/table/connectors/formats/avro.md%}#data-type-mapping) for the mapping between Avro and Flink DataTypes. 

In addition to the types listed there, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/current/spec.html) for more information about Avro types.
