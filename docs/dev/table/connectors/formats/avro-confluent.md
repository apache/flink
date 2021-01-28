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

The Avro Schema Registry format can only be used in conjunction with the [Apache Kafka SQL connector]({% link dev/table/connectors/kafka.md %}) or the [Upsert Kafka SQL Connector]({% link dev/table/connectors/upsert-kafka.md %}).

Dependencies
------------

{% assign connector = site.data.sql-connectors['avro-confluent'] %} 
{% include sql-connector-download-table.html 
    connector=connector
%}

How to create tables with Avro-Confluent format
--------------

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">

Example of a table using raw UTF-8 string as Kafka key and Avro records registered in the Schema Registry as Kafka values:

{% highlight sql %}
CREATE TABLE user_created (

  -- one column mapped to the Kafka raw UTF-8 key
  the_kafka_key STRING,
  
  -- a few columns mapped to the Avro fields of the Kafka value
  id STRING,
  name STRING, 
  email STRING

) WITH (

  'connector' = 'kafka',
  'topic' = 'user_events_example1',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- UTF-8 string as Kafka keys, using the 'the_kafka_key' table column
  'key.format' = 'raw',
  'key.fields' = 'the_kafka_key',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY'
)
{% endhighlight %}

We can write data into the kafka table as follows:

{% highlight sql %}
INSERT INTO user_created
SELECT
  -- replicating the user id into a column mapped to the kafka key
  id as the_kafka_key,

  -- all values
  id, name, email
FROM some_table
{% endhighlight %}

---

Example of a table with both the Kafka key and value registered as Avro records in the Schema Registry:

{% highlight sql %}
CREATE TABLE user_created (
  
  -- one column mapped to the 'id' Avro field of the Kafka key
  kafka_key_id STRING,
  
  -- a few columns mapped to the Avro fields of the Kafka value
  id STRING,
  name STRING, 
  email STRING
  
) WITH (

  'connector' = 'kafka',
  'topic' = 'user_events_example2',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- Watch out: schema evolution in the context of a Kafka key is almost never backward nor
  -- forward compatible due to hash partitioning.
  'key.format' = 'avro-confluent',
  'key.avro-confluent.schema-registry.url' = 'http://localhost:8082',
  'key.fields' = 'kafka_key_id',

  -- In this example, we want the Avro types of both the Kafka key and value to contain the field 'id'
  -- => adding a prefix to the table column associated to the Kafka key field avoids clashes
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY',
   
  -- subjects have a default value since Flink 1.13, though can be overriden:
  'key.avro-confluent.schema-registry.subject' = 'user_events_example2-key2',
  'value.avro-confluent.schema-registry.subject' = 'user_events_example2-value2'
)
{% endhighlight %}

---
Example of a table using the upsert connector with the Kafka value registered as an Avro record in the Schema Registry:

{% highlight sql %}
CREATE TABLE user_created (
  
  -- one column mapped to the Kafka raw UTF-8 key
  kafka_key_id STRING,
  
  -- a few columns mapped to the Avro fields of the Kafka value
  id STRING, 
  name STRING, 
  email STRING, 
  
  -- upsert-kafka connector requires a primary key to define the upsert behavior
  PRIMARY KEY (kafka_key_id) NOT ENFORCED

) WITH (

  'connector' = 'upsert-kafka',
  'topic' = 'user_events_example3',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- UTF-8 string as Kafka keys
  -- We don't specify 'key.fields' in this case since it's dictated by the primary key of the table
  'key.format' = 'raw',
  
  -- In this example, we want the Avro types of both the Kafka key and value to contain the field 'id'
  -- => adding a prefix to the table column associated to the kafka key field to avoid clashes
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY'
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
      <td>The URL of the Confluent Schema Registry to fetch/register schemas.</td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.subject</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>The Confluent Schema Registry subject under which to register the schema used by this format during serialization. By default, kafka and upsert-kafka connectors use "&lt;topic_name&gt;-value" or "&lt;topic_name&gt;-key" as the default subject name if avro-confluent is used as the value or key format. But for other connectors (e.g. filesystem), the subject option is required when used as sink.</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, Apache Flink always uses the table schema to derive the Avro reader schema during deserialization and Avro writer schema during serialization. Explicitly defining an Avro schema is not supported yet.
See the [Apache Avro Format]({% link dev/table/connectors/formats/avro.md%}#data-type-mapping) for the mapping between Avro and Flink DataTypes. 

In addition to the types listed there, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/current/spec.html) for more information about Avro types.
