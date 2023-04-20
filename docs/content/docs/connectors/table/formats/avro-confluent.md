---
title: Confluent Avro
weight: 4
type: docs
aliases:
  - /dev/table/connectors/formats/avro-confluent.html
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

# Confluent Avro Format

{{< label "Format: Serialization Schema" >}}
{{< label "Format: Deserialization Schema" >}}

The Avro Schema Registry (``avro-confluent``) format allows you to read records that were serialized by the ``io.confluent.kafka.serializers.KafkaAvroSerializer`` and to write records that can in turn be read by the ``io.confluent.kafka.serializers.KafkaAvroDeserializer``. 

When reading (deserializing) a record with this format the Avro writer schema is fetched from the configured Confluent Schema Registry based on the schema version id encoded in the record while the reader schema is inferred from table schema. 

When writing (serializing) a record with this format the Avro schema is inferred from the table schema and used to retrieve a schema id to be encoded with the data. The lookup is performed with in the configured Confluent Schema Registry under the [subject](https://docs.confluent.io/current/schema-registry/index.html#schemas-subjects-and-topics) given in `avro-confluent.subject`.

The Avro Schema Registry format can only be used in conjunction with the [Apache Kafka SQL connector]({{< ref "docs/connectors/table/kafka" >}}) or the [Upsert Kafka SQL Connector]({{< ref "docs/connectors/table/upsert-kafka" >}}).

Dependencies
------------

{{< sql_download_table "avro-confluent" >}}

For Maven, SBT, Gradle, or other build automation tools, please also ensure that Confluent's maven repository at `https://packages.confluent.io/maven/` is configured in your project's build files.

How to create tables with Avro-Confluent format
--------------

Example of a table using raw UTF-8 string as Kafka key and Avro records registered in the Schema Registry as Kafka values:

```sql
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
  'value.avro-confluent.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY'
)
```

We can write data into the kafka table as follows:

```sql
INSERT INTO user_created
SELECT
  -- replicating the user id into a column mapped to the kafka key
  id as the_kafka_key,

  -- all values
  id, name, email
FROM some_table
```

---

Example of a table with both the Kafka key and value registered as Avro records in the Schema Registry:

```sql
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
  'key.avro-confluent.url' = 'http://localhost:8082',
  'key.fields' = 'kafka_key_id',

  -- In this example, we want the Avro types of both the Kafka key and value to contain the field 'id'
  -- => adding a prefix to the table column associated to the Kafka key field avoids clashes
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY',
   
  -- subjects have a default value since Flink 1.13, though can be overridden:
  'key.avro-confluent.subject' = 'user_events_example2-key2',
  'value.avro-confluent.subject' = 'user_events_example2-value2'
)
```

---
Example of a table using the upsert-kafka connector with the Kafka value registered as an Avro record in the Schema Registry:

```sql
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
  -- => adding a prefix to the table column associated to the kafka key field avoids clashes
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY'
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
            <td>Specify what format to use, here should be <code>'avro-confluent'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.basic-auth.credentials-source</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Basic auth credentials source for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.basic-auth.user-info</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Basic auth user info for schema registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.bearer-auth.credentials-source</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Bearer auth credentials source for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.bearer-auth.token</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Bearer auth token for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.properties</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Map</td>
            <td>Properties map that is forwarded to the underlying Schema Registry. This is useful for options that are not officially exposed via Flink config options. However, note that Flink options have higher precedence.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.keystore.location</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Location / File of SSL keystore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.keystore.password</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Password for SSL keystore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.truststore.location</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Location / File of SSL truststore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.truststore.password</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Password for SSL truststore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.schema</h5></td>
            <td>optional</td>
            <td>no</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The schema registered or to be registered in the Confluent Schema Registry. If no schema is provided Flink converts the table schema to avro schema. The schema provided must match the table schema.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.subject</h5></td>
            <td>optional</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The Confluent Schema Registry subject under which to register the schema used by this format during serialization. By default, 'kafka' and 'upsert-kafka' connectors use '&lt;topic_name&gt;-value' or '&lt;topic_name&gt;-key' as the default subject name if this format is used as the value or key format. But for other connectors (e.g. 'filesystem'), the subject option is required when used as sink.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.url</h5></td>
            <td>required</td>
            <td>yes</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The URL of the Confluent Schema Registry to fetch/register schemas.</td>
        </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, Apache Flink always uses the table schema to derive the Avro reader schema during deserialization and Avro writer schema during serialization. Explicitly defining an Avro schema is not supported yet.
See the [Apache Avro Format]({{< ref "docs/connectors/table/formats/avro" >}}#data-type-mapping) for the mapping between Avro and Flink DataTypes. 

In addition to the types listed there, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/current/spec.html) for more information about Avro types.
