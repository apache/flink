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

The Glue Schema Registry (``avro-glue``) format allows you to read records that were serialized by the ``com.amazonaws.services.schemaregistry.serializers.avro.AWSKafkaAvroSerializer`` and to write records that can in turn be read by the ``com.amazonaws.services.schemaregistry.deserializers.avro.AWSKafkaAvroDeserializer``.  These records have their schemas stored out-of-band in a configured registry provided by the AWS Glue Schema Registry [service](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-schemas).

When reading (deserializing) a record with this format the Avro writer schema is fetched from the configured AWS Glue Schema Registry based on the schema version id encoded in the record while the reader schema is inferred from table schema. 

When writing (serializing) a record with this format the Avro schema is inferred from the table schema and used to retrieve a schema id to be encoded with the data. The lookup is performed against the configured AWS Glue Schema Registry under the [subject](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html#schema-registry-schemas) given in `avro-glue.subject`.

The Avro Glue Schema Registry format can only be used in conjunction with the [Apache Kafka SQL connector]({{< ref "docs/connectors/table/kafka" >}}) or the [Upsert Kafka SQL Connector]({{< ref "docs/connectors/table/upsert-kafka" >}}).

Dependencies
------------

{{< sql_download_table "avro-glue" >}}

How to create tables with Avro-Glue format
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

  'value.format' = 'avro-glue',
  'value.avro-glue.region' = 'us-east-1',
  'value.avro-glue.registry.name' = 'my-schema-registry',
  'value.avro-glue.subject' = 'my-schema-name',
  'value.fields-include' = 'EXCEPT_KEY'
)
```

Format Options
----------------

Yes, these options have inconsistent naming convnetions.  No, I can't fix it.  This is for consistentcy with the existing [AWS Glue client code](https://github.com/awslabs/aws-glue-schema-registry/blob/master/common/src/main/java/com/amazonaws/services/schemaregistry/utils/AWSSchemaRegistryConstants.java#L20).

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
            <td>Specify what format to use, here should be <code>'avro-glue'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.region</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Specify what AWS region to use, such as <code>'us-east-1'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.registry.name</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The name (not the ARN) of the Glue schema registry in which to store the schemas.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.subject</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The subject name under which to store the schema in the registry.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.compression</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">NONE</td>
            <td>String</td>
            <td>What kind of compression to use.  Valid values are <code>'NONE'</code> and <code>'ZLIB'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.endpoint</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The HTTP endpoint to use for AWS calls.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.avroRecordType</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">GENERIC_RECORD</td>
            <td>String</td>
            <td>Valid values are <code>'GENERIC_RECORD'</code> and <code>'SPECIFIC_RECORD'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.compatibility</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">BACKWARD</td>
            <td>String</td>
            <td>The compatbility mode under which to store the schema.  Valid values are 
              <code>'NONE'</code>,
              <code>'BACKWARD'</code>,
              <code>'BACKWARD_TRANSITIVE'</code>,
              <code>'FORWARD'</code>,
              <code>'FORWARD_TRANSITIVE'</code>,
              <code>'FULL'</code>, and 
              <code>'FULL_TRANSITIVE'</code>
            </td>.
        </tr>
        <tr>
            <td><h5>avro-glue.schemaAutoRegistrationEnabled</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">true</td>
            <td>Boolean</td>
            <td>Whether new schemas should be automatically registered rather than treated as errors.</td>
        </tr>
        <tr>
            <td><h5>avro-glue.cacheSize</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">200</td>
            <td>Integer</td>
            <td>The size (in number of items, not bytes) of the cache the Glue client code should manage</td>
        </tr>
        <tr>
            <td><h5>avro-glue.timeToLiveMillis</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">1 day (24 * 60 * 60 * 1000)</td>
            <td>Integer</td>
            <td>The TTL for cache entries.</td>
        </tr>
    </tbody>
</table>

Data Type Mapping
----------------

Currently, Apache Flink always uses the table schema to derive the Avro reader schema during deserialization and Avro writer schema during serialization. Explicitly defining an Avro schema is not supported yet.
See the [Apache Avro Format]({{< ref "docs/connectors/table/formats/avro" >}}#data-type-mapping) for the mapping between Avro and Flink DataTypes. 

In addition to the types listed there, Flink supports reading/writing nullable types. Flink maps nullable types to Avro `union(something, null)`, where `something` is the Avro type converted from Flink type.

You can refer to [Avro Specification](https://avro.apache.org/docs/current/spec.html) for more information about Avro types.
