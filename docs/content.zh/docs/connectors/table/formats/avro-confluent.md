---
title: Confluent Avro
weight: 4
type: docs
aliases:
  - /zh/dev/table/connectors/formats/avro-confluent.html
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

<span class="label label-info">Format: Serialization Schema</span>
<span class="label label-info">Format: Deserialization Schema</span>



Avro Schema Registry (``avro-confluent``) 格式能让你读取被 ``io.confluent.kafka.serializers.KafkaAvroSerializer``序列化的记录，以及可以写入成能被 ``io.confluent.kafka.serializers.KafkaAvroDeserializer``反序列化的记录。

当以这种格式读取（反序列化）记录时，将根据记录中编码的 schema 版本 id 从配置的 Confluent Schema Registry 中获取 Avro writer schema ，而从 table schema 中推断出 reader schema。

当以这种格式写入（序列化）记录时，Avro schema 是从 table schema 中推断出来的，并会用来检索要与数据一起编码的 schema id。我们会在配置的 Confluent Schema Registry 中配置的 [subject](https://docs.confluent.io/current/schema-registry/index.html#schemas-subjects-and-topics) 下，检索 schema id。subject 通过 `avro-confluent.schema-registry.subject` 参数来制定。

The Avro Schema Registry format can only be used in conjunction with the [Apache Kafka SQL connector]({{< ref "docs/connectors/table/kafka" >}}) or the [Upsert Kafka SQL Connector]({{< ref "docs/connectors/table/upsert-kafka" >}}).

依赖
------------

{{< sql_download_table "avro-confluent" >}}

如何创建使用 Avro-Confluent 格式的表
----------------

以下是一个使用 Kafka 连接器和 Confluent Avro 格式创建表的示例。

{{< tabs "3df131fd-0e20-4635-a8f9-3574a764db7a" >}}
{{< tab "SQL" >}}

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
  'value.avro-confluent.schema-registry.url' = 'http://localhost:8082',
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
```

---
Example of a table using the upsert connector with the Kafka value registered as an Avro record in the Schema Registry:

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
  -- => adding a prefix to the table column associated to the kafka key field to avoid clashes
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.schema-registry.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY'
)
```
{{< /tab >}}
{{< /tabs >}}

Format 参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 8%">是否必选</th>
        <th class="text-center" style="width: 7%">默认值</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的格式，这里应该是 <code>'avro-confluent'</code>。</td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.url</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于获取/注册 schemas 的 Confluent Schema Registry 的URL。</td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.subject</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Confluent Schema Registry 主题，用于在序列化期间注册此格式使用的 schema。默认 kafka 和 upsert-kafka 连接器会使用 "&lt;topic_name&gt;-value" 或者 "&lt;topic_name&gt;-key" 作为 subject 名字。但是对于其他连接器（如 filesystem）则在当做 sink 使用时需要显式指定 subject 名字。</td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

目前 Apache Flink 都是从 table schema 去推断反序列化期间的 Avro reader schema 和序列化期间的 Avro writer schema。显式地定义 Avro schema 暂不支持。
[Apache Avro Format]({{< ref "docs/connectors/table/formats/avro" >}}#data-type-mapping)中描述了 Flink 数据类型和 Avro 类型的对应关系。 

除了此处列出的类型之外，Flink 还支持读取/写入可为空（nullable）的类型。 Flink 将可为空的类型映射到 Avro `union(something, null)`, 其中 `something` 是从 Flink 类型转换的 Avro 类型。

您可以参考 [Avro Specification](https://avro.apache.org/docs/current/spec.html) 以获取有关 Avro 类型的更多信息。
