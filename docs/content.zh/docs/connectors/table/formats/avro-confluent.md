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



Avro Schema Registry (``avro-confluent``) 格式能让你读取被 ``io.confluent.kafka.serializers.KafkaAvroSerializer`` 序列化的记录，以及可以写入成能被 ``io.confluent.kafka.serializers.KafkaAvroDeserializer`` 反序列化的记录。

当以这种格式读取（反序列化）记录时，将根据记录中编码的 schema 版本 id 从配置的 Confluent Schema Registry 中获取 Avro writer schema ，而从 table schema 中推断出 reader schema。

当以这种格式写入（序列化）记录时，Avro schema 是从 table schema 中推断出来的，并会用来检索要与数据一起编码的 schema id。我们会在配置的 Confluent Schema Registry 中配置的 [subject](https://docs.confluent.io/current/schema-registry/index.html#schemas-subjects-and-topics) 下，检索 schema id。subject 通过 `avro-confluent.subject` 参数来制定。

Avro Schema Registry 格式只能与 [Apache Kafka SQL 连接器]({{< ref "docs/connectors/table/kafka" >}})或 [Upsert Kafka SQL 连接器]({{< ref "docs/connectors/table/upsert-kafka" >}})一起使用。

依赖
------------

{{< sql_download_table "avro-confluent" >}}

For Maven, SBT, Gradle, or other build automation tools, please also ensure that Confluent's maven repository at `https://packages.confluent.io/maven/` is configured in your project's build files.

如何创建使用 Avro-Confluent 格式的表
----------------

以下是一个使用 Kafka 连接器和 Confluent Avro 格式创建表的示例。

{{< tabs "3df131fd-0e20-4635-a8f9-3574a764db7a" >}}
{{< tab "SQL" >}}

使用原始的 UTF-8 字符串作为 Kafka 的 key，Schema Registry 中注册的 Avro 记录作为 Kafka 的 values 的表的示例：

```sql
CREATE TABLE user_created (

  -- 该列映射到 Kafka 原始的 UTF-8 key
  the_kafka_key STRING,
  
  -- 映射到 Kafka value 中的 Avro 字段的一些列
  id STRING,
  name STRING,
  email STRING

) WITH (

  'connector' = 'kafka',
  'topic' = 'user_events_example1',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- UTF-8 字符串作为 Kafka 的 keys，使用表中的 'the_kafka_key' 列
  'key.format' = 'raw',
  'key.fields' = 'the_kafka_key',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY'
)
```

我们可以像下面这样将数据写入到 kafka 表中：

```sql
INSERT INTO user_created
SELECT
  -- 将 user id 复制至映射到 kafka key 的列中
  id as the_kafka_key,

  -- 所有的 values
  id, name, email
FROM some_table
```

---

Kafka 的 key 和 value 在 Schema Registry 中都注册为 Avro 记录的表的示例：

```sql
CREATE TABLE user_created (
  
  -- 该列映射到 Kafka key 中的 Avro 字段 'id'
  kafka_key_id STRING,
  
  -- 映射到 Kafka value 中的 Avro 字段的一些列
  id STRING,
  name STRING, 
  email STRING
  
) WITH (

  'connector' = 'kafka',
  'topic' = 'user_events_example2',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- 注意：由于哈希分区，在 Kafka key 的上下文中，schema 升级几乎从不向后也不向前兼容。
  'key.format' = 'avro-confluent',
  'key.avro-confluent.url' = 'http://localhost:8082',
  'key.fields' = 'kafka_key_id',

  -- 在本例中，我们希望 Kafka 的 key 和 value 的 Avro 类型都包含 'id' 字段
  -- => 给表中与 Kafka key 字段关联的列添加一个前缀来避免冲突
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8082',
  'value.fields-include' = 'EXCEPT_KEY',
   
  -- 自 Flink 1.13 起，subjects 具有一个默认值, 但是可以被覆盖：
  'key.avro-confluent.subject' = 'user_events_example2-key2',
  'value.avro-confluent.subject' = 'user_events_example2-value2'
)
```

---
使用 upsert-kafka 连接器，Kafka 的 value 在 Schema Registry 中注册为 Avro 记录的表的示例：

```sql
CREATE TABLE user_created (
  
  -- 该列映射到 Kafka 原始的 UTF-8 key
  kafka_key_id STRING,
  
  -- 映射到 Kafka value 中的 Avro 字段的一些列
  id STRING, 
  name STRING, 
  email STRING, 
  
  -- upsert-kafka 连接器需要一个主键来定义 upsert 行为
  PRIMARY KEY (kafka_key_id) NOT ENFORCED

) WITH (

  'connector' = 'upsert-kafka',
  'topic' = 'user_events_example3',
  'properties.bootstrap.servers' = 'localhost:9092',

  -- UTF-8 字符串作为 Kafka 的 keys
  -- 在本例中我们不指定 'key.fields'，因为它由表的主键决定
  'key.format' = 'raw',
  
  -- 在本例中，我们希望 Kafka 的 key 和 value 的 Avro 类型都包含 'id' 字段
  -- => 给表中与 Kafka key 字段关联的列添加一个前缀来避免冲突
  'key.fields-prefix' = 'kafka_key_',

  'value.format' = 'avro-confluent',
  'value.avro-confluent.url' = 'http://localhost:8082',
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
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Specify what format to use, here should be <code>'avro-confluent'</code>.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.basic-auth.credentials-source</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Basic auth credentials source for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.basic-auth.user-info</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Basic auth user info for schema registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.bearer-auth.credentials-source</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Bearer auth credentials source for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.bearer-auth.token</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Bearer auth token for Schema Registry</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.properties</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>Map</td>
            <td>Properties map that is forwarded to the underlying Schema Registry. This is useful for options that are not officially exposed via Flink config options. However, note that Flink options have higher precedence.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.keystore.location</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Location / File of SSL keystore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.keystore.password</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Password for SSL keystore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.truststore.location</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Location / File of SSL truststore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.ssl.truststore.password</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>Password for SSL truststore</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.subject</h5></td>
            <td>optional</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The Confluent Schema Registry subject under which to register the schema used by this format during serialization. By default, 'kafka' and 'upsert-kafka' connectors use '&lt;topic_name&gt;-value' or '&lt;topic_name&gt;-key' as the default subject name if this format is used as the value or key format. But for other connectors (e.g. 'filesystem'), the subject option is required when used as sink.</td>
        </tr>
        <tr>
            <td><h5>avro-confluent.url</h5></td>
            <td>required</td>
            <td style="word-wrap: break-word;">(none)</td>
            <td>String</td>
            <td>The URL of the Confluent Schema Registry to fetch/register schemas.</td>
        </tr>
    </tbody>
</table>

数据类型映射
----------------

目前 Apache Flink 都是从 table schema 去推断反序列化期间的 Avro reader schema 和序列化期间的 Avro writer schema。显式地定义 Avro schema 暂不支持。
[Apache Avro Format]({{< ref "docs/connectors/table/formats/avro" >}}#data-type-mapping)中描述了 Flink 数据类型和 Avro 类型的对应关系。 

除了此处列出的类型之外，Flink 还支持读取/写入可为空（nullable）的类型。 Flink 将可为空的类型映射到 Avro `union(something, null)`, 其中 `something` 是从 Flink 类型转换的 Avro 类型。

您可以参考 [Avro Specification](https://avro.apache.org/docs/current/spec.html) 以获取有关 Avro 类型的更多信息。
