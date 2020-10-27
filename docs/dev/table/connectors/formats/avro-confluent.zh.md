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

The Avro Schema Registry format can only be used in conjunction with [Apache Kafka SQL connector]({% link dev/table/connectors/kafka.zh.md %}). 

依赖
------------

为了建立Avro Schema Registry格式，下列的表格提供了为项目使用自动化工具（例如Maven或者SBT）以及SQL客户端使用SQL JAR包的依赖信息。

| Maven依赖                     | SQL 客户端 JAR         |
| :----------------------------------- | :----------------------|
| `flink-avro-confluent-registry`      | {% if site.is_stable %} [下载](https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-avro-confluent-registry/{{site.version}}/flink-sql-avro-confluent-registry-{{site.version}}.jar) {% else %} Only available for stable releases. {% endif %} |

如何创建使用 Avro-Confluent 格式的表
----------------

以下是一个使用 Kafka 连接器和 Confluent Avro 格式创建表的示例。

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
  'topic' = 'user_behavior'
  'format' = 'avro-confluent',
  'avro-confluent.schema-registry.url' = 'http://localhost:8081',
  'avro-confluent.schema-registry.subject' = 'user_behavior'
)
{% endhighlight %}
</div>
</div>

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
      <td>指定要使用的格式，这里应该是 <code>'avro-confluent'</code>.</td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.url</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>用于获取/注册schemas的Confluent Schema Registry的URL </td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.subject</h5></td>
      <td>sink 必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Confluent Schema Registry主题，用于在序列化期间注册此格式使用的schema </td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

目前Apache Flink都是从table schema去推断反序列化期间的Avro reader schema和序列化期间的Avro writer schema。显式地定义 Avro schema 暂不支持。
[Apache Avro Format]({% link dev/table/connectors/formats/avro.zh.md%}#data-type-mapping)中描述了flink数据和Avro数据的对应关系。 

除了此处列出的类型之外，Flink还支持读取/写入可为空的类型。 Flink将可为空的类型映射到Avro `union(something, null)`, 其中 `something` 是从Flink类型转换的Avro类型。

您可以参考 [Avro Specification](https://avro.apache.org/docs/current/spec.html) 以获取有关Avro类型的更多信息。
