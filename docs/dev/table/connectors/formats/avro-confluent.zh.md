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

Avro Schema Registry (``avro-confluent``) 格式能让你读取被 ``io.confluent.kafka.serializers.KafkaAvroSerializer``序列化的记录，以及可以写入成能被 ``io.confluent.kafka.serializers.KafkaAvroDeserializer``反序列化的记录。

当以这种格式读取（反序列化）记录时，将根据记录中编码的 schema 版本 id 从配置的 Confluent Schema Registry 中获取 Avro writer schema ，而从 table schema 中推断出 reader schema。

当以这种格式写入（序列化）记录时，Avro schema 是从 table schema 中推断出来的，并会用来检索要与数据一起编码的 schema id。我们会在配置的 Confluent Schema Registry 中配置的 [subject](https://docs.confluent.io/current/schema-registry/index.html#schemas-subjects-and-topics) 下，检索 schema id。subject 通过 `avro-confluent.schema-registry.subject` 参数来制定。

Avro Schema Registry 格式只能与[Apache Kafka SQL连接器]({% link dev/table/connectors/kafka.zh.md %})结合使用。

依赖
------------

{% assign connector = site.data.sql-connectors['avro-confluent'] %} 
{% include sql-connector-download-table.zh.html 
    connector=connector
%}

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
      <td>用于获取/注册 schemas 的 Confluent Schema Registry 的URL </td>
    </tr>
    <tr>
      <td><h5>avro-confluent.schema-registry.subject</h5></td>
      <td>sink 必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Confluent Schema Registry主题，用于在序列化期间注册此格式使用的 schema </td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

目前 Apache Flink 都是从 table schema 去推断反序列化期间的 Avro reader schema 和序列化期间的 Avro writer schema。显式地定义 Avro schema 暂不支持。
[Apache Avro Format]({% link dev/table/connectors/formats/avro.zh.md%}#data-type-mapping)中描述了 Flink 数据类型和 Avro 类型的对应关系。 

除了此处列出的类型之外，Flink 还支持读取/写入可为空（nullable）的类型。 Flink 将可为空的类型映射到 Avro `union(something, null)`, 其中 `something` 是从 Flink 类型转换的 Avro 类型。

您可以参考 [Avro Specification](https://avro.apache.org/docs/current/spec.html) 以获取有关 Avro 类型的更多信息。
