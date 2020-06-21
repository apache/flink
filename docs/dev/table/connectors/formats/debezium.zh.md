---
title: "Debezium 格式化"
nav-title: Debezium
nav-parent_id: sql-formats
nav-pos: 4
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

<span class="label label-info">Changelog-Data-Capture Format</span>
<span class="label label-info">Format: Deserialization Schema</span>

* This will be replaced by the TOC
{:toc}

[Debezium](https://debezium.io/) Debezium 是一个 CDC（Changelog数据捕获）的工具，可以把来自 MySQL、PostgreSQL、Oracle、Microsoft SQL Server 和许多其他数据库的更改实时流式传输到 Kafka 中。 Debezium 为变更日志提供了统一的格式结构，并支持使用 JSON 和 Apache Avro 序列化消息。

Flink 支持将 Debezium JSON 消息解释为 INSERT / UPDATE / DELETE 消息到 Flink SQL 系统中。在很多情况下，利用这个特性非常的有用，例如
 - 将增量数据从数据库同步到其他系统
 - 审核日志
 - 关于数据库的实时物化视图
 - 临时联接更改数据库表的历史记录等等。

*注意: 路线图上支持解释 Debezium Avro 消息和发出 Debezium 消息。*

依赖
------------

为了设置 Debezium 格式，下表提供了使用构建自动化工具（例如 Maven 或 SBT）和带有 SQL JAR 包的 SQL Client 的两个项目的依赖项信息。

| Maven dependency   | SQL Client JAR         |
| :----------------- | :----------------------|
| `flink-json`       | Built-in               |

*注意: 请参考 [Debezium documentation](https://debezium.io/documentation/reference/1.1/index.html) 文档，了解如何设置 Debezium Kafka Connect 用来将变更日志同步到 Kafka 主题。*


如何使用 Debezium 格式
----------------


Debezium 为变更日志提供了统一的格式，这是一个从 MySQL product 表捕获的更新操作的简单示例:

```json
{
  "before": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.18
  },
  "after": {
    "id": 111,
    "name": "scooter",
    "description": "Big 2-wheel scooter",
    "weight": 5.15
  },
  "source": {...},
  "op": "u",
  "ts_ms": 1589362330904,
  "transaction": null
}
```

*注意: 请参考 [Debezium documentation](https://debezium.io/documentation/reference/1.1/connectors/mysql.html#mysql-connector-events_debezium) 文档，了解每个字段的含义。*

MySQL 产品表有4列（id、name、description、weight）。上面的 JSON 消息是 products 表上的 update change 事件，其中 id = 111 的行的 weight 值从 5.18 更改为 5.15。假设此消息已同步到 Kafka 主题 products_binlog，则可以使用以下 DDL 来使用此主题并解释更改事件。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE topic_products (
  -- 结构与 MySQL 的 products 表完全相同
  id BIGINT,
  name STRING,
  description STRING,
  weight DECIMAL(10, 2)
) WITH (
 'connector' = 'kafka',
 'topic' = 'products_binlog',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'debezium-json'  -- using debezium-json as the format
)
{% endhighlight %}
</div>
</div>

在某些情况下，用户可以使用 Kafka 的配置 “value.converter.schemas.enable” 设置 Debezium Kafka Connect，用来在消息中包括结构的描述信息。然后，Debezium JSON 消息可能如下所示:

```json
{
  "schema": {...},
  "payload": {
    "before": {
      "id": 111,
      "name": "scooter",
      "description": "Big 2-wheel scooter",
      "weight": 5.18
    },
    "after": {
      "id": 111,
      "name": "scooter",
      "description": "Big 2-wheel scooter",
      "weight": 5.15
    },
    "source": {...},
    "op": "u",
    "ts_ms": 1589362330904,
    "transaction": null
  }
}
```

为了说明这一类信息，你需要在上述 DDL WITH 子句中添加选项'debezium-json.schema-include'='true'（默认为 false）。通常情况下，建议不要包含结构的描述，因为这样会使消息变得非常冗长，并降低解析性能。

在将主题注册为 Flink 表之后，可以将 Debezium 消息用作变更日志源。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
-- MySQL “products” 的实时物化视图
-- 计算相同产品的最新平均重量
SELECT name, AVG(weight) FROM topic_products GROUP BY name;

-- 将 MySQL “products” 表的所有数据和增量更改同步到
-- Elasticsearch “products” 索引，供将来查找
INSERT INTO elasticsearch_products
SELECT * FROM topic_products;
{% endhighlight %}
</div>
</div>


格式选项
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
      <td>Specify what format to use, here should be <code>'debezium-json'</code>.</td>
    </tr>
    <tr>
      <td><h5>debezium-json.schema-include</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>When setting up a Debezium Kafka Connect, users may enable a Kafka configuration <code>'value.converter.schemas.enable'</code> to include schema in the message.
          This option indicates whether the Debezium JSON message includes the schema or not. </td>
    </tr>
    <tr>
      <td><h5>debezium-json.ignore-parse-errors</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>Skip fields and rows with parse errors instead of failing.
      Fields are set to null in case of errors.</td>
    </tr>
    <tr>
      <td><h5>debezium-json.timestamp-format.standard</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;"><code>'SQL'</code></td>
      <td>String</td>
      <td>Specify the input and output timestamp format. Currently supported values are <code>'SQL'</code> and <code>'ISO-8601'</code>:
      <ul>
        <li>Option <code>'SQL'</code> will parse input timestamp in "yyyy-MM-dd HH:mm:ss.s{precision}" format, e.g '2020-12-30 12:13:14.123' and output timestamp in the same format.</li>
        <li>Option <code>'ISO-8601'</code>will parse input timestamp in "yyyy-MM-ddTHH:mm:ss.s{precision}" format, e.g '2020-12-30T12:13:14.123' and output timestamp in the same format.</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

数据类型映射
----------------

目前，Debezium 格式使用 JSON 格式进行反序列化。有关数据类型映射的更多详细信息，请参考 JSON 格式文档。[JSON format documentation]({% link /zh/dev/table/connectors/formats/json.zh.md %}#data-type-mapping)。

