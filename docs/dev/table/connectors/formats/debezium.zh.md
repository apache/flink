---
title: "Debezium Format"
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

[Debezium](https://debezium.io/) 是一个 CDC（Changelog Data Capture，变更数据捕获）的工具，可以把来自 MySQL、PostgreSQL、Oracle、Microsoft SQL Server 和许多其他数据库的更改实时流式传输到 Kafka 中。 Debezium 为变更日志提供了统一的格式结构，并支持使用 JSON 和 Apache Avro 序列化消息。

Flink 支持将 Debezium JSON 消息解析为 INSERT / UPDATE / DELETE 消息到 Flink SQL 系统中。在很多情况下，利用这个特性非常的有用，例如
 - 将增量数据从数据库同步到其他系统
 - 日志审计
 - 数据库的实时物化视图
 - 关联维度数据库的变更历史，等等。

*注意: 支持解析 Debezium Avro 消息和输出 Debezium 消息已经规划在路线图上了。*

依赖
------------

为了设置 Debezium Format，下表提供了使用构建自动化工具（例如 Maven 或 SBT）和带有 SQL JAR 包的 SQL Client 的两个项目的依赖项信息。

| Maven 依赖   | SQL Client JAR         |
| :----------------- | :----------------------|
| `flink-json`       | 内置               |

*注意: 请参考 [Debezium 文档](https://debezium.io/documentation/reference/1.1/index.html)，了解如何设置 Debezium Kafka Connect 用来将变更日志同步到 Kafka 主题。*


如何使用 Debezium Format
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

*注意: 请参考 [Debezium 文档](https://debezium.io/documentation/reference/1.1/connectors/mysql.html#mysql-connector-events_debezium)，了解每个字段的含义。*

MySQL 产品表有4列（`id`、`name`、`description`、`weight`）。上面的 JSON 消息是 `products` 表上的一条更新事件，其中 `id = 111` 的行的 `weight` 值从 `5.18` 更改为 `5.15`。假设此消息已同步到 Kafka 主题 `products_binlog`，则可以使用以下 DDL 来使用此主题并解析更改事件。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
CREATE TABLE topic_products (
  -- schema 与 MySQL 的 products 表完全相同
  id BIGINT,
  name STRING,
  description STRING,
  weight DECIMAL(10, 2)
) WITH (
 'connector' = 'kafka',
 'topic' = 'products_binlog',
 'properties.bootstrap.servers' = 'localhost:9092',
 'properties.group.id' = 'testGroup',
 'format' = 'debezium-json'  -- 使用 debezium-json 作为 format
)
{% endhighlight %}
</div>
</div>

在某些情况下，用户在设置 Debezium Kafka Connect 时，可能会开启 Kafka 的配置 `'value.converter.schemas.enable'`，用来在消息体中包含 schema 信息。然后，Debezium JSON 消息可能如下所示:

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

为了解析这一类信息，你需要在上述 DDL WITH 子句中添加选项 `'debezium-json.schema-include' = 'true'`（默认为 false）。通常情况下，建议不要包含 schema 的描述，因为这样会使消息变得非常冗长，并降低解析性能。

在将主题注册为 Flink 表之后，可以将 Debezium 消息用作变更日志源。

<div class="codetabs" markdown="1">
<div data-lang="SQL" markdown="1">
{% highlight sql %}
-- MySQL "products" 的实时物化视图
-- 计算相同产品的最新平均重量
SELECT name, AVG(weight) FROM topic_products GROUP BY name;

-- 将 MySQL "products" 表的所有数据和增量更改同步到
-- Elasticsearch "products" 索引，供将来查找
INSERT INTO elasticsearch_products
SELECT * FROM topic_products;
{% endhighlight %}
</div>
</div>


Format 参数
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">参数</th>
        <th class="text-center" style="width: 10%">是否必选</th>
        <th class="text-center" style="width: 10%">默认值</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 45%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的格式，此处应为 <code>'debezium-json'</code>。</td>
    </tr>
    <tr>
      <td><h5>debezium-json.schema-include</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>设置 Debezium Kafka Connect 时，用户可以启用 Kafka 配置 <code>'value.converter.schemas.enable'</code> 以在消息中包含 schema。此选项表明 Debezium JSON 消息是否包含 schema。</td>
    </tr>
    <tr>
      <td><h5>debezium-json.ignore-parse-errors</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>当解析异常时，是跳过当前字段或行，还是抛出错误失败（默认为 false，即抛出错误失败）。如果忽略字段的解析异常，则会将该字段值设置为<code>null</code>。</td>
    </tr>
    <tr>
      <td><h5>debezium-json.timestamp-format.standard</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;"><code>'SQL'</code></td>
      <td>String</td>
      <td>声明输入和输出的时间戳格式。当前支持的格式为<code>'SQL'</code> 以及 <code>'ISO-8601'</code>：
      <ul>
        <li>可选参数 <code>'SQL'</code> 将会以 "yyyy-MM-dd HH:mm:ss.s{precision}" 的格式解析时间戳, 例如 '2020-12-30 12:13:14.123'，且会以相同的格式输出。</li>
        <li>可选参数 <code>'ISO-8601'</code> 将会以 "yyyy-MM-ddTHH:mm:ss.s{precision}" 的格式解析输入时间戳, 例如 '2020-12-30T12:13:14.123' ，且会以相同的格式输出。</li>
      </ul>
      </td>
    </tr>
    </tbody>
</table>

注意事项
----------------

### 消费 Debezium Postgres Connector 产生的数据

如果你正在使用 [Debezium PostgreSQL Connector](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html) 捕获变更到 Kafka，请确保被监控表的 [REPLICA IDENTITY](https://www.postgresql.org/docs/current/sql-altertable.html#SQL-CREATETABLE-REPLICA-IDENTITY) 已经被配置成 `FULL` 了，默认值是 `DEFAULT`。
否则，Flink SQL 将无法正确解析 Debezium 数据。

当配置为 `FULL` 时，更新和删除事件将完整包含所有列的之前的值。当为其他配置时，更新和删除事件的 "before" 字段将只包含 primary key 字段的值，或者为 null（没有 primary key）。
你可以通过运行 `ALTER TABLE <your-table-name> REPLICA IDENTITY FULL` 来更改 `REPLICA IDENTITY` 的配置。
请阅读 [Debezium 关于 PostgreSQL REPLICA IDENTITY 的文档](https://debezium.io/documentation/reference/1.2/connectors/postgresql.html#postgresql-replica-identity) 了解更多。

数据类型映射
----------------

目前，Debezium Format 使用 JSON Format 进行反序列化。有关数据类型映射的更多详细信息，请参考 [JSON Format 文档]({% link dev/table/connectors/formats/json.zh.md %}#data-type-mapping)。

