---
title: Ogg
weight: 8
type: docs
aliases:
- /dev/table/connectors/formats/ogg.html
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

# Ogg Format

{{< label "Changelog-Data-Capture Format" >}} {{< label "Format: Serialization Schema" >}} {{<
label "Format: Deserialization Schema" >}}

[Oracle GoldenGate](https://www.oracle.com/integration/goldengate/) (a.k.a ogg) 是一个实现异构 IT 环境间数据实时数据集成和复制的综合软件包。
该产品集支持高可用性解决方案、实时数据集成、事务更改数据捕获、运营和分析企业系统之间的数据复制、转换和验证。Ogg 为变更日志提供了统一的格式结构，并支持使用 JSON 序列化消息。

Flink 支持将 Ogg JSON 消息解析为 INSERT/UPDATE/DELETE 消息到 Flink SQL 系统中。在很多情况下，利用这个特性非常有用，例如

- 将增量数据从数据库同步到其他系统
- 日志审计
- 数据库的实时物化视图
- 关联维度数据库的变更历史，等等

Flink 还支持将 Flink SQL 中的 INSERT/UPDATE/DELETE 消息编码为 Ogg JSON 格式的消息, 输出到 Kafka 等存储中。
但需要注意, 目前 Flink 还不支持将 UPDATE_BEFORE 和 UPDATE_AFTER 合并为一条 UPDATE 消息. 因此, Flink 将 UPDATE_BEFORE 和 UPDATE_AFTER
分别编码为 DELETE 和 INSERT 类型的 Ogg 消息。

Dependencies
------------

#### Ogg Json

{{< sql_download_table "ogg-json" >}}

*注意: 请参考 [Ogg Kafka Handler documentation](https://docs.oracle.com/en/middleware/goldengate/big-data/19.1/gadbd/using-kafka-handler.html)，
了解如何设置 Ogg Kafka handler 来将变更日志同步到 Kafka 的 Topic。*


How to use Ogg format
----------------

Ogg 为变更日志提供了统一的格式, 这是一个 JSON 格式的从 Oracle `PRODUCTS` 表捕获的更新操作的简单示例：

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
  "op_type": "U",
  "op_ts": "2020-05-13 15:40:06.000000",
  "current_ts": "2020-05-13 15:40:07.000000",
  "primary_keys": [
    "id"
  ],
  "pos": "00000000000000000000143",
  "table": "PRODUCTS"
}
```

*注意：请参考 [Debezium documentation](https://debezium.io/documentation/reference/1.3/connectors/oracle.html#oracle-events)
了解每个字段的含义.*

Oracle `PRODUCTS` 表 有 4 列 (`id`, `name`, `description` and `weight`). 上面的 JSON 消息是 `PRODUCTS` 表上的一条更新事件，其中 `id = 111` 的行的
`weight` 值从 `5.18` 更改为 `5.15`. 假设此消息已同步到 Kafka 的 Topic `products_ogg`, 则可以使用以下 DDL 来使用该 Topic 并解析更新事件。

```sql
CREATE TABLE topic_products (
  -- schema is totally the same to the Oracle "products" table
  id BIGINT,
  name STRING,
  description STRING,
  weight DECIMAL(10, 2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'products_ogg',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'format' = 'ogg-json'
)
```

再将 Kafka Topic 注册为 Flink 表之后， 可以将 OGG 消息变为变更日志源。

```sql
-- a real-time materialized view on the Oracle "PRODUCTS"
-- which calculate the latest average of weight for the same products
SELECT name, AVG(weight)
FROM topic_products
GROUP BY name;

-- synchronize all the data and incremental changes of Oracle "PRODUCTS" table to
-- Elasticsearch "products" index for future searching
INSERT INTO elasticsearch_products
SELECT *
FROM topic_products;
```

Available Metadata
------------------

The following format metadata can be exposed as read-only (`VIRTUAL`) columns in a table definition.

<span class="label label-danger">Attention</span> Format metadata fields are only available if the
corresponding connector forwards format metadata. Currently, only the Kafka connector is able to
expose metadata fields for its value format.

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">Key</th>
      <th class="text-center" style="width: 40%">Data Type</th>
      <th class="text-center" style="width: 40%">Description</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>table</code></td>
      <td><code>STRING NULL </code></td>
      <td>Contains fully qualified table name. The format of the fully qualified table name is: 
        CATALOG NAME.SCHEMA NAME.TABLE NAME</td>
    </tr>
    <tr>
      <td><code>primary-keys</code></td>
      <td><code>ARRAY&lt;STRING&gt; NULL</code></td>
      <td>An array variable holding the column names of the primary keys of the source table. 
        The primary-keys field is only include in the JSON output if the includePrimaryKeys 
        configuration property is set to true.</td>
    </tr>
    <tr>
      <td><code>ingestion-timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(6) NULL</code></td>
      <td>The timestamp at which the connector processed the event. Corresponds to the current_ts field in the Ogg record.</td>
    </tr>
    <tr>
      <td><code>event-timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(6) NULL</code></td>
      <td>The timestamp at which the source system created the event. Corresponds to the op_ts field in the Ogg record.</td>
    </tr>
    </tbody>
</table>

The following example shows how to access Ogg metadata fields in Kafka:

```sql
CREATE TABLE KafkaTable (
  origin_ts TIMESTAMP(3) METADATA FROM 'value.ingestion-timestamp' VIRTUAL,
  event_time TIMESTAMP(3) METADATA FROM 'value.event-timestamp' VIRTUAL,
  origin_table STRING METADATA FROM 'value.table' VIRTUAL,
  primary_keys ARRAY<STRING> METADATA FROM 'value.primary_keys' VIRTUAL,
  user_id BIGINT,
  item_id BIGINT,
  behavior STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'ogg-json'
);
```

Format Options
----------------

<table class="table table-bordered">
    <thead>
      <tr>
        <th class="text-left" style="width: 25%">选项</th>
        <th class="text-center" style="width: 8%">要求</th>
        <th class="text-center" style="width: 7%">默认</th>
        <th class="text-center" style="width: 10%">类型</th>
        <th class="text-center" style="width: 50%">描述</th>
      </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>format</h5></td>
      <td>必填</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>指定要使用的格式，此处应为 <code>'ogg-json'</code>.</td>
    </tr>
    <tr>
      <td><h5>ogg-json.ignore-parse-errors</h5></td>
      <td>选填</td>
      <td style="word-wrap: break-word;">false</td>
      <td>Boolean</td>
      <td>当解析异常时，是跳过当前字段或行，还是抛出错误失败（默认为 false，即抛出错误失败）。如果忽略字段的解析异常，则会将该字段值设置为null。</td>
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
    <tr>
      <td><h5>ogg-json.map-null-key.mode</h5></td>
      <td>选填</td>
      <td style="word-wrap: break-word;"><code>'FAIL'</code></td>
      <td>String</td>
      <td>指定处理 Map 中 key 值为空的方法. 当前支持的值有 <code>'FAIL'</code>, <code>'DROP'</code> 和 <code>'LITERAL'</code>:
      <ul>
        <li>Option <code>'FAIL'</code> 将抛出异常。</li>
        <li>Option <code>'DROP'</code> 将丢弃 Map 中 key 值为空的数据项。</li>
        <li>Option <code>'LITERAL'</code> 将使用字符串常量来替换 Map 中的空 key 值。字符串常量的值由 <code>ogg-json.map-null-key.literal</code> 定义。</li>
      </ul>
      </td>
    </tr>
    <tr>
      <td><h5>ogg-json.map-null-key.literal</h5></td>
      <td>选填</td>
      <td style="word-wrap: break-word;">'null'</td>
      <td>String</td>
      <td>当 <code>'ogg-json.map-null-key.mode'</code> 是 LITERAL 的时候，指定字符串常量替换 Map 中的空 key 值。</td>
    </tr>
    </tbody>
</table>

Data Type Mapping
----------------

目前, Ogg format 使用 JSON format 进行序列化和反序列化。有关数据类型映射的更多详细信息，请参考 [JSON Format 文档]({{< ref "docs/connectors/table/formats/json" >}}#data-type-mapping)。
