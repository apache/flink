---
title: Kafka
weight: 3
type: docs
aliases:
  - /zh/dev/table/connectors/kafka.html
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

# Apache Kafka SQL 连接器

{{< label "Scan Source: Bounded" >}}
{{< label "Scan Source: Unbounded" >}}
{{< label "Sink: Streaming Append Mode" >}}

Kafka 连接器提供从 Kafka topic 中消费和写入数据的能力。

依赖
------------

{{< sql_download_table "kafka" >}}

Kafka 连接器目前并不包含在 Flink 的二进制发行版中，请查阅[这里]({{< ref "docs/dev/configuration/overview" >}})了解如何在集群运行中引用 Kafka 连接器。

如何创建 Kafka 表
----------------

以下示例展示了如何创建 Kafka 表：

```sql
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
)
```

可用的元数据
------------------

以下的连接器元数据可以在表定义中通过元数据列的形式获取。

`R/W` 列定义了一个元数据是可读的（`R`）还是可写的（`W`）。
只读列必须声明为 `VIRTUAL` 以在 `INSERT INTO` 操作中排除它们。

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">键</th>
      <th class="text-center" style="width: 30%">数据类型</th>
      <th class="text-center" style="width: 40%">描述</th>
      <th class="text-center" style="width: 5%">R/W</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><code>topic</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>Kafka 记录的 Topic 名。</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>partition</code></td>
      <td><code>INT NOT NULL</code></td>
      <td>Kafka 记录的 partition ID。</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>headers</code></td>
      <td><code>MAP<STRING, BYTES> NOT NULL</code></td>
      <td>二进制 Map 类型的 Kafka 记录头（Header）。</td>
      <td><code>R/W</code></td>
    </tr>
    <tr>
      <td><code>leader-epoch</code></td>
      <td><code>INT NULL</code></td>
      <td>Kafka 记录的 Leader epoch（如果可用）。</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>offset</code></td>
      <td><code>BIGINT NOT NULL</code></td>
      <td>Kafka 记录在 partition 中的 offset。</td>
      <td><code>R</code></td>
    </tr>
    <tr>
      <td><code>timestamp</code></td>
      <td><code>TIMESTAMP_LTZ(3) NOT NULL</code></td>
      <td>Kafka 记录的时间戳。</td>
      <td><code>R/W</code></td>
    </tr>
    <tr>
      <td><code>timestamp-type</code></td>
      <td><code>STRING NOT NULL</code></td>
      <td>Kafka 记录的时间戳类型。可能的类型有 "NoTimestampType"，
          "CreateTime"（会在写入元数据时设置），或 "LogAppendTime"。</td>
      <td><code>R</code></td>
    </tr>
    </tbody>
</table>

以下扩展的 `CREATE TABLE` 示例展示了使用这些元数据字段的语法：

```sql
CREATE TABLE KafkaTable (
  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  `partition` BIGINT METADATA VIRTUAL,
  `offset` BIGINT METADATA VIRTUAL,
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);
```

**格式元信息**

连接器可以读出消息格式的元数据。格式元数据的配置键以 `'value.'` 作为前缀。

以下示例展示了如何获取 Kafka 和 Debezium 的元数据字段：

```sql
CREATE TABLE KafkaTable (
  `event_time` TIMESTAMP_LTZ(3) METADATA FROM 'value.source.timestamp' VIRTUAL,  -- from Debezium format
  `origin_table` STRING METADATA FROM 'value.source.table' VIRTUAL, -- from Debezium format
  `partition_id` BIGINT METADATA FROM 'partition' VIRTUAL,  -- from Kafka connector
  `offset` BIGINT METADATA VIRTUAL,  -- from Kafka connector
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  'topic' = 'user_behavior',
  'properties.bootstrap.servers' = 'localhost:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'value.format' = 'debezium-json'
);
```

连接器参数
----------------

<table class="table table-bordered">
    <thead>
    <tr>
      <th class="text-left" style="width: 25%">参数</th>
      <th class="text-center" style="width: 8%">是否必选</th>
      <th class="text-center" style="width: 7%">默认值</th>
      <th class="text-center" style="width: 10%">数据类型</th>
      <th class="text-center" style="width: 50%">描述</th>
    </tr>
    </thead>
    <tbody>
    <tr>
      <td><h5>connector</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>指定使用的连接器，Kafka 连接器使用 <code>'kafka'</code>。</td>
    </tr>
    <tr>
      <td><h5>topic</h5></td>
      <td>required for sink</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>当表用作 source 时读取数据的 topic 名。亦支持用分号间隔的 topic 列表，如 <code>'topic-1;topic-2'</code>。注意，对 source 表而言，'topic' 和 'topic-pattern' 两个选项只能使用其中一个。当表被用作 sink 时，该配置表示写入的 topic 名。注意 sink 表不支持 topic 列表。</td>
    </tr>
    <tr>
      <td><h5>topic-pattern</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>匹配读取 topic 名称的正则表达式。在作业开始运行时，所有匹配该正则表达式的 topic 都将被 Kafka consumer 订阅。注意，对 source 表而言，'topic' 和 'topic-pattern' 两个选项只能使用其中一个。</td>
    </tr>
    <tr>
      <td><h5>properties.bootstrap.servers</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>逗号分隔的 Kafka broker 列表。</td>
    </tr>
    <tr>
      <td><h5>properties.group.id</h5></td>
      <td>对 source 可选，不适用于 sink</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>Kafka source 的消费组 id。如果未指定消费组 ID，则会使用自动生成的 "KafkaSource-{tableIdentifier}" 作为消费组 ID。</td>
    </tr>
    <tr>
      <td><h5>properties.*</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>
         可以设置和传递任意 Kafka 的配置项。后缀名必须匹配在 <a href="https://kafka.apache.org/documentation/#configuration">Kafka 配置文档</a> 中定义的配置键。Flink 将移除 "properties." 配置键前缀并将变换后的配置键和值传入底层的 Kafka 客户端。例如，你可以通过 <code>'properties.allow.auto.create.topics' = 'false'</code> 来禁用 topic 的自动创建。但是某些配置项不支持进行配置，因为 Flink 会覆盖这些配置，例如 <code>'key.deserializer'</code> 和 <code>'value.deserializer'</code>。
      </td>
    </tr>
    <tr>
      <td><h5>format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>用来序列化或反序列化 Kafka 消息的格式。
      请参阅 <a href="{{< ref "docs/connectors/table/formats/overview" >}}">格式</a> 页面以获取更多关于格式的细节和相关配置项。
      注意：该配置项和 <code>'value.format'</code> 二者必需其一。
      </td>
    </tr>
    <tr>
      <td><h5>key.format</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>用来序列化和反序列化 Kafka 消息键（Key）的格式。
      请参阅 <a href="{{< ref "docs/connectors/table/formats/overview" >}}">格式</a> 页面以获取更多关于格式的细节和相关配置项。
      注意：如果定义了键格式，则配置项 <code>'key.fields'</code> 也是必需的。
      否则 Kafka 记录将使用空值作为键。
      </td>
    </tr>
    <tr>
      <td><h5>key.fields</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">[]</td>
      <td>List&lt;String&gt;</td>
      <td>表结构中用来配置消息键（Key）格式数据类型的字段列表。默认情况下该列表为空，因此消息键没有定义。
        列表格式为 <code>'field1;field2'</code>。
      </td>
    </tr>
    <tr>
      <td><h5>key.fields-prefix</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>为所有消息键（Key）格式字段指定自定义前缀，以避免与消息体（Value）格式字段重名。默认情况下前缀为空。
        如果定义了前缀，表结构和配置项 <code>'key.fields'</code> 都需要使用带前缀的名称。
        当构建消息键格式字段时，前缀会被移除，消息键格式将会使用无前缀的名称。
        请注意该配置项要求必须将 <code>'value.fields-include'</code> 配置为 <code>'EXCEPT_KEY'</code>。
      </td>
    </tr>
    <tr>
      <td><h5>value.format</h5></td>
      <td>必选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>序列化和反序列化 Kafka 消息体时使用的格式。
      请参阅 <a href="{{< ref "docs/connectors/table/formats/overview" >}}">格式</a> 页面以获取更多关于格式的细节和相关配置项。
      注意：该配置项和 <code>'format'</code> 二者必需其一。
      </td>
    </tr>
    <tr>
      <td><h5>value.fields-include</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">ALL</td>
      <td><p>枚举类型</p> 可选值：[ALL, EXCEPT_KEY]</td>
      <td>定义消息体（Value）格式如何处理消息键（Key）字段的策略。
        默认情况下，表结构中 <code>'ALL'</code> 即所有的字段都会包含在消息体格式中，即消息键字段在消息键和消息体格式中都会出现。
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.mode</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">group-offsets</td>
      <td>Enum</td>
      <td>Kafka consumer 的启动模式。有效值为：<code>'earliest-offset'</code>，<code>'latest-offset'</code>，<code>'group-offsets'</code>，<code>'timestamp'</code> 和 <code>'specific-offsets'</code>。
       请参阅下方 <a href="#起始消费位点">起始消费位点</a> 以获取更多细节。</td>
    </tr>
    <tr>
      <td><h5>scan.startup.specific-offsets</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>String</td>
      <td>在使用 <code>'specific-offsets'</code> 启动模式时为每个 partition 指定 offset，例如 <code>'partition:0,offset:42;partition:1,offset:300'</code>。
      </td>
    </tr>
    <tr>
      <td><h5>scan.startup.timestamp-millis</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>Long</td>
      <td>在使用 <code>'timestamp'</code> 启动模式时指定启动的时间戳（单位毫秒）。</td>
    </tr>
    <tr>
      <td><h5>scan.bounded.mode</h5></td>
      <td>optional</td>
      <td style="word-wrap: break-word;">unbounded</td>
      <td>Enum</td>
      <td>Bounded mode for Kafka consumer, valid values are <code>'latest-offset'</code>, <code>'group-offsets'</code>, <code>'timestamp'</code> and <code>'specific-offsets'</code>.
       See the following <a href="#bounded-ending-position">Bounded Ending Position</a> for more details.</td>
    </tr>
    <tr>
      <td><h5>scan.bounded.specific-offsets</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>String</td>
      <td>Specify offsets for each partition in case of <code>'specific-offsets'</code> bounded mode, e.g. <code>'partition:0,offset:42;partition:1,offset:300'. If an offset
       for a partition is not provided it will not consume from that partition.</code>.
      </td>
    </tr>
    <tr>
      <td><h5>scan.bounded.timestamp-millis</h5></td>
      <td>optional</td>
      <td>yes</td>
      <td style="word-wrap: break-word;">(none)</td>
      <td>Long</td>
      <td>End at the specified epoch timestamp (milliseconds) used in case of <code>'timestamp'</code> bounded mode.</td>
    </tr>
    <tr>
      <td><h5>scan.topic-partition-discovery.interval</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>Duration</td>
      <td>Consumer 定期探测动态创建的 Kafka topic 和 partition 的时间间隔。</td>
    </tr>
    <tr>
      <td><h5>sink.partitioner</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">'default'</td>
      <td>String</td>
      <td>Flink partition 到 Kafka partition 的分区映射关系，可选值有：
      <ul>
        <li><code>default</code>：使用 Kafka 默认的分区器对消息进行分区。</li>
        <li><code>fixed</code>：每个 Flink partition 最终对应最多一个 Kafka partition。</li>
        <li><code>round-robin</code>：Flink partition 按轮循（round-robin）的模式对应到 Kafka partition。只有当未指定消息的消息键时生效。</li>
        <li>自定义 <code>FlinkKafkaPartitioner</code> 的子类：例如 <code>'org.mycompany.MyPartitioner'</code>。</li>
      </ul>
      请参阅下方 <a href="#sink-分区">Sink 分区</a> 以获取更多细节。
      </td>
    </tr>
    <tr>
      <td><h5>sink.semantic</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">at-least-once</td>
      <td>String</td>
      <td>定义 Kafka sink 的语义。有效值为 <code>'at-least-once'</code>，<code>'exactly-once'</code> 和 <code>'none'</code>。请参阅 <a href='#一致性保证'>一致性保证</a> 以获取更多细节。</td>
    </tr>
    <tr>
      <td><h5>sink.parallelism</h5></td>
      <td>可选</td>
      <td style="word-wrap: break-word;">（无）</td>
      <td>Integer</td>
      <td>定义 Kafka sink 算子的并行度。默认情况下，并行度由框架定义为与上游串联的算子相同。</td>
    </tr>
    </tbody>
</table>

特性
----------------

### 消息键（Key）与消息体（Value）的格式

Kafka 消息的消息键和消息体部分都可以使用某种 [格式]({{< ref "docs/connectors/table/formats/overview" >}}) 来序列化或反序列化成二进制数据。

**消息体格式**

由于 Kafka 消息中消息键是可选的，以下语句将使用消息体格式读取和写入消息，但不使用消息键格式。
`'format'` 选项与 `'value.format'` 意义相同。
所有的格式配置使用格式识别符作为前缀。

```sql
CREATE TABLE KafkaTable (
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  ...

  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
)
```

消息体格式将配置为以下的数据类型：

```text
ROW<`user_id` BIGINT, `item_id` BIGINT, `behavior` STRING>
```

**消息键和消息体格式**

以下示例展示了如何配置和使用消息键和消息体格式。
格式配置使用 `'key'` 或 `'value'` 加上格式识别符作为前缀。

```sql
CREATE TABLE KafkaTable (
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp',
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  ...

  'key.format' = 'json',
  'key.json.ignore-parse-errors' = 'true',
  'key.fields' = 'user_id;item_id',

  'value.format' = 'json',
  'value.json.fail-on-missing-field' = 'false',
  'value.fields-include' = 'ALL'
)
```

消息键格式包含了在 `'key.fields'` 中列出的字段（使用 `';'` 分隔）和字段顺序。
因此将配置为以下的数据类型：

```text
ROW<`user_id` BIGINT, `item_id` BIGINT>
```

由于消息体格式配置为 `'value.fields-include' = 'ALL'`，所以消息键字段也会出现在消息体格式的数据类型中：

```text
ROW<`user_id` BIGINT, `item_id` BIGINT, `behavior` STRING>
```

**重名的格式字段**

如果消息键字段和消息体字段重名，连接器无法根据表结构信息将这些列区分开。
`'key.fields-prefix'` 配置项可以在表结构中为消息键字段指定一个唯一名称，并在配置消息键格式的时候保留原名。

以下示例展示了在消息键和消息体中同时包含 `version` 字段的情况：

```sql
CREATE TABLE KafkaTable (
  `k_version` INT,
  `k_user_id` BIGINT,
  `k_item_id` BIGINT,
  `version` INT,
  `behavior` STRING
) WITH (
  'connector' = 'kafka',
  ...

  'key.format' = 'json',
  'key.fields-prefix' = 'k_',
  'key.fields' = 'k_version;k_user_id;k_item_id',

  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY'
)
```

消息体格式必须配置为 `'EXCEPT_KEY'` 模式。格式将被配置为以下的数据类型：

```text
消息键格式：
ROW<`version` INT, `user_id` BIGINT, `item_id` BIGINT>

消息体格式：
ROW<`version` INT, `behavior` STRING>
```

### Topic 和 Partition 的探测

`topic` 和 `topic-pattern` 配置项决定了 source 消费的 topic 或 topic 的匹配规则。`topic` 配置项可接受使用分号间隔的 topic 列表，例如 `topic-1;topic-2`。
`topic-pattern` 配置项使用正则表达式来探测匹配的 topic。例如 `topic-pattern` 设置为 `test-topic-[0-9]`，则在作业启动时，所有匹配该正则表达式的 topic（以 `test-topic-` 开头，以一位数字结尾）都将被 consumer 订阅。

为允许 consumer 在作业启动之后探测到动态创建的 topic，请将 `scan.topic-partition-discovery.interval` 配置为一个非负值。这将使 consumer 能够探测匹配名称规则的 topic 中新的 partition。

请参阅 [Kafka DataStream 连接器文档]({{< ref "docs/connectors/datastream/kafka" >}}#kafka-consumer-topic-和分区发现) 以获取更多关于 topic 和 partition 探测的信息。

注意 topic 列表和 topic 匹配规则只适用于 source。对于 sink 端，Flink 目前只支持单一 topic。

### 起始消费位点

`scan.startup.mode` 配置项决定了 Kafka consumer 的启动模式。有效值为：

* `group-offsets`：从 Zookeeper/Kafka 中某个指定的消费组已提交的偏移量开始。
* `earliest-offset`：从可能的最早偏移量开始。
* `latest-offset`：从最末尾偏移量开始。
* `timestamp`：从用户为每个 partition 指定的时间戳开始。
* `specific-offsets`：从用户为每个 partition 指定的偏移量开始。

默认值 `group-offsets` 表示从 Zookeeper/Kafka 中最近一次已提交的偏移量开始消费。

如果使用了 `timestamp`，必须使用另外一个配置项 `scan.startup.timestamp-millis` 来指定一个从格林尼治标准时间 1970 年 1 月 1 日 00:00:00.000 开始计算的毫秒单位时间戳作为起始时间。

如果使用了 `specific-offsets`，必须使用另外一个配置项 `scan.startup.specific-offsets` 来为每个 partition 指定起始偏移量，
例如，选项值 `partition:0,offset:42;partition:1,offset:300` 表示 partition `0` 从偏移量 `42` 开始，partition `1` 从偏移量 `300` 开始。

### Bounded Ending Position

The config option `scan.bounded.mode` specifies the bounded mode for Kafka consumer. The valid enumerations are:
<ul>
<li><span markdown="span">`group-offsets`</span>: bounded by committed offsets in ZooKeeper / Kafka brokers of a specific consumer group. This is evaluated at the start of consumption from a given partition.</li>
<li><span markdown="span">`latest-offset`</span>: bounded by latest offsets. This is evaluated at the start of consumption from a given partition.</li>
<li><span markdown="span">`timestamp`</span>: bounded by a user-supplied timestamp.</li>
<li><span markdown="span">`specific-offsets`</span>: bounded by user-supplied specific offsets for each partition.</li>
</ul>

If config option value `scan.bounded.mode` is not set the default is an unbounded table.

If `timestamp` is specified, another config option `scan.bounded.timestamp-millis` is required to specify a specific bounded timestamp in milliseconds since January 1, 1970 00:00:00.000 GMT.

If `specific-offsets` is specified, another config option `scan.bounded.specific-offsets` is required to specify specific bounded offsets for each partition,
e.g. an option value `partition:0,offset:42;partition:1,offset:300` indicates offset `42` for partition `0` and offset `300` for partition `1`. If an offset for a partition is not provided it will not consume from that partition.

### CDC 变更日志（Changelog） Source

Flink 原生支持使用 Kafka 作为 CDC 变更日志（changelog） source。如果 Kafka topic 中的消息是通过变更数据捕获（CDC）工具从其他数据库捕获的变更事件，则你可以使用 CDC 格式将消息解析为 Flink SQL 系统中的插入（INSERT）、更新（UPDATE）、删除（DELETE）消息。

在许多情况下，变更日志（changelog） source 都是非常有用的功能，例如将数据库中的增量数据同步到其他系统，审核日志，数据库的物化视图，时态表关联数据库表的更改历史等。

Flink 提供了几种 CDC 格式： 

* [debezium]({{< ref "docs/connectors/table/formats/debezium.md" >}}) 
* [canal]({{< ref "docs/connectors/table/formats/canal.md" >}})
* [maxwell]({{< ref "docs/connectors/table/formats/maxwell.md" >}}) 

### Sink 分区

配置项 `sink.partitioner` 指定了从 Flink 分区到 Kafka 分区的映射关系。
默认情况下，Flink 使用 [Kafka 默认分区器](https://github.com/apache/kafka/blob/trunk/clients/src/main/java/org/apache/kafka/clients/producer/internals/DefaultPartitioner.java) 来对消息分区。默认分区器对没有消息键的消息使用 [粘性分区策略（sticky partition strategy）](https://www.confluent.io/blog/apache-kafka-producer-improvements-sticky-partitioner/) 进行分区，对含有消息键的消息使用 murmur2 哈希算法计算分区。

为了控制数据行到分区的路由，也可以提供一个自定义的 sink 分区器。'fixed' 分区器会将同一个 Flink 分区中的消息写入同一个 Kafka 分区，从而减少网络连接的开销。

### 一致性保证

默认情况下，如果查询在 [启用 checkpoint]({{< ref "docs/dev/datastream/fault-tolerance/checkpointing" >}}#enabling-and-configuring-checkpointing) 模式下执行时，Kafka sink 按照至少一次（at-lease-once）语义保证将数据写入到 Kafka topic 中。

当 Flink checkpoint 启用时，`kafka` 连接器可以提供精确一次（exactly-once）的语义保证。

除了启用 Flink checkpoint，还可以通过传入对应的 `sink.semantic` 选项来选择三种不同的运行模式：

 * `none`：Flink 不保证任何语义。已经写出的记录可能会丢失或重复。
 * `at-least-once` (默认设置)：保证没有记录会丢失（但可能会重复）。
 * `exactly-once`：使用 Kafka 事务提供精确一次（exactly-once）语义。当使用事务向 Kafka 写入数据时，请将所有从 Kafka 中消费记录的应用中的 `isolation.level` 配置项设置成实际所需的值（`read_committed` 或 `read_uncommitted`，后者为默认值）。

请参阅 [Kafka 文档]({{< ref "docs/connectors/datastream/kafka" >}}#kafka-producers-和容错) 以获取更多关于语义保证的信息。

### Source 按分区 Watermark

Flink 对于 Kafka 支持发送按分区的 watermark。Watermark 在 Kafka consumer 中生成。
按分区 watermark 的合并方式和在流 shuffle 时合并 Watermark 的方式一致。
Source 输出的 watermark 由读取的分区中最小的 watermark 决定。
如果 topic 中的某些分区闲置，watermark 生成器将不会向前推进。
你可以在表配置中设置 [`'table.exec.source.idle-timeout'`]({{< ref "docs/dev/table/config" >}}#table-exec-source-idle-timeout) 选项来避免上述问题。

请参阅 [Kafka watermark 策略]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#watermark-策略和-kafka-连接器) 以获取更多细节。

### 安全
要启用加密和认证相关的安全配置，只需将安全配置加上 "properties." 前缀配置在 Kafka 表上即可。下面的代码片段展示了如何配置 Kafka 表以使用
PLAIN 作为 SASL 机制并提供 JAAS 配置：
```sql
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  ...
  'properties.security.protocol' = 'SASL_PLAINTEXT',
  'properties.sasl.mechanism' = 'PLAIN',
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";'
)
```
另一个更复杂的例子，使用 SASL_SSL 作为安全协议并使用 SCRAM-SHA-256 作为 SASL 机制：
```sql
CREATE TABLE KafkaTable (
  `user_id` BIGINT,
  `item_id` BIGINT,
  `behavior` STRING,
  `ts` TIMESTAMP_LTZ(3) METADATA FROM 'timestamp'
) WITH (
  'connector' = 'kafka',
  ...
  'properties.security.protocol' = 'SASL_SSL',
  /* SSL 配置 */
  /* 配置服务端提供的 truststore (CA 证书) 的路径 */
  'properties.ssl.truststore.location' = '/path/to/kafka.client.truststore.jks',
  'properties.ssl.truststore.password' = 'test1234',
  /* 如果要求客户端认证，则需要配置 keystore (私钥) 的路径 */
  'properties.ssl.keystore.location' = '/path/to/kafka.client.keystore.jks',
  'properties.ssl.keystore.password' = 'test1234',
  /* SASL 配置 */
  /* 将 SASL 机制配置为 as SCRAM-SHA-256 */
  'properties.sasl.mechanism' = 'SCRAM-SHA-256',
  /* 配置 JAAS */
  'properties.sasl.jaas.config' = 'org.apache.kafka.common.security.scram.ScramLoginModule required username=\"username\" password=\"password\";'
)
```

如果在作业 JAR 中 Kafka 客户端依赖的类路径被重置了（relocate class），登录模块（login module）的类路径可能会不同，因此请根据登录模块在
JAR 中实际的类路径来改写以上配置。例如在 SQL client JAR 中，Kafka client 依赖被重置在了 `org.apache.flink.kafka.shaded.org.apache.kafka` 路径下，
因此 plain 登录模块的类路径应写为 `org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule`。

关于安全配置的详细描述，请参阅 <a href="https://kafka.apache.org/documentation/#security">Apache Kafka 文档中的"安全"一节</a>。

数据类型映射
----------------

Kafka 将消息键值以二进制进行存储，因此 Kafka 并不存在 schema 或数据类型。Kafka 消息使用格式配置进行序列化和反序列化，例如 csv，json，avro。
因此，数据类型映射取决于使用的格式。请参阅 [格式]({{< ref "docs/connectors/table/formats/overview" >}}) 页面以获取更多细节。

{{< top >}}
