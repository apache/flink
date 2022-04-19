---
title: Pulsar
weight: 9
type: docs
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

# Apache Pulsar 连接器

Flink 当前提供 [Apache Pulsar](https://pulsar.apache.org) Source 和 Sink 连接器，用户可以使用它从 Pulsar 读取数据，并保证每条数据只被处理一次。

## 添加依赖

Pulsar Source 当前支持 Pulsar 2.8.1 之后的版本，但是 Pulsar Source 使用到了 Pulsar 的[事务机制](https://pulsar.apache.org/docs/zh-CN/txn-what/)，建议在 Pulsar 2.9.2 及其之后的版本上使用 Pulsar Source 进行数据读取。

如果想要了解更多关于 Pulsar API 兼容性设计，可以阅读文档 [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification)。

{{< artifact flink-connector-pulsar >}}

Flink 的流连接器并不会放到发行文件里面一同发布，阅读[此文档]({{< ref "docs/dev/configuration/overview" >}})，了解如何将连接器添加到集群实例内。

## Pulsar Source

{{< hint info >}}
Pulsar Source 基于 Flink 最新的[批流一体 API]({{< ref "docs/dev/datastream/sources.md" >}}) 进行开发。
{{< /hint >}}

### 使用示例

Pulsar Source 提供了 builder 类来构造 `PulsarSource` 实例。下面的代码实例使用 builder 类创建的实例会从 “persistent://public/default/my-topic” 的数据开始端进行消费。对应的 Pulsar Source 使用了 **Exclusive**（独占）的订阅方式消费消息，订阅名称为 `my-subscription`，并把消息体的二进制字节流以 UTF-8 的方式编码为字符串。

```java
PulsarSource<String> pulsarSource = PulsarSource.builder()
    .setServiceUrl(serviceUrl)
    .setAdminUrl(adminUrl)
    .setStartCursor(StartCursor.earliest())
    .setTopics("my-topic")
    .setDeserializationSchema(PulsarDeserializationSchema.flinkSchema(new SimpleStringSchema()))
    .setSubscriptionName("my-subscription")
    .setSubscriptionType(SubscriptionType.Exclusive)
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar Source");
```

如果使用构造类构造 `PulsarSource`，一定要提供下面几个属性：

- Pulsar 数据消费的地址，使用 `setServiceUrl(String)` 方法提供。
- Pulsar HTTP 管理地址，使用 `setAdminUrl(String)` 方法提供。
- Pulsar 订阅名称，使用 `setSubscriptionName(String)` 方法提供。
- 需要消费的 Topic 或者是 Topic 下面的分区，详见[指定消费的 Topic 或者 Topic 分区](#指定消费的-topic-或者-topic-分区)。
- 解码 Pulsar 消息的反序列化器，详见[反序列化器](#反序列化器)。

### 指定消费的 Topic 或者 Topic 分区

Pulsar Source 提供了两种订阅 Topic 或 Topic 分区的方式。

- Topic 列表，从这个 Topic 的所有分区上消费消息，例如：
  ```java
  PulsarSource.builder().setTopics("some-topic1", "some-topic2");

  // 从 topic "topic-a" 的 0 和 1 分区上消费
  PulsarSource.builder().setTopics("topic-a-partition-0", "topic-a-partition-2");
  ```

- Topic 正则，Pulsar Source 使用给定的正则表达式匹配出所有合规的 Topic，例如：
  ```java
  PulsarSource.builder().setTopicPattern("topic-*");
  ```

#### Topic 名称简写

从 Pulsar 2.0 之后，完整的 Topic 名称格式为 `{persistent|non-persistent}://租户/命名空间/topic`。但是 Pulsar Source 不需要提供 Topic 名称的完整定义，因为 Topic 类型、租户、命名空间都设置了默认值。

| Topic 属性 | 默认值          |
|:---------|:-------------|
| Topic 类型 | `persistent` |
| 租户       | `public`     |
| 命名空间     | `default`    |

下面的表格提供了当前 Pulsar Topic 支持的简写方式：

| Topic 名称简写                        | 翻译后的 Topic 名称                                  |
|:----------------------------------|:-----------------------------------------------|
| `my-topic`                        | `persistent://public/default/my-topic`         |
| `my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic` |

{{< hint warning >}}
对于 Non-persistent（非持久化）Topic，Pulsar Source 不支持简写名称。所以无法将 `non-persistent://public/default/my-topic` 简写成 `non-persistent://my-topic`。
{{< /hint >}}

#### Pulsar Topic 层次结构

对于 Pulsar 而言，Topic 分区也是一种 Topic。Pulsar 会将一个有分区的 Topic 在内部按照分区的大小拆分成等量的无分区 Topic。

由于 Pulsar 内部的分区实际实现为一个 Topic，我们将用“分区”来指代“仅有一个分区的 Topic（Non-partitioned Topic）”和“具有多个分区的 Topic 下属的分区”。

例如，在 Pulsar 的 `sample` 租户下面的 `flink` 命名空间里面创建了一个有 3 个分区的 Topic，给它起名为 `simple-string`。可以在 Pulsar 上看到如下的 Topic 列表：

| Topic 名称                                              | 是否分区 |
|:------------------------------------------------------|:-----|
| `persistent://sample/flink/simple-string`             | 是    |
| `persistent://sample/flink/simple-string-partition-0` | 否    |
| `persistent://sample/flink/simple-string-partition-1` | 否    |
| `persistent://sample/flink/simple-string-partition-2` | 否    |

这意味着，用户可以用上面的子 Topic 去直接消费分区里面的数据，不需要再去基于上层的父 Topic 去消费全部分区的数据。例如：使用 `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")` 将会只消费 Topic `sample/flink/simple-string` 分区 1 和 2 里面的消息。

#### 配置 Topic 正则表达式

前面提到了 Pulsar Topic 有 `persistent`、`non-persistent` 两种类型，使用正则表达式消费数据的时候，Pulsar Source 会尝试从正则表达式里面解析出消息的类型。例如：`PulsarSource.builder().setTopicPattern("non-persistent://my-topic*")` 会解析出 `non-persistent` 这个 Topic 类型。如果用户使用 Topic 名称简写的方式，Pulsar Source 会使用默认的消息类型 `persistent`。

如果想用正则去消费 `persistent` 和 `non-persistent` 类型的 Topic，需要使用 `RegexSubscriptionMode` 定义 Topic 类型，例如：`setTopicPattern("topic-*", RegexSubscriptionMode.AllTopics)`。

### 反序列化器

反序列化器用于解析 Pulsar 消息，Pulsar Source 使用 `PulsarDeserializationSchema` 来定义反序列化器。用户可以在 builder 类中使用 `setDeserializationSchema(PulsarDeserializationSchema)` 方法配置反序列化器。

如果用户只关心消息体的二进制字节流，并不需要其他属性来解析数据。可以直接使用预定义的 `PulsarDeserializationSchema`。Pulsar Source里面提供了 3 种预定义的反序列化器。

- 使用 Pulsar 的 [Schema](https://pulsar.apache.org/docs/zh-CN/schema-understand/) 解析消息。
  ```java
  // 基础数据类型
  PulsarDeserializationSchema.pulsarSchema(Schema);

  // 结构类型 (JSON, Protobuf, Avro, etc.)
  PulsarDeserializationSchema.pulsarSchema(Schema, Class);

  // 键值对类型
  PulsarDeserializationSchema.pulsarSchema(Schema, Class, Class);
  ```
- 使用 Flink 的 `DeserializationSchema` 解析消息。
  ```java
  PulsarDeserializationSchema.flinkSchema(DeserializationSchema);
  ```
- 使用 Flink 的 `TypeInformation` 解析消息。
  ```java
  PulsarDeserializationSchema.flinkTypeInfo(TypeInformation, ExecutionConfig);
  ```

Pulsar 的 `Message<byte[]>` 包含了很多 [额外的属性](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#%E6%B6%88%E6%81%AF)。例如，消息的 key、消息发送时间、消息生产时间、用户在消息上自定义的键值对属性等。可以使用 `Message<byte[]>` 接口来获取这些属性。

如果用户需要基于这些额外的属性来解析一条消息，可以实现 `PulsarDeserializationSchema` 接口。并一定要确保 `PulsarDeserializationSchema.getProducedType()` 方法返回的 `TypeInformation` 是正确的结果。Flink 使用 `TypeInformation` 将解析出来的结果序列化传递到下游算子。

### Pulsar 订阅

订阅是命名好的配置规则，指导消息如何投递给消费者。Pulsar Source 需要提供一个独立的订阅名称,支持 Pulsar 的四种订阅模式：

- [exclusive（独占）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#exclusive)
- [shared（共享）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#shared%E5%85%B1%E4%BA%AB)
- [failover（灾备）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#failover%E7%81%BE%E5%A4%87)
- [key_shared（key 共享）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#key_shared)

当前 Pulsar Source 里，`独占` 和 `灾备` 的实现没有区别，如果 Flink 的一个 reader 挂了，Pulsar Source 会把所有未消费的数据交给其他的 reader 来消费数据。

默认情况下，如果没有指定订阅类型，Pulsar Source 使用共享订阅类型（`SubscriptionType.Shared`）。

```java
// 名为 "my-shared" 的共享订阅
PulsarSource.builder().setSubscriptionName("my-shared");

// 名为 "my-exclusive" 的独占订阅
PulsarSource.builder().setSubscriptionName("my-exclusive").setSubscriptionType(SubscriptionType.Exclusive);
```

如果想在 Pulsar Source 里面使用 `key 共享` 订阅，需要提供 `RangeGenerator` 实例。`RangeGenerator` 会生成一组消息 key 的 hash 范围，Pulsar Source 会基于给定的范围来消费数据。

Pulsar Source 也提供了一个名为 `UniformRangeGenerator` 的默认实现，它会基于 flink 数据源的并行度将 hash 范围均分。

### 起始消费位置

Pulsar Source 使用 `setStartCursor(StartCursor)` 方法给定开始消费的位置。内置的开始消费位置有：

- 从 Topic 里面最早的一条消息开始消费。
  ```java
  StartCursor.earliest();
  ```
- 从 Topic 里面最新的一条消息开始消费。
  ```java
  StartCursor.latest();
  ```
- 从给定的消息开始消费。
  ```java
  StartCursor.fromMessageId(MessageId);
  ```
- 与前者不同的是，给定的消息可以跳过，再进行消费。
  ```java
  StartCursor.fromMessageId(MessageId, boolean);
  ```
- 从给定的消息时间开始消费。
  ```java
  StartCursor.fromMessageTime(long);
  ```

{{< hint info >}}
每条消息都有一个固定的序列号，这个序列号在 Pulsar 上有序排列，其包含了 ledger、entry、partition 等原始信息，用于在 Pulsar 底层存储上查找到具体的消息。

Pulsar 称这个序列号为 `MessageId`，用户可以使用 `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)` 创建它。
{{< /hint >}}

### 边界

Pulsar Source 默认情况下使用流的方式消费数据。除非任务失败或者被取消，否则将持续消费数据。用户可以使用 `setBoundedStopCursor(StopCursor)` 给定停止消费的位置，这种情况下会使用批的方式进行消费。使用流的方式一样可以给定停止位置，使用 `setUnboundedStopCursor(StopCursor)` 方法即可。

在批模式下，使用 `setBoundedStopCursor(StopCursor)` 来指定一个消费停止位置。

内置的停止消费位置如下：

- 永不停止。
  ```java
  StopCursor.never();
  ```
- 停止于 Pulsar 启动时 Topic 里面最新的那条数据。
  ```java
  StopCursor.latest();
  ```
- 停止于某条消息，结果里不包含此消息。
  ```java
  StopCursor.atMessageId(MessageId);
  ```
- 停止于某条消息之后，结果里包含此消息。
  ```java
  StopCursor.afterMessageId(MessageId);
  ```
- 停止于某个给定的消息发布时间戳，比如 `Message<byte[]>.getPublishTime()`。
  ```java
  StopCursor.atPublishTime(long);
  ```

{{< hint warning >}}
StopCursor.atEventTime(long) 目前已经处于弃用状态。
{{< /hint >}}

### Source 配置项

除了前面提到的配置选项，Pulsar Source 还提供了丰富的选项供 Pulsar 专家使用，在 builder 类里通过 `setConfig(ConfigOption<T>, T)` 和 `setConfig(Configuration)` 方法给定下述的全部配置。

#### Pulsar Java 客户端配置项

Pulsar Source 使用 [Java 客户端](https://pulsar.apache.org/docs/zh-CN/client-libraries-java/)来创建消费实例，相关的配置定义于 Pulsar 的 `ClientConfigurationData` 内。在 `PulsarOptions` 选项中，定义大部分的可供用户定义的配置。

{{< generated/pulsar_client_configuration >}}

#### Pulsar 管理 API 配置项

[管理 API](https://pulsar.apache.org/docs/zh-CN/admin-api-overview/) 用于查询 Topic 的元数据和用正则订阅的时候的 Topic 查找，它与 Java 客户端共享大部分配置。下面列举的配置只供管理 API 使用，`PulsarOptions` 包含了这些配置 。

{{< generated/pulsar_admin_configuration >}}

#### Pulsar 消费者 API 配置项

Pulsar 提供了消费者 API 和读者 API 两套 API 来进行数据消费，它们可用于不同的业务场景。Flink 上的 Pulsar Source 使用消费者 API 进行消费，它的配置定义于 Pulsar 的 `ConsumerConfigurationData` 内。Pulsar Source 将其中大部分的可供用户定义的配置定义于 `PulsarSourceOptions` 内。

{{< generated/pulsar_consumer_configuration >}}

#### Pulsar Source配置项

下述配置主要用于性能调优或者是控制消息确认的行为。如非必要，可以不用强制配置。

{{< generated/pulsar_source_configuration >}}

### 动态分区发现

为了能在启动 Flink 任务之后还能发现在 Pulsar 上扩容的分区或者是新创建的 Topic，Pulsar Source 提供了动态分区发现机制。该机制不需要重启 Flink 任务。对选项 `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS` 设置一个正整数即可启用。

```java
// 10 秒查询一次分区信息
PulsarSource.builder()
        .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```

{{< hint warning >}}
默认情况下，Pulsar 启用动态分区发现，查询间隔为 30 秒。用户可以给定一个负数，将该功能禁用。如果使用批的方式消费数据，将无法启用该功能。
{{< /hint >}}

### 事件时间和水位线

默认情况下，Pulsar Source 使用 Pulsar 的 `Message<byte[]>` 里面的时间作为解析结果的时间戳。用户可以使用 `WatermarkStrategy` 来自行解析出想要的消息时间，并向下游传递对应的水位线。

```java
env.fromSource(pulsarSource, new CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy");
```

[这篇文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}})详细讲解了如何定义 `WatermarkStrategy`。

### 消息确认

一旦在 Topic 上创建了订阅，消息便会[存储](https://pulsar.apache.org/docs/zh-CN/concepts-architecture-overview/#%E6%8C%81%E4%B9%85%E5%8C%96%E5%AD%98%E5%82%A8)在 Pulsar 里。即使没有消费者，消息也不会被丢弃。只有当 Pulsar Source 同 Pulsar 确认此条消息已经被消费，该消息才以某种机制会被移除。Pulsar Source 支持四种订阅方式，它们的消息确认方式也大不相同。

#### 独占和灾备订阅下的消息确认

`独占` 和 `灾备` 订阅下，Pulsar Source 使用累进式确认方式。确认某条消息已经被处理时，其前面消息会自动被置为已读。Pulsar Source 会在 Flink 完成检查点时将对应时刻消费的消息置为已读，以此来保证 Pulsar 状态与 Flink 状态一致。

如果用户没有在 Flink 上启用检查点，Pulsar Source 可以使用周期性提交来将消费状态提交给 Pulsar，使用配置 `PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL` 来进行定义。

需要注意的是，此种场景下，Pulsar Source 并不依赖于提交到 Pulsar 的状态来做容错。消息确认只是为了能在 Pulsar 端看到对应的消费处理情况。

#### 共享和 key 共享订阅下的消息确认

`共享` 和 `key 共享` 需要依次确认每一条消息，所以 Pulsar Source 在 Pulsar 事务里面进行消息确认，然后将事务提交到 Pulsar。

首先需要在 Pulsar 的 `borker.conf` 文件里面启用事务：

```text
transactionCoordinatorEnabled=true
```

Pulsar Source 创建的事务的默认超时时间为 3 小时，请确保这个时间大于 Flink 检查点的间隔。用户可以使用 `PulsarSourceOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS` 来设置事务的超时时间。

如果用户无法启用 Pulsar 的事务，或者是因为项目禁用了检查点，需要将 `PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE` 选项设置为 `true`，消息从 Pulsar 消费后会被立刻置为已读。Pulsar Source 无法保证此种场景下的消息一致性。

Pulsar Source 在 Pulsar 上使用日志的形式记录某个事务下的消息确认，为了更好的性能，请缩短 Flink 做检查点的间隔。

## Pulsar Sink

Pulsar Sink 连接器可以将经过 Flink 处理后的数据写入一个或多个 Pulsar Topic 或者 Topic 下的某些分区。

{{< hint info >}}
Pulsar Sink 基于 Flink 最新的 [Sink API](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) 实现。

如果想要使用旧版的使用 `SinkFuntion` 接口实现的 Sink 连接器，可以使用 StreamNative 维护的 [pulsar-flink](https://github.com/streamnative/pulsar-flink)。
{{< /hint >}}

### 使用示例

Pulsar Sink 使用 builder 类来创建 `PulsarSink` 实例。

下面示例展示了如何通过 Pulsar Sink 以“至少一次”的语义将字符串类型的数据发送给 topic1。

```java
DataStream<String> stream = ...

PulsarSink<String> sink = PulsarSink.builder()
    .setServiceUrl(serviceUrl)
    .setAdminUrl(adminUrl)
    .setTopics("topic1")
    .setSerializationSchema(PulsarSerializationSchema.flinkSchema(new SimpleStringSchema()))
    .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
    .build();

stream.sinkTo(sink);
```

下列为创建一个 `PulsarSink` 实例必需的属性：

- Pulsar 数据消费的地址，使用 `setServiceUrl(String)` 方法提供。
- Pulsar HTTP 管理地址，使用 `setAdminUrl(String)` 方法提供。
- 需要发送到的 Topic 或者是 Topic 下面的分区，详见[指定写入的topic或者topic分区](#指定写入的topic或者topic分区)。
- 编码 Pulsar 消息的序列化器，详见[序列化器](#序列化器)。

在创建 `PulsarSink` 时，建议使用 `setProducerName(String)` 来指定 `PulsarSink` 内部使用的 Pulsar 生产者名称。这样方便在数据监控页面找到对应的生产者监控指标。

### 指定写入的 Topic 或者 Topic 分区

`PulsarSink` 指定写入 Topic 的方式和 Pulsar Source [指定消费的 Topic 或者 Topic 分区](#指定消费的-topic-或者-topic-分区)的方式类似。`PulsarSink` 支持以 mixin 风格指定写入的 Topic 或分区。因此，可以指定一组 Topic 或者分区或者是两者都有。

```java
// Topic "some-topic1" 和 "some-topic2"
PulsarSink.builder().setTopics("some-topic1", "some-topic2")

// Topic "topic-a" 的分区 0 和 2 
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")

// Topic "topic-a" 以及 Topic "some-topic2" 分区 0 和 2 
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2", "some-topic2")
```

动态分区发现默认处于开启状态，这意味着 `PulsarSink` 将会周期性地从 Pulsar 集群中查询 Topic 的元数据来获取可能有的分区数量变更信息。使用 `PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` 配置项来指定查询的间隔时间。

可以选择实现 `TopicRouter` 接口来自定义[消息路由策略](#消息路由策略)。此外，阅读 [Topic 名称简写](#topic-名称简写)将有助于理解 Pulsar 的分区在 Pulsar 连接器中的配置方式。

{{< hint warning >}}
如果在 `PulsarSink` 中同时指定了某个 Topic 和其下属的分区，那么 `PulsarSink` 将会自动将两者合并，仅使用外层的 Topic。

举个例子，如果通过 `PulsarSink.builder().setTopics("some-topic1", "some-topic1-partition-0")` 来指定写入的 Topic，那么其结果等价于 `PulsarSink.builder().setTopics("some-topic1")`。
{{< /hint >}}

### 序列化器

序列化器（`PulsarSerializationSchema`）负责将 Flink 中的每条记录序列化成 byte 数组，并通过网络发送至指定的写入 Topic。和 Pulsar Source 类似的是，序列化器同时支持使用基于 Flink 的 `SerializationSchema` 接口实现序列化器和使用 Pulsar 原生的 `Schema` 类型实现的序列化器。不过序列化器并不支持 Pulsar 的 `Schema.AUTO_PRODUCE_BYTES()`。

如果不需要指定 [Message](https://pulsar.apache.org/api/client/2.9.0-SNAPSHOT/org/apache/pulsar/client/api/Message.html) 接口中提供的 key 或者其他的消息属性，可以从上述 2 种预定义的 `PulsarSerializationSchema` 实现中选择适合需求的一种使用。

- 使用 Pulsar 的 [Schema](https://pulsar.apache.org/docs/zh-CN/schema-understand/) 来序列化 Flink 中的数据。
  ```java
  // 原始数据类型
  PulsarSerializationSchema.pulsarSchema(Schema)

  // 有结构数据类型（JSON、Protobuf、Avro 等）
  PulsarSerializationSchema.pulsarSchema(Schema, Class)

  // 键值对类型
  PulsarSerializationSchema.pulsarSchema(Schema, Class, Class)
  ```
- 使用 Flink 的 `SerializationSchema` 来序列化数据。
  ```java
  PulsarSerializationSchema.flinkSchema(SerializationSchema)
  ```

同时使用 `PulsarSerializationSchema.pulsarSchema()` 以及在 builder 中指定 `PulsarSinkBuilder.enableSchemaEvolution()` 可以启用 [Schema evolution](https://pulsar.apache.org/docs/zh-CN/schema-evolution-compatibility/#schema-evolution) 特性。该特性会使用 Pulsar Broker 端提供的 Schema 版本兼容性检测以及 Schema 版本演进。下列示例展示了如何启用 Schema Evolution。

```java
Schema<SomePojo> schema = Schema.AVRO(SomePojo.class);
PulsarSerializationSchema<SomePojo> pulsarSchema = PulsarSerializationSchema.pulsarSchema(schema, SomePojo.class);

PulsarSink<String> sink = PulsarSink.builder()
    ...
    .setSerializationSchema(pulsarSchema)
    .enableSchemaEvolution()
    .build();
```

{{< hint warning >}}
如果想要使用 Pulsar 原生的 Schema 序列化消息而不需要 Schema Evolution 特性，那么写入的 Topic 会使用 `Schema.BYTES` 作为消息的 Schema，对应 Topic 的消费者需要自己负责反序列化的工作。

例如，如果使用 `PulsarSerializationSchema.pulsarSchema(Schema.STRING)` 而不使用 `PulsarSinkBuilder.enableSchemaEvolution()`。那么在写入 Topic 中所记录的消息 Schema 将会是 `Schema.BYTES`。
{{< /hint >}}

### 消息路由策略

在 Pulsar Sink 中，消息路由发生在于分区之间，而非上层 Topic。对于给定 Topic 的情况，路由算法会首先会查询出 Topic 之上所有的分区信息，并在这些分区上实现消息的路由。Pulsar Sink 默认提供 2 种路由策略的实现。

- `KeyHashTopicRouter`：使用消息的 key 对应的哈希值来取模计算出消息对应的 Topic 分区。

  使用此路由可以将具有相同 key 的消息发送至同一个 Topic 分区。消息的 key 可以在自定义 `PulsarSerializationSchema` 时，在 `serialize()` 方法内使用 `PulsarMessageBuilder.key(String key)` 来予以指定。

  如果消息没有包含 key，此路由策略将从 Topic 分区中随机选择一个发送。

  可以使用 `MessageKeyHash.JAVA_HASH` 或者 `MessageKeyHash.MURMUR3_32_HASH` 两种不同的哈希算法来计算消息 key 的哈希值。使用 `PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH` 配置项来指定想要的哈希算法。

- `RoundRobinRouter`：轮换使用用户给定的 Topic 分区。
  
  消息将会轮替地选取 Topic 分区，当往某个 Topic 分区里写入指定数量的消息后，将会轮换至下一个 Topic 分区。使用 `PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES` 指定向一个 Topic 分区中写入的消息数量。

还可以通过实现 `TopicRouter` 接口来自定义消息路由策略，请注意 TopicRouter 的实现需要能被序列化。

在 `TopicRouter` 内可以指定任意的 Topic 分区（即使这个 Topic 分区不在 `setTopics()` 指定的列表中）。因此，当使用自定义的 `TopicRouter` 时，`PulsarSinkBuilder.setTopics` 选项是可选的。

```java
@PublicEvolving
public interface TopicRouter<IN> extends Serializable {

    String route(IN in, List<String> partitions, PulsarSinkContext context);

    default void open(SinkConfiguration sinkConfiguration) {
        // 默认无操作
    }
}
```

{{< hint info >}}
如前文所述，Pulsar 分区的内部被实现为一个无分区的 Topic，一般情况下 Pulsar 客户端会隐藏这个实现，并且提供内置的消息路由策略。Pulsar Sink 并没有使用 Pulsar 客户端提供的路由策略和封装，而是使用了 Pulsar 客户端更底层的 API 自行实现了消息路由逻辑。这样做的主要目的是能够在属于不同 Topic 的分区之间定义更灵活的消息路由策略。

详情请参考 Pulsar 的 [partitioned topics](https://pulsar.apache.org/docs/zh-CN/cookbooks-partitioned/)。
{{< /hint >}}

### 发送一致性

`PulsarSink` 支持三种发送一致性。

- `NONE`：Flink 应用运行时可能出现数据丢失的情况。在这种模式下，Pulsar Sink 发送消息后并不会检查消息是否发送成功。此模式具有最高的吞吐量，可用于一致性没有要求的场景。
- `AT_LEAST_ONCE`：每条消息**至少有**一条对应消息发送至 Pulsar，发送至 Pulsar 的消息可能会因为 Flink 应用重启而出现重复。
- `EXACTLY_ONCE`：每条消息**有且仅有**一条对应消息发送至 Pulsar。发送至 Pulsar 的消息不会有重复也不会丢失。Pulsar Sink 内部依赖 [Pulsar 事务](https://pulsar.apache.org/docs/zh-CN/transactions/)和两阶段提交协议来保证每条记录都能正确发往 Pulsar。

### 消息延时发送

[消息延时发送](https://pulsar.apache.org/docs/zh-CN/next/concepts-messaging/#%E6%B6%88%E6%81%AF%E5%BB%B6%E8%BF%9F%E4%BC%A0%E9%80%92)特性可以让指定发送的每一条消息需要延时一段时间后才能被下游的消费者所消费。当延时消息发送特性启用时，Pulsar Sink 会**立刻**将消息发送至 Pulsar Broker。但该消息在指定的延迟时间到达前将会保持对下游消费者不可见。

消息延时发送仅在 `Shared` 订阅模式下有效，在 `Exclusive` 和 `Failover` 模式下该特性无效。

可以使用 `MessageDelayer.fixed(Duration)` 创建一个 `MessageDelayer` 来为所有消息指定恒定的接收时延，或者实现 `MessageDelayer` 接口来为不同的消息指定不同的接收时延。

{{< hint warning >}}
消息对下游消费者的可见时间应当基于 `PulsarSinkContext.processTime() `计算得到。
{{< /hint >}}

### Sink 配置项

可以在 builder 类里通过 `setConfig(ConfigOption<T>, T)` 和 `setConfig(Configuration)` 方法给定下述的全部配置。

#### PulsarClient 和 PulsarAdmin 配置项

Pulsar Sink 和 Pulsar Source 公用的配置选项可参考

- [Pulsar Java 客户端配置项](#pulsar-java-客户端配置项)
- [Pulsar 管理 API 配置项](#pulsar-管理-API-配置项)

#### Pulsar 生产者 API 配置项

Pulsar Sink 使用生产者 API 来发送消息。Pulsar 的 `ProducerConfigurationData` 中大部分的配置项被映射为 `PulsarSinkOptions` 里的选项。

{{< generated/pulsar_producer_configuration >}}

#### Pulsar Sink 配置项

下述配置主要用于性能调优或者是控制消息确认的行为。如非必要，可以不用考虑配置。

{{< generated/pulsar_sink_configuration >}}

### Sink 监控指标

下列表格列出了当前 Sink 支持的监控指标，前 6 个指标是 [FLIP-33: Standardize Connector Metrics]([https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics)) 中规定的 Sink 连接器应当支持的标准指标。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">Scope</th>
      <th class="text-left" style="width: 18%">Metrics</th>
      <th class="text-left" style="width: 18%">User Variables</th>
      <th class="text-left" style="width: 39%">Description</th>
      <th class="text-left" style="width: 10%">Type</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <th rowspan="13">Operator</th>
        <td>numBytesOut</td>
        <td>n/a</td>
        <td>Pulsar Sink 启动后总共发出的字节数</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>numBytesOutPerSecond</td>
        <td>n/a</td>
        <td>每秒发送的字节数</td>
        <td>Meter</td>
    </tr>
    <tr>
        <td>numRecordsOut</td>
        <td>n/a</td>
        <td>Pulsar Sink 启动后总共发出的消息数</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>numRecordsOutPerSecond</td>
        <td>n/a</td>
        <td>每秒发送的消息数</td>
        <td>Meter</td>
    </tr>
    <tr>
        <td>numRecordsOutErrors</td>
        <td>n/a</td>
        <td>总共发送消息失败的次数</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>currentSendTime</td>
        <td>n/a</td>
        <td>最近一条消息从被放入客户端缓冲队列到收到消息确认的时间</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.numAcksReceived</td>
        <td>n/a</td>
        <td>总共收到的确认数</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>PulsarSink.sendLatencyMax</td>
        <td>n/a</td>
        <td>所有生产者的最大发送延迟</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency50Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟的中位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency75Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟的 75 百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency95Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟的 95 百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency99Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟的 99 百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency999Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟的 99.9 百分位数</td>
        <td>Gauge</td>
    </tr>
  </tbody>
</table>

{{< hint info >}}
指标 `numBytesOut`、`numRecordsOut` 和 `numRecordsOutErrors` 从 Pulsar Producer 实例的监控指标中获得。

`currentSendTime` 记录了最近一条消息从放入生产者的缓冲队列到消息被消费确认所耗费的时间。这项指标在 `NONE` 发送一致性下不可用。
{{< /hint >}}

默认情况下，Pulsar 生产者每隔 60 秒才会刷新一次监控数据，然而 Pulsar Sink 每 500 毫秒就会从 Pulsar 生产者中获得最新的监控数据。因此 `numRecordsOut`、`numBytesOut`、`numAcksReceived` 以及 `numRecordsOutErrors` 4 个指标实际上每 60 秒才会刷新一次。

如果想要更高地刷新评率，可以通过 `builder.setConfig(PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS. 1L)` 来将 Pulsar 生产者的监控数据刷新频率调整至相应值（最低为1s）。

`numBytesOutRate` 和 `numRecordsOutRate` 指标是 Flink 内部通过 `numBytesOut` 和 `numRecordsOut` 计数器，在一个 60 秒的窗口内计算得到的。

### 设计思想简述

Pulsar Sink 遵循 [FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) 中定义的 Sink API 设计。

#### 无状态的 SinkWriter

在 `EXACTLY_ONCE` 一致性下，Pulsar Sink 不会将事务相关的信息存放于检查点快照中。这意味着当 Flink 应用重启时，Pulsar Sink 会创建新的事务实例。上一次运行过程中任何未提交事务中的消息会因为超时中止而无法被下游的消费者所消费。这样的设计保证了 SinkWriter 是无状态的。

#### Pulsar Schema Evolution

[Pulsar Schema Evolution](https://pulsar.apache.org/docs/zh-CN/schema-evolution-compatibility/) 允许用户在一个 Flink 应用程序中使用的数据模型发生特定改变后（比如向基于 ARVO 的 POJO 类中增加或删除一个字段），仍能使用同一个 Flink 应用程序的代码。

可以在 Pulsar 集群内指定哪些类型的数据模型的改变是被允许的，详情请参阅 [Pulsar Schema Evolution](https://pulsar.apache.org/docs/zh-CN/schema-evolution-compatibility/)。

## 升级至最新的连接器

常见的升级步骤，请参阅[升级应用程序和 Flink 版本]({{< ref "docs/ops/upgrading" >}})。Pulsar 连接器没有在 Flink 端存储消费的状态，所有的消费信息都推送到了 Pulsar。所以需要注意下面的事项：

* 不要同时升级 Pulsar 连接器和 Pulsar 服务端的版本。
* 使用最新版本的 Pulsar 客户端来消费消息。

## 问题诊断

使用 Flink 和 Pulsar 交互时如果遇到问题，由于 Flink 内部实现只是基于 Pulsar 的 [Java 客户端](https://pulsar.apache.org/docs/zh-CN/client-libraries-java/)和[管理 API](https://pulsar.apache.org/docs/zh-CN/admin-api-overview/) 而开发的。

用户遇到的问题可能与 Flink 无关，请先升级 Pulsar 的版本、Pulsar 客户端的版本，或者修改 Pulsar 的配置、Pulsar 连接器的配置来尝试解决问题。

### 在 Java 11 上使用不稳定

Pulsar connector 在 Java 11 中有一些尚未修复的问题。我们当前推荐在 Java 8 环境中运行Pulsar connector.
{{< top >}}
