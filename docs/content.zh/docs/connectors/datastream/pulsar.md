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

Flink 当前只提供 [Apache Pulsar](https://pulsar.apache.org) 数据源，用户可以使用它从 Pulsar 读取数据，并保证每条数据只被处理一次。

## 添加依赖

Pulsar Source 当前支持 Pulsar 2.9.0 之后的版本，但是 Pulsar Source 使用到了 Pulsar 的[事务机制](https://pulsar.apache.org/docs/zh-CN/txn-what/)，建议在 Pulsar 2.10.0 及其之后的版本上使用 Pulsar Source 进行数据读取。

如果想要了解更多关于 Pulsar API 兼容性设计，可以阅读文档 [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification)。

{{< artifact flink-connector-pulsar withScalaVersion >}}

Flink 的流连接器并不会放到发行文件里面一同发布，阅读[此文档]({{< ref "docs/dev/datastream/project-configuration" >}})，了解如何将连接器添加到集群实例内。

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

  // 从 topic "topic-a" 的 0 和 2 分区上消费
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
- 从给定的消息发布时间开始消费，这个方法因为名称容易导致误解现在已经不建议使用。你可以使用方法 `StartCursor.fromPublishTime(long)`。
  ```java
  StartCursor.fromMessageTime(long);
  ```
- 从给定的消息发布时间开始消费。
  ```java
  StartCursor.fromPublishTime(long);
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
- 停止于某个给定的消息事件时间戳，比如 `Message<byte[]>.getEventTime()`，消费结果里不包含此时间戳的消息。
  ```java
  StopCursor.atEventTime(long);
  ```
- 停止于某个给定的消息事件时间戳，比如 `Message<byte[]>.getEventTime()`，消费结果里包含此时间戳的消息。
  ```java
  StopCursor.afterEventTime(long);
  ```
- 停止于某个给定的消息发布时间戳，比如 `Message<byte[]>.getPublishTime()`，消费结果里不包含此时间戳的消息。
  ```java
  StopCursor.atPublishTime(long);
  ```
- 停止于某个给定的消息发布时间戳，比如 `Message<byte[]>.getPublishTime()`，消费结果里包含此时间戳的消息。
  ```java
  StopCursor.afterPublishTime(long);
  ```

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
