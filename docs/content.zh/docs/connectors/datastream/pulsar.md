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

Pulsar Source当前支持 Pulsar 2.8.1 之后的版本，但是Pulsar Source使用到了 Pulsar 的[事务机制](https://pulsar.apache.org/docs/zh-CN/txn-what/),建议在 Pulsar 2.9.2
及其之后的版本上使用Pulsar Source进行数据读取。

如果想要了解更多关于 Pulsar API 兼容性设计，可以阅读文档 [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification)。

{{< artifact flink-connector-pulsar >}}

Flink 的流连接器并不会放到发行文件里面一同发布，阅读[此文档]({{< ref "docs/dev/datastream/project-configuration" >}})，了解如何将连接器添加到集群实例内。

## Pulsar Source

{{< hint info >}}
Pulsar Source基于 Flink 最新的[批流一体 API]({{< ref "docs/dev/datastream/sources.md" >}}) 进行开发。

如果要想使用基于旧版的 `SourceFunction` 实现的 Pulsar Source，或者是项目的 Flink 版本低于 1.14，可以使用 StreamNative 单独维护的 [pulsar-flink](https://github.com/streamnative/pulsar-flink)。
{{< /hint >}}

### 使用示例

Pulsar Source提供了 builder 类来构造`PulsarSource`实例。下面的代码实例使用 builder 类创建的实例会从 topic "persistent://public/default/my-topic" 的数据开始端进行消费。
Pulsar Source使用了 **Exclusive**（独占）的订阅方式消费消息，订阅名称为 `my-subscription`，并把消息体的二进制字节流以 UTF-8 的方式编码为字符串。

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

- Pulsar 数据消费的地址，使用 `setServiceUrl(String)` 方法提供
- Pulsar HTTP 管理地址，使用 `setAdminUrl(String)` 方法提供
- Pulsar 订阅名称，使用 `setSubscriptionName(String)` 方法提供
- 需要消费的 topic 或者是 topic 下面的分区，详见[指定消费的 Topic 或者 Topic 分区](#指定消费的-topic-或者-topic-分区)
- 解码 Pulsar 消息的反序列化器，详见[反序列化器](#反序列化器)

### 指定消费的 Topic 或者 Topic 分区

Pulsar Source提供了两种订阅 topic 或 topic 分区的方式。

- Topic 列表，从这个 Topic 的所有分区上消费消息，例如：
  ```java
  PulsarSource.builder().setTopics("some-topic1", "some-topic2")

  // 从 topic "topic-a" 的 0 和 1 分区上消费
  PulsarSource.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")
  ```

- Topic 正则，Pulsar Source使用给定的正则表达式匹配出所有合规的 topic，例如：
  ```java
  PulsarSource.builder().setTopicPattern("topic-*")
  ```

#### Topic 名称简写

从 Pulsar 2.0 之后，完整的 topic 名称格式为 `{persistent|non-persistent}://租户/命名空间/topic`。
但是Pulsar Source不需要提供 topic 名称的完整定义，因为 topic 类型、租户、命名空间都设置了默认值。

Topic 属性 | 默认值
:------------|:-------
topic 类型 | `persistent`
租户 | `public`
命名空间 | `default`

下面的表格提供了当前 Pulsar 支持的简写方式：

topic 名称简写 | 翻译后的 topic 名称
:----------------|:---------------------
`my-topic` | `persistent://public/default/my-topic`
`my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic`

{{< hint warning >}}
对于 non-persistent（非持久化） topic，Pulsar Source不支持简写名称。所以无法将 `non-persistent://public/default/my-topic` 简写成 `non-persistent://my-topic`。
{{< /hint >}}

#### Pulsar Topic 层次结构


对于 Pulsar 而言，Topic 分区也是一种 Topic。Pulsar 会将一个有分区的 Topic 在内部按照分区的大小拆分成等量的无分区 Topic。

由于Pulsar内部的分区实际实现为一个topic，我们将用"分区"来指代"仅有一个分区的topic(non-partitioned topic)"和"具有多个分区的topic下属的分区"

例如，在 Pulsar 的 `sample` 租户下面的 `flink` 命名空间里面创建了一个有 3 个分区的 topic，给它起名为 `simple-string`。
可以在 Pulsar 上看到如下的 topic 列表：

Topic 名称 | 是否分区
:--------- | :----------
`persistent://sample/flink/simple-string` | 是
`persistent://sample/flink/simple-string-partition-0` | 否
`persistent://sample/flink/simple-string-partition-1` | 否
`persistent://sample/flink/simple-string-partition-2` | 否

这意味着，用户可以用上面的子 topic 去直接消费分区里面的数据，不需要再去基于上层的父 topic 去消费全部分区的数据。
例如：使用 `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")` 将会只消费 topic `sample/flink/simple-string` 上面的分区 1 和 2 里面的消息。

#### 配置 Topic 正则表达式

前面提到了 Pulsar topic 有 `persistent`、`non-persistent` 两种类型，使用正则表达式消费数据的时候，Pulsar Source会尝试从正则表达式里面解析出消息的类型。
例如：`PulsarSource.builder().setTopicPattern("non-persistent://my-topic*")` 会解析出 `non-persistent` 这个 topic 类型。
如果用户使用 topic 名称简写的方式，Pulsar Source会使用默认的消息类型 `persistent`。

如果想用正则去消费 `persistent` 和 `non-persistent` 类型的 topic，需要使用 `RegexSubscriptionMode` 定义 topic 类型，例如：`setTopicPattern("topic-*", RegexSubscriptionMode.AllTopics)`。

### 反序列化器

反序列化器用于解析 Pulsar 消息，Pulsar Source使用 `PulsarDeserializationSchema` 来定义反序列化器。
用户可以在 builder 类中使用 `setDeserializationSchema(PulsarDeserializationSchema)` 方法配置反序列化器。

如果用户只关心消息体的二进制字节流，并不需要其他属性来解析数据。可以直接使用预定义的 `PulsarDeserializationSchema`。 Pulsar Source里面提供了 3 种预定义好的反序列化器。

- 使用 Pulsar 的 [Schema](https://pulsar.apache.org/docs/zh-CN/schema-understand/) 解析消息。
  ```java
  // 基础数据类型
  PulsarDeserializationSchema.pulsarSchema(Schema)

  // 结构类型 (JSON, Protobuf, Avro, etc.)
  PulsarDeserializationSchema.pulsarSchema(Schema, Class)

  // 键值对类型
  PulsarDeserializationSchema.pulsarSchema(Schema, Class, Class)
  ```
- 使用 Flink 的 `DeserializationSchema` 解析消息。
  ```java
  PulsarDeserializationSchema.flinkSchema(DeserializationSchema)
  ```
- 使用 Flink 的 `TypeInformation` 解析消息。
  ```java
  PulsarDeserializationSchema.flinkTypeInfo(TypeInformation, ExecutionConfig)
  ```

Pulsar 的 `Message<byte[]>` 包含了很多 [额外的属性](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#%E6%B6%88%E6%81%AF)。
例如,消息的 key，消息发送时间，消息生产时间，用户在消息上自定义的键值对属性等。可以使用 `Message<byte[]>` 接口来获取这些属性。

如果用户需要基于这些额外的属性来解析一条消息，可以实现 `PulsarDeserializationSchema` 接口。
并一定要确保 `PulsarDeserializationSchema.getProducedType()` 方法返回的 `TypeInformation` 是正确的结果。
Flink 使用 `TypeInformation` 将解析出来的结果序列化传递到下游算子。

### Pulsar 订阅

订阅是命名好的配置规则，指导消息如何投递给消费者。Pulsar Source需要提供一个独立的订阅名称,支持 Pulsar 的四种订阅模式：

- [exclusive（独占）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#exclusive)
- [shared（共享）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#shared%E5%85%B1%E4%BA%AB)
- [failover（灾备）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#failover%E7%81%BE%E5%A4%87)
- [key_shared（key 共享）](https://pulsar.apache.org/docs/zh-CN/concepts-messaging/#key_shared)

当前 Pulsar Source里，`独占` 和 `灾备` 的实现没有区别，如果 Flink 的一个 reader 挂了，Pulsar Source会把所有未消费的数据交给其他的 reader 来消费数据。

默认情况下，如果没有指定订阅类型，Pulsar Source使用共享订阅类型（`SubscriptionType.Shared`）。

```java
// 名为 "my-shared" 的共享订阅
PulsarSource.builder().setSubscriptionName("my-shared")

// 名为 "my-exclusive" 的独占订阅
PulsarSource.builder().setSubscriptionName("my-exclusive").setSubscriptionType(SubscriptionType.Exclusive)
```

如果想在 Pulsar Source里面使用 `key 共享` 订阅，需要提供 `RangeGenerator` 实例。`RangeGenerator` 会生成一组消息 key 的 hash 范围，Pulsar Source会基于给定的范围来消费数据。

Pulsar Source也提供了一个名为 `UniformRangeGenerator` 的默认实现，它会基于 flink 数据源的并行度将 hash 范围均分。

### 起始消费位置

Pulsar Source使用 `setStartCursor(StartCursor)` 方法给定开始消费的位置。内置的消费位置有：

- 从 topic 里面最早的一条消息开始消费。
  ```java
  StartCursor.earliest()
  ```
- 从 topic 里面最新的一条消息开始消费。
  ```java
  StartCursor.latest()
  ```
- 从给定的消息开始消费。
  ```java
  StartCursor.fromMessageId(MessageId)
  ```
- 与前者不同的是，给定的消息可以跳过，再进行消费。
  ```java
  StartCursor.fromMessageId(MessageId, boolean)
  ```
- 从给定的消息时间开始消费。
  ```java
  StartCursor.fromMessageTime(long)
  ```

{{< hint info >}}
每条消息都有一个固定的序列号，这个序列号在 Pulsar 上有序排列，其包含了 ledger、entry、partition 等原始信息，用于在 Pulsar 底层存储上查找到具体的消息。
Pulsar 称这个序列号为 `MessageId`，用户可以使用 `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)` 创建它。
{{< /hint >}}

### 边界

Pulsar Source默认情况下使用流的方式消费数据。除非任务失败或者被取消，否则将持续消费数据。
用户可以使用 `setBoundedStopCursor(StopCursor)` 给定停止消费的位置，这种情况下会使用批的方式进行消费。

使用流的方式一样可以给定停止位置，使用 `setUnboundedStopCursor(StopCursor)` 方法即可。

在批模式下，使用 `setBoundedStopCursor(StopCursor)`来指定一个消费停止位置。

内置的停止位置如下:

- 永不停止。
  ```java
  StopCursor.never()
  ```
- 停止于 Pulsar 启动时 topic 里面最新的那条数据。
  ```java
  StopCursor.latest()
  ```
- 停止于某条消息，结果里不包含此消息。
  ```java
  StopCursor.atMessageId(MessageId)
  ```
- 停止于某条消息之后，结果里包含此消息。
  ```java
  StopCursor.afterMessageId(MessageId)
  ```
- 停止于某个给定的消息时间戳。
  ```java
  StopCursor.atEventTime(long)
  ```

### 其他配置项

除了前面提到的配置选项，Pulsar Source还提供了丰富的选项供 Pulsar 专家使用，在 builder 类里通过 `setConfig(ConfigOption<T>, T)` 和 `setConfig(Configuration)` 方法给定下述的全部配置。

#### Pulsar Java 客户端配置项

Pulsar Source使用[Java 客户端](https://pulsar.apache.org/docs/zh-CN/client-libraries-java/)来创建消费实例，相关的配置定义于 Pulsar 的 `ClientConfigurationData` 内。在 `PulsarOptions` 选项中，定义大部分的可供用户定义的配置。

{{< generated/pulsar_client_configuration >}}

#### Pulsar 管理 API 配置项

[管理 API](https://pulsar.apache.org/docs/zh-CN/admin-api-overview/) 用于查询 topic 的元数据和用正则订阅的时候的 topic 查找，它与
Java 客户端共享大部分配置。下面列举的配置只供管理 API 使用， `PulsarOptions`包含了这些配置 。

{{< generated/pulsar_admin_configuration >}}

#### Pulsar 消费者 API 配置项

Pulsar 提供了消费者 API 和读者 API 两套 API 来进行数据消费，它们可用于不同的业务场景。
Flink 上的 Pulsar Source使用消费者 API 进行消费，它的配置定义于 Pulsar 的 `ConsumerConfigurationData` 内。Pulsar Source将其中大部分的可供用户定义的配置定义于 `PulsarSourceOptions` 内。

{{< generated/pulsar_consumer_configuration >}}

#### Pulsar Source配置项

下述配置主要用于性能调优或者是控制消息确认的行为。如非必要，可以不用强制配置。

{{< generated/pulsar_source_configuration >}}

### 动态分区发现

为了能在启动 Flink 任务之后还能发现在 Pulsar 上扩容的分区或者是新创建的 topic，Pulsar Source提供了动态分区发现机制。该机制不需要重启 Flink 任务。
对选项 `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS` 设置一个正整数即可启用。

```java
// 10 秒查询一次分区信息
PulsarSource.builder()
        .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```

{{< hint warning >}}
默认情况下，Pulsar 启用动态分区发现，查询间隔为 30 秒。用户可以给定一个负数，将该功能禁用。如果使用批的方式消费数据，将无法启用该功能。
{{< /hint >}}

### 事件时间和水位线

默认情况下，Pulsar Source使用 Pulsar 的 `Message<byte[]>` 里面的时间作为解析结果的时间戳。用户可以使用 `WatermarkStrategy` 来自行解析出想要的消息时间，并向下游传递对应的水位线。

```java
env.fromSource(pulsarSource, new CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy")
```

[这篇文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}) 详细讲解了如何定义 `WatermarkStrategy`。

### 消息确认

一旦在 topic 上创建了订阅，消息便会[存储](https://pulsar.apache.org/docs/zh-CN/concepts-architecture-overview/#%E6%8C%81%E4%B9%85%E5%8C%96%E5%AD%98%E5%82%A8)在 Pulsar 里。即使没有消费者，消息也不会被丢弃。只有当Pulsar Source同 Pulsar 确认此条消息已经被消费，该消息才以某种机制会被移除。Pulsar Source支持四种订阅方式，它们的消息确认方式也大不相同。

#### 独占和灾备订阅下的消息确认

`独占` 和 `灾备` 订阅下,Pulsar Source使用累进式确认方式。确认某条消息已经被处理时，其前面被消费的消息会自动被置为已读。Pulsar Source会在 Flink 完成检查点时将对应时刻消费的消息置为已读，以此来保证 Pulsar 状态与 Flink 状态一致。

如果用户没有在 Flink 上启用检查点，Pulsar Source可以使用周期性提交来将消费状态提交给 Pulsar，使用配置 `PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL` 来进行定义。

需要注意的是，此种场景下，Pulsar Source并不依赖于提交到 Pulsar 的状态来做容错。消息确认只是为了能在 Pulsar 端看到对应的消费处理情况。

#### 共享和 key 共享订阅下的消息确认

`共享` 和 `key 共享` 需要依次确认每一条消息，所以Pulsar Source在 Pulsar 事务里面进行消息确认，然后将事务提交到 Pulsar。

首先需要在 Pulsar 的 `borker.conf` 文件里面启用事务：

```text
transactionCoordinatorEnabled=true
```

Pulsar Source创建的事务的默认超时时间为 3 小时，请确保这个时间大于 Flink 检查点的间隔。用户可以使用 `PulsarSourceOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS` 来设置事务的超时时间。

如果用户无法启用 Pulsar 的事务，或者是因为项目禁用了检查点，需要将 `PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE` 选项设置为 `true`，消息从 Pulsar 消费后会被立刻置为已读。Pulsar Source无法保证此种场景下的消息一致性。

Pulsar Source在 Pulsar 上使用日志的形式记录某个事务下的消息确认，为了更好的性能，请缩短 Flink 做检查点的间隔。

## Pulsar Sink

Pulsar sink 连接器可以将经过link处理后的数据写入一个或多个Pulsar topic或者topic下的某些分区。

{{< hint info >}}
Pulsar Sink基于Flink最新的
[data sink](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) API 实现.

如果你想要使用旧版的使用`SinkFuntion` 接口实现的sink连接器，可以使用StreamNative维护的
[pulsar-flink](https://github.com/streamnative/pulsar-flink).
{{< /hint >}}

### 使用示例

Pulsar sink 使用一个builder类来创建 `PulsarSink`实例.

下列示例展示了如何通过Pulsar Sink以"至少一次"的语义将字符串类型的数据发送给一个Pulsar topic/

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

下列为创建一个`PulsarSink`实例必需的属性

- Pulsar 数据消费的地址，使用 `setServiceUrl(String)` 方法提供
- Pulsar HTTP 管理地址，使用 `setAdminUrl(String)` 方法提供
- 需要发送到的 topic 或者是 topic 下面的分区，详见[指定写入的topic或者topic分区](#指定写入的topic或者topic分区)
- 编码 Pulsar 消息的序列化器，详见[序列化器](#序列化器)

在创建`PulsarSink`时，建议使用`setProducerName(String)`来指定`PulsarSink`内部使用的Pulsar生产者名称。这样方便
在数据监控页面找到对应的生产者监控指标。


### 指定写入的topic或者topic分区

`PulsarSink`指定写入topic的方式和Pulsar source[指定消费的 Topic 或者 Topic 分区](#指定消费的-topic-或者-topic-分区)的方式类似。
`PulsarSink` 支持以mixin风格指定写入的topic或分区，因此，你可以指定一组topic或者分区或者是两者都有。
, partitions or both of them.

```java
// Topic "some-topic1" 和 "some-topic2"
PulsarSink.builder().setTopics("some-topic1", "some-topic2")

// Topic "topic-a" 的分区 0 和 2 
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")

//  topic "topic-a" 以及 topic "some-topic2" 分区 0 和 2 
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2", "some-topic2")
```

动态分区发现默认处于开启状态，这意味着`PulsarSink`将会周期性地从pulsar 集群中查询topic的元数据来获取可能有的分区数量变更信息。
使用`PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` 配置项来指定查询的间隔时间。

您可以选择实现[`TopicRouter`][消息路由策略](#消息路由策略)接口来自定义topic路由逻辑。此外[Topic 名称简写](#topic-名称简写) 
可以帮助您更好地理解pulsar的分区在pulsar连接器中的配置方式。

{{< hint warning >}}
如果您在`PulsarSink`中同时指定了一个topic A和topic A下属的分区，那么`PulsarSink`将会自动将两者合并，仅使用topic A。

举个例子，如果您通过`PulsarSink.builder().setTopics("some-topic1", "some-topic1-partition-0")`来指定写入的topic，
那么其结果等效于指定`PulsarSink.builder().setTopics("some-topic1")`。

{{< /hint >}}

### 序列化器

序列化器负责将Flink中的每条记录序列化成bytes数组以通过网络发送至指定的写入Pulsar topic。和Pulsar source
类似，Pulsar sink同时支持使用Flink的`SerializationSchema`接口实现的序列化器和Pulsar原生的`Schema`类型的
序列化器。不过Pulsar sink并不支持Pulsar提供的`Schema.AUTO_PRODUCE_BYTES()`。

如果您不需要指定[Message](https://pulsar.apache.org/api/client/2.9.0-SNAPSHOT/org/apache/pulsar/client/api/Message.html)
接口中提供的key或者其他的消息属性properties，您可以从2种内置的`PulsarSerializationSchema`实现中选择适合您需求的一种使用。

- 使用Pulsar的 [Schema](https://pulsar.apache.org/docs/zh-CN/schema-understand/)来序列化Flink中的数据.
  ```java
  // 原始数据类型
  PulsarSerializationSchema.pulsarSchema(Schema)

  // 有结构数据类型 (JSON, Protobuf, Avro, etc.)
  PulsarSerializationSchema.pulsarSchema(Schema, Class)

  // 键值对类型
  PulsarSerializationSchema.pulsarSchema(Schema, Class, Class)
  ```
- Encode the message by using Flink's `SerializationSchema`
  ```java
  PulsarSerializationSchema.flinkSchema(SerializationSchema)
  ```

同时使用`PulsarSerializationSchema.pulsarSchema()` 以及在builder中指定`PulsarSinkBuilder.enableSchemaEvolution()`
可以启用[Schema evolution](https://pulsar.apache.org/docs/zh-CN/schema-evolution-compatibility/#schema-evolution)
特性。该特性会支持Pulsar broker端提供的schema版本兼容性检测以及schema版本演进。下列示例展示了如何启用Schema Evolution。

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
如果您想要使用Pulsar原生的Schema而不启用Schema evolution特性，那么写入的topic会使用`Schema.BYTES`作为schema，
而且那些写入topics的消费者需要自己负责反序列化的工作。

举个例子，如果您使用`PulsarSerializationSchema.pulsarSchema(Schema.STRING)`而不指定`PulsarSinkBuilder.enableSchemaEvolution()`
那么在写入topic中所记录的schema将会是`Schema.BYTES`。
{{< /hint >}}

### 消息路由策略

在Pulsar Sink中，消息路由发生在各个分区之间。对于一组分区topic，路由算法会首先将属于不同topic的分区提取出来，并
在这些分区上实现路由算法。Pulsar Sink默认提供2种路由策略的实现。

- `KeyHashTopicRouter`: 使用由消息的key哈希值来计算需要该消息应当发往哪个topic或分区。

  在实现`PulsarSerializationSchema#serialize()`方法时，消息的key可由 `PulsarMessageBuilder.key(String key)`方法指定。
  您可以使用该方法来将具有相同key的消息发送至同一个topic或分区。

  如果您不指定消息的key，那么该路由策略将会从topic或分区列表中随机选择一个发送。

  可以使用`MessageKeyHash.JAVA_HASH` 或者 `MessageKeyHash.MURMUR3_32_HASH`两种不同的哈希算法来计算key的哈希值。 
  使用`PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH`配置项来选择适合的哈希算法。

- `RoundRobinRouter`: 轮替发往topic或分区列表。
  
  消息将会以Round-robin轮替的方式发送，当往第一个写入topic或分区中发送一定数量的消息后，将会切换至下一个topic或分区继续发送。
  使用`PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES` 指定向一个topic或分区中写入的数量。

您可以通过实现`TopicRouter`接口来自定义消息路由策略，请注意一个TopicRouter实现应当是可序列化的。

在`TopicRouter`内可以指定任意的topic（即使这个topic不在`setTopics()` 指定的列表中）。因此，当你使用
自定义的`TopicRouter`时，`PulsarSinkBuilder.setTopics`选项不一定是必需的。


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
如前文所述，Pulsar分区在内部被实现为一个topic，通常来讲Pulsar客户端会隐藏这个实现，并且也有内置的消息路由策略。
Pulsar Sink并没有直接使用Pulsar 客户端使用的路由策略和封装，而是使用了Pulsar客户端更底层的API实现了一套
独有的消息路由逻辑，这样做的主要目的是能够在属于不同topic的分区之间定义更灵活的消息路由策略。

详情请参考Pulsar的[partitioned topics](https://pulsar.apache.org/docs/zh-CN/cookbooks-partitioned/).
{{< /hint >}}


### 传递语义

`PulsarSink` 支持三种消息传递语义

- `NONE`: Flink应用运行时可能出现数据丢失的情况。在这种模式下，Pulsar sink发送消息后并不准备接受ack消息。通常来讲
  该模式具有最好的吞吐量。
- `AT_LEAST_ONCE`:"至少一次" 语义，发送到下游的数据可能会由于Flink应用重启而出现重复。
- `EXACTLY_ONCE`: "精确一次"语义。发送到下游的数据不会有重复也不会丢数据。Pulsar Sink内部依赖
  [Pulsar 事务](https://pulsar.apache.org/docs/zh-CN/transactions/)
  和二阶段提交协议来保证每条记录只会被发往下游一次。


### 延时消息发送

[延时消息发送](https://pulsar.apache.org/docs/zh-CN/next/concepts-messaging/#%E6%B6%88%E6%81%AF%E5%BB%B6%E8%BF%9F%E4%BC%A0%E9%80%92)特性
可以让您指定发送的一条消息需要延时一段时间后才能被下游的消费者所消费。当延时消息发送特性启用时，Pulsar sink仍然会**立刻**将
消息发送至Puslar broker，但该消息在指定的延迟时间到达前将会保持对下游消费者不可见。

延时消息发送仅在`Shared` 订阅模式下有效，在 `Exclusive` 和 `Failover`模式下该特性无效。

您需要通过`MessageDelayer.fixed(Duration)` 创建一个`MessageDelayer`来指定固定的延时时间，或者实现
`MessageDelayer`接口来为不同的消息指定不同的延时时间。

{{< hint warning >}}
消息对下游消费者的可见时间应当由`PulsarSinkContext.processTime()`计算得到.
{{< /hint >}}

### Sink 配置项

Pulsar sink和Pulsar source公用的内容可参考

- [Pulsar Java 客户端配置项](#pulsar-java-客户端配置项)
- [Pulsar 管理 API 配置项](#pulsar-管理-API-配置项)

#### Pulsar 生产者 API 配置项

Pulsar Ssink使用生产者API来发送消息。Pulsar的`ProducerConfigurationData`中大部分的配置项被映射到了
`PulsarSinkOptions`类中。

{{< generated/pulsar_producer_configuration >}}

#### Pulsar Sink 配置项

下述配置主要用于性能调优或者是控制消息确认的行为。如非必要，可以不用强制配置。

{{< generated/pulsar_sink_configuration >}}

### 监控指标

下列表格列出了当前sink支持的监控指标
前6个指标是[FLIP-33: Standardize Connector Metrics]([https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics))
中规定的Sink连接器应该支持的标准指标。

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
        <td>Pulsar Sink启动后总共发出的字节数</td>
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
        <td>Pulsar Sink启动后总共发出的消息数</td>
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
        <td最Gaugxe</td>
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
        <td>某个生产者在过去的一个窗口内的发送延迟50百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency75Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟75百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency95Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延950百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency99Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟99百分位数</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency999Pct</td>
        <td>ProducerName</td>
        <td>某个生产者在过去的一个窗口内的发送延迟99.9百分位数</td>
        <td>Gauge</td>
    </tr>
  </tbody>
</table>



{{< hint info >}}
- `numBytesOut`, `numRecordsOut`, `numRecordsOutErrors` 是从Pulsar Producer实例的监控指标中获得的。
- `currentSendTime` 记录了最近一条消息从 `sendAync()`调用后被放入生产者的缓冲队列为起始，到消息被确认所耗费的时间。
  这项指标在`NONE` 传递语义下不可用。
{{< /hint >}}

默认情况下，Pulsar 生产者每隔60秒才会刷新一次监控数据，然而Puslar sink每500毫秒就会从Pulsar生产者中获得最新的监控数据。
因此`numRecordsOut`, `numBytesOut`, `numAcksReceived`, 以及 `numRecordsOutErrors`4个指标实际上没60秒才会
刷新一次。如果您想要更快的刷新评率，你可以通过`builder.setConfig(PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS. 1L)`
来将Pulsar 生产者的监控数据刷新评率调整至相应值（最低为1s）。

`numBytesOutRate` 和 `numRecordsOutRate`指标是Flink内部通过计算`numBytesOut` 和 `numRecordsOut`计数器，
在一个 60秒的窗口内计算得到的。


### 设计思想简述

Pulsar sink遵循
[FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction)
中定义的Sink API设计。

#### 无状态的SinkWriter

在`EXACTLY_ONCE`模式下，Pulsar sink不会将事务相关的信息存放于检查点快照中。这意味着当Flink应用重启时，
Pulsar Sink会创建新的事务实例。而上一次运行过程中任何未提交事务中的消息会因为超时中止而无法被下游的消费者所消费。
这样的设计保证了SinkWriter是无状态的。


#### Pulsar Schema Evolution

[Pulsar Schema Evolution](https://pulsar.apache.org/docs/zh-CN/schema-evolution-compatibility/) 允许用户
在一个Flink应用程序中使用的数据模型发生特定改变后（比如向基于ARVO的POJO类中增加或删除一个字段），仍能使用同一个Flink应用程序的代码。

您可以在Pulsar 集群内指定哪些类型的数据模型的改变是被允许的，详情请参阅
[Pulsar Schema Evolution](https://pulsar.apache.org/docs/zh-CN/schema-evolution-compatibility/).

## 升级至最新的连接器

常见的升级步骤，请参阅[升级应用程序和 Flink 版本]({{< ref "docs/ops/upgrading" >}})。Pulsar 连接器没有在 Flink 端存储消费的状态，所有的消费信息都推送到了 Pulsar。所以需要注意下面的事项：

* 不要同时升级 Pulsar 连接器和 Pulsar 服务端的版本。
* 使用最新版本的 Pulsar 客户端来消费消息。

## 问题诊断

使用 Flink 和 Pulsar 交互时如果遇到问题，由于Flink内部实现只是基于
Pulsar 的[Java 客户端](https://pulsar.apache.org/docs/zh-CN/client-libraries-java/) 
和[管理 API](https://pulsar.apache.org/docs/zh-CN/admin-api-overview/)而开发的。
用户遇到的问题很有可能与 Flink 无关，请先升级 Pulsar 的版本、Pulsar 客户端的版本、或者修改 Pulsar 的配置
Pulsar 连接器的配置来尝试解决问题。

{{< top >}}
