---
title: Kafka
weight: 3
type: docs
aliases:
  - /zh/dev/connectors/kafka.html
  - /zh/apis/streaming/connectors/kafka.html
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

# Apache Kafka 连接器

Flink 提供了 [Apache Kafka](https://kafka.apache.org) 连接器使用精确一次（Exactly-once）的语义在 Kafka topic 中读取和写入数据。

## 依赖

Apache Flink 集成了通用的 Kafka 连接器，它会尽力与 Kafka client 的最新版本保持同步。
该连接器使用的 Kafka client 版本可能会在 Flink 版本之间发生变化。
当前 Kafka client 向后兼容 0.10.0 或更高版本的 Kafka broker。
有关 Kafka 兼容性的更多细节，请参考 [Kafka 官方文档](https://kafka.apache.org/protocol.html#protocol_compatibility)。

{{< artifact flink-connector-kafka >}}

如果使用 Kafka source，```flink-connector-base``` 也需要包含在依赖中：

{{< artifact flink-connector-base >}}

Flink 目前的流连接器还不是二进制发行版的一部分。
[在此处]({{< ref "docs/dev/datastream/project-configuration" >}})可以了解到如何链接它们，从而在集群中运行。

## Kafka Source
{{< hint info >}}
该文档描述的是基于[新数据源 API]({{< ref "docs/dev/datastream/sources.md" >}}) 的 Kafka Source。
{{< /hint >}}

### 使用方法
Kafka Source 提供了构建类来创建 ```KafkaSource``` 的实例。以下代码片段展示了如何构建 ```KafkaSource```
来消费 “input-topic” 最早位点的数据， 使用消费组 “my-group”，并且将 Kafka 消息体反序列化为字符串：
```java
KafkaSource<String> source = KafkaSource.<String>builder()
    .setBootstrapServers(brokers)
    .setTopics("input-topic")
    .setGroupId("my-group")
    .setStartingOffsets(OffsetsInitializer.earliest())
    .setValueOnlyDeserializer(new SimpleStringSchema())
    .build();

env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
```
以下属性在构建 KafkaSource 时是必须指定的：
- Bootstrap server，通过 ```setBootstrapServers(String)``` 方法配置
- 消费者组 ID，通过 ```setGroupId(String)``` 配置
- 要订阅的 Topic / Partition，请参阅 <a href="#topic-partition-subscription">Topic / Partition 订阅</a>一节
- 用于解析 Kafka 消息的反序列化器（Deserializer），请参阅<a href="#deserializer">消息解析</a>一节

### Topic / Partition 订阅
Kafka Source 提供了 3 种 Topic / Partition 的订阅方式：
- Topic 列表，订阅 Topic 列表中所有 Partition 的消息：
  ```java
  KafkaSource.builder().setTopics("topic-a", "topic-b")
  ```
- 正则表达式匹配，订阅与正则表达式所匹配的 Topic 下的所有 Partition：
  ```java
  KafkaSource.builder().setTopicPattern("topic.*")
  ```
- Partition 列表，订阅指定的 Partition：
  ```java
  final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
          new TopicPartition("topic-a", 0),    // Partition 0 of topic "topic-a"
          new TopicPartition("topic-b", 5)));  // Partition 5 of topic "topic-b"
  KafkaSource.builder().setPartitions(partitionSet)
  ```
### 消息解析
代码中需要提供一个反序列化器（Deserializer）来对 Kafka 的消息进行解析。
反序列化器通过 ```setDeserializer(KafkaRecordDeserializationSchema)``` 来指定，其中 ```KafkaRecordDeserializationSchema``` 
定义了如何解析 Kafka 的 ```ConsumerRecord```。

如果只需要 Kafka 消息中的消息体（value）部分的数据，可以使用 KafkaSource 构建类中的
```setValueOnlyDeserializer(DeserializationSchema)``` 方法，其中 ```DeserializationSchema``` 定义了如何解析 Kafka 
消息体中的二进制数据。

也可使用 [Kafka 提供的解析器](https://kafka.apache.org/24/javadoc/org/apache/kafka/common/serialization/Deserializer.html) 
来解析 Kafka 消息体。例如使用 ```StringDeserializer``` 来将 Kafka 消息体解析成字符串：
```java
import org.apache.kafka.common.serialization.StringDeserializer;

KafkaSource.<String>builder()
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringSerializer.class));
```

### 起始消费位点
Kafka source 能够通过位点初始化器（```OffsetsInitializer```）来指定从不同的偏移量开始消费 。内置的位点初始化器包括：

```java
KafkaSource.builder()
    // 从消费组提交的位点开始消费，不指定位点重置策略
    .setStartingOffsets(OffsetsInitializer.committedOffsets())
    // 从消费组提交的位点开始消费，如果提交位点不存在，使用最早位点
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
    // 从时间戳大于等于指定时间的数据开始消费
    .setStartingOffsets(OffsetsInitializer.timestamp(1592323200L))
    // 从最早位点开始消费
    .setStartingOffsets(OffsetsInitializer.earliest())
    // 从最末尾位点开始消费
    .setStartingOffsets(OffsetsInitializer.latest())
```
如果内置的初始化器不能满足需求，也可以实现自定义的位点初始化器（```OffsetsInitializer```）。

如果未指定位点初始化器，将默认使用 ```OffsetsInitializer.earliest()```。

### 有界 / 无界模式
Kafka Source 支持流式和批式两种运行模式。默认情况下，KafkaSource 设置为以流模式运行，因此作业永远不会停止，直到 Flink 作业失败或被取消。
可以使用 ```setBounded(OffsetsInitializer)``` 指定停止偏移量使 Kafka Source 以批处理模式运行。当所有分区都达到其停止偏移量时，Kafka Source 
会退出运行。

流模式下运行通过使用 ```setUnbounded(OffsetsInitializer)``` 也可以指定停止消费位点，当所有分区达到其指定的停止偏移量时，Kafka Source 会退出运行。

### 其他属性
除了上述属性之外，您还可以使用 setProperties(Properties) 和 setProperty(String, String) 为 Kafka Source 和 Kafka Consumer 
设置任意属性。KafkaSource 有以下配置项：
- ```client.id.prefix```，指定用于 Kafka Consumer 的客户端 ID 前缀
- ```partition.discovery.interval.ms```，定义 Kafka Source 检查新分区的时间间隔。
  请参阅下面的<a href="#dynamic-partition-discovery">动态分区检查</a>一节
- ```register.consumer.metrics``` 指定是否在 Flink 中注册 Kafka Consumer 的指标
- ```commit.offsets.on.checkpoint``` 指定是否在进行 checkpoint 时将消费位点提交至 Kafka broker

Kafka consumer 的配置可以参考 [Apache Kafka 文档](http://kafka.apache.org/documentation/#consumerconfigs)。

请注意，即使指定了以下配置项，构建器也会将其覆盖：
- ```key.deserializer``` 始终设置为 ByteArrayDeserializer
- ```value.deserializer``` 始终设置为 ByteArrayDeserializer
- ```auto.offset.reset.strategy``` 被 OffsetsInitializer#getAutoOffsetResetStrategy() 覆盖
- ```partition.discovery.interval.ms``` 会在批模式下被覆盖为 -1

下面的代码片段展示了如何配置 Kafka consumer 以使用“PLAIN”作为 SASL 机制并提供 JAAS 配置：
```java
KafkaSource.builder()
    .setProperty("sasl.mechanism", "PLAIN")
    .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";")
```

### 动态分区检查
为了在不重启 Flink 作业的情况下处理 Topic 扩容或新建 Topic 等场景，可以将 Kafka Source 配置为在提供的 Topic / Partition 
订阅模式下定期检查新分区。要启用动态分区检查，请将 ```partition.discovery.interval.ms``` 设置为非负值：

```java
KafkaSource.builder()
    .setProperty("partition.discovery.interval.ms", "10000") // 每 10 秒检查一次新分区
```
{{< hint warning >}}
分区检查功能默认**不开启**。需要显式地设置分区检查间隔才能启用此功能。
{{< /hint >}}

### 事件时间和水印
默认情况下，Kafka Source 使用 Kafka 消息中的时间戳作为事件时间。您可以定义自己的水印策略（Watermark Strategy）
以从消息中提取事件时间，并向下游发送水印：
```java
env.fromSource(kafkaSource, new CustomWatermarkStrategy(), "Kafka Source With Custom Watermark Strategy")
```
[这篇文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}})描述了如何自定义水印策略（```WatermarkStrategy```）。

### 消费位点提交
Kafka source 在 checkpoint **完成**时提交当前的消费位点 ，以保证 Flink 的 checkpoint 状态和 Kafka broker 上的提交位点一致。如果未开启 
checkpoint，Kafka source 依赖于 Kafka consumer 内部的位点定时自动提交逻辑，自动提交功能由 ```enable.auto.commit``` 和 
```auto.commit.interval.ms``` 两个 Kafka consumer 配置项进行配置。

注意：Kafka source **不依赖**于 broker 上提交的位点来恢复失败的作业。提交位点只是为了上报 Kafka consumer 和消费组的消费进度，以在 broker 端进行监控。

### 监控

Kafka source 会在不同的[范围 (Scope)]({{< ref "docs/ops/metrics" >}}/#scope)中汇报下列指标。

#### 指标范围

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">范围</th>
      <th class="text-left" style="width: 18%">指标</th>
      <th class="text-left" style="width: 18%">用户变量</th>
      <th class="text-left" style="width: 39%">描述</th>
      <th class="text-left" style="width: 10%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <th rowspan="8">算子</th>
        <td>currentEmitEventTimeLag</td>
        <td>n/a</td>
        <td>数据的事件时间与数据离开 Source 时的间隔¹：<code>currentEmitEventTimeLag = EmitTime - EventTime</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>watermarkLag</td>
        <td>n/a</td>
        <td>水印时间滞后于当前时间的时长：<code>watermarkLag = CurrentTime - Watermark</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>sourceIdleTime</td>
        <td>n/a</td>
        <td>Source 闲置时长：<code>sourceIdleTime = CurrentTime - LastRecordProcessTime</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>pendingRecords</td>
        <td>n/a</td>
        <td>尚未被 Source 拉取的数据数量，即 Kafka partition 当前消费位点之后的数据数量。</td>
        <td>Gauge</td>
    </tr>
    <tr>
      <td>KafkaSourceReader.commitsSucceeded</td>
      <td>n/a</td>
      <td>位点成功提交至 Kafka 的总次数，在开启了位点提交功能时适用。</td>
      <td>Counter</td>
    </tr>
    <tr>
       <td>KafkaSourceReader.commitsFailed</td>
       <td>n/a</td>
       <td>位点未能成功提交至 Kafka 的总次数，在开启了位点提交功能时适用。注意位点提交仅是为了向 Kafka 上报消费进度，因此提交失败并不影响
       Flink checkpoint 中存储的位点信息的完整性。</td>
       <td>Counter</td>
    </tr>
    <tr>
       <td>KafkaSourceReader.committedOffsets</td>
       <td>topic, partition</td>
       <td>每个 partition 最近一次成功提交至 Kafka 的位点。各个 partition 的指标可以通过指定 topic 名称和 partition ID 获取。</td>
       <td>Gauge</td>
    </tr>
    <tr>
      <td>KafkaSourceReader.currentOffsets</td>
      <td>topic, partition</td>
      <td>每个 partition 当前读取的位点。各个 partition 的指标可以通过指定 topic 名称和 partition ID 获取。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>

¹ 该指标反映了最后一条数据的瞬时值。之所以提供瞬时值是因为统计延迟直方图会消耗更多资源，瞬时值通常足以很好地反映延迟。

#### Kafka Consumer 指标
Kafka consumer 的所有指标都注册在指标组 ```KafkaSourceReader.KafkaConsumer``` 下。例如 Kafka consumer 的指标 
```records-consumed-total``` 将在该 Flink 指标中汇报：
```<some_parent_groups>.operator.KafkaSourceReader.KafkaConsumer.records-consumed-total```。

您可以使用配置项 ```register.consumer.metrics``` 配置是否注册 Kafka consumer 的指标 。默认此选项设置为 true。

关于 Kafka consumer 的指标，您可以参考 [Apache Kafka 文档](http://kafka.apache.org/documentation/#consumer_monitoring)
了解更多详细信息。

### 实现细节
{{< hint info >}}
如果你对 Kafka source 在新的 Source API 中的设计感兴趣，可阅读该部分作为参考。关于新 Source API 的细节，[Source
 API 文档]({{< ref "docs/dev/datastream/sources.md" >}})和 
[FLIP-27](https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface) 提供了更详细的描述。
{{< /hint >}}

在新 Source API 的抽象中，Kafka source  由以下几个部分组成：
#### 数据源分片（Source Split）
Kafka source 的数据源分片（source split）表示 Kafka topic 中的一个 partition。Kafka 的数据源分片包括：
- 该分片表示的 topic 和 partition
- 该 partition 的起始位点
- 该 partition 的停止位点，当 source 运行在批模式时适用

Kafka source 分片的状态同时存储该 partition 的当前消费位点，该分片状态将会在 Kafka 源读取器（source reader）进行快照（snapshot）
时将当前消费位点保存为起始消费位点以将分片状态转换成不可变更的分片。

可查看 ```KafkaPartitionSplit``` 和 ```KafkaPartitionSplitState``` 类来了解细节。

#### 分片枚举器（Split Enumerator）
Kafka source 的分片枚举器负责检查在当前的 topic / partition 订阅模式下的新分片（partition），并将分片轮流均匀地分配给源读取器（source reader）。
注意 Kafka source 的分片枚举器会将分片主动推送给源读取器，因此它无需处理来自源读取器的分片请求。

#### 源读取器（Source Reader）
Kafka source 的源读取器扩展了 ```SourceReaderBase```，并使用单线程复用（single thread multiplex）的线程模型，使用一个由分片读取器
（split reader）驱动的 ```KafkaConsumer``` 来处理多个分片（partition）。消息会在从 Kafka 拉取下来后在分片读取器中立刻被解析。分片的状态
即当前的消息消费进度会在 ```KafkaRecordEmitter``` 中更新，同时会在数据发送至下游时指定事件时间。

## Kafka SourceFunction
{{< hint warning >}}
`FlinkKafkaConsumer` 已被弃用并将在 Flink 1.15 中移除，请改用 ```KafkaSource```。
{{< /hint >}}

如需参考，请参阅 Flink 1.13 [文档](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/#kafka-sourcefunction)。

## Kafka Sink

`KafkaSink` 可将数据流写入一个或多个 Kafka topic。

### 使用方法

Kafka sink 提供了构建类来创建 ```KafkaSink``` 的实例。以下代码片段展示了如何将字符串数据按照至少一次（at lease once）的语义保证写入 Kafka
topic：

```java
DataStream<String> stream = ...
        
KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("topic-name")
            .setValueSerializationSchema(new SimpleStringSchema())
            .build()
        )
        .setDeliverGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
        .build();
        
stream.sinkTo(sink);
```

以下属性在构建 KafkaSink 时是必须指定的：

- Bootstrap servers, ```setBootstrapServers(String)```
- 消息序列化器（Serializer）, ``setRecordSerializer(KafkaRecordSerializationSchema)``
- 如果使用```DeliveryGuarantee.EXACTLY_ONCE``` 的语义保证，则需要使用 ```setTransactionalIdPrefix(String)```

### 序列化器

构建时需要提供 ```KafkaRecordSerializationSchema``` 来将输入数据转换为 Kafka 的 ```ProducerRecord```。Flink 提供了 schema 构建器
以提供一些通用的组件，例如消息键（key）/消息体（value）序列化、topic 选择、消息分区，同样也可以通过实现对应的接口来进行更丰富的控制。

```java
KafkaRecordSerializationSchema.builder()
    .setTopicSelector((element) -> {<your-topic-selection-logic>})
    .setValueSerializationSchema(new SimpleStringSchema())
    .setKeySerializationSchema(new SimpleStringSchema())
    .setPartitioner(new FlinkFixedPartitioner())
    .build();
```

其中消息体（value）序列化方法和 topic 的选择方法是必须指定的，此外也可以通过 ```setKafkaKeySerializer(Serializer)``` 或
```setKafkaValueSerializer(Serializer)``` 来使用 Kafka 提供而非 Flink 提供的序列化器。

### 容错

```KafkaSink``` 总共支持三种不同的语义保证（```DeliveryGuarantee```）。对于 ```DeliveryGuarantee.AT_LEAST_ONCE``` 和
```DeliveryGuarantee.EXACTLY_ONCE```，Flink checkpoint 必须启用。默认情况下 ```KafkaSink``` 使用 ```DeliveryGuarantee.NONE```。
以下是对不同语义保证的解释：

- ```DeliveryGuarantee.NONE``` 不提供任何保证：消息有可能会因 Kafka broker 的原因发生丢失或因 Flink 的故障发生重复。
- ```DeliveryGuarantee.AT_LEAST_ONCE```: sink 在 checkpoint 时会等待 Kafka 缓冲区中的数据全部被 Kafka producer 确认。消息不会因
  Kafka broker 端发生的事件而丢失，但可能会在 Flink 重启时重复，因为 Flink 会重新处理旧数据。
- ```DeliveryGuarantee.EXACTLY_ONCE```: 该模式下，Kafka sink 会将所有数据通过在 checkpoint 时提交的事务写入。因此，如果 consumer 
  只读取已提交的数据（参见 Kafka consumer 配置 ```isolation.level```），在 Flink 发生重启时不会发生数据重复。然而这会使数据在 checkpoint
  完成时才会可见，因此请按需调整 checkpoint 的间隔。请确认事务 ID 的前缀（transactionIdPrefix）对不同的应用是唯一的，以保证不同作业的事务
  不会互相影响！此外，强烈建议将 Kafka 的事务超时时间调整至远大于 checkpoint 最大间隔 + 最大重启时间，否则 Kafka 对未提交事务的过期处理会导致数据丢失。

### 监控

Kafka sink 会在不同的[范围（Scope）]({{< ref "docs/ops/metrics" >}}/#scope)中汇报下列指标。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left" style="width: 15%">范围</th>
      <th class="text-left" style="width: 18%">指标</th>
      <th class="text-left" style="width: 18%">用户变量</th>
      <th class="text-left" style="width: 39%">描述</th>
      <th class="text-left" style="width: 10%">类型</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <th rowspan="1">算子</th>
        <td>currentSendTime</td>
        <td>n/a</td>
        <td>发送最近一条数据的耗时。该指标反映最后一条数据的瞬时值。</td>
        <td>Gauge</td>
    </tr>
  </tbody>
</table>

## Kafka Producer

{{< hint warning >}}
`FlinkKafkaProducer` 已被弃用并将在 Flink 1.15 中移除，请改用 ```KafkaSink```。
{{< /hint >}}

如需参考，请参阅 Flink 1.13 [文档](https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/#kafka-producer)。

## Kafka 连接器指标

Flink 的 Kafka 连接器通过 Flink 的[指标系统]({{< ref "docs/ops/metrics" >}})提供一些指标来帮助分析 connector 的行为。
各个版本的 Kafka producer 和 consumer 会通过 Flink 的指标系统汇报 Kafka 内部的指标。
[该 Kafka 文档](http://kafka.apache.org/documentation/#selector_monitoring)列出了所有汇报的指标。

同样也可通过将 Kafka source 在[该章节]({{< relref "#kafka-connector-metrics" >}})描述的 `register.consumer.metrics`，或 Kafka
sink 的 `register.producer.metrics` 配置设置为 false 来关闭 Kafka 指标的注册。

## 启用 Kerberos 身份验证

Flink 通过 Kafka 连接器提供了一流的支持，可以对 Kerberos 配置的 Kafka 安装进行身份验证。只需在 `flink-conf.yaml` 中配置 Flink。像这样为 Kafka 启用 Kerberos 身份验证：

1. 通过设置以下内容配置 Kerberos 票据
- `security.kerberos.login.use-ticket-cache`：默认情况下，这个值是 `true`，Flink 将尝试在 `kinit` 管理的票据缓存中使用 Kerberos 票据。注意！在 YARN 上部署的 Flink  jobs 中使用 Kafka 连接器时，使用票据缓存的 Kerberos 授权将不起作用。
- `security.kerberos.login.keytab` 和 `security.kerberos.login.principal`：要使用 Kerberos keytabs，需为这两个属性设置值。

2. 将 `KafkaClient` 追加到 `security.kerberos.login.contexts`：这告诉 Flink 将配置的 Kerberos 票据提供给 Kafka 登录上下文以用于 Kafka 身份验证。

一旦启用了基于 Kerberos 的 Flink 安全性后，只需在提供的属性配置中包含以下两个设置（通过传递给内部 Kafka 客户端），即可使用 Flink Kafka Consumer 或 Producer 向 Kafk a进行身份验证：

- 将 `security.protocol` 设置为 `SASL_PLAINTEXT`（默认为 `NONE`）：用于与 Kafka broker 进行通信的协议。使用独立 Flink 部署时，也可以使用 `SASL_SSL`；请在[此处](https://kafka.apache.org/documentation/#security_configclients)查看如何为 SSL 配置 Kafka 客户端。
- 将 `sasl.kerberos.service.name` 设置为 `kafka`（默认为 `kafka`）：此值应与用于 Kafka broker 配置的 `sasl.kerberos.service.name` 相匹配。客户端和服务器配置之间的服务名称不匹配将导致身份验证失败。

有关 Kerberos 安全性 Flink 配置的更多信息，请参见[这里]({{< ref "docs/deployment/config" >}}})。你也可以在[这里]({{< ref "docs/deployment/security/security-kerberos" >}})进一步了解 Flink 如何在内部设置基于 kerberos 的安全性。

<a name="upgrading-to-the-latest-connector-version"></a>

## 升级到最近的连接器版本

通用的升级步骤概述见 [升级 Jobs 和 Flink 版本指南]({{< ref "docs/ops/upgrading" >}})。对于 Kafka，你还需要遵循这些步骤：

* 不要同时升级 Flink 和 Kafka 连接器
* 确保你对 Consumer 设置了 `group.id`
* 在 Consumer 上设置 `setCommitOffsetsOnCheckpoints(true)`，以便读 offset 提交到 Kafka。务必在停止和恢复 savepoint 前执行此操作。你可能需要在旧的连接器版本上进行停止/重启循环来启用此设置。
* 在 Consumer 上设置 `setStartFromGroupOffsets(true)`，以便我们从 Kafka 获取读 offset。这只会在 Flink 状态中没有读 offset 时生效，这也是为什么下一步非要重要的原因。
* 修改 source/sink 分配到的 `uid`。这会确保新的 source/sink 不会从旧的 sink/source 算子中读取状态。
* 使用 `--allow-non-restored-state` 参数启动新 job，因为我们在 savepoint 中仍然有先前连接器版本的状态。

<a name="troubleshooting"></a>

## 问题排查

{{< hint warning >}}
如果你在使用 Flink 时对 Kafka 有问题，请记住，Flink 只封装 <a href="https://kafka.apache.org/documentation/#consumerapi">KafkaConsumer</a> 或 <a href="https://kafka.apache.org/documentation/#producerapi">KafkaProducer</a>，你的问题可能独立于 Flink，有时可以通过升级 Kafka broker 程序、重新配置 Kafka broker 程序或在 Flink 中重新配置 <tt>KafkaConsumer</tt> 或 <tt>KafkaProducer</tt> 来解决。下面列出了一些常见问题的示例。
{{< /hint >}}

<a name="data-loss"></a>

### 数据丢失

根据你的 Kafka 配置，即使在 Kafka 确认写入后，你仍然可能会遇到数据丢失。特别要记住在 Kafka 的配置中设置以下属性：

- `acks`
- `log.flush.interval.messages`
- `log.flush.interval.ms`
- `log.flush.*`

上述选项的默认值是很容易导致数据丢失的。请参考 Kafka 文档以获得更多的解释。

<a name="unknowntopicorpartitionexception"></a>

### UnknownTopicOrPartitionException

导致此错误的一个可能原因是正在进行新的 leader 选举，例如在重新启动 Kafka broker 之后或期间。这是一个可重试的异常，因此 Flink job 应该能够重启并恢复正常运行。也可以通过更改 producer 设置中的 `retries` 属性来规避。但是，这可能会导致重新排序消息，反过来可以通过将 `max.in.flight.requests.per.connection` 设置为 1 来避免不需要的消息。


### ProducerFencedException

这个错误是由于 `FlinkKafkaProducer` 所生成的 `transactional.id` 与其他应用所使用的的产生了冲突。多数情况下，由于 `FlinkKafkaProducer` 产生的 ID 都是以 `taskName + "-" + operatorUid` 为前缀的，这些产生冲突的应用也是使用了相同 Job Graph 的 Flink Job。
我们可以使用 `setTransactionalIdPrefix()` 方法来覆盖默认的行为，为每个不同的 Job 分配不同的 `transactional.id` 前缀来解决这个问题。

{{< top >}}
