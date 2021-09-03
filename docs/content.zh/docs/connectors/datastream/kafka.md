---
title: Kafka
weight: 2
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

Flink 提供了 [Apache Kafka](https://kafka.apache.org) 连接器，用于从 Kafka topic 中读取或者向其中写入数据，可提供精确一次的处理语义。

<a name="dependency"></a>

## 依赖

Apache Flink 集成了通用的 Kafka 连接器，它会尽力与 Kafka client 的最新版本保持同步。该连接器使用的 Kafka client 版本可能会在 Flink 版本之间发生变化。
当前 Kafka client 向后兼容 0.10.0 或更高版本的 Kafka broker。
有关 Kafka 兼容性的更多细节，请参考  [Kafka 官方文档](https://kafka.apache.org/protocol.html#protocol_compatibility)。

{{< artifact flink-connector-kafka withScalaVersion >}}

当使用Kafka source时，还需要添加```flink-connector-base```的依赖:

{{< artifact flink-connector-base >}}

Flink 目前的流连接器还不是二进制发行版的一部分。
[在此处]({{< ref "docs/dev/datastream/project-configuration" >}})可以了解到如何链接它们，从而在集群中运行。

## Kafka Source
{{< hint info >}}
这部分主要介绍基于新的[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API 的Kafka Source。
{{< /hint >}}

### 使用方法
Kafka source提供builder类来建立KafkaSource实例。下面的代码展示了如何建立一个KafkaSource来消费消息，这个KafkaSource将从"input-topic"这个topic的最早offset进行消费，消费组设置为"my-group"，并且消息的值会被反序列化为string类型。

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

以下属性在建立KafkaSource时**必须配置**:

- Bootstrap servers, 通过 ```setBootstrapServers(String)```配置
- 需要订阅的Topics / partitions, 查看<a href="#topic-partition-subscription">Topic-partition订阅</a>获得更多细节
- Kafka消息的反序列化器, 查看<a href="#deserializer">Deserializer</a>获取更多细节.

### Topic-partition订阅
Kafka source 提供3种方式订阅topic-partition:

- Topic列表, 从提供的topic列表的所有分区中订阅消息。例如:

  ```java
  KafkaSource.builder().setTopics("topic-a", "topic-b")
  ```
  
- Topic匹配, 从与提供的正则表达式配置的topic的所有分区中订阅消息。例如:

  ```java
  KafkaSource.builder().setTopicPattern("topic.*")
  ```
- 分区集合, 订阅提供的分区集合的所有分区。例如:

  ```java
  final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
          new TopicPartition("topic-a", 0),    // Partition 0 of topic "topic-a"
          new TopicPartition("topic-b", 5)));  // Partition 5 of topic "topic-b"
  KafkaSource.builder().setPartitions(partitionSet)
  ```
  
### 反序列化器
为了分析Kafka消息需要提供反序列化器（Deserializer）。反序列化器(Deserialization schema)可以通过```setDeserializer(KakfaRecordDeserializationSchema)```配置, ```KafkaRecordDeserializationSchema``` 定义了如何反序列化Kafka ```ConsumerRecord```.

如果只需要Kafka ```ConsumerRecord```的value, 可以在builder中使用```setValueOnlyDeserializer(DeserializationSchema)```, 其中```DeserializationSchema``` 定义了如何反序列化Kafka消息valie的二进制数据。

目前也支持使用 <a href="https://kafka.apache.org/24/javadoc/org/apache/kafka/common/serialization/Deserializer.html">```Kafka Deserializer```</a>
来反序列化Kafka消息的value. 例如使用```StringDeserializer```来反序列化Kafka消息的value为String:

```java
import org.apache.kafka.common.serialization.StringDeserializer;

KafkaSource.<String>builder()
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringSerializer.class));
```

### Starting Offset
Kafka source可以从某个不同的offset消费数据，通过设置```OffsetsInitializer```来指定。内置的initializers包含如下:

```java
KafkaSource.builder()
    // 从对应消费组的已提交offset处开始消费, 不提供reset策略
    .setStartingOffsets(OffsetsInitializer.committedOffsets())
    // 从对应消费组的已提交offset处开始消费, 当不存在已提交到offset时，使用从最早offset开始消费的reset策略消费
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
    // 从第一条大于等于指定时间戳的记录开始消费
    .setStartingOffsets(OffsetsInitializer.timestamp(1592323200L))
    // 从最早的记录开始消费
    .setStartingOffsets(OffsetsInitializer.earliest())
    // 从最新的记录开始消费
    .setStartingOffsets(OffsetsInitializer.latest())
```

当内置的offsets initializer无法满足需求时，你可以实现自定义的offsets initializer。

如果没有设置offsets initializer, 默认使用**OffsetsInitializer.earliest()**.

### Boundedness
Kafka source设置支持流和批的运行模式。KafkaSource默认运行在流模式下，只有在Flink任务失败或取消时才会停止。可以通过```setBounded(OffsetsInitializer)``` 来定义stopping offsets，来让source运行在批模式下。当所有分区都到达stopping offset后，source将退出。

KafkaSource也可以设置为运行在流模式下, 当仍然可以在```setUnbounded(OffsetsInitializer)```设置的stopping offset处停止。当所有分区都到达stopping offset后，source将退出。

### 其他属性
除了以上描述的属性, 可以对 KafkaSource 和 KafkaConsumer 设置任意的属性，通过 ```setProperties(Properties)``` 和 ```setProperty(String, String)```方法进行设置。

KafkaSource 还有以下配置选项:

- ```client.id.prefix``` 定义了 Kafka consumer的 client ID的前缀
- ```partition.discovery.interval.ms``` 定义了Kafka source发现新分区的时间间隔（毫秒为单位）。 查看 <a href="#dynamic-partition-discovery">动态分区发现</a>获取更多细节.
- ```register.consumer.metrics``` 指定是否在Flink metric group中注册KafkaConsumer的指标

对于KafkaConsumer的配置, 可以查看<a href="http://kafka.apache.org/documentation/#consumerconfigs">Apache Kafka 文档</a>获取更多信息。

需要注意以下的属性即使进行设置，也将被builder覆盖:

- ```key.deserializer``` 总是设置为 ```ByteArrayDeserializer```
- ```value.deserializer``` 总是设置为 ```ByteArrayDeserializer```
- ```auto.offset.reset.strategy``` 被starting offsets的```OffsetsInitializer#getAutoOffsetResetStrategy()```方法返回值覆盖
- 当设置```setBounded(OffsetsInitializer)```后，```partition.discovery.interval.ms``` 覆盖为 -1

下面的代码展示了如何配置 KafkaConsumer 来使用"PLAIN"作为SASL mechanism并提供JAAS配置:
```java
KafkaSource.builder()
    .setProperty("sasl.mechanism", "PLAIN")
    .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";")
```

### 动态分区发现
为了在topic扩展或者topic创建时不重启Flink任务, Kafka source可以通过配置的topic正则匹配来定期发现新分区。为了开启动态分区发现功能，需要将 ```partition.discovery.interval.ms```设置一个非负值:

```java
KafkaSource.builder()
    .setProperty("partition.discovery.interval.ms", "10000") // discover new partitions per 10 seconds
```

{{< hint warning >}}
分区发现功能默认**关闭**，需要设置分区发现时间间隔后开启。
{{< /hint >}}

### 事件时间和水位线
记录会默认使用Kafka ```ConsumerRecord``` 中的内置时间戳作为事件时间。可以提供 ```WatermarkStrategy``` 来修改时间戳的提取和水位线生成逻辑:

```java
env.fromSource(kafkaSource, new CustomWatermarkStrategy(), "Kafka Source With Custom Watermark Strategy")
```

[这个文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}) 详细描述了如何实现自定义的```WatermarkStrategy```.

### Consumer Offset提交
Kafka source在checkpoint**完成**时提交当前已经消费的offset, 来保持Flink's checkpoint state中的offset和Kafka broker上提交的offsets是一致的。

如果没有开启checkpoint, Kafka source使用Kafka consumer内部的自动定期提交offset的逻辑来提交，自动提交可以对Kafka consumer设置 ```enable.auto.commit``` 和 ```auto.commit.interval.ms``` 属性来调整。

注意Kafka source**不依赖于**提交offset来实现容错。提交offset只是为了对外暴露consumer和消费组的消费进程。

### 监控

Kafka source在各自的[scope]({{< ref "docs/ops/metrics" >}}/#scope)中提供以下指标。

#### 指标的Scope

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
        <th rowspan="8">Operator</th>
        <td>currentEmitEventTimeLag</td>
        <td>n/a</td>
        <td>数据事件时间和数据从source连接器发出的时间之间的跨度¹: <code>currentEmitEventTimeLag = EmitTime - EventTime.</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>watermarkLag</td>
        <td>n/a</td>
        <td>水位线落后于wall-clock time的时间: <code>watermarkLag = CurrentTime - Watermark</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>sourceIdleTime</td>
        <td>n/a</td>
        <td>source没有处理数据的时间长度: <code>sourceIdleTime = CurrentTime - LastRecordProcessTime</code></td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>pendingRecords</td>
        <td>n/a</td>
        <td>没有被source获取的数据条数，如Kafka分区的consumer offser后的有效数据</td>
        <td>Gauge</td>
    </tr>
    <tr>
      <td>KafkaSourceReader.commitsSucceeded</td>
      <td>n/a</td>
      <td>提交offset到Kafka的成功次数, 当开启提交offset和checkpoint时有效</td>
      <td>Counter</td>
    </tr>
    <tr>
       <td>KafkaSourceReader.commitsFailed</td>
       <td>n/a</td>
       <td>提交offset到Kafka的失败次数, 当开启提交offset和checkpoint时有效。提交offset只是为了暴露消费进程，提交失败并不影响已经checkpoint做完快照的分区offset的完整性。</td>
       <td>Counter</td>
    </tr>
    <tr>
       <td>KafkaSourceReader.committedOffsets</td>
       <td>topic, partition</td>
       <td>每个分区最新成功提交的offset。通过topic名称和分区id来获取指定分区的指标。</td>
       <td>Gauge</td>
    </tr>
    <tr>
      <td>KafkaSourceReader.currentOffsets</td>
      <td>topic, partition</td>
      <td>每个分区consumer读到的offset位置。通过topic名称和分区id来获取指定分区的指标。</td>
      <td>Gauge</td>
    </tr>
  </tbody>
</table>


¹ 这个指标记录的是最近处理数据的瞬时值。This metric is provided because latency histogram could be expensive. The instantaneous latency value is usually a good enough indication of the latency.  


#### Kafka Consumer指标
Kafka consumer的全部指标也会在```KafkaSourceReader.KafkaConsumer```组下注册。
例如, Kafka consumer的指标 "records-consumed-total" 会在以下指标中表示:
```<some_parent_groups>.operator.KafkaSourceReader.KafkaConsumer.records-consumed-total``` 。

我们可以通过配置```register.consumer.metrics```属性来指定是否注册Kafka consumer的指标。这个配置项默认设置为true。

可以阅读
<a href="http://kafka.apache.org/documentation/#consumer_monitoring">Apache Kafka 文档</a>
来获取Kafka consumer指标的更多内容。

### 更多细节
{{< hint info >}}
如果你对Kafka source如何在新的data source api设计下工作感兴趣，你可能想要阅读这个小节做为参考。新的data source api的细节和讨论可以查看[data source文档]({{< ref "docs/dev/datastream/sources.md" >}}) 和
<a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface">FLIP-27</a>
。
{{< /hint >}}

在新的data source API的抽象中, Kafka source 包括以下几个部分:

#### Source Split
Source split表示Kafka topic的一个分区。Kafka source split 包括：

- ```TopicPartition```
- 分区的Starting offset
- 分区的Stopping offset, 当source运行在bounded模式下时有效

Kafka source split的状态也存储了分区消费到的offset，在source做快照时状态会转换为不可变的split，这个不可变的split的starting offset会设置为当前消费到的offset。

可以查看 ```KafkaPartitionSplit``` 和 ```KafkaPartitionSplitState```类获取更多细节。

#### Split Enumerator
Kafka的Split enumerator 负责发现符合提供topic匹配模式的topic的新split（也就是分区），然后分配这些split给reader，通过round-robin的方式均匀的分布在subtask中。注意Kafka的split enumerator发现新分区后会直接分配split给source reader，而不需要处理来自source reader的split分配请求。

#### Source Reader
Kafka的source reader继承自 ```SourceReaderBase```, 使用single-thread-multiplexed线程模型, single-thread-multiplexed线程模型使用同一个KafkaConsumer读取多个split，这个KafkaConsumer封装在```SplitReader```中。 通过```SplitReader```从Kafka获取到的消息会直接被反序列化。Split的状态（也就是消息消费进程）会被```KafkaRecordEmitter```更新，```KafkaRecordEmitter```也负责在数据发往下游时分配事件时间。

## Kafka SourceFunction
{{< hint warning >}}
`FlinkKafkaConsumer` 已经废弃并将在版本1.15中移除, 请使用`KafkaSource`来替换。
{{< /hint >}}

在以前的版本的使用可以查看<a href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/#kafka-sourcefunction">Flink 1.13 文档</a>.
 
## Kafka Sink

`KafkaSink`可以用于写入数据流到一个或更多的Kafka topic。

### 使用方法

Kafka sink 提供了builder类来构造KafkaSink实例。下面的代码展示如何写入String类型的数据到Kafka topic，设置了 at least once的语义保证。

```java
DataStream<String> stream = ...
        
KafkaSink<String> sink = KafkaSink.<String>builder()
        .setBootstrapServers(brokers)
        .setRecordSerializer(KafkaRecordSerializationSchema.builder()
            .setTopic("topic-name")
            .setValueSerializationSchema(new SimpleStringSchema())
            .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
            .build()
        )
        .build();
        
stream.sinkTo(sink);
```

下面的属性在建立KafkaSink时**必须配置**:

- Bootstrap servers, ```setBootstrapServers(String)```
- Record serializer, ``setRecordSerializer(KafkaRecordSerializationSchema)``
- 如果使用 ```DeliveryGuarantee.EXACTLY_ONCE```语义，则必须配置 ```setTransactionalIdPrefix(String)```

### 序列化器

Sink需要提供序列化器 ```KafkaRecordSerializationSchema``` 将数据流读入的元素转换为Kafka producer record。
Flink 通过schema builder 来提供一些常见的构建，如key/value 序列化, topic选择, 分区. 也可以自行实现接口提供更多的控制。

```java
KafkaRecordSerializationSchema.builder()
    .setTopicSelector((element) -> {<your-topic-selection-logic>})
    .setValueSerializationSchema(new SimpleStringSchema())
    .setKeySerializationSchema(new SimpleStringSchema())
    .setPartitioner(new FlinkFixedPartitioner())
    .build();
```

value序列化方法和topic选择方式是**必须配置**的。
此外, 可以通过如下设置来使用 Kafka serializers代替Flink serializer： 
```setKafkaKeySerializer(Serializer)``` 或 ```setKafkaValueSerializer(Serializer)```.

### 容错

```KafkaSink``` 支持三种不同的 ```DeliveryGuarantee```。 使用```DeliveryGuarantee.AT_LEAST_ONCE``` 或 ```DeliveryGuarantee.EXACTLY_ONCE```时，必须开启Flink checkpoint。```KafkaSink```默认使用```DeliveryGuarantee.NONE```。 以下是对不同语义保证的解释。

- ```DeliveryGuarantee.NONE``` 不提供任何保证: Kafka broker出现问题时可能丢失数据，而且Flink出现问题时也可能发生数据重复。
- ```DeliveryGuarantee.AT_LEAST_ONCE```: Sink会等待所有Kafka buffer中未完成的数据在checkpoint中被Kafka producer确认。当Kafka broker出现问题时，不会出现数据丢失，但是当Flink任务重启时，可能由于对老数据的重新处理而出现重复数据。
- ```DeliveryGuarantee.EXACTLY_ONCE```: 在这个模式下，KafkaSink会通过Kafka事务来写入消息，每个事务会在checkpoint完成后进行提交。如果consumer只读取已提交的数据（查看Kafka consumer配置isolation.level），在Flink重启后也不会读到重复数据。然而这些延迟的数据在下一次checkpoint后才可见，需要调整checkpoint的间隔时间来调节及时性。请确保同一个Kafka集群上运行的Flink任务使用唯一的transactionalIdPrefix，这样多个运行中的任务才能保证在事务中不会互相影响。此外，建议调整Kafka事务超时时间(查看Kafka producer transaction.timeout.ms) >> 最大checkpoint持续时间 + 最大重启时间，否则当Kafka因过期丢弃了一个未提交的事务时会导致数据丢失。

## 监控

Kafka sink 在各自的[scope]({{< ref "docs/ops/metrics" >}}/#scope)中提供以下监控指标。

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
        <th rowspan="1">Operator</th>
        <td>currentSendTime</td>
        <td>n/a</td>
        <td>发送最新一条数据的时间。这个指标记录了发送最近一条数据的瞬时值。</td>
        <td>Gauge</td>
    </tr>
  </tbody>
</table>

## Kafka Producer

{{< hint warning >}}
`FlinkKafkaProducer`已经被弃用，并将在1.15版本移除，请改为使用`KafkaSink`。
{{< /hint >}}

以前版本的实现可以查看Flink 1.13 <a href="https://nightlies.apache.org/flink/flink-docs-release-1.13/docs/connectors/datastream/kafka/#kafka-producer">文档</a>.

<a name="kafka-connector-metrics"></a>

## Kafka 连接器指标

Flink 的 Kafka 连接器通过 Flink 的 [metric 系统]({{< ref "docs/ops/metrics" >}}) 提供一些指标来分析 Kafka Connector 的状况。Producer 通过 Flink 的 metrics 系统为所有支持的版本导出 Kafka 的内部指标。Kafka 在其[文档](http://kafka.apache.org/documentation/#selector_monitoring)中列出了所有导出的指标。

Kafka指标的发送可以通过对KafkaSource设置[指标章节]({{< relref "#kafka-connector-metrics" >}})列举的`register.consumer.metrics`配置，或者在使用KafkaSink时设置producer的`register.producer.metrics`属性为`false`来进行关闭。

除了这些指标之外，所有 consumer 都暴露了每个主题分区的 `current-offsets` 和 `committed-offsets`。`current-offsets` 是指分区中的当前偏移量。指的是我们成功检索和发出的最后一个元素的偏移量。`committed-offsets` 是最后提交的偏移量。这为用户提供了 at-least-once 语义，用于提交给 Zookeeper 或 broker 的偏移量。对于 Flink 的偏移检查点，系统提供精准一次语义。

提交给 ZK 或 broker 的偏移量也可以用来跟踪 Kafka consumer 的读取进度。每个分区中提交的偏移量和最近偏移量之间的差异称为 *consumer lag*。如果 Flink 拓扑消耗来自 topic 的数据的速度比添加新数据的速度慢，那么延迟将会增加，consumer 将会滞后。对于大型生产部署，我们建议监视该指标，以避免增加延迟。

<a name="enabling-kerberos-authentication"></a>

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

{{< hint info >}}
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