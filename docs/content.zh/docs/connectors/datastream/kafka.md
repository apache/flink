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

Flink 提供了 [Apache Kafka](https://kafka.apache.org) 连接器，用于从 Kafka topic 中读取或者向其中写入数据，可提供精确一次的处理语义。

<a name="dependency"></a>

## 依赖

Apache Flink 集成了通用的 Kafka 连接器，它会尽力与 Kafka client 的最新版本保持同步。该连接器使用的 Kafka client 版本可能会在 Flink 版本之间发生变化。
当前 Kafka client 向后兼容 0.10.0 或更高版本的 Kafka broker。
有关 Kafka 兼容性的更多细节，请参考  [Kafka 官方文档](https://kafka.apache.org/protocol.html#protocol_compatibility)。

{{< artifact flink-connector-kafka withScalaVersion >}}

Flink 目前的流连接器还不是二进制发行版的一部分。
[在此处]({{< ref "docs/dev/datastream/project-configuration" >}})可以了解到如何链接它们，从而在集群中运行。

<a name="kafka-consumer"></a>

## Kafka Consumer

Flink 的 Kafka consumer 称为 `FlinkKafkaConsumer`。它提供对一个或多个 Kafka topics 的访问。

构造函数接受以下参数：

1. Topic 名称或者名称列表
2. 用于反序列化 Kafka 数据的 DeserializationSchema 或者 KafkaDeserializationSchema
3. Kafka 消费者的属性。需要以下属性：
  - "bootstrap.servers"（以逗号分隔的 Kafka broker 列表）
  - "group.id" 消费组 ID

{{< tabs "fdf41307-604d-426f-9863-666250ce0cdc" >}}
{{< tab "Java" >}}
```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
    .addSource(new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
val stream = env
    .addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties))
```
{{< /tab >}}
{{< /tabs >}}

<a name="the-deserializationschema"></a>

### `DeserializationSchema`

Flink Kafka Consumer 需要知道如何将 Kafka 中的二进制数据转换为 Java 或者 Scala 对象。`KafkaDeserializationSchema` 允许用户指定这样的 schema，每条 Kafka 中的消息会调用 `T deserialize(ConsumerRecord<byte[], byte[]> record)` 反序列化。

为了方便使用，Flink 提供了以下几种 schemas：

1. `TypeInformationSerializationSchema`（和 `TypeInformationKeyValueSerializationSchema`) 基于 Flink 的 `TypeInformation` 创建 `schema`。
    如果该数据的读和写都发生在 Flink 中，那么这将是非常有用的。此 schema 是其他通用序列化方法的高性能 Flink 替代方案。

2. `JsonDeserializationSchema`（和 `JSONKeyValueDeserializationSchema`）将序列化的 JSON 转化为 ObjectNode 对象，可以使用 `objectNode.get("field").as(Int/String/...)()` 来访问某个字段。
    KeyValue objectNode 包含一个含所有字段的 key 和 values 字段，以及一个可选的"metadata"字段，可以访问到消息的 offset、partition、topic 等信息。

3. `AvroDeserializationSchema` 使用静态提供的 schema 读取 Avro 格式的序列化数据。
    它能够从 Avro 生成的类（`AvroDeserializationSchema.forSpecific(...)`）中推断出 schema，或者可以与 `GenericRecords`
    一起使用手动提供的 schema（用 `AvroDeserializationSchema.forGeneric(...)`）。此反序列化 schema 要求序列化记录不能包含嵌入式架构！

    - 此模式还有一个版本，可以在 [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html) 中查找编写器的 schema（用于编写记录的 schema）。
    - 使用这些反序列化 schema 记录将读取从 schema 注册表检索到的 schema 转换为静态提供的 schema（或者通过 `ConfluentRegistryAvroDeserializationSchema.forGeneric(...)` 或 `ConfluentRegistryAvroDeserializationSchema.forSpecific(...)`）。
    
    - 您还可以使用AWS实现的[AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)来查找编写器的 schema 。相似地，反序列化的记录会读取从 AWS Glue Schema Registry 检索到的 schema 并转换为静态提供的 schema
     （或者通过 `GlueSchemaRegistryAvroDeserializationSchema.forGeneric(...)` 或 `GlueSchemaRegistryAvroDeserializationSchema.forSpecific(...)`）。有关 AWS Glue Schema Registry 与 Apache Flink 适配的更多信息，请参见
      [Use Case: Amazon Kinesis Data Analytics for Apache Flink](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kinesis-data-analytics-apache-flink).

    <br>要使用此反序列化 schema 必须添加以下依赖：

{{< tabs "28c9b976-d85a-4d98-ad0b-7ca427c85b57" >}}
{{< tab "AvroDeserializationSchema" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro</artifactId>
  <version>{{< version >}}</version>
</dependency>
```
{{< /tab >}}
{{< tab "ConfluentRegistryAvroDeserializationSchema" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro-confluent-registry</artifactId>
  <version>{{< version >}}</version>
</dependency>
```
{{< /tab >}}
{{< tab "GlueSchemaRegistryAvroDeserializationSchema" >}}
```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro-glue-schema-registry</artifactId>
  <version>{{< version >}}</version>
</dependency>
```
{{< /tab >}}
{{< /tabs >}}

当遇到因一些原因而无法反序列化的损坏消息时，反序列化 schema 会返回 `null`，以允许 Flink Kafka 消费者悄悄地跳过损坏的消息。请注意，由于 consumer 的容错能力（请参阅下面的部分以获取更多详细信息），在损坏的消息上失败作业将使 consumer 尝试再次反序列化消息。因此，如果反序列化仍然失败，则 consumer 将在该损坏的消息上进入不间断重启和失败的循环。

<a name="kafka-consumers-start-position-configuration"></a>

### 配置 Kafka Consumer 开始消费的位置 

Flink Kafka Consumer 允许通过配置来确定 Kafka 分区的起始位置。

{{< tabs "dd71055b-6b2d-4e61-8c4b-5e93aeaf939a" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(...);
myConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始
myConsumer.setStartFromLatest();       // 从最新的记录开始
myConsumer.setStartFromTimestamp(...); // 从指定的时间开始（毫秒）
myConsumer.setStartFromGroupOffsets(); // 默认的方法

DataStream<String> stream = env.addSource(myConsumer);
...
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val myConsumer = new FlinkKafkaConsumer[String](...)
myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
myConsumer.setStartFromLatest()        // 从最新的记录开始
myConsumer.setStartFromTimestamp(...)  // 从指定的时间开始（毫秒）
myConsumer.setStartFromGroupOffsets()  // 默认的方法

val stream = env.addSource(myConsumer)
...
```
{{< /tab >}}
{{< /tabs >}}

Flink Kafka Consumer 的所有版本都具有上述明确的起始位置配置方法。

 * `setStartFromGroupOffsets`（默认方法）：从 Kafka brokers 中的 consumer 组（consumer 属性中的 `group.id` 设置）提交的偏移量中开始读取分区。
    如果找不到分区的偏移量，那么将会使用配置中的 `auto.offset.reset` 设置。
 * `setStartFromEarliest()` 或者 `setStartFromLatest()`：从最早或者最新的记录开始消费，在这些模式下，Kafka 中的 committed offset 将被忽略，不会用作起始位置。
 * `setStartFromTimestamp(long)`：从指定的时间戳开始。对于每个分区，其时间戳大于或等于指定时间戳的记录将用作起始位置。如果一个分区的最新记录早于指定的时间戳，则只从最新记录读取该分区数据。在这种模式下，Kafka 中的已提交 offset 将被忽略，不会用作起始位置。

你也可以为每个分区指定 consumer 应该开始消费的具体 offset：

{{< tabs "3fc8a5ad-77df-4ebb-bc02-d954d1eb29a7" >}}
{{< tab "Java" >}}
```java
Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);

myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)

myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
```
{{< /tab >}}
{{< /tabs >}}

上面的例子中使用的配置是指定从 `myTopic` 主题的 0 、1 和 2 分区的指定偏移量开始消费。offset 值是 consumer 应该为每个分区读取的下一条消息。请注意：如果 consumer 需要读取在提供的 offset 映射中没有指定 offset 的分区，那么它将回退到该特定分区的默认组偏移行为（即 `setStartFromGroupOffsets()`）。


请注意：当 Job 从故障中自动恢复或使用 savepoint 手动恢复时，这些起始位置配置方法不会影响消费的起始位置。在恢复时，每个 Kafka 分区的起始位置由存储在 savepoint 或 checkpoint 中的 offset 确定（有关 checkpointing 的信息，请参阅下一节，以便为 consumer 启用容错功能）。

<a name="kafka-consumers-and-fault-tolerance"></a>

### Kafka Consumer 和容错

伴随着启用 Flink 的 checkpointing 后，Flink Kafka Consumer 将使用 topic 中的记录，并以一致的方式定期检查其所有 Kafka offset 和其他算子的状态。如果 Job 失败，Flink 会将流式程序恢复到最新 checkpoint 的状态，并从存储在 checkpoint 中的 offset 开始重新消费 Kafka 中的消息。

因此，设置 checkpoint 的间隔定义了程序在发生故障时最多需要返回多少。

为了使 Kafka Consumer 支持容错，需要在 [执行环境]({{< ref "docs/deployment/config" >}}#execution-checkpointing-interval) 中启用拓扑的 checkpointing。

如果未启用 checkpoint，那么 Kafka consumer 将定期向 Zookeeper 提交 offset。

<a name="kafka-consumers-topic-and-partition-discovery"></a>

### Kafka Consumer Topic 和分区发现

<a name="partition-discovery"></a>

#### 分区发现

Flink Kafka Consumer 支持发现动态创建的 Kafka 分区，并使用精准一次的语义保证去消耗它们。在初始检索分区元数据之后（即，当 Job 开始运行时）发现的所有分区将从最早可能的 offset 中消费。

默认情况下，是禁用了分区发现的。若要启用它，请在提供的属性配置中为 `flink.partition-discovery.interval-millis` 设置大于 0 的值，表示发现分区的间隔是以毫秒为单位的。

<a name="topic-discovery"></a>

#### Topic 发现

在更高的级别上，Flink Kafka Consumer 还能够使用正则表达式基于 Topic 名称的模式匹配来发现 Topic。请看下面的例子：

{{< tabs "46a10932-ea0f-4cba-aa4e-e12930963406" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(
    java.util.regex.Pattern.compile("test-topic-[0-9]"),
    new SimpleStringSchema(),
    properties);

DataStream<String> stream = env.addSource(myConsumer);
...
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val myConsumer = new FlinkKafkaConsumer[String](
  java.util.regex.Pattern.compile("test-topic-[0-9]"),
  new SimpleStringSchema,
  properties)

val stream = env.addSource(myConsumer)
...
```
{{< /tab >}}
{{< /tabs >}}

在上面的例子中，当 Job 开始运行时，Consumer 将订阅名称与指定正则表达式匹配的所有主题（以 `test-topic` 开头并以单个数字结尾）。

要允许 consumer 在作业开始运行后发现动态创建的主题，那么请为 `flink.partition-discovery.interval-millis` 设置非负值。这允许 consumer 发现名称与指定模式匹配的新主题的分区。

<a name="kafka-consumers-offset-committing-behaviour-configuration"></a>

### Kafka Consumer 提交 Offset 的行为配置

Flink Kafka Consumer 允许有配置如何将 offset 提交回 Kafka broker 的行为。请注意：Flink Kafka Consumer 不依赖于提交的 offset 来实现容错保证。提交的 offset 只是一种方法，用于公开 consumer 的进度以便进行监控。

配置 offset 提交行为的方法是否相同，取决于是否为 job 启用了 checkpointing。

 - *禁用 Checkpointing：* 如果禁用了 checkpointing，则 Flink Kafka Consumer 依赖于内部使用的 Kafka client 自动定期 offset 提交功能。
 因此，要禁用或启用 offset 的提交，只需将 `enable.auto.commit` 或者 `auto.commit.interval.ms` 的Key 值设置为提供的 `Properties` 配置中的适当值。

 - *启用 Checkpointing：* 如果启用了 checkpointing，那么当 checkpointing 完成时，Flink Kafka Consumer 将提交的 offset 存储在 checkpoint 状态中。
 这确保 Kafka broker 中提交的 offset 与 checkpoint 状态中的 offset 一致。
 用户可以通过调用 consumer 上的 `setCommitOffsetsOnCheckpoints(boolean)` 方法来禁用或启用 offset 的提交(默认情况下，这个值是 true )。
 注意，在这个场景中，`Properties` 中的自动定期 offset 提交设置会被完全忽略。

<a name="kafka-consumers-and-timestamp-extractionwatermark-emission"></a>

### Kafka Consumer 和 时间戳抽取以及 watermark 发送

在许多场景中，记录的时间戳是(显式或隐式)嵌入到记录本身中。此外，用户可能希望定期或以不规则的方式 Watermark，例如基于 Kafka 流中包含当前事件时间的 watermark 的特殊记录。对于这些情况，Flink Kafka Consumer 允许指定 `AssignerWithPeriodicWatermarks` 或 `AssignerWithPunctuatedWatermarks`。

你可以按照[此处]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}})的说明指定自定义时间戳抽取器或者 Watermark 发送器，或者使用 [内置的]({{< ref "docs/dev/datastream/event-time/built_in" >}})。你也可以通过以下方式将其传递给你的 consumer：

{{< tabs "c706ebfc-5d9b-49b2-8899-f3ac259a55cc" >}}
{{< tab "Java" >}}
```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer<String> myConsumer =
    new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(20)));

DataStream<String> stream = env.addSource(myConsumer);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val myConsumer =
    new FlinkKafkaConsumer("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy
        .forBoundedOutOfOrderness(Duration.ofSeconds(20)))

val stream = env.addSource(myConsumer)
```
{{< /tab >}}
{{< /tabs >}}

**请注意**：如果 watermark assigner 依赖于从 Kafka 读取的消息来上涨其 watermark （通常就是这种情况），那么所有主题和分区都需要有连续的消息流。否则，整个应用程序的 watermark 将无法上涨，所有基于时间的算子（例如时间窗口或带有计时器的函数）也无法运行。单个的 Kafka 分区也会导致这种反应。考虑设置适当的 [idleness timeouts]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#dealing-with-idle-sources) 来缓解这个问题。

<a name="kafka-producer"></a>

## Kafka Producer

Flink Kafka Producer 被称为 `FlinkKafkaProducer`。它允许将消息流写入一个或多个 Kafka topic。

构造器接收下列参数：

1. 事件被写入的默认输出 topic
2. 序列化数据写入 Kafka 的 SerializationSchema / KafkaSerializationSchema
3. Kafka client 的 Properties。下列 property 是必须的：
	* “bootstrap.servers” （逗号分隔 Kafka broker 列表）
4. 容错语义

{{< tabs "f6c1b77e-6b17-4fd3-837a-c9257e6c7c00" >}}
{{< tab "Java" >}}
```java
env.fromSource(kafkaSource, new CustomWatermarkStrategy(), "Kafka Source With Custom Watermark Strategy")
```
[这篇文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}})描述了如何自定义水印策略（```WatermarkStrategy```）。

### 空闲
如果并行度高于分区数，Kafka Source 不会自动进入空闲状态。您将需要降低并行度或向水印策略添加空闲超时。如果在这段时间内没有记录在流的分区中流动，则该分区被视为“空闲”并且不会阻止下游操作符中水印的进度。
[这篇文档]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}#dealing-with-idle-sources) 描述了有关如何定义 ```WatermarkStrategy#withIdleness``` 的详细信息.

### 消费位点提交
Kafka source 在 checkpoint **完成**时提交当前的消费位点 ，以保证 Flink 的 checkpoint 状态和 Kafka broker 上的提交位点一致。如果未开启 
checkpoint，Kafka source 依赖于 Kafka consumer 内部的位点定时自动提交逻辑，自动提交功能由 ```enable.auto.commit``` 和 
```auto.commit.interval.ms``` 两个 Kafka consumer 配置项进行配置。

注意：Kafka source **不依赖**于 broker 上提交的位点来恢复失败的作业。提交位点只是为了上报 Kafka consumer 和消费组的消费进度，以在 broker 端进行监控。

### 监控
DataStream<String> stream = ...;

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");

FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<String>(
        "my-topic",                  // 目标 topic
        new SimpleStringSchema(),    // 序列化 schema
        properties,                  // producer 配置
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // 容错

stream.addSink(myProducer);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val stream: DataStream[String] = ...

val properties = new Properties
properties.setProperty("bootstrap.servers", "localhost:9092")

val myProducer = new FlinkKafkaProducer[String](
        "my-topic",               // 目标 topic
        new SimpleStringSchema(), // 序列化 schema
        properties,               // producer 配置
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE) // 容错

stream.addSink(myProducer)
```
{{< /tab >}}
{{< /tabs >}}

除此之外，我们还可以使用下列配置方法：

- {{< javadoc name="setWriteTimestampToKafka(boolean writeTimestampToKafka)" file="org/apache/flink/streaming/connectors/kafka/FlinkKafkaProducer.html#setWriteTimestampToKafka-boolean-" >}}，给每条记录设置时间戳
- {{< javadoc name="setLogFailuresOnly(boolean logFailuresOnly)" file="org/apache/flink/streaming/connectors/kafka/FlinkKafkaProducer.html#setLogFailuresOnly-boolean-" >}}，设置是否在 Producer 发生异常时仅仅记录日志
- {{< javadoc name="setTransactionalIdPrefix(String transactionalIdPrefix)" file="org/apache/flink/streaming/connectors/kafka/FlinkKafkaProducer.html#setTransactionalIdPrefix-java.lang.String-" >}}，设置自定义的 `transactional.id` 前缀
- {{< javadoc name="ignoreFailuresAfterTransactionTimeout()" file="org/apache/flink/streaming/connectors/kafka/FlinkKafkaProducer.html#ignoreFailuresAfterTransactionTimeout--" >}}，在恢复时忽略事务超时异常

<a name="the-serializationschema"></a>

## `SerializationSchema`

Flink Kafka Producer 需要知道如何将 Java/Scala 对象转化为二进制数据。

`KafkaSerializationSchema` 允许用户指定这样的 schema。它会为每个记录调用 `ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp)` 方法，产生一个写入到 Kafka 的 `ProducerRecord`。

用户可以对如何将数据写到 Kafka 进行细粒度的控制。你可以通过 producer record：

* 设置 header 值
* 为每个 record 定义 key
* 指定数据的自定义分区

<a name="kafka-producers-and-fault-tolerance"></a>

### Kafka Producer 和容错

启用 Flink 的 checkpointing 后，`FlinkKafkaProducer` 可以提供精确一次的语义保证。

除了启用 Flink 的 checkpointing，你也可以通过将适当的 `semantic` 参数传递给 `FlinkKafkaProducer` 来选择三种不同的操作模式：

* `Semantic.NONE`：Flink 不会有任何语义的保证，产生的记录可能会丢失或重复。
* `Semantic.AT_LEAST_ONCE`（默认设置）：可以保证不会丢失任何记录（但是记录可能会重复）
* `Semantic.EXACTLY_ONCE`：使用 Kafka 事务提供精确一次语义。无论何时，在使用事务写入 Kafka 时，都要记得为所有消费 Kafka 消息的应用程序设置所需的 `isolation.level`（`read_committed` 或 `read_uncommitted` - 后者是默认值）。

<a name="caveats"></a>

##### 注意事项

`Semantic.EXACTLY_ONCE` 模式依赖于事务提交的能力。事务提交发生于触发 checkpoint 之前，以及从 checkpoint 恢复之后。如果从 Flink 应用程序崩溃到完全重启的时间超过了 Kafka 的事务超时时间，那么将会有数据丢失（Kafka 会自动丢弃超出超时时间的事务）。考虑到这一点，请根据预期的宕机时间来合理地配置事务超时时间。

默认情况下，Kafka broker 将 `transaction.max.timeout.ms` 设置为 15 分钟。此属性不允许为大于其值的 producer 设置事务超时时间。
默认情况下，`FlinkKafkaProducer` 将 producer config 中的 `transaction.timeout.ms` 属性设置为 1 小时，因此在使用 `Semantic.EXACTLY_ONCE` 模式之前应该增加 `transaction.max.timeout.ms` 的值。

在 `KafkaConsumer` 的 `read_committed` 模式中，任何未结束（既未中止也未完成）的事务将阻塞来自给定 Kafka topic 的未结束事务之后的所有读取数据。
换句话说，在遵循如下一系列事件之后：

1. 用户启动了 `transaction1` 并使用它写了一些记录
2. 用户启动了 `transaction2` 并使用它编写了一些其他记录
3. 用户提交了 `transaction2`

即使 `transaction2` 中的记录已提交，在提交或中止 `transaction1` 之前，消费者也不会看到这些记录。这有 2 层含义：

 * 首先，在 Flink 应用程序的正常工作期间，用户可以预料 Kafka 主题中生成的记录的可见性会延迟，相当于已完成 checkpoint 之间的平均时间。
 * 其次，在 Flink 应用程序失败的情况下，此应用程序正在写入的供消费者读取的主题将被阻塞，直到应用程序重新启动或配置的事务超时时间过去后，才恢复正常。此标注仅适用于有多个 agent 或者应用程序写入同一 Kafka 主题的情况。

**注意**：`Semantic.EXACTLY_ONCE` 模式为每个 `FlinkKafkaProducer` 实例使用固定大小的 KafkaProducer 池。每个 checkpoint 使用其中一个 producer。如果并发 checkpoint 的数量超过池的大小，`FlinkKafkaProducer` 将抛出异常，并导致整个应用程序失败。请合理地配置最大池大小和最大并发 checkpoint 数量。

**注意**：`Semantic.EXACTLY_ONCE` 会尽一切可能不留下任何逗留的事务，否则会阻塞其他消费者从这个 Kafka topic 中读取数据。但是，如果 Flink 应用程序在第一次 checkpoint 之前就失败了，那么在重新启动此类应用程序后，系统中不会有先前池大小（pool size）相关的信息。因此，在第一次 checkpoint 完成前对 Flink 应用程序进行缩容，且并发数缩容倍数大于安全系数 `FlinkKafkaProducer.SAFE_SCALE_DOWN_FACTOR` 的值的话，是不安全的。同样，在这种情况使用 `setTransactionalIdPrefix()` 改变 `transactional.id` 也是不安全的，因为系统也不知道先前使用的 `transactional.id` 前缀。

<a name="kafka-connector-metrics"></a>

## Kafka 连接器指标

Flink 的 Kafka 连接器通过 Flink 的 [metric 系统]({{< ref "docs/ops/metrics" >}}) 提供一些指标来分析 Kafka Connector 的状况。Producer 通过 Flink 的 metrics 系统为所有支持的版本导出 Kafka 的内部指标。consumer 从 Kafka 0.10 版本开始导出所有指标。Kafka 在其[文档](http://kafka.apache.org/documentation/#selector_monitoring)中列出了所有导出的指标。

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

有关 Kerberos 安全性 Flink 配置的更多信息，请参见[这里]({{< ref "docs/deployment/config" >}})。你也可以在[这里]({{< ref "docs/deployment/security/security-kerberos" >}})进一步了解 Flink 如何在内部设置基于 kerberos 的安全性。

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
