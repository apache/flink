---
title: "Apache Kafka 连接器"
nav-title: Kafka
nav-parent_id: connectors
nav-pos: 1
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

* This will be replaced by the TOC
{:toc}

此连接器提供了访问 [Apache Kafka](https://kafka.apache.org/) 事件流的服务。

Flink 提供了专门的 Kafka 连接器，向 Kafka topic 中读取或者写入数据。Flink Kafka Consumer 集成了 Flink 的 Checkpoint 机制，可提供 exactly-once 的处理语义。为此，Flink 并不完全依赖于跟踪 Kafka 消费组的偏移量，而是在内部跟踪和检查偏移量。

根据你的用例和环境选择相应的包（maven artifact id）和类名。对于大多数用户来说，使用 `FlinkKafkaConsumer010`（ `flink-connector-kafka` 的一部分）是比较合适的。

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">Maven 依赖</th>
      <th class="text-left">自从哪个版本<br>开始支持</th>
      <th class="text-left">消费者和<br>生产者的类名称</th>
      <th class="text-left">Kafka 版本</th>
      <th class="text-left">注意</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>flink-connector-kafka-0.10{{ site.scala_version_suffix }}</td>
        <td>1.2.0</td>
        <td>FlinkKafkaConsumer010<br>
        FlinkKafkaProducer010</td>
        <td>0.10.x</td>
        <td>这个连接器支持 <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message">带有时间戳的 Kafka 消息</a>，用于生产和消费。</td>
    </tr>
    <tr>
        <td>flink-connector-kafka-0.11{{ site.scala_version_suffix }}</td>
        <td>1.4.0</td>
        <td>FlinkKafkaConsumer011<br>
        FlinkKafkaProducer011</td>
        <td>0.11.x</td>
        <td>Kafka 从 0.11.x 版本开始不支持 Scala 2.10。此连接器支持了 <a href="https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging">Kafka 事务性的消息传递</a>来为生产者提供 Exactly once 语义。</td>
    </tr>
    <tr>
        <td>flink-connector-kafka{{ site.scala_version_suffix }}</td>
        <td>1.7.0</td>
        <td>FlinkKafkaConsumer<br>
        FlinkKafkaProducer</td>
        <td>>= 1.0.0</td>
        <td>
        这个通用的 Kafka 连接器尽力与 Kafka client 的最新版本保持同步。该连接器使用的 Kafka client 版本可能会在 Flink 版本之间发生变化。从 Flink 1.9 版本开始，它使用 Kafka 2.2.0 client。当前 Kafka 客户端向后兼容 0.10.0 或更高版本的 Kafka broker。
        但是对于 Kafka 0.11.x 和 0.10.x 版本，我们建议你分别使用专用的 flink-connector-kafka-0.11{{ site.scala_version_suffix }} 和 flink-connector-kafka-0.10{{ site.scala_version_suffix }} 连接器。
        </td>
    </tr>
  </tbody>
</table>

接着，在你的 maven 项目中导入连接器：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

请注意：目前流连接器还不是二进制分发的一部分。
[在此处]({{ site.baseurl }}/zh/dev/project-configuration.html)可以了解到如何链接它们以实现在集群中执行。

## 安装 Apache Kafka

* 按照 [ Kafka 快速入门](https://kafka.apache.org/documentation.html#quickstart)的说明下载代码并启动 Kafka 服务器（每次启动应用程序之前都需要启动 Zookeeper 和 Kafka server）。
* 如果 Kafka 和 Zookeeper 服务器运行在远端机器上，那么必须要将 `config/server.properties` 文件中的 `advertised.host.name`属性设置为远端设备的 IP 地址。

## Kafka 1.0.0+ 连接器

从 Flink 1.7 开始，有一个新的通用 Kafka 连接器，它不跟踪特定的 Kafka 主版本。相反，它是在 Flink 发布时跟踪最新版本的 Kafka。
如果你的 Kafka broker 版本是 1.0.0 或 更新的版本，你应该使用这个 Kafka 连接器。
如果你使用的是 Kafka 的旧版本( 0.11 或 0.10 )，那么你应该使用与 Kafka broker 版本相对应的连接器。

### 兼容性

通过 Kafka client API 和 broker 的兼容性保证，通用的 Kafka 连接器兼容较旧和较新的 Kafka broker。
它兼容 Kafka broker 0.11.0 或者更高版本，具体兼容性取决于所使用的功能。有关 Kafka 兼容性的详细信息，请参考 [Kafka 文档](https://kafka.apache.org/protocol.html#protocol_compatibility)。

### 将 Kafka Connector 从 0.11 迁移到通用版本

以便执行迁移，请参考 [升级 Jobs 和 Flink 版本指南]({{ site.baseurl }}/zh/ops/upgrading.html)：
* 在全程中使用 Flink 1.9 或更新版本。
* 不要同时升级 Flink 和 Operator。
* 确保你的 Job 中所使用的 Kafka Consumer 和 Kafka Producer 分配了唯一的标识符（uid）。
* 使用 stop with savepoint 的特性来执行 savepoint（例如，使用 `stop --withSavepoint`）[CLI 命令]({{ site.baseurl }}/zh/ops/cli.html)。

### 用法

要使用通用的 Kafka 连接器，请为它添加依赖关系：

{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-kafka{{ site.scala_version_suffix }}</artifactId>
  <version>{{ site.version }}</version>
</dependency>
{% endhighlight %}

然后，实例化 source（ `FlinkKafkaConsumer`）和 sink（ `FlinkKafkaProducer`）。除了从模块和类名中删除了特定的 Kafka 版本外，这个 API 向后兼容 Kafka 0.11 版本的 connector。

## Kafka Consumer

Flink 的 Kafka consumer 称为 `FlinkKafkaConsumer010`（或适用于 Kafka 0.11.0.x 版本的 `FlinkKafkaConsumer011`，或适用于 Kafka >= 1.0.0 的版本的 `FlinkKafkaConsumer`）。它提供对一个或多个 Kafka topics 的访问。

构造函数接受以下参数：

1. Topic 名称或者名称列表
2. 用于反序列化 Kafka 数据的 DeserializationSchema 或者 KafkaDeserializationSchema
3. Kafka 消费者的属性。需要以下属性：
  - "bootstrap.servers"（以逗号分隔的 Kafka broker 列表）
  - "group.id" 消费组 ID

示例：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");
DataStream<String> stream = env
  .addSource(new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), properties));
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")
stream = env
    .addSource(new FlinkKafkaConsumer010[String]("topic", new SimpleStringSchema(), properties))
    .print()
{% endhighlight %}
</div>
</div>

### `DeserializationSchema`

Flink Kafka Consumer 需要知道如何将 Kafka 中的二进制数据转换为 Java 或者 Scala 对象。`DeserializationSchema` 允许用户指定这样的 schema，为每条 Kafka 消息调用 `T deserialize(byte[] message)` 方法，传递来自 Kafka 的值。

从 `AbstractDeserializationSchema` 开始通常很有帮助，它负责将生成的 Java 或 Scala 类型描述为 Flink 的类型系统。
用户如果要自己去实现一个`DeserializationSchema`，需要自己去实现 `getProducedType(...)`方法。

为了访问 Kafka 消息的 key、value 和元数据，`KafkaDeserializationSchema` 具有以下反序列化方法 `T deserialize(ConsumerRecord<byte[], byte[]> record)`。

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

    <br>要使用此反序列化 schema 必须添加以下依赖：

<div class="codetabs" markdown="1">
<div data-lang="AvroDeserializationSchema" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
<div data-lang="ConfluentRegistryAvroDeserializationSchema" markdown="1">
{% highlight xml %}
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-avro-confluent-registry</artifactId>
  <version>{{site.version }}</version>
</dependency>
{% endhighlight %}
</div>
</div>

当遇到因一些原因而无法反序列化的损坏消息时，这里有两个选项 - 从 `deserialize（...）` 方法抛出异常会导致作业失败并重新启动，或返回 `null`，以允许 Flink Kafka 消费者悄悄地跳过损坏的消息。请注意，由于 Consumer 的容错能力（请参阅下面的部分以获取更多详细信息），在损坏的消息上失败作业将使 consumer 尝试再次反序列化消息。因此，如果反序列化仍然失败，则 consumer 将在该损坏的消息上进入不间断重启和失败的循环。

### 配置 Kafka Consumer 开始消费的位置 

Flink Kafka Consumer 允许通过配置来确定 Kafka 分区的起始位置。

例如：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

FlinkKafkaConsumer010<String> myConsumer = new FlinkKafkaConsumer010<>(...);
myConsumer.setStartFromEarliest();     // 尽可能从最早的记录开始
myConsumer.setStartFromLatest();       // 从最新的记录开始
myConsumer.setStartFromTimestamp(...); // 从指定的时间开始（毫秒）
myConsumer.setStartFromGroupOffsets(); // 默认的方法

DataStream<String> stream = env.addSource(myConsumer);
...
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val myConsumer = new FlinkKafkaConsumer010[String](...)
myConsumer.setStartFromEarliest()      // 尽可能从最早的记录开始
myConsumer.setStartFromLatest()        // 从最新的记录开始
myConsumer.setStartFromTimestamp(...)  // 从指定的时间开始（毫秒）
myConsumer.setStartFromGroupOffsets()  // 默认的方法

val stream = env.addSource(myConsumer)
...
{% endhighlight %}
</div>
</div>

Flink Kafka Consumer 的所有版本都具有上述明确的起始位置配置方法。

 * `setStartFromGroupOffsets`（默认方法）：从 Kafka brokers 中的 consumer 组（consumer 属性中的 `group.id` 设置）提交的偏移量中开始读取分区。
  如果找不到分区的偏移量，那么将会使用配置中的 `auto.offset.reset` 设置。
 * `setStartFromEarliest()` 或者 `setStartFromLatest()`：从最早或者最新的记录开始消费，在这些模式下，Kafka 中的 committed offset 将被忽略，不会用作起始位置。
 * `setStartFromTimestamp(long)`：从指定的时间戳开始。对于每个分区，其时间戳大于或等于指定时间戳的记录将用作起始位置。如果一个分区的最新记录早于指定的时间戳，则只从最新记录读取该分区数据。在这种模式下，Kafka 中的已提交 offset 将被忽略，不会用作起始位置。

你也可以为每个分区指定 consumer 应该开始消费的具体 offset：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L);
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L);
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L);

myConsumer.setStartFromSpecificOffsets(specificStartOffsets);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val specificStartOffsets = new java.util.HashMap[KafkaTopicPartition, java.lang.Long]()
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 0), 23L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 1), 31L)
specificStartOffsets.put(new KafkaTopicPartition("myTopic", 2), 43L)

myConsumer.setStartFromSpecificOffsets(specificStartOffsets)
{% endhighlight %}
</div>
</div>

上面的例子中使用的配置是指定从 `myTopic` 主题的 0 、1 和 2 分区的指定偏移量开始消费。offset 值是 consumer 应该为每个分区读取的下一条消息。请注意：如果 consumer 需要读取在提供的 offset 映射中没有指定 offset 的分区，那么它将回退到该特定分区的默认组偏移行为（即 `setStartFromGroupOffsets()`）。


请注意：当 Job 从故障中自动恢复或使用 savepoint 手动恢复时，这些起始位置配置方法不会影响消费的起始位置。在恢复时，每个 Kafka 分区的起始位置由存储在 savepoint 或 checkpoint 中的 offset 确定（有关 checkpointing 的信息，请参阅下一节，以便为 consumer 启用容错功能）。

### Kafka Consumer 和容错

伴随着启用 Flink 的 checkpointing 后，Flink Kafka Consumer 将使用 topic 中的记录，并以一致的方式定期检查其所有 Kafka offset 和其他算子的状态。如果 Job 失败，Flink 会将流式程序恢复到最新 checkpoint 的状态，并从存储在 checkpoint 中的 offset 开始重新消费 Kafka 中的消息。

因此，设置 checkpoint 的间隔定义了程序在发生故障时最多需要返回多少。

要使用容错的 Kafka Consumer，需要在执行环境中启用拓扑的 checkpointing。

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // 每隔 5000 毫秒 执行一次 checkpoint
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // 每隔 5000 毫秒 执行一次 checkpoint
{% endhighlight %}
</div>
</div>

另请注意，只有当可用的 slots 足够时，Flink 才能重新启动。因此，如果拓扑由于丢失了 TaskManager 而失败，那么之后必须要一直有足够可用的 solt。Flink on YARN 支持自动重启丢失的 YARN 容器。

如果未启用 checkpoint，那么 Kafka consumer 将定期向 Zookeeper 提交 offset。

### Kafka Consumer Topic 和分区发现

#### 分区发现

Flink Kafka Consumer 支持发现动态创建的 Kafka 分区，并使用精准一次的语义保证去消耗它们。在初始检索分区元数据之后（即，当 Job 开始运行时）发现的所有分区将从最早可能的 offset 中消费。

默认情况下，是禁用了分区发现的。若要启用它，请在提供的属性配置中为 `flink.partition-discovery.interval-millis` 设置大于 0 的值，表示发现分区的间隔是以毫秒为单位的。

<span class="label label-danger">局限性</span> 当从 Flink 1.3.x 之前的 Flink 版本的 savepoint 恢复 consumer 时，分区发现无法在恢复运行时启用。如果启用了，那么还原将会失败并且出现异常。在这种情况下，为了使用分区发现，请首先在 Flink 1.3.x 中使用 savepoint，然后再从 savepoint 中恢复。

#### Topic 发现

在更高的级别上，Flink Kafka Consumer 还能够使用正则表达式基于 Topic 名称的模式匹配来发现 Topic。请看下面的例子：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer011<String> myConsumer = new FlinkKafkaConsumer011<>(
    java.util.regex.Pattern.compile("test-topic-[0-9]"),
    new SimpleStringSchema(),
    properties);

DataStream<String> stream = env.addSource(myConsumer);
...
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val myConsumer = new FlinkKafkaConsumer010[String](
  java.util.regex.Pattern.compile("test-topic-[0-9]"),
  new SimpleStringSchema,
  properties)

val stream = env.addSource(myConsumer)
...
{% endhighlight %}
</div>
</div>

在上面的例子中，当 Job 开始运行时，Consumer 将订阅名称与指定正则表达式匹配的所有主题（以 `test-topic` 开头并以单个数字结尾）。

要允许 consumer 在作业开始运行后发现动态创建的主题，那么请为 `flink.partition-discovery.interval-millis` 设置非负值。这允许 consumer 发现名称与指定模式匹配的新主题的分区。

### Kafka Consumer 提交 Offset 的行为配置

Flink Kafka Consumer 允许有配置如何将 offset 提交回 Kafka broker 的行为。请注意：Flink Kafka Consumer 不依赖于提交的 offset 来实现容错保证。提交的 offset 只是一种方法，用于公开 consumer 的进度以便进行监控。

配置 offset 提交行为的方法是否相同，取决于是否为 job 启用了 checkpointing。

 - *禁用 Checkpointing：* 如果禁用了 checkpointing，则 Flink Kafka Consumer 依赖于内部使用的 Kafka client 自动定期 offset 提交功能。
 因此，要禁用或启用 offset 的提交，只需将 `enable.auto.commit` 或者 `auto.commit.interval.ms` 的Key 值设置为提供的 `Properties` 配置中的适当值。

 - *启用 Checkpointing：* 如果启用了 checkpointing，那么当 checkpointing 完成时，Flink Kafka Consumer 将提交的 offset 存储在 checkpoint 状态中。
 这确保 Kafka broker 中提交的 offset 与 checkpoint 状态中的 offset 一致。
 用户可以通过调用 consumer 上的 `setCommitOffsetsOnCheckpoints(boolean)` 方法来禁用或启用 offset 的提交(默认情况下，这个值是 true )。
 注意，在这个场景中，`Properties` 中的自动定期 offset 提交设置会被完全忽略。

### Kafka Consumer 和 时间戳抽取以及 watermark 发送

在许多场景中，记录的时间戳是(显式或隐式)嵌入到记录本身中。此外，用户可能希望定期或以不规则的方式 Watermark，例如基于 Kafka 流中包含当前事件时间的 watermark 的特殊记录。对于这些情况，Flink Kafka Consumer 允许指定 `AssignerWithPeriodicWatermarks` 或 `AssignerWithPunctuatedWatermarks`。

你可以按照[此处]({{ site.baseurl }}/zh/dev/event_timestamps_watermarks.html)的说明指定自定义时间戳抽取器或者 Watermark 发送器，或者使用 [内置的]({{ site.baseurl }}/zh/dev/event_timestamp_extractors.html)。你也可以通过以下方式将其传递给你的 consumer：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer010<String> myConsumer =
    new FlinkKafkaConsumer010<>("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter());

DataStream<String> stream = env
  .addSource(myConsumer)
  .print();
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val properties = new Properties()
properties.setProperty("bootstrap.servers", "localhost:9092")
properties.setProperty("group.id", "test")

val myConsumer = new FlinkKafkaConsumer010[String]("topic", new SimpleStringSchema(), properties)
myConsumer.assignTimestampsAndWatermarks(new CustomWatermarkEmitter())
stream = env
    .addSource(myConsumer)
    .print()
{% endhighlight %}
</div>
</div>

在内部，每个 Kafka 分区执行一个 assigner 实例。当指定了这样的 assigner 时，对于从 Kafka 读取的每条消息，调用 `extractTimestamp(T element, long previousElementTimestamp)` 来为记录分配时间戳，并为 `Watermark getCurrentWatermark()`（定期形式）或 `Watermark checkAndGetNextWatermark(T lastElement, long extractedTimestamp)`（打点形式）以确定是否应该发出新的 watermark 以及使用哪个时间戳。

**请注意**：如果 watermark assigner 依赖于从 Kafka 读取的消息来上涨其 watermark (通常就是这种情况)，那么所有主题和分区都需要有连续的消息流。否则，整个应用程序的 watermark 将无法上涨，所有基于时间的算子(例如时间窗口或带有计时器的函数)也无法运行。单个的 Kafka 分区也会导致这种反应。这是一个已在计划中的 Flink 改进，目的是为了防止这种情况发生（请见[FLINK-5479: Per-partition watermarks in FlinkKafkaConsumer should consider idle partitions](https://issues.apache.org/jira/browse/FLINK-5479)）。同时，可能的解决方法是将*心跳消息*发送到所有 consumer 的分区里，从而上涨空闲分区的 watermark。

## Kafka Producer

Flink Kafka Producer 被称为 `FlinkKafkaProducer011`（或适用于 Kafka 0.10.0.x 版本的 `FlinkKafkaProducer010`，或适用于 Kafka >= 1.0.0 版本的 `FlinkKafkaProducer`）。它允许将消息流写入一个或多个 Kafka topic。

示例：

<div class="codetabs" markdown="1">
<div data-lang="java" markdown="1">
{% highlight java %}
DataStream<String> stream = ...;

FlinkKafkaProducer011<String> myProducer = new FlinkKafkaProducer011<String>(
        "localhost:9092",            // broker 列表
        "my-topic",                  // 目标 topic
        new SimpleStringSchema());   // 序列化 schema

// 0.10+ 版本的 Kafka 允许在将记录写入 Kafka 时附加记录的事件时间戳；
// 此方法不适用于早期版本的 Kafka
myProducer.setWriteTimestampToKafka(true);

stream.addSink(myProducer);
{% endhighlight %}
</div>
<div data-lang="scala" markdown="1">
{% highlight scala %}
val stream: DataStream[String] = ...

val myProducer = new FlinkKafkaProducer011[String](
        "localhost:9092",         // broker 列表
        "my-topic",               // 目标 topic
        new SimpleStringSchema)   // 序列化 schema

// 0.10+ 版本的 Kafka 允许在将记录写入 Kafka 时附加记录的事件时间戳；
// 此方法不适用于早期版本的 Kafka
myProducer.setWriteTimestampToKafka(true)

stream.addSink(myProducer)
{% endhighlight %}
</div>
</div>

上面的例子演示了创建 Flink Kafka Producer 来将流消息写入单个 Kafka 目标 topic 的基本用法。
对于更高级的用法，这还有其他构造函数变体允许提供以下内容：

 * *提供自定义属性*：producer 允许为内部 `KafkaProducer` 提供自定义属性配置。有关如何配置 Kafka Producer 的详细信息，请参阅  [Apache Kafka 文档](https://kafka.apache.org/documentation.html)。
 * *自定义分区器*：要将消息分配给特定的分区，可以向构造函数提供一个 `FlinkKafkaPartitioner` 的实现。这个分区器将被流中的每条记录调用，以确定消息应该发送到目标 topic 的哪个具体分区里。有关详细信息，请参阅 [Kafka Producer 分区方案](#kafka-producer-分区方案)。
 * *高级的序列化 schema*：与 consumer 类似，producer 还允许使用名为 `KeyedSerializationSchema` 的高级序列化 schema，该 schema 允许单独序列化 key 和 value。它还允许覆盖目标 topic，以便 producer 实例可以将数据发送到多个 topic。

### Kafka Producer 分区方案

默认情况下，如果没有为 Flink Kafka Producer 指定自定义分区程序，则 producer 将使用 `FlinkFixedPartitioner` 为每个 Flink Kafka Producer 并行子任务映射到单个 Kafka 分区（即，接收子任务接收到的所有消息都将位于同一个 Kafka 分区中）。

可以通过扩展 `FlinkKafkaPartitioner` 类来实现自定义分区程序。所有 Kafka 版本的构造函数都允许在实例化 producer 时提供自定义分区程序。
注意：分区器实现必须是可序列化的，因为它们将在 Flink 节点之间传输。此外，请记住分区器中的任何状态都将在作业失败时丢失，因为分区器不是 producer 的 checkpoint 状态的一部分。

也可以完全避免使用分区器，并简单地让 Kafka 通过其附加 key 写入的消息进行分区（使用提供的序列化 schema 为每条记录确定分区）。
为此，在实例化 producer 时提供 `null` 自定义分区程序，提供 `null` 作为自定义分区器是很重要的; 如上所述，如果未指定自定义分区程序，则默认使用 `FlinkFixedPartitioner`。

### Kafka Producer 和容错

#### Kafka 0.10

启用 Flink 的 checkpointing 后，`FlinkKafkaProducer010` 可以提供至少一次的语义。

除了启用 Flink 的 checkpointing 之外，还应该适当地配置 setter 方法，`setLogFailuresOnly(boolean)` 和 `setFlushOnCheckpoint(boolean)`。

 * `setLogFailuresOnly(boolean)`：默认情况下，此值设置为 `false`。启用这个选项将使 producer 仅记录失败而不是捕获和重新抛出它们。这基本上是记录了成功的记录，即使它从未写入目标 Kafka topic。对 at-least-once 的语义，这个方法必须禁用。
 * `setFlushOnCheckpoint(boolean)`：默认情况下，此值设置为 `true`。启用此功能后，Flink 的 checkpoint 将在 checkpoint 成功之前等待 Kafka 确认 checkpoint 时的任意即时记录。这样可确保 checkpoint 之前的所有记录都已写入 Kafka。对 at-least-once 的语义，这个方法必须启用。

总之，默认情况下，Kafka producer 中，`setLogFailureOnly` 设置为 `false` 及  `setFlushOnCheckpoint` 设置为 `true`  会为 0.10 版本提供 at-least-once 语义。

**注意**：默认情况下，重试次数设置为 0。这意味着当 `setLogFailuresOnly` 设置为 `false` 时，producer 会立即失败，包括 leader 更改。该值默认设置为 0，以避免重试导致目标 topic 中出现重复的消息。对于大多数频繁更改 broker 的生产环境，我们建议将重试次数设置为更高的值。

**注意**：目前还没有 Kafka 的事务 producer，所以 Flink 不能保证写入 Kafka topic 的精准一次语义。

#### Kafka 0.11 和更新的版本

启用 Flink 的 checkpointing 后，`FlinkKafkaProducer011`（适用于 Kafka >= 1.0.0 版本的 `FlinkKafkaProducer`）可以提供精准一次的语义保证。

除了启用 Flink 的 checkpointing，还可以通过将适当的 `semantic` 参数传递给 `FlinkKafkaProducer011`（适用于 Kafka >= 1.0.0 版本的 `FlinkKafkaProducer`）来选择三种不同的操作模式：

 * `Semantic.NONE`：Flink 不会有任何语义的保证，产生的记录可能会丢失或重复。
 * `Semantic.AT_LEAST_ONCE`（默认设置）：类似 `FlinkKafkaProducer010` 中的 `setFlushOnCheckpoint(true)`，这可以保证不会丢失任何记录（虽然记录可能会重复）。
 * `Semantic.EXACTLY_ONCE`：使用 Kafka 事务提供精准一次的语义。无论何时，在使用事务写入 Kafka 时，都要记得为所有消费 Kafka 消息的应用程序设置所需的 `isolation.level`（ `read_committed` 或 `read_uncommitted`  - 后者是默认值）。

##### 注意事项

`Semantic.EXACTLY_ONCE` 模式依赖于事务提交的能力。事务提交发生于触发 checkpoint 之前，以及从 checkpoint 恢复之后。如果从 Flink 应用程序崩溃到完全重启的时间超过了 Kafka 的事务超时时间，那么将会有数据丢失（Kafka 会自动丢弃超出超时时间的事务）。考虑到这一点，请根据预期的宕机时间来合理地配置事务超时时间。

默认情况下，Kafka broker 将 `transaction.max.timeout.ms` 设置为 15 分钟。此属性不允许为大于其值的 producer 设置事务超时时间。
默认情况下，`FlinkKafkaProducer011` 将 producer config 中的 `transaction.timeout.ms` 属性设置为 1 小时，因此在使用 `Semantic.EXACTLY_ONCE` 模式之前应该增加 `transaction.max.timeout.ms` 的值。

在 `KafkaConsumer` 的 `read_committed` 模式中，任何未结束（既未中止也未完成）的事务将阻塞来自给定 Kafka topic 的未结束事务之后的所有读取数据。
换句话说，在遵循如下一系列事件之后：

1. 用户启动了 `transaction1` 并使用它写了一些记录
2. 用户启动了 `transaction2` 并使用它编写了一些其他记录
3. 用户提交了 `transaction2`

即使 `transaction2` 中的记录已提交，在提交或中止 `transaction1` 之前，消费者也不会看到这些记录。这有 2 层含义：

 * 首先，在 Flink 应用程序的正常工作期间，用户可以预料 Kafka 主题中生成的记录的可见性会延迟，相当于已完成 checkpoint 之间的平均时间。
 * 其次，在 Flink 应用程序失败的情况下，此应用程序正在写入的供消费者读取的主题将被阻塞，直到应用程序重新启动或配置的事务超时时间过去后，才恢复正常。此标注仅适用于有多个 agent 或者应用程序写入同一 Kafka 主题的情况。

**注意**：`Semantic.EXACTLY_ONCE` 模式为每个 `FlinkKafkaProducer011` 实例使用固定大小的 KafkaProducer 池。每个 checkpoint 使用其中一个 producer。如果并发 checkpoint 的数量超过池的大小，`FlinkKafkaProducer011` 将抛出异常，并导致整个应用程序失败。请合理地配置最大池大小和最大并发 checkpoint 数量。

**注意**：`Semantic.EXACTLY_ONCE` 会尽一切可能不留下任何逗留的事务，否则会阻塞其他消费者从这个 Kafka topic 中读取数据。但是，如果 Flink 应用程序在第一次 checkpoint 之前就失败了，那么在重新启动此类应用程序后，系统中不会有先前池大小（pool size）相关的信息。因此，在第一次 checkpoint 完成前对 Flink 应用程序进行缩容，且并发数缩容倍数大于安全系数 `FlinkKafkaProducer011.SAFE_SCALE_DOWN_FACTOR` 的值的话，是不安全的。

## 在 Kafka 0.10.x 中使用 Kafka 时间戳和 Flink 事件时间

自 Apache Kafka 0.10+ 以来，Kafka 的消息可以携带[时间戳](https://cwiki.apache.org/confluence/display/KAFKA/KIP-32+-+Add+timestamps+to+Kafka+message)，指示事件发生的时间（请参阅 [Apache Flink 中的"事件时间"](../event_time.html)）或消息写入 Kafka broker 的时间。

如果 Flink 中的时间特性设置为 `TimeCharacteristic.EventTime`（ `StreamExecutionEnvironment.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)`），则 `FlinkKafkaConsumer010` 将发出附加时间戳的记录。

Kafka consumer 不会发出 watermark。为了发出 watermark，可采用上文 “Kafka Consumer 和时间戳抽取以及 watermark 发送” 章节中描述的 `assignTimestampsAndWatermarks` 方法。

使用 Kafka 的时间戳时，无需定义时间戳提取器。`extractTimestamp()` 方法的 `previousElementTimestamp` 参数包含 `Kafka` 消息携带的时间戳。

Kafka consumer 的时间戳提取器应该是这样的：
{% highlight java %}
public long extractTimestamp(Long element, long previousElementTimestamp) {
    return previousElementTimestamp;
}
{% endhighlight %}



只有设置了 `setWriteTimestampToKafka(true)`，则 `FlinkKafkaProducer010` 才会发出记录时间戳。

{% highlight java %}
FlinkKafkaProducer010.FlinkKafkaProducer010Configuration config = FlinkKafkaProducer010.writeToKafkaWithTimestamps(streamWithTimestamps, topic, new SimpleStringSchema(), standardProps);
config.setWriteTimestampToKafka(true);
{% endhighlight %}



## Kafka 连接器指标

Flink 的 Kafka 连接器通过 Flink 的 [metric 系统]({{ site.baseurl }}/zh/monitoring/metrics.html) 提供一些指标来分析 Kafka Connector 的状况。Producer 通过 Flink 的 metrics 系统为所有支持的版本导出 Kafka 的内部指标。consumer 从 Kafka 0.10 版本开始导出所有指标。Kafka 文档在其[文档](http://kafka.apache.org/documentation/#selector_monitoring)中列出了所有导出的指标。

除了这些指标之外，所有 consumer 都暴露了每个主题分区的 `current-offsets` 和 `committed-offsets`。`current-offsets` 是指分区中的当前偏移量。指的是我们成功检索和发出的最后一个元素的偏移量。`committed-offsets` 是最后提交的偏移量。这为用户提供了 at-least-once 语义，用于提交给 Zookeeper 或 broker 的偏移量。对于 Flink 的偏移检查点，系统提供精准一次语义。

提交给 ZK 或 broker 的偏移量也可以用来跟踪 Kafka consumer 的读取进度。每个分区中提交的偏移量和最近偏移量之间的差异称为 *consumer lag*。如果 Flink 拓扑消耗来自 topic 的数据的速度比添加新数据的速度慢，那么延迟将会增加，consumer 将会滞后。对于大型生产部署，我们建议监视该指标，以避免增加延迟。

## 启用 Kerberos 身份验证

Flink 通过 Kafka 连接器提供了一流的支持，可以对 Kerberos 配置的 Kafka 安装进行身份验证。只需在 `flink-conf.yaml` 中配置 Flink。像这样为 Kafka 启用 Kerberos 身份验证：

1. 通过设置以下内容配置 Kerberos 票据 
 - `security.kerberos.login.use-ticket-cache`：默认情况下，这个值是 `true`，Flink 将尝试在 `kinit` 管理的票据缓存中使用 Kerberos 票据。注意！在 YARN 上部署的 Flink  jobs 中使用 Kafka 连接器时，使用票据缓存的 Kerberos 授权将不起作用。使用 Mesos 进行部署时也是如此，因为 Mesos 部署不支持使用票据缓存进行授权。
 - `security.kerberos.login.keytab` 和 `security.kerberos.login.principal`：要使用 Kerberos keytabs，需为这两个属性设置值。
 
2. 将 `KafkaClient` 追加到 `security.kerberos.login.contexts`：这告诉 Flink 将配置的 Kerberos 票据提供给 Kafka 登录上下文以用于 Kafka 身份验证。

一旦启用了基于 Kerberos 的 Flink 安全性后，只需在提供的属性配置中包含以下两个设置（通过传递给内部 Kafka 客户端），即可使用 Flink Kafka Consumer 或 Producer 向 Kafk a进行身份验证：

- 将 `security.protocol` 设置为 `SASL_PLAINTEXT`（默认为 `NONE`）：用于与 Kafka broker 进行通信的协议。使用独立 Flink 部署时，也可以使用 `SASL_SSL`;请在[此处](https://kafka.apache.org/documentation/#security_configclients)查看如何为 SSL 配置 Kafka 客户端。
- 将 `sasl.kerberos.service.name` 设置为 `kafka`（默认为 `kafka`）：此值应与用于 Kafka broker 配置的 `sasl.kerberos.service.name` 相匹配。客户端和服务器配置之间的服务名称不匹配将导致身份验证失败。

有关 Kerberos 安全性 Flink 配置的更多信息，请参见[这里]({{ site.baseurl }}/zh/ops/config.html)。你也可以在[这里]({{ site.baseurl }}/zh/ops/security-kerberos.html)进一步了解 Flink 如何在内部设置基于 kerberos 的安全性。

## 问题排查

<div class="alert alert-warning">
如果你在使用 Flink 时对 Kafka 有问题，请记住，Flink 只封装 <a href="https://kafka.apache.org/documentation/#consumerapi">KafkaConsumer</a> 或 <a href="https://kafka.apache.org/documentation/#producerapi">KafkaProducer</a>，你的问题可能独立于 Flink，有时可以通过升级 Kafka broker 程序、重新配置 Kafka broker 程序或在 Flink 中重新配置 <tt>KafkaConsumer</tt> 或 <tt>KafkaProducer</tt> 来解决。下面列出了一些常见问题的示例。
</div>

### 数据丢失

根据你的 Kafka 配置，即使在 Kafka 确认写入后，你仍然可能会遇到数据丢失。特别要记住在 Kafka 的配置中设置以下属性：

- `acks`
- `log.flush.interval.messages`
- `log.flush.interval.ms`
- `log.flush.*`

上述选项的默认值是很容易导致数据丢失的。请参考 Kafka 文档以获得更多的解释。

### UnknownTopicOrPartitionException

导致此错误的一个可能原因是正在进行新的 leader 选举，例如在重新启动 Kafka broker 之后或期间。这是一个可重试的异常，因此 Flink job 应该能够重启并恢复正常运行。也可以通过更改 producer 设置中的 `retries` 属性来规避。但是，这可能会导致重新排序消息，反过来可以通过将 `max.in.flight.requests.per.connection` 设置为 1 来避免不需要的消息。

{% top %}
