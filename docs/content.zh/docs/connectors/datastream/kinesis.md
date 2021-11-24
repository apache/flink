---
title: Kinesis
weight: 5
type: docs
aliases:
  - /zh/dev/connectors/kinesis.html
  - /zh/apis/streaming/connectors/kinesis.html
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

# Amazon Kinesis 数据流连接器

Kinesis连接器提供了对以下内容的访问 [Amazon AWS Kinesis Streams](http://aws.amazon.com/kinesis/streams/)。

要使用该连接器，请在你的项目中添加以下Maven依赖项：

{{< artifact flink-connector-kinesis >}}

{{< hint warning >}}
**注意** 在 Flink 1.10.0 版本之前，`flink-connector-kinesis` 依赖于 [Amazon Software License](https://aws.amazon.com/asl/) 下许可的代码。
链接到之前版本的 flink-connector-kinesis 将把这段代码纳入你的应用程序。
{{< /hint >}}

由于许可问题，`flink-connector-kinesis` 工件没有被部署到之前版本的 Maven 中心。更多信息请见具体版本的文档。

## 使用 Amazon Kinesis Streams 流服务
请按照 [Amazon Kinesis Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html)
来设置Kinesis流.

## 使用 IAM 配置对 Kinesis 的访问
确保创建适当的 IAM 策略以允许对 Kinesis 流进行读写。 请看例子 [here](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html).

根据你的部署，你会选择不同的 Credentials Provider 来允许访问 Kinesis。
默认情况下，使用 `AUTO` Credentials Provider。
如果在配置中设置了访问密钥 ID 和秘密密钥，则使用 `BASIC` Provider。  

通过使用 `AWSConfigConstants.AWS_CREDENTIALS_PROVIDER` 设置，可以 **选择** 地设置一个特定的证书提供者。

受支持的证书提供者有：
* `AUTO` - 使用默认的 AWS 凭证提供者链，按以下顺序搜索凭证。`ENV_VARS`, `SYS_PROPS`, `WEB_IDENTITY_TOKEN`, `PROFILE`和 EC2/ECS 凭证提供者。
* `BASIC` - 使用作为配置提供的访问密钥 ID 和秘密密钥。
* `ENV_VAR` - 使用 `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY` 环境变量。
* `SYS_PROP` - 使用 Java 系统属性 aws.accessKeyId 和 aws.secretKey。
* `PROFILE` - 使用 AWS 凭证档案文件来创建 AWS 凭证。
* `ASSUME_ROLE` - 通过承担一个角色创建 AWS 凭证。必须提供承担该角色的凭证。
* `WEB_IDENTITY_TOKEN` - 通过使用 Web 身份令牌承担一个角色来创建 AWS 凭证。

## Kinesis 消费者

`FlinkKinesisConsumer` 是一个完全一次性的并行流数据源，它在同一 AWS 服务区域内订阅了多个 AWS Kinesis 流，并能在作业运行时透明地处理流的重新分片。
消费者的每个子任务负责从多个 Kinesis 碎片中获取数据记录。每个子任务获取的分片数量会随着 Kinesis 关闭和创建分片而改变。

在从 Kinesis 流中消耗数据之前，确保所有的流在 AWS 仪表盘中被创建为 "ACTIVE" 状态。

{{< tabs "58b6c235-48ee-4cf7-aabc-41e0679a3370" >}}
{{< tab "Java" >}}
```java
Properties consumerConfig = new Properties();
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), consumerConfig));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val consumerConfig = new Properties()
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
consumerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
consumerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

val env = StreamExecutionEnvironment.getExecutionEnvironment

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, consumerConfig))
```
{{< /tab >}}
{{< /tabs >}}

以上是一个使用消费者的简单例子。消费者的配置是由 `java.util.Properties` 实例提供的，其配置键可以在 `AWSConfigConstants`（ AWS 特定参数）和 `ConsumerConfigConstants` （ Kinesis 消费者参数）中找到。
这个例子演示了在 AWS 区域 "us-east-1 "消费一个 Kinesis 流。AWS 凭证是使用基本方法提供的，其中 AWS 访问密钥 ID 和秘密访问密钥是在配置中直接提供的。
另外，数据是从 Kinesis 流中的最新位置开始消耗的（另一个选择是将 `ConsumerConfigConstants.STREAM_INITIAL_POSITION` 设置为 `TRIM_HORIZON`，这让消费者从最早的记录开始读取 Kinesis 流）。

消费者的其他可选的配置键可以在 `ConsumerConfigConstants` 中找到。

请注意，Flink Kinesis 消费者源的配置的并行性可以完全独立于 Kinesis 流中的分片总数。
当分片的数量大于消费者的并行度时，那么每个消费者子任务可以订阅多个分片。否则，如果分片的数量小于消费者的并行度，那么一些消费者子任务就会被闲置，等待它被分配到新的分片（即，当流被重新分片以增加分片的数量，从而获得更高的 Kinesis 服务吞吐量时）。

还需要注意的是，默认情况下，分片到子任务的分配是基于分片和流名称的哈希值，这将或多或少地平衡分片在子任务中的位置。
然而，假设在流上使用默认的 Kinesis 碎片管理（ UpdateShardCount with `UNIFORM_SCALING` ），将 `UniformShardAssigner`
设置为消费者上的碎片分配器将更均匀地将碎片分配给子任务。假设传入的 Kinesis 记录被随机分配给 Kinesis `PartitionKey` 或 `ExplicitHashKey` 值，结果是一致的子任务加载。

如果默认分配器和`UniformShardAssigner`都不够用，可以设置 `KinesisShardAssigner` 的自定义实现。

### `DeserializationSchema`

Flink Kinesis Consumer 也需要一个模式来知道如何将 Kinesis 数据流中的二进制数据变成 Java 对象。
`KinesisDeserializationSchema` 允许用户指定这样的模式。`T deserialize(byte[] recordValue, String partitionKey, String seqNum, long approxArrivalTimestamp, String stream, String shardId)`
方法会被每个Kinesis记录调用。

为方便起见，Flink 提供了以下开箱即用的模式：
  
1. `TypeInformationSerializationSchema`，基于 Flink 的 `TypeInformation` 创建一个模式。
    如果数据既被 Flink 写入又被读取，这就很有用。
   这个模式是一个针对 Flink 的高性能的替代方案，可以替代其他通用的序列化方法。
    
2. `GlueSchemaRegistryJsonDeserializationSchema` 提供了在[AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)中查找写入者模式（用于写入记录的模式）的能力。
   使用这个功能，反序列化模式记录将用从 AWS Glue Schema Registry 检索到的模式来读取，并转化为 `com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema`，
   具有手动提供模式的通用记录或由[mbknor-jackson-jsonSchema]（https://github.com/mbknor/mbknor-jackson-jsonSchema）生成的JAVA POJO。

   <br>为了使用这个反序列化模式，必须添加以下额外的依赖关系：
       
{{< tabs "8c6721c7-4a48-496e-b0fe-6522cf6a5e13" >}}
{{< tab "GlueSchemaRegistryJsonDeserializationSchema" >}}
{{< artifact flink-jsonschema-glue-schema-registry >}}
{{< /tab >}}
{{< /tabs >}}

3. `AvroDeserializationSchema` 使用静态提供的模式读取以 Avro 格式序列化的数据。
    它可以从 Avro 生成的类中推断出模式（`AvroDeserializationSchema.forSpecific(...)`），也可以用手动提供的模式与 `GenericRecords` 一起工作（用 `AvroDeserializationSchema.forGeneric(...)`）。
    这个反序列化模式希望序列化的记录不包含嵌入式模式。
   
    - 你可以使用 [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)来检索写入者的模式。
      同样的，反序列化记录也将从 AWS Glue Schema Registry 读取模式并进行转换（通过 `GlueSchemaRegistryAvroDeserializationSchema.forGeneric(..)` 或 `GlueSchemaRegistryAvroDeserializationSchema.forSpecific(..)`）。
      关于将 AWS Glue Schema Registry 与 Apache Flink 集成的更多信息，请参见 [Use Case: Amazon Kinesis Data Analytics for Apache Flink](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kinesis-data-analytics-apache-flink)。

    <br>为了使用这个反序列化模式，必须添加以下额外的依赖关系：
    
{{< tabs "8c6721c7-4a48-496e-b0fe-6522cf6a5e13" >}}
{{< tab "AvroDeserializationSchema" >}}
{{< artifact flink-avro >}}
{{< /tab >}}
{{< tab "GlueSchemaRegistryAvroDeserializationSchema" >}}
{{< artifact flink-avro-glue-schema-registry >}}
{{< /tab >}}
{{< /tabs >}}

### 配置起始位置

Flink Kinesis Consumer 目前提供了以下选项来配置从哪里开始读取 Kinesis 流，只需在提供的配置属性中将 `ConsumerConfigConstants.STREAM_INITIAL_POSITION` 设置为以下数值之一（选项的命名完全遵循[the namings used by the AWS Kinesis Streams service](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax)）：

- `LATEST`：读取所有流的所有碎片，从最新记录开始。
- `TRIM_HORIZON`：读取所有流的所有碎片，从最早的记录开始（根据保留设置，数据可能被 Kinesis 裁剪）。
- `AT_TIMESTAMP`：读取从指定时间戳开始的所有数据流的所有碎片。时间戳也必须在配置属性中指定，为 `ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP` 提供一个值，以下列日期模式之一：
    - 一个非负的双倍值，代表从Unix epoch开始已经过去的秒数（例如，`1459799926.480`）。
    - 一个用户定义的模式，它是由 `ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT` 提供的 `SimpleDateFormat` 的有效模式。 
    如果 `ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT` 没有定义，那么默认模式将是 `yyyy-MM-dd'T'HH:mm:ss.SSXXX`（例如，时间戳值是 `2016-04-04`，模式是用户给出的 `yyyy-MM-dd`，或者时间戳值是 `2016-04-04T19:58:46.480-00:00`，没有给出模式）。
      

### 用户定义的完全一次的状态更新语义的容错问题

启用Flink的检查点功能后，Flink Kinesis Consumer 将从 Kinesis 流中的分片中消耗记录，并定期检查每个分片的进度。
在工作失败的情况下，Flink 会将流媒体程序恢复到最新的完整检查点的状态，并从检查点中存储的进度开始，重新消耗 Kinesis 分片的记录。


因此，绘制检查点的间隔定义了在发生故障的情况下，程序最多可能要返回多少次。

为了使用容错的 Kinesis 消费者，需要在执行环境中启用拓扑结构的检查点：

{{< tabs "b1399ed7-5855-446d-9684-7a49de9b4c97" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
env.enableCheckpointing(5000); // checkpoint every 5000 msecs
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()
env.enableCheckpointing(5000) // checkpoint every 5000 msecs
```
{{< /tab >}}
{{< /tabs >}}

还要注意的是，Flink 只有在有足够的处理 slots 来重启拓扑结构时才能重启。
因此，如果拓扑结构因损失一个任务管理器而失败，之后仍必须有足够的 slots 可用。
YARN 上的 Flink 支持对丢失的 YARN 容器进行自动重启。

### 使用 Enhanced Fan-Out

[Enhanced Fan-Out (EFO)](https://aws.amazon.com/blogs/aws/kds-enhanced-fanout/) 增加每个 Kinesis 流的最大并发消费者数量。
在没有 EFO 的情况下，所有并发的消费者在每个分片上共享一个读取配额。
使用 EFO，每个消费者在每个分片上都得到一个独特的专用读取配额，允许读取吞吐量随着消费者的数量而扩展。
使用EFO将 [incur additional cost](https://aws.amazon.com/kinesis/data-streams/pricing/)。

为了启用 EFO，需要两个额外的配置参数：

- `RECORD_PUBLISHER_TYPE`: 决定是否使用 `EFO' 或 `POLLING`。默认的 `RecordPublisher` 是 `POLLING`。
- `EFO_CONSUMER_NAME`: 用于识别消费者的名称。 
对于一个给定的 Kinesis 数据流，每个消费者必须有一个唯一的名字。
然而，消费者名称在不同的数据流中不一定是唯一的。
重复使用一个消费者名称将导致现有的订阅被终止。

下面的代码片断显示了一个配置 EFO 消费者的简单例子。

{{< tabs "42345893-70c3-4678-a348-4c419b337eb1" >}}
{{< tab "Java" >}}
```java
Properties consumerConfig = new Properties();
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
    ConsumerConfigConstants.RecordPublisherType.EFO.name());
consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), consumerConfig));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val consumerConfig = new Properties()
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
    ConsumerConfigConstants.RecordPublisherType.EFO.name());
consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

val env = StreamExecutionEnvironment.getExecutionEnvironment()

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, consumerConfig))
```
{{< /tab >}}
{{< /tabs >}}

#### EFO 流消费者注册/注销

为了使用 EFO，必须针对你想消费的每个流注册一个流消费者。
默认情况下，`FlinkKinesisConsumer` 将在 Flink 作业启动时自动注册流消费者。
流消费者将使用 `EFO_CONSUMER_NAME` 配置提供的名称进行注册。
`FlinkKinesisConsumer`提供了三种注册策略：

- 注册
  - `LAZY` (默认): 流消费者在 Flink 工作开始运行时被注册。
    如果流消费者已经存在，它将被重用。
    这是大多数应用的首选策略。
    但是，并行度大于 1 的工作将导致任务竞争注册和获取流消费者 ARN。
    对于具有非常大的并行性的工作，这可能导致启动时间的增加。
    描述操作有20个限制 [transactions per second](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamConsumer.html),
    这意味着应用程序的启动时间将增加大约 `parallelism/20 seconds`。
  - `EAGER`: 流消费者在 `FlinkKinesisConsumer` 构造函数中被注册。
    如果流消费者已经存在，它将被重用。
    这将导致在构建作业时发生注册，无论是在 Flink 作业管理器还是提交作业的客户端环境。
    使用这种策略的结果是单线程注册和检索流消费者 ARN，比 `LAZY` （具有大的并行性）减少启动时间。
    然而，考虑到客户环境将需要访问 AWS 服务。
  - `NONE`: 流消费者注册不是由 `FlinkKinesisConsumer` 执行的。
    注册必须在外部使用 [AWS CLI或SDK](https://aws.amazon.com/tools/) 进行。
    来调用 [RegisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html)。
    流消费者 ARN 应该通过消费者配置提供给工作。
- 取消注册
  - `LAZY|EAGER` (默认): 当作业被优雅地关闭时，流消费者被取消注册。
     如果一项工作在执行关闭钩子的过程中终止，流消费者将保持活动。
     在这种情况下，当应用程序重新启动时，流消费者将被优雅地重新使用。
    
  - `NONE`: 流消费者取消注册不是由 `FlinkKinesisConsumer` 执行。

下面是一个使用 `EAGER` 注册策略的配置实例：

{{< tabs "a85d716b-6c1c-46d8-9ee4-12d8380a0c06" >}}
{{< tab "Java" >}}
```java
Properties consumerConfig = new Properties();
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
    ConsumerConfigConstants.RecordPublisherType.EFO.name());
consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

consumerConfig.put(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, 
    ConsumerConfigConstants.EFORegistrationType.EAGER.name());

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), consumerConfig));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val consumerConfig = new Properties()
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
    ConsumerConfigConstants.RecordPublisherType.EFO.name());
consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

consumerConfig.put(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, 
    ConsumerConfigConstants.EFORegistrationType.EAGER.name());

val env = StreamExecutionEnvironment.getExecutionEnvironment()

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, consumerConfig))
```
{{< /tab >}}
{{< /tabs >}}

下面是一个使用 `NONE` 注册策略的配置实例：

{{< tabs "00b46c87-7740-4263-8040-2aa7e2960513" >}}
{{< tab "Java" >}}
```java
Properties consumerConfig = new Properties();
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST");

consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
    ConsumerConfigConstants.RecordPublisherType.EFO.name());
consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

consumerConfig.put(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, 
    ConsumerConfigConstants.EFORegistrationType.NONE.name());
consumerConfig.put(ConsumerConfigConstants.efoConsumerArn("stream-name"), 
    "arn:aws:kinesis:<region>:<account>>:stream/<stream-name>/consumer/<consumer-name>:<create-timestamp>");

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

DataStream<String> kinesis = env.addSource(new FlinkKinesisConsumer<>(
    "kinesis_stream_name", new SimpleStringSchema(), consumerConfig));
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val consumerConfig = new Properties()
consumerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
consumerConfig.put(ConsumerConfigConstants.STREAM_INITIAL_POSITION, "LATEST")

consumerConfig.put(ConsumerConfigConstants.RECORD_PUBLISHER_TYPE, 
    ConsumerConfigConstants.RecordPublisherType.EFO.name());
consumerConfig.put(ConsumerConfigConstants.EFO_CONSUMER_NAME, "my-flink-efo-consumer");

consumerConfig.put(ConsumerConfigConstants.EFO_REGISTRATION_TYPE, 
    ConsumerConfigConstants.EFORegistrationType.NONE.name());
consumerConfig.put(ConsumerConfigConstants.efoConsumerArn("stream-name"), 
    "arn:aws:kinesis:<region>:<account>>:stream/<stream-name>/consumer/<consumer-name>:<create-timestamp>");

val env = StreamExecutionEnvironment.getExecutionEnvironment()

val kinesis = env.addSource(new FlinkKinesisConsumer[String](
    "kinesis_stream_name", new SimpleStringSchema, consumerConfig))
```
{{< /tab >}}
{{< /tabs >}}

### 消耗的记录事件时间

如果流拓扑结构选择使用[event time notion]({{< ref "docs/concepts/time" >}})作为记录的时间戳，那么默认会使用 *近似的到达时间戳* 一旦记录被成功接收并被流存储，这个时间戳就会被 Kinesis 附加到记录上。
请注意，这个时间戳通常被称为 Kinesis 服务器端的时间戳，并且不保证其准确性或顺序的正确性（即，时间戳不一定是升序的）。

用户可以选择用自定义的时间戳覆盖这个默认值，如[here]（{{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}），或者使用 [predefined ones]（{{< ref "docs/dev/datastream/event-time/built_in" >}}）中的一个。
这样做之后，可以通过以下方式将其传递给消费者。

{{< tabs "8fbaf5cb-3b76-4c62-a74e-db51b60f6600" >}}
{{< tab "Java" >}}
```java
FlinkKinesisConsumer<String> consumer = new FlinkKinesisConsumer<>(
    "kinesis_stream_name",
    new SimpleStringSchema(),
    kinesisConsumerConfig);
consumer.setPeriodicWatermarkAssigner(new CustomAssignerWithPeriodicWatermarks());
DataStream<String> stream = env
	.addSource(consumer)
	.print();
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val consumer = new FlinkKinesisConsumer[String](
    "kinesis_stream_name",
    new SimpleStringSchema(),
    kinesisConsumerConfig);
consumer.setPeriodicWatermarkAssigner(new CustomAssignerWithPeriodicWatermarks());
val stream = env
	.addSource(consumer)
	.print();
```
{{< /tab >}}
{{< /tabs >}}

在内部，每个分片/消费者线程执行一个分配器的实例（见下文线程模型）。
当指定分配器时，对于从 Kinesis 读取的每条记录，都会调用 extractTimestamp(T element, long previousElementTimestamp)
来为记录分配一个时间戳，并调用 getCurrentWatermark() 来确定分片的新水印。
然后，消费者子任务的水印被确定为其所有碎片的最小水印并定期发射。
每个分片的水印对于处理分片之间不同的消费速度至关重要，否则会导致依赖水印的下游逻辑出现问题，例如不正确的后期数据丢弃。

默认情况下，如果分片没有交付新的记录，水印就会停滞。
属性 `ConsumerConfigConstants.SHARD_IDLE_INTERVAL_MILLIS` 可用于通过超时来避免这个潜在的问题，该超时将允许水印在空闲的碎片中进行。

### 分区消费者的事件时间对齐

Flink Kinesis Consumer 可选择支持并行的消费者子任务（及其线程）之间的同步，以避免[Event time synchronization across sources](https://issues.apache.org/jira/browse/FLINK-10886)中描述的事件时间偏移相关问题。

为了启用同步，在消费者身上设置水印跟踪器：

```java
JobManagerWatermarkTracker watermarkTracker =
    new JobManagerWatermarkTracker("myKinesisSource");
consumer.setWatermarkTracker(watermarkTracker);
```

`JobManagerWatermarkTracker` 将使用一个全局聚合来同步每个子任务的水印。
每个子任务使用每个分片的队列，根据队列中下一个记录比全局水印领先的程度，控制记录向下游排放的速度。

"emit ahead" 的限制是通过 `ConsumerConfigConstants.WATERMARK_LOOKAHEAD_MILLIS·配置的。
较小的值会减少偏移，但也会减少吞吐量。较大的值将允许子任务在等待全局水印前进之前进一步进行。

吞吐量方程中的另一个变量是跟踪器传播水印的频率。
间隔可以通过 `ConsumerConfigConstants.WATERMARK_SYNC_MILLIS`来配置。
较小的值可以减少发射器的等待时间，但代价是增加与作业管理器的通信。


由于发生倾斜时记录在队列中积累，需要预期增加内存消耗。
多少取决于平均记录大小。对于较大的尺寸，可能需要通过 `ConsumerConfigConstants.WATERMARK_SYNC_QUEUE_CAPACITY·调整发射器队列容量。


### Threading 模式

Flink Kinesis Consumer 使用多个线程进行分片发现和数据消费。

#### 碎片发现

对于碎片发现，每个并行的消费者子任务都会有一个单线程，不断地查询 Kinesi s的碎片信息，即使子任务在启动消费者时最初没有碎片可读。
换句话说，如果消费者以 10 的并行度运行，那么无论订阅的流中有多少碎片，都会有 10 个线程不断查询 Kinesis。

#### Polling (default) 记录发布器

对于 `POLLING` 数据消耗，将创建一个线程来消耗每个发现的分片。
当它负责消费的分片由于流的重新分片而被关闭时，线程将终止。换句话说，每个开放的分片将始终有一个线程。


#### Enhanced Fan-Out 记录发布器

对于 `EFO`数据消耗，线程模型与 `POLLING` 相同，有额外的线程池来处理与 Kinesis 的异步通信。
AWS SDK v2.x `KinesisAsyncClient` 使用 Netty 的额外线程来处理 IO 和异步响应。
每个并行的消费者子任务将有他们自己的 `KinesisAsyncClient` 实例。
换句话说，如果消费者以 10 的并行度运行，将有总共 10 个 `KinesisAsyncClient` 实例。在注册和取消注册流消费者时，将创建一个单独的客户端，随后销毁。


### 内部使用的Kinesis APIs

Flink Kinesis 消费者在内部使用[AWS Java SDK](http://aws.amazon.com/sdk-for-java/)来调用 Kinesis APIs 进行分片发现和数据消费。
由于亚马逊对 API 的 [service limits for Kinesis Streams](http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)，消费者将与用户可能正在运行的其他非 Flink 消费的应用程序竞争。
下面是消费者调用的 API 列表，以及消费者如何使用 API 的描述，还有关于如何处理 Flink Kinesis 消费者由于这些服务限制而可能出现的任何错误或警告的信息。


#### 碎片发现

- *[ListShards](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html)*：
这是由每个并行消费者子任务中的一个线程不断调用的，以发现任何新的分片，作为流重新分片的结果。
默认情况下，消费者以 10 秒的时间间隔进行分片发现，并且会无限期地重试，直到从 Kinesis 那里得到结果。
如果这干扰了其他非 Flink 消费的应用程序，用户可以通过在提供的配置属性中设置 `ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS`的值来减缓消费者调用这个 API。
这将发现间隔设置为一个不同的值。请注意，这个设置直接影响到发现一个新的分片和开始消费它的最大延迟，因为分片不会在这个间隔期间被发现。


#### 轮询（默认）记录发布器

- *[GetShardIterator](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)*：
当每个分片的消费线程被启动时，这个功能只被调用一次，如果 Kinesis 抱怨说该 API 的交易限制已经超过了，它将重试，最多默认为 3 次。
请注意，由于该 API 的速率限制是每个分片（而不是每个流），消费者本身不应该超过该限制。
通常情况下，如果发生这种情况，用户可以尝试减缓任何其他非 Flink 消耗的应用程序调用这个 API，或者通过在提供的配置属性中设置以 `ConsumerConfigConstants.SHARD_GETITERATOR_*` 为前缀的键来修改消费者中这个 API 调用的重试行为。


- *[GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html)*：
这是由每个分片的消费线程不断调用的，以从 Kinesis 获取记录。当一个碎片有多个并发的消费者时（当有任何其他非 Flink 消费的应用程序运行时），每个碎片的速率限制可能会被超过。
默认情况下，在每次调用该 API 时，如果 Kinesis 抱怨该 API 的数据大小/交易限制已经超过，消费者将重试，默认情况下最多重试 3 次。
用户可以尝试减缓其他非 Flink 消耗的应用程序，或者通过设置所提供的配置属性中的 `ConsumerConfigConstants.SHARD_GETRECORDS_MAX` 和 `ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS` 键来调整消费者的产量。
设置前者可以调整每个消费线程在每次调用时试图从碎片中获取的最大记录数（默认为 10,000），而后者可以修改每次获取的睡眠间隔（默认为 200）。消费者在调用该 API 时的重试行为也可以通过使用其他以 `ConsumerConfigConstants.SHARD_GETRECORDS_*` 为前缀的键来修改。


#### Enhanced Fan-Out 记录发布器

- *[SubscribeToShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html)*: this is called
由每个分片的消费线程来获得分片订阅。一个分片的订阅通常在 5 分钟内有效，但如果有任何可恢复的错误被抛出，订阅将被重新获取。
一旦获得一个订阅，消费者将收到一个 [SubscribeToShardEvents](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShardEvent.html)的流。
重试和退订参数可以使用 `ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_*` 键来配置。

- *[DescribeStreamSummary](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html)*: this is called
在流消费者注册期间，每个流有一次。默认情况下，`LAZY` 注册策略将根据作业的并行性来调整调用次数。`EAGER` 将在每个流中调用一次，`NONE` 将不调用这个 API。
Retry 和 backoff 参数可以使用 `ConsumerConfigConstants.STREAM_DESCRIBE_*` 键来配置。

- *[DescribeStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamConsumer.html)*:
在流消费者注册和取消注册期间，这个服务被调用。对于每个流，这个服务将被定期调用，直到流消费者被报告为 `ACTIVE` / `not found` 的注册/取消注册。默认情况下，`LAZY` 注册策略将通过工作的并行性来扩展调用的数量。
`EAGER` 将在每个流中只调用一次服务进行注册。`NONE` 将不调用该服务。Retry 和 backof f参数可以使用 `ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_*` 键来配置。 

- *[RegisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html)*:
除非配置了 `NONE` 注册策略，否则在流消费者注册期间，每个流会被调用一次。
可以使用 `ConsumerConfigConstants.REGISTER_STREAM_*` 键来配置重试和退订参数。

- *[DeregisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DeregisterStreamConsumer.html)*:
除非配置了 `NONE` 或 `EAGER` 注册策略，否则在流消费者取消注册时，每个流会被调用一次。
可以使用 `ConsumerConfigConstants.DEREGISTER_STREAM_*` 键来配置重试和退订参数。 

## Kinesis 消费者

`FlinkKinesisProducer` 采用了 [Kinesis Producer Library (KPL)](http://docs.aws.amazon.com/streams/latest/dev/developing-producers-with-kpl.html) 将数据从 Flink 流放入 Kinesis 流中。

请注意，生产者不参与Flink的检查点，也不提供精确的一次处理保证。另外，Kinesis 生产者并不保证记录是按顺序写入分片中的(请看 [here](https://github.com/awslabs/amazon-kinesis-producer/issues/23) 和 [here](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecord.html#API_PutRecord_RequestSyntax) 了解更多详情).

如果发生故障或重新分片，数据将被再次写入 Kinesis，导致重复。这种行为通常被称为 "at-least-once" 语义。

要把数据放到 Kinesis 流中，确保该流在 AWS 仪表盘中被标记为 "ACTIVE"。

为了使监测工作顺利进行，访问流的用户需要访问 CloudWatch 服务。

{{< tabs "6df3b696-c2ca-4f44-bea0-96cf8275d61c" >}}
{{< tab "Java" >}}
```java
Properties producerConfig = new Properties();
// Required configs
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
// Optional configs
producerConfig.put("AggregationMaxCount", "4294967295");
producerConfig.put("CollectionMaxCount", "1000");
producerConfig.put("RecordTtl", "30000");
producerConfig.put("RequestTimeout", "6000");
producerConfig.put("ThreadPoolSize", "15");

// Disable Aggregation if it's not supported by a consumer
// producerConfig.put("AggregationEnabled", "false");
// Switch KinesisProducer's threading model
// producerConfig.put("ThreadingModel", "PER_REQUEST");

FlinkKinesisProducer<String> kinesis = new FlinkKinesisProducer<>(new SimpleStringSchema(), producerConfig);
kinesis.setFailOnError(true);
kinesis.setDefaultStream("kinesis_stream_name");
kinesis.setDefaultPartition("0");

DataStream<String> simpleStringStream = ...;
simpleStringStream.addSink(kinesis);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val producerConfig = new Properties()
// Required configs
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
// Optional KPL configs
producerConfig.put("AggregationMaxCount", "4294967295")
producerConfig.put("CollectionMaxCount", "1000")
producerConfig.put("RecordTtl", "30000")
producerConfig.put("RequestTimeout", "6000")
producerConfig.put("ThreadPoolSize", "15")

// Disable Aggregation if it's not supported by a consumer
// producerConfig.put("AggregationEnabled", "false")
// Switch KinesisProducer's threading model
// producerConfig.put("ThreadingModel", "PER_REQUEST")

val kinesis = new FlinkKinesisProducer[String](new SimpleStringSchema, producerConfig)
kinesis.setFailOnError(true)
kinesis.setDefaultStream("kinesis_stream_name")
kinesis.setDefaultPartition("0")

val simpleStringStream = ...
simpleStringStream.addSink(kinesis)
```
{{< /tab >}}
{{< /tabs >}}

以上是一个使用生产者的简单例子。为了初始化 `FlinkKinesisProducer`，用户需要通过 `java.util.Properties` 实例传递 `AWS_REGION`、`AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY`。用户也可以将 KPL 的配置作为可选参数传入，以定制 KPL 底层的 `FlinkKinesisProducer`。KPL 配置的完整列表和解释可以在[here]（https://github.com/awslabs/amazon-kinesis-producer/blob/master/java/amazon-kinesis-producer-sample/default_config.properties）找到。这个例子演示了在 AWS 区域 "us-east-1" 产生一个单一的 Kinesis 流。

如果用户没有指定任何 KPL 的配置和值，`FlinkKinesisProducer` 将使用 KPL 的默认配置值，除了 `RateLimit`。`RateLimit` 限制了一个分片的最大允许投放率，是后端限制的一个百分比。KPL 的默认值是 150，但它使 KPL 过于频繁地抛出 `RateLimitExceededException`，从而破坏了 Flink sink。因此，`FlinkKinesisProducer` 将 KPL 的默认值改为 100。

它不支持 `SerializationSchema`，而是支持 `KinesisSerializationSchema`。
`KinesisSerializationSchema` 允许将数据发送到多个流中。这是通过 `KinesisSerializationSchema.getTargetStream(T element)` 方法实现的。
返回 `null` 将指示生产者将元素写入默认流。

### Threading Model

从Flink 1.4.0 开始，`FlinkKinesisProducer` 将其默认的底层 KPL 从每请求一个线程的模式切换到线程池模式。线程池模式的 KPL 使用一个队列和线程池来执行对 Kinesis 的请求。这限制了 KPL 的本地进程可能创建的线程数量，因此大大降低了 CPU 的利用率，提高了效率。**因此，我们强烈建议Flink用户使用线程池模式** 默认的线程池大小为 `10`。用户可以在 `java.util.Properties` 实例中用 `ThreadPoolSize` 键设置线程池大小，如上例所示。

用户仍然可以通过在 `java.util.Properties` 中设置 `ThreadingModel` 和 `PER_REQUEST `的键值对来切换回每请求一线程模式，如上例中注释的代码所示。

### 反压

默认情况下，`FlinkKinesisProducer` 不进行反压。
相反，由于每个分片每秒 1MB 的速率限制而无法发送的记录被缓冲在一个无界队列中，并在其 `RecordTtl` 过期时被丢弃。

为了避免数据丢失，你可以通过限制内部队列的大小来启用反压:

```
// 200 Bytes per record, 1 shard
kinesis.setQueueLimit(500);
```

`queueLimit` 的值取决于预期的记录大小。要选择一个好的值，考虑到 Kinesis 的速率限制在每秒 1MB，每个分片。
如果缓冲的记录少于一秒钟，那么队列可能无法满负荷运行。
默认的 `RecordMaxBufferedTime` 为 100ms，每个分片的队列大小为 100kB 就足够了。然后，`queueLimit` 可以通过以下方式计算出来

```
queue limit = (number of shards * queue size per shard) / record size
```

例如，对于每条记录 200Bytes 和 8 个分片，队列限制为 4000 是一个好的起点。如果队列大小限制了吞吐量（每个分片低于每秒 1MB），请尝试稍微增加队列限制。


## Using Custom Kinesis Endpoints

有时需要让 Flink 作为消费者或生产者针对 Kinesis VPC 端点或非 AWS 端点进行操作。例如 [Kinesalite](https://github.com/mhart/kinesalite); 
这在对Flink应用程序进行功能测试时特别有用。通常由 Flink 配置中设置的 AWS 区域推断出的 AWS 端点必须通过配置属性进行重写。

要覆盖 AWS 端点，设置 `AWSConfigConstants.AWS_ENDPOINT` 和 `AWSConfigConstants.AWS_REGION` 属性。该区域将被用于签署端点的 URL。

{{< tabs "bcadd466-8416-4d3c-a6a7-c46eee0cbd4a" >}}
{{< tab "Java" >}}
```java
Properties producerConfig = new Properties();
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1");
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val producerConfig = new Properties()
producerConfig.put(AWSConfigConstants.AWS_REGION, "us-east-1")
producerConfig.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
producerConfig.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
producerConfig.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567")
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
