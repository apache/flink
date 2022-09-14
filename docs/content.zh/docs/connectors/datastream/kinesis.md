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

# 亚马逊 Kinesis 数据流 SQL 连接器

Kinesis 连接器提供访问 [Amazon Kinesis Data Streams](http://aws.amazon.com/kinesis/streams/) 。

使用此连接器, 取决于您是否读取数据和/或写入数据，增加下面依赖项的一个或多个到您的项目中:

<table class="table table-bordered">
  <thead>
    <tr>
      <th class="text-left">KDS Connectivity</th>
      <th class="text-left">Maven Dependency</th>
    </tr>
  </thead>
  <tbody>
    <tr>
        <td>Source</td>
        <td>{{< artifact flink-connector-kinesis >}}</td>
    </tr>
    <tr>
        <td>Sink</td>
        <td>{{< artifact flink-connector-aws-kinesis-streams >}}</td>
    </tr>
  </tbody>
</table>

由于许可证问题，以前的版本中 `flink-connector-kinesis` 工件没有部署到Maven中心库。有关更多信息，请参阅特定版本的文档。

{{< py_download_link "kinesis" >}}

## 使用亚马逊 Kinesis 流服务
遵循 [Amazon Kinesis Streams Developer Guide](https://docs.aws.amazon.com/streams/latest/dev/learning-kinesis-module-one-create-stream.html) 的指令建立 Kinesis 流。

## 配置用 IAM 访问 Kinesis
确保创建合适的 IAM 策略允许从 Kinesis 中读取和写入数据。查阅例子 [这里](https://docs.aws.amazon.com/streams/latest/dev/controlling-access.html) 。

取决于您的部署，您可以选择一个不同的证书提供商来允许访问 Kinesis 。
缺省使用 `AUTO` 的证书供应商。
如果访问密钥ID和秘密密钥在部署配置中设置，结果使用 `BASIC` 提供商。

可以通过使用 `AWSConfigConstants.AWS_CREDENTIALS_PROVIDER` 选择性地设置一个特定的证书提供商。

支持的证书提供者是:

* `AUTO` - 使用缺省的 AWS Credentials Provider chain 按如下顺序搜索证书: `ENV_VARS`, `SYS_PROPS`, `WEB_IDENTITY_TOKEN`, `PROFILE` 和 EC2/ECS 证书提供者。
* `BASIC` - 使用访问密钥ID和秘密密钥作为配置。
* `ENV_VAR` - 使用 `AWS_ACCESS_KEY_ID` & `AWS_SECRET_ACCESS_KEY` 环境变量。
* `SYS_PROP` - 使用 Java 系统属性 `aws.accessKeyId` and `aws.secretKey`。
* `PROFILE` - 使用一个 AWS 证书档案创建 AWS 证书。
* `ASSUME_ROLE` - 通过承担角色创建 AWS 证书。 需要提供承担角色的证书。
* `WEB_IDENTITY_TOKEN` - 通过使用网络身份令牌承担的角色创建 AWS 证书。

## Kinesis 消费者

`FlinkKinesisConsumer` 是在相同的 AWS 服务区域订阅了多个 AWS Kinesis 流的一个精确一次消费的并行流数据源，可以在作业运行时透明地处理流的重新分片。
消费者的每个子任务负责从多个Kinesis分片中取数据记录。每个子任务取的分片数量在 Kinesis 关闭和创建分片时发生改变。

从 Kinesis 流中消费数据前，确保所有的流在亚马逊 Kinesis 数据流控制台使用状态 "ACTIVE" 创建。

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
{{< tab "Python" >}}
```python
consumer_config = {
    'aws.region': 'us-east-1',
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'flink.stream.initpos': 'LATEST'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer("stream-1", SimpleStringSchema(), consumer_config))
```
{{< /tab >}}
{{< /tabs >}}

上面是一个使用消费者的简单例子。 消费者使用 `java.util.Properties` 实例配置，配置的健可以在 `AWSConfigConstants` (AWS 特定的参数)
和 `ConsumerConfigConstants` (Kinesis 消费者参数)。这个例子演示了在 AWS区域 "us-east-1" 消费一个 Kinesis流。AWS 证书使用基本的方法提供：AWS访问密钥
ID和秘密访问密钥直接在配置中提供。数据从 Kinesis流中最新的位置消费（另一个选项是把 `ConsumerConfigConstants.STREAM_INITIAL_POSITION`
设置为 `TRIM_HORIZON` ，让消费者从 Kinesis 流中可能的最早记录开始读取）。

别的消费者的可选配置项可以在 `ConsumerConfigConstants` 找到。

请注意，Flink Kinesis 消费源的配置并行度可以完全独立于 Kinesis 流的总的分片数。
当分片数大于消费者的并行度时，每个消费者子任务可以订阅多个分片；否则
如果分片的数量小于消费者的并行度，那么一些消费者子任务将处于空闲状态，并等待它被分配
新的分片（即当流被重新分片以增加用于更高配置的 Kinesis 服务吞吐量的分片数）。

也请注意，默认基于分片和流名称的哈希分配分片给子任务，
这将或多或少地平衡子任务之间的分片。
然而，假设在流上使用默认的 Kinesis 分片管理（UpdateShardCount 用 `UNIFORM_SCALING`），
将 `UniformShardAssigner` 设置为消费者的分片分配器将更均匀地将分片分配到子任务。
假设传入的 Kinesis 记录被分配了随机的 Kinesis `PartitionKey` 或 `ExplicitHashKey` 值，结果是一致的子任务加载。
如果默认分配器和 `UniformShardAssigner` 都不够，则可以设置自定义实现的 `KinesisShardAssigner` 。

### `DeserializationSchema`

Flink Kinesis消费者还需要一个模式来了解如何将 Kinesis 数据流中的二进制数据转换为 Java 对象。
`KinesisDeserializationSchema` 允许用户指定这样的模式。`T deserialize(byte[] recordValue, String partitionKey, String seqNum, long approxArrivalTimestamp, String stream, String shardId)` 
方法让每个 Kinesis 记录被调用。

为了方便, Flink 提供下面开箱即用的模式:
  
1. `TypeInformationSerializationSchema` 基于 Flink 的 `TypeInformation` 创建一个模式。
    如果数据被Flink读和写，是有用的。
	这是替代其它通用序列化方法的性能良好的 Flink特定的模式。
    
2. `GlueSchemaRegistryJsonDeserializationSchema` 提供查找写模式的能力（模式用于写记录）。
   在 [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html). 使用此模式, 反序列化模式记录将被
   从 AWS Glue Schema Registry 中检索到的模式读取并将其转换为用手动提供的模式描述通用化的记录 `com.amazonaws.services.schemaregistry.serializers.json.JsonDataWithSchema` 
   或者 [mbknor-jackson-jsonSchema](https://github.com/mbknor/mbknor-jackson-jsonSchema) 生成的一个 JAVA POJO。

   <br>要使用此反序列化模式，必须添加以下附加依赖项:
   
{{< tabs "8c6721c7-4a48-496e-b0fe-6522cf6a5e13" >}}
{{< tab "GlueSchemaRegistryJsonDeserializationSchema" >}}
{{< artifact flink-jsonschema-glue-schema-registry >}}
{{< /tab >}}
{{< /tabs >}}
    
3. `AvroDeserializationSchema` 它使用静态提供的模式读取以 Avro 格式序列化的数据。它可以
    推断从 Avro 生成的类的模式（ `AvroDeserializationSchema.forSpecific(...)` ) 或者它可以与 `GenericRecords` 一起使用
    一个手动提供的模式（使用 `AvroDeserializationSchema.forGeneric(...)`）。这个反序列化的模式期望
    序列化模式记录没有包含嵌入的模式。

    - 您可以使用 [AWS Glue Schema Registry](https://docs.aws.amazon.com/glue/latest/dg/schema-registry.html)
      检索写模式。类似地，反序列化记录将用来自 AWS Glue Schema Registry的模式读取并转换
	  （通过 `GlueSchemaRegistryAvroDeserializationSchema.forGeneric(...)` 或 `GlueSchemaRegistryAvroDeserializationSchema.forSpecific(...)` ）。
	  更多 AWS Glue Schema Registry 和 Apache Flink 集成的信息参阅
      [Use Case: Amazon Kinesis Data Analytics for Apache Flink](https://docs.aws.amazon.com/glue/latest/dg/schema-registry-integrations.html#schema-registry-integrations-kinesis-data-analytics-apache-flink).

    <br>要使用此反序列化模式，必须添加以下附加依赖项:
    
{{< tabs "8c6721c7-4a48-496e-b0fe-6522cf6a5e13" >}}
{{< tab "AvroDeserializationSchema" >}}
{{< artifact flink-avro >}}
{{< /tab >}}
{{< tab "GlueSchemaRegistryAvroDeserializationSchema" >}}
{{< artifact flink-avro-glue-schema-registry >}}
{{< /tab >}}
{{< /tabs >}}

### 配置开始位置

Flink Kinesis Consumer 当前提供如下的选项来配置读取 Kinesis 流的开始位置，只需在提供的配置属性中设置 `ConsumerConfigConstants.STREAM_INITIAL_POSITION` 
为以下值之一（选项的命名完全遵循 [the namings used by the AWS Kinesis Streams service](http://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html#API_GetShardIterator_RequestSyntax) ）:

您可以配置源表通过 `scan.stream.initpos` 选项从一个特定的位置开始读取一个 Kinesis 数据流支持的表。

- `LATEST`: 读取从最新的记录开始的分片。
- `TRIM_HORIZON`: 读取从最早可能的记录开始的分片（取决于保留的设置，数据可能被 Kinesis 修剪）。
- `AT_TIMESTAMP`: 读取从一个特定的时间戳开始的分片。时间戳也必须在配置中指定属性，为 `ConsumerConfigConstants.STREAM_INITIAL_TIMESTAMP` 提供值，采用以下日期模式之一：
   - 一个非负双精度值表示从 Unix 纪元开始的秒的数量 （例如，`1459799926.480`）。
   - 一个用户定义的模式，`ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT` 提供的 `SimpleDateFormat` 有效模式，
     如果 `ConsumerConfigConstants.STREAM_TIMESTAMP_DATE_FORMAT` 未被定义，那么默认的模式是 `yyyy-MM-dd'T'HH:mm:ss.SSSXXX`。
	 例如，时间戳的值是 `2016-04-04` 而用户定义的模式是 `yyyy-MM-dd` 或者时间戳的值是 `2016-04-04T19:58:46.480-00:00` 而用户未定义模式。

### 精确一次的用户定义状态更新语义的容错性

启用 Flink 的检查点后，Flink Kinesis Consuemer 将消费 Kinesis 流中分片的记录，并且
定期检查每个分片的进度。如果作业失败， Flink 会将流程序恢复到
最新完成的检查点的状态，并从存储在检查点中的程序开始重新消费 Kinesis 分片中的记录。

产生检查点的间隔定义了程序在出现故障时最多需要返回的量。

要使用容错的 Kinesis Consumers，需要在执行环境中启用拓扑检查点：

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
{{< tab "Python" >}}
```python
env = StreamExecutionEnvironment.get_execution_environment()
env.enable_checkpointing(5000) # checkpoint every 5000 msecs
```
{{< /tab >}}
{{< /tabs >}}

还请注意，只有当有足够的处理槽可用于重新启动拓扑时， Flink 才能重新启动拓扑。
因此，如果拓扑由于 TaskManager 的丢失而失败，那么之后必须仍然有足够的可用插槽。
Flink on YARN 支持自动重启丢失的 YARN 容器。

### 使用增强型扇出

[Enhanced Fan-Out (EFO)](https://aws.amazon.com/blogs/aws/kds-enhanced-fanout/) 增加了每个 Kinesis 数据流的并行消费者的最大数量。
没有 EFO , 所有的并行 Kinesis 消费者共享一个单一的按分片分配的读取配额。
有了 EFO , 每个消费者获取一个不同的专用的按分片分配的读取配额，允许读吞吐量按消费者的数量放大。
使用 EFO 将 [incur additional cost](https://aws.amazon.com/kinesis/data-streams/pricing/) 。

为了启动 EFO ，两个附加的配置参数是需要的:

- `RECORD_PUBLISHER_TYPE`: 确定是使用 `EFO` 还是 `POLLING`. 默认 `RecordPublisher` 是 `POLLING` 。
- `EFO_CONSUMER_NAME`: 用于识别消费者的名字。 
对于给定的 Kinesis 数据流，每个消费者必须具有唯一的名称。
然而，消费者名称在数据流中不必是唯一的。
重用消费名称将导致现有订阅终止。

下面的代码片段显示了配置 EFO 消费者的简单示例。

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
{{< tab "Python" >}}
```python
consumer_config = {
    'aws.region': 'us-east-1',
    'flink.stream.initpos': 'LATEST',
    'flink.stream.recordpublisher':  'EFO',
    'flink.stream.efo.consumername': 'my-flink-efo-consumer'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer(
    "kinesis_stream_name", SimpleStringSchema(), consumer_config))
```
{{< /tab >}}
{{< /tabs >}}

#### EFO Stream Consumer 注册/注销

为了使用EFO，必须针对您希望使用的每个流注册流消费者。
默认情况下，`FlinkKinesisConsumer` 将在 Flink 作业启动时自动注册流消费者。
流消费者将使用 `EFO_CONSUMER_NAME` 配置提供的名称注册。
`FlinkKinesisConsumer` 提供了三种注册策略：

- 注册
  - `LAZY` (默认): Flink作业开始运行时，将注册流消费者。            
    如果流消费者已经存在，则将重用它。
    这是大多数应用程序的首选策略。
    然而，并行度大于1的作业将导致任务竞争注册和获取流消费者 ARN。
    对于并行度非常大的作业，这可能会导致启动时间增加。
    描述操作限制为20 [transactions per second](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamConsumer.html),
    这意味着应用程序启动时间将增加大约 `parallelism/20 seconds`。 
  - `EAGER`: 流消费者在 `FlinkKinesisConsumer` 构造函数中注册。
    如果流消费者已经存在，则将重用它。
    这将导致在构建作业时发生注册，
    在 Flink JobManager 或客户端环境中提交作业。
    使用此策略会导致单线程注册和检索流消费者 ARN，
    通过 `LAZY`（具有大并行度）减少启动时间。
    然而，请考虑客户端环境将需要访问 AWS 服务。
  - `NONE`: 流消费者注册不是由 `FlinkKinesisConsumer` 执行的。
    必须使用 [AWS CLI或SDK] 在外部执行注册(https://aws.amazon.com/tools/) 。
    调用 [RegisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html) 。
    应通过消费者配置向作业提供流消费者 ARN。
- 注销
  - `LAZY|EAGER` (默认):当作业正常关闭时，将注销流消费者。
    如果作业在执行关闭钩子程序时终止，流消费者将保持活动状态。
    在这种情况下，当应用程序重新启动时，流消费者将被优雅地重用。
  - `NONE`: 流消费者注销不是由 `FlinkKinesisConsumer` 执行的。

下面是一个使用 `EAGER` 注册策略的配置例子：

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
{{< tab "Python" >}}
```python
consumer_config = {
    'aws.region': 'us-east-1',
    'flink.stream.initpos': 'LATEST',
    'flink.stream.recordpublisher':  'EFO',
    'flink.stream.efo.consumername': 'my-flink-efo-consumer',
    'flink.stream.efo.registration': 'EAGER'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer(
    "kinesis_stream_name", SimpleStringSchema(), consumer_config))
```
{{< /tab >}}
{{< /tabs >}}

下面是一个使用 `NONE` 注册策略的配置例子：

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
{{< tab "Python" >}}
```python
consumer_config = {
    'aws.region': 'us-east-1',
    'flink.stream.initpos': 'LATEST',
    'flink.stream.recordpublisher':  'EFO',
    'flink.stream.efo.consumername': 'my-flink-efo-consumer',
    'flink.stream.efo.consumerarn.stream-name':
        'arn:aws:kinesis:<region>:<account>>:stream/<stream-name>/consumer/<consumer-name>:<create-timestamp>'
}

env = StreamExecutionEnvironment.get_execution_environment()

kinesis = env.add_source(FlinkKinesisConsumer(
    "kinesis_stream_name", SimpleStringSchema(), consumer_config))
```
{{< /tab >}}
{{< /tabs >}}

### 消费记录的事件时间

如果流拓扑选择使用 [event time notion]({{< ref "docs/concepts/time" >}}) 进行记录
时间戳，默认情况下将使用 *approximate arrival timestamp*。一旦记录被流成功接收和存储，这个时间戳就会被 Kinesis 附加到记录上。
请注意，此时间戳通常称为 Kinesis 服务器端时间戳，并且不能保证准确性或顺序正确性（即时间戳可能并不总是升序）。

用户可以选择使用自定义时间戳覆盖此默认值如描述 [here]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}),
或者使用 [predefined ones]({{< ref "docs/dev/datastream/event-time/built_in" >}}) 中的一个文件。之后，它可以通过以下方式传递给消费者：

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
{{< tab "Python" >}}
```python
consumer = FlinkKinesisConsumer(
    "kinesis_stream_name",
    SimpleStringSchema(),
    consumer_config)
stream = env.add_source(consumer).print()
```
{{< /tab >}}
{{< /tabs >}}

在内部，每个分片/消费者线程执行一个分配器器实例（请参阅下面的线程模型）。
当指定分配器时，对于从 Kinesis 读取的每个记录， extractTimestamp(T element, long previousElementTimestamp)
被调用来为记录分配时间戳，并调用 getCurrentWatermark 来确定分片的新水印。
然后，消费者子任务的水印被确定为其所有分片的最小水印，并定期发出。
每个分片的水印对于处理分片之间的不同消费速度至关重要，否则可能导致
解决依赖水印的下游逻辑问题，例如错误的延迟数据丢弃。

默认情况下，如果分片不提供新记录，水印将暂停。
`ConsumerConfigConstants.SHARD_IDLE_INTERVAL_MILLIS`  属性通过超时允许水印在存在空闲分片的情况下继续。

### 分片消费者的事件时间对齐

Flink Kinesis Consumer 可选地支持并行消费者子任务（及其线程）之间的同步
避免事件时间偏离，相关的问题描述在 [Event time synchronization across sources](https://issues.apache.org/jira/browse/FLINK-10886) 。

要启用同步，请在消费者上设置水印跟踪器：

{{< tabs "8fbaf5cb-3b76-4c62-a74e-db51b60f6601" >}}
{{< tab "Java" >}}
```java
JobManagerWatermarkTracker watermarkTracker =
    new JobManagerWatermarkTracker("myKinesisSource");
consumer.setWatermarkTracker(watermarkTracker);
```
{{< /tab >}}
{{< tab "Python" >}}
```python
watermark_tracker = WatermarkTracker.job_manager_watermark_tracker("myKinesisSource")
consumer.set_watermark_tracker(watermark_tracker)
```
{{< /tab >}}
{{< /tabs >}}

`JobManagerWatermarkTracker` 使用全局聚合来同步每个子任务的水印。每个子任务
使用每个分片队列来控制记录的传输速率，该速率基于水印队列中的下一条记录在全局队列的领先程度。

"提前发送" 限制通过 `ConsumerConfigConstants.WATERMARK_LOOKAHEAD_MILLIS` 配置。较小的值减少
倾斜和吞吐量。较大的值允许子任务在等待全局水印推进前继续运行。

吞吐量方程的另一个可变因素是跟踪器传播水印的频率。可以通过 `ConsumerConfigConstants.WATERMARK_SYNC_MILLIS` 
配置间隔。较小的值会减少发送等待，并以增加与作业管理器的通信为代价。

由于发生倾斜时记录会在队列中累积，因此需要增加内存消耗。内存消耗增加的多少取决于平均记录大小。对于较大的尺寸，
可能需要通过 `ConsumerConfigConstants.WATERMARK_SYNC_QUEUE_CAPACITY` 调整发射队列容量。

### 线程模型

Flink Kinesis Consumer 使用多个线程进行分片发现和数据消费。

#### 分片发现

对于分片发现，每个并行消费者子任务都将有一个线程，该线程不断查询 Kinesis 获取分片信息
即使在消费者启动时，子任务最初没有可读取的分片。换句话说，如果消费者以10的并行度运行，
不管订阅流的分片总的数量多少，总共会有10个线程不断查询 Kinesis。

#### 轮询（默认）记录发布者

对于 `POLLING` 数据消费，将创建一个线程来消费每个发现的分片。线程将在它负责消费的分片
由于流重分片而关闭。换句话说，每个开放的分片总会有一个线程消费。

#### 增强的扇出记录发布者

对于 `EFO` 数据消费，线程模型与 `POLLING` 相同，需要额外的线程池处理和 Kinesis 的异步通信。
AWS SDK v2.x `KinesisAsyncClient` 为 Netty 使用附加的线程池处理 IO 和异步响应。每个并行消费
子任务都有自己的 `KinesisAsyncClient` 实例。换句话说，如果消费者以 10 的并行度运行，那么总共
将有 10 个`KinesisAsyncClient` 实例。
在注册和注销流消费者时，一个单独的客户端将会被创建和随后销毁。

### 内部使用的 Kinesis APIs

Flink Kinesis Consumer 内部使用 [AWS Java SDK](http://aws.amazon.com/sdk-for-java/) 调用 Kinesis APIs
用于分片和数据消费。由于亚马逊在 API 的 [service limits for Kinesis Streams](http://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html)
消费者将与用户正在运行的其他非 Flink 消费应用程序竞争。
下面是消费者调用的 API 列表，其中描述了消费者如何使用 API 以及信息关于如何处理 Flink Kinesis Consumer 因这些服务限制而可能出现的任何错误或警告。

#### Shard Discovery

- *[ListShards](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_ListShards.html)*: 通常为每个并
行消费子任务中的单个线程调用来发现由于流重分片而产生的任何新分片。默认情况下，消费者每隔10秒执行分片发现，并
将无限期重试，直到得到来自 Kinesis 的结果。如果这会干扰其他非 Flink 消费应用程序，则用户可以在提供的配置属性
设置 `ConsumerConfigConstants.SHARD_DISCOVERY_INTERVAL_MILLIS` 的值调用此 API 减慢消费。这把发现的间隔设置为
了不同的值。请注意，此设置直接影响了发现一个新的分片开启消费的最大延迟，因为在间隔期间不会发现分片。

#### 轮询（默认）记录发布者

- *[GetShardIterator](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetShardIterator.html)*: 这
仅在每个分片消费线程启动时调用一次，如果 Kinesis 抱怨 API 已超过，默认情况下最多尝试3次。注意，由于此API的速率
是按每个分片（而不是每个流）限制，消费者本身不应超过限制。通常，如果发生这种情况，用户可以尝试减慢任何其它调用
此 API 的非 Flink 消费应用程序的速度，或通过在提供的配置属性设置以 `ConsumerConfigConstants.SHARD_GETITERATOR_*`
为前缀的键以修改消费者对此API调用的重试行为。

- *[GetRecords](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_GetRecords.html)*: 通常为每个分片线
程调用从 Kinesis 中获取记录。当一个分片有多个并发消费者时（当如果任何其他非Flink消费应用程序正在运行），则可能会
超过每个分片的速率限制。默认情况下，每次 API 调用时，如果 Kinesis 投诉 API 的数据大小/事务限制已超过限制，消费者
将重试，默认最多重试3次。用户可以尝试减慢其它非 Flink 消费应用程序的速度，也可以通过在提供的配置属性设置
`ConsumerConfigConstants.SHARD_GETRECORDS_MAX` 和 `ConsumerConfigConstants.SHARD_GETRECORDS_INTERVAL_MILLIS` 的
键来调整消费者的吞吐量。设置前者调整每个消费线程在每次调用时尝试从分片中获取的最大记录数（默认值为10000），同时
设置后者修改每次获取之间的睡眠间隔的睡眠间隔时间（默认值为200）。调用此 API 的消费者的重试行为也可以通过使用
`ConsumerConfigConstants.SHARD_GETRECORDS_*` 为前缀的其它键进行修改。

#### 增强的扇出记录发布者

- *[SubscribeToShard](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShard.html)*: 通常为
每个分片消费线程调用来获得分片订阅。分片订阅通常在5分钟内处于活动状态，但如果抛出任何可恢复的错误，则需要重新订阅。一
旦获得订阅，消费将收到流 [SubscribeToShardEvents](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_SubscribeToShardEvent.html)s 。
重试和补偿参数可以使用 `ConsumerConfigConstants.SUBSCRIBE_TO_SHARD_*` 键配置。

- *[DescribeStreamSummary](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamSummary.html)*: 
通常为每个流调用在流消费者注册时调用一次。默认情况下，`LAZY` 注册策略按作业并行度放大调用数。`EAGER` 注册策略按每个流
调用一次。`NONE` 注册策略不调用。重试和补偿参数可以使用 `ConsumerConfigConstants.STREAM_DESCRIBE_*` 键进行配置。

- *[DescribeStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DescribeStreamConsumer.html)*:
流消费者注册和注销时调用。对于每个流，将定期调用此服务直到流消费者在注册/注销收到报告 `ACTIVE`/`not found`。默认情况下，
`LAZY` 注册策略按作业并行度放大调用数。`EAGER` 注册策略按每个流只调用一次此服务用于注册。`NONE` 注册策略不调用次服务。
重试和补偿参数可以使用 `ConsumerConfigConstants.DESCRIBE_STREAM_CONSUMER_*` 键进行配置。

- *[RegisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_RegisterStreamConsumer.html)*: 
每个流消费者注册时调用一次，除非配置了 `NONE` 注册策略。重试和补偿参数可以使用 
`ConsumerConfigConstants.REGISTER_STREAM_*` 键进行配置。

- *[DeregisterStreamConsumer](https://docs.aws.amazon.com/kinesis/latest/APIReference/API_DeregisterStreamConsumer.html)*: 
每个流消费者注销时调用一次，除非配置了 `NONE` 或者 `EAGER` 注册策略。重试和补偿参数可以使用 
`ConsumerConfigConstants.DEREGISTER_STREAM_*` 键进行配置。

## Kinesis 流接收器
Kinesis Streams 接收器 (此后简称 "Kinesis 接收器") 使用 [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html) 
从 Flink 流数据写入数据到 Kinesis 流。

写数据到 Kinesis 流，确保流在 Amazon Kinesis Data Stream 控制台标记为 "ACTIVE"。

为了让监控工作，访问流的用户需要访问 CloudWatch 服务。

{{< tabs "6df3b696-c2ca-4f44-bea0-96cf8275d61c" >}}
{{< tab "Java" >}}
```java
Properties sinkProperties = new Properties();
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "us-east-1");

// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");

KinesisStreamsSink<String> kdsSink =
    KinesisStreamsSink.<String>builder()
        .setKinesisClientProperties(sinkProperties)                               // Required
        .setSerializationSchema(new SimpleStringSchema())                         // Required
        .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))  // Required
        .setStreamName("your-stream-name")                                        // Required
        .setFailOnError(false)                                                    // Optional
        .setMaxBatchSize(500)                                                     // Optional
        .setMaxInFlightRequests(50)                                               // Optional
        .setMaxBufferedRequests(10_000)                                           // Optional
        .setMaxBatchSizeInBytes(5 * 1024 * 1024)                                  // Optional
        .setMaxTimeInBufferMS(5000)                                               // Optional
        .setMaxRecordSizeInBytes(1 * 1024 * 1024)                                 // Optional
        .build();

DataStream<String> simpleStringStream = ...;
simpleStringStream.sinkTo(kdsSink);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val sinkProperties = new Properties()
// Required
sinkProperties.put(AWSConfigConstants.AWS_REGION, "us-east-1")

// Optional, provide via alternative routes e.g. environment variables
sinkProperties.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
sinkProperties.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")

val kdsSink = KinesisStreamsSink.<String>builder()
    .setKinesisClientProperties(sinkProperties)                               // Required
    .setSerializationSchema(new SimpleStringSchema())                         // Required
    .setPartitionKeyGenerator(element -> String.valueOf(element.hashCode()))  // Required
    .setStreamName("your-stream-name")                                        // Required
    .setFailOnError(false)                                                    // Optional
    .setMaxBatchSize(500)                                                     // Optional
    .setMaxInFlightRequests(50)                                               // Optional
    .setMaxBufferedRequests(10000)                                            // Optional
    .setMaxBatchSizeInBytes(5 * 1024 * 1024)                                  // Optional
    .setMaxTimeInBufferMS(5000)                                               // Optional
    .setMaxRecordSizeInBytes(1 * 1024 * 1024)                                 // Optional
    .build()

val simpleStringStream = ...
simpleStringStream.sinkTo(kdsSink)
```
{{< /tab >}}
{{< tab "Python" >}}
```python
# Required
sink_properties = {
    # Required
    'aws.region': 'us-east-1',
    # Optional, provide via alternative routes e.g. environment variables
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'aws.endpoint': 'http://localhost:4567'
}

kds_sink = KinesisStreamsSink.builder() \
    .set_kinesis_client_properties(sink_properties) \                      # Required
    .set_serialization_schema(SimpleStringSchema()) \                      # Required
    .set_partition_key_generator(PartitionKeyGenerator.fixed()) \          # Required
    .set_stream_name("your-stream-name") \                                 # Required
    .set_fail_on_error(False) \                                            # Optional
    .set_max_batch_size(500) \                                             # Optional
    .set_max_in_flight_requests(50) \                                      # Optional
    .set_max_buffered_requests(10000) \                                    # Optional
    .set_max_batch_size_in_bytes(5 * 1024 * 1024) \                        # Optional
    .set_max_time_in_buffer_ms(5000) \                                     # Optional
    .set_max_record_size_in_bytes(1 * 1024 * 1024) \                       # Optional
    .build()

simple_string_stream = ...
simple_string_stream.sink_to(kds_sink)
```
{{< /tab >}}
{{< /tabs >}}

上面是使用Kinesis 接收器的一个简单例子。从创建一个配置了 `AWS_REGION`, `AWS_ACCESS_KEY_ID` 和 `AWS_SECRET_ACCESS_KEY``java.util.Properties` 的 `java.util.Properties` 实例开始。
然后可以使用构建器构造接收器。可选配置的默认值如上所示。其中一些值是根据 [configuration on KDS](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html) 设置的。

您将始终需要指定您的序列化模式和逻辑用于从一个记录中生成 [partition key](https://docs.aws.amazon.com/streams/latest/dev/key-concepts.html#partition-key) 。

由于多种原因，Kinesis 数据流可能无法持久化请求中的部分或全部记录。如果 `failOnError` 发生，则会引发运行时异常。否则，这些记录将在缓冲区中重新入队列以供重试。

Kinesis Sink 通过 Flink 的 [metrics system]({{< ref "docs/ops/metrics" >}}) 提供了一些指标分析连接器的行为。所有公开指标的列表可在 [here]({{<ref "docs/ops/metrics#kinesis-sink">}}) 找到。

根据 Kinesis Data Streams 的最大值，接收器默认最大记录大小为1MB，最大批量大小为5MB 。可以找到详细说明这些最大值的 AWS 文档 [here](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html) 。

### Kinesis 接收器和容错

接收器设计用于参与 Flink 的检查点，以提供至少一次处理的保证。它通过在执行检查点的同时完成任何等待的请求来实现这一点。这有效地确保了在继续处理更多记录之前，在检查点之前触发的所有请求都已成功传送到 Kinesis Data Streams 。

如果Flink需要从检查点（或保存点）还原，则自该检查点以来写入的数据将再次写入 Kinesis，从而导致流中出现重复。此外，接收器在内部使用 `PutRecords` API 调用，这并不保证维持事件顺序。

### 背压

当接收器缓冲区填满并写入接收器时，接收器中的背压会增加开始表现出阻塞行为。有关 Kinesis Data Streams 的速率限制的更多信息，可以在
[Quotas and Limits](https://docs.aws.amazon.com/streams/latest/dev/service-sizes-and-limits.html) 找到。

通常通过增加内部队列的大小来减少背压：

{{< tabs "6df3b696-c2ca-4f44-bea0-96cf8275d61d" >}}
{{< tab "Java" >}}
```java
```
KinesisStreamsSink<String> kdsSink =
    KinesisStreamsSink.<String>builder()
        ...
        .setMaxBufferedRequests(10_000)
        ...
```
{{< /tab >}}
{{< tab "Python" >}}
```python
kds_sink = KinesisStreamsSink.builder() \
    .set_max_buffered_requests(10000) \
    .build()
```
{{< /tab >}}
{{< /tabs >}}

## Kinesis 生产者
{{< hint warning >}}
旧的 Kinesis 接收器 `org.apache.flink.streaming.connectors.kinesis.FlinkKinesisProducer` 已弃用，可能会随 Flink 的未来版本一起删除, 请用 [Kinesis Sink]({{<ref "docs/connectors/datastream/kinesis#kinesis-streams-sink">}}) 代替。
{{< /hint >}}

新的接收器使用 [AWS v2 SDK for Java](https://docs.aws.amazon.com/sdk-for-java/latest/developer-guide/home.html) 然而老的接收器使用 Kinesis Producer Library. 因此，新的接收器不支持 [aggregation](https://docs.aws.amazon.com/streams/latest/dev/kinesis-kpl-concepts.html#kinesis-kpl-concepts-aggretation) 。

## 使用自定义 Kinesis 端点

有时，需要让 Flink 作为源或接收器，针对 Kinesis VPC 端点或非 AWS 进行操作Kinesis 端点，例如 [Kinesalite](https://github.com/mhart/kinesalite); 这在执行 Flink
应用程序时特别有用。通常由 Flink 配置中设置的 AWS 区域集推断的 AWS 端点必须通过配置属性覆盖。

要覆盖 AWS 端点，设置 `AWSConfigConstants.AWS_ENDPOINT` 和 `AWSConfigConstants.AWS_REGION` 属性。该区域将用于对端点 URL 进行签名。

{{< tabs "bcadd466-8416-4d3c-a6a7-c46eee0cbd4a" >}}
{{< tab "Java" >}}
```java
Properties config = new Properties();
config.put(AWSConfigConstants.AWS_REGION, "us-east-1");
config.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id");
config.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key");
config.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567");
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val config = new Properties()
config.put(AWSConfigConstants.AWS_REGION, "us-east-1")
config.put(AWSConfigConstants.AWS_ACCESS_KEY_ID, "aws_access_key_id")
config.put(AWSConfigConstants.AWS_SECRET_ACCESS_KEY, "aws_secret_access_key")
config.put(AWSConfigConstants.AWS_ENDPOINT, "http://localhost:4567")
```
{{< /tab >}}
{{< tab "Python" >}}
```python
config = {
    'aws.region': 'us-east-1',
    'aws.credentials.provider.basic.accesskeyid': 'aws_access_key_id',
    'aws.credentials.provider.basic.secretkey': 'aws_secret_access_key',
    'aws.endpoint': 'http://localhost:4567'
}
```
{{< /tab >}}
{{< /tabs >}}

{{< top >}}
