---
title: Kafka
weight: 2
type: docs
aliases:
  - /dev/connectors/kafka.html
  - /apis/streaming/connectors/kafka.html
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

# Apache Kafka Connector

Flink provides an [Apache Kafka](https://kafka.apache.org) connector for reading data from and writing data to Kafka topics with exactly-once guarantees.

## Dependency

Apache Flink ships with a universal Kafka connector which attempts to track the latest version of the Kafka client.
The version of the client it uses may change between Flink releases.
Modern Kafka clients are backwards compatible with broker versions 0.10.0 or later.
For details on Kafka compatibility, please refer to the official [Kafka documentation](https://kafka.apache.org/protocol.html#protocol_compatibility).

{{< artifact flink-connector-kafka withScalaVersion >}}

if you are using Kafka source, ```flink-connector-base``` is also required as dependency:

{{< artifact flink-connector-base >}}

Flink's streaming connectors are not currently part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/datastream/project-configuration" >}}).

## Kafka Source
{{< hint info >}}
This part describes the Kafka source based on the new 
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.
{{< /hint >}}

### Usage
Kafka source provides a builder class for constructing instance of KafkaSource. The code snippet
below shows how to build a KafkaSource to consume messages from the earliest offset of topic
"input-topic", with consumer group "my-group" and deserialize only the value of message as string.
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
The following properties are **required** for building a KafkaSource:
- Bootstrap servers, configured by ```setBootstrapServers(String)```
- Topics / partitions to subscribe, see the following
  <a href="#topic-partition-subscription">Topic-partition subscription</a> for more details.
- Deserializer to parse Kafka messages, see the following
  <a href="#deserializer">Deserializer</a> for more details.

### Topic-partition Subscription
Kafka source provide 3 ways of topic-partition subscription:
- Topic list, subscribing messages from all partitions in a list of topics. For example:
  ```java
  KafkaSource.builder().setTopics("topic-a", "topic-b")
  ```
- Topic pattern, subscribing messages from all topics whose name matches the provided regular
  expression. For example:
  ```java
  KafkaSource.builder().setTopicPattern("topic.*")
  ```
- Partition set, subscribing partitions in the provided partition set. For example:
  ```java
  final HashSet<TopicPartition> partitionSet = new HashSet<>(Arrays.asList(
          new TopicPartition("topic-a", 0),    // Partition 0 of topic "topic-a"
          new TopicPartition("topic-b", 5)));  // Partition 5 of topic "topic-b"
  KafkaSource.builder().setPartitions(partitionSet)
  ```
### Deserializer
A deserializer is required for parsing Kafka messages. Deserializer (Deserialization schema) can be
configured by ```setDeserializer(KakfaRecordDeserializationSchema)```, where
```KafkaRecordDeserializationSchema``` defines how to deserialize a Kafka ```ConsumerRecord```.

If only the value of Kafka ```ConsumerRecord``` is needed, you can use
```setValueOnlyDeserializer(DeserializationSchema)``` in the builder, where
```DeserializationSchema``` defines how to deserialize binaries of Kafka message value.

You can also use a <a href="https://kafka.apache.org/24/javadoc/org/apache/kafka/common/serialization/Deserializer.html">```Kafka Deserializer```</a>
for deserializing Kafka message value. For example using ```StringDeserializer``` for deserializing
Kafka message value as string:
```java
import org.apache.kafka.common.serialization.StringDeserializer;

KafkaSource.<String>builder()
        .setDeserializer(KafkaRecordDeserializationSchema.valueOnly(StringSerializer.class));
```

### Starting Offset
Kafka source is able to consume messages starting from different offsets by specifying
```OffsetsInitializer```. Built-in initializers include:

```java
KafkaSource.builder()
    // Start from committed offset of the consuming group, without reset strategy
    .setStartingOffsets(OffsetsInitializer.committedOffsets())
    // Start from committed offset, also use EARLIEST as reset strategy if committed offset doesn't exist
    .setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
    // Start from the first record whose timestamp is greater than or equals a timestamp
    .setStartingOffsets(OffsetsInitializer.timestamp(1592323200L))
    // Start from earliest offset
    .setStartingOffsets(OffsetsInitializer.earliest())
    // Start from latest offset
    .setStartingOffsets(OffsetsInitializer.latest())
```

You can also implement a custom offsets initializer if built-in initializers above cannot fulfill
your requirement.

If offsets initializer is not specified, **OffsetsInitializer.earliest()** will be
used by default.

### Boundedness
Kafka source is designed to support both streaming and batch running mode. By default, the KafkaSource
is set to run in streaming manner, thus never stops until Flink job fails or is cancelled. You can use
```setBounded(OffsetsInitializer)``` to specify stopping offsets and set the source running in
batch mode. When all partitions have reached their stoping offsets, the source will exit.

You can also set KafkaSource running in streaming mode, but still stop at the stopping offset by
using ```setUnbounded(OffsetsInitializer)```. The source will exit when all partitions reach their
specified stopping offset.

### Additional Properties
In addition to properties described above, you can set arbitrary properties for KafkaSource and
KafkaConsumer by using ```setProperties(Properties)``` and ```setProperty(String, String)```.
KafkaSource has following options for configuration:
- ```client.id.prefix``` defines the prefix to use for Kafka consumer's client ID
- ```partition.discovery.interval.ms``` defines the interval im milliseconds for Kafka source
  to discover new partitions. See <a href="#dynamic-partition-discovery">Dynamic Partition Discovery</a>
  below for more details.
- ```register.consumer.metrics``` specifies whether to register metrics of KafkaConsumer in Flink
metric group
- ```commit.offsets.on.checkpoint``` specifies whether to commit consuming offsets to Kafka brokers on checkpoint

For configurations of KafkaConsumer, you can refer to
<a href="http://kafka.apache.org/documentation/#consumerconfigs">Apache Kafka documentation</a>
for more details.

Please note that the following keys will be overridden by the builder even if
it is configured:
- ```key.deserializer``` is always set to ```ByteArrayDeserializer```
- ```value.deserializer``` is always set to ```ByteArrayDeserializer```
- ```auto.offset.reset.strategy``` is overridden by ```OffsetsInitializer#getAutoOffsetResetStrategy()```
  for the starting offsets
- ```partition.discovery.interval.ms``` is overridden to -1 when
  ```setBounded(OffsetsInitializer)``` has been invoked

The code snippet below shows configuring KafkaConsumer to use "PLAIN" as SASL mechanism and provide
JAAS configuration:
```java
KafkaSource.builder()
    .setProperty("sasl.mechanism", "PLAIN")
    .setProperty("sasl.jaas.config", "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"username\" password=\"password\";")
```

### Dynamic Partition Discovery
In order to handle scenarios like topic scaling-out or topic creation without restarting the Flink
job, Kafka source can be configured to periodically discover new partitions under provided 
topic-partition subscribing pattern. To enable partition discovery, set a non-negative value for 
property ```partition.discovery.interval.ms```:
```java
KafkaSource.builder()
    .setProperty("partition.discovery.interval.ms", "10000") // discover new partitions per 10 seconds
```
{{< hint warning >}}
Partition discovery is **disabled** by default. You need to explicitly set the partition discovery
interval to enable this feature.
{{< /hint >}}

### Event Time and Watermarks
By default, the record will use the timestamp embedded in Kafka ```ConsumerRecord``` as the event
time. You can define your own ```WatermarkStrategy``` for extract event time from the record itself,
and emit watermark downstream:
```java
env.fromSource(kafkaSource, new CustomWatermarkStrategy(), "Kafka Source With Custom Watermark Strategy")
```
[This documentation]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}) describes
details about how to define a ```WatermarkStrategy```.

### Consumer Offset Committing
Kafka source commits the current consuming offset when checkpoints are **completed**, for 
ensuring the consistency between Flink's checkpoint state and committed offsets on Kafka brokers. 

If checkpointing is not enabled, Kafka source relies on Kafka consumer's internal automatic periodic 
offset committing logic, configured by ```enable.auto.commit``` and ```auto.commit.interval.ms``` in
the properties of Kafka consumer.

Note that Kafka source does **NOT** rely on committed offsets for fault tolerance. Committing offset
is only for exposing the progress of consumer and consuming group for monitoring.

### Monitoring
Kafka source exposes metrics in Flink's metric group for monitoring and diagnosing.
#### Scope of Metric
All metrics of Kafka source reader are registered under group ```KafkaSourceReader```, which is a 
child group of operator metric group. Metrics related to a specific topic partition will be registered
in the group ```KafkaSourceReader.topic.<topic_name>.partition.<partition_id>```.

For example, current consuming offset of topic "my-topic" and partition 1 will be reported in metric: 
```<some_parent_groups>.operator.KafkaSourceReader.topic.my-topic.partition.1.currentOffset``` ,

and number of successful commits will be reported in metric:
```<some_parent_groups>.operator.KafkaSourceReader.commitsSucceeded``` .

#### List of Metrics

|    Metric Name   |                   Description                   |       Scope       |
|:----------------:|:-----------------------------------------------:|:-----------------:|
|   currentOffset  | Current consuming offset of the topic partition |   TopicPartition  |
|  committedOffset | Committed offset of the topic partition         |   TopicPartition  |
| commitsSucceeded | Number of successful commits                    | KafkaSourceReader |
|   commitsFailed  | Number of failed commits                        | KafkaSourceReader |

#### Kafka Consumer Metrics
All metrics of Kafka consumer are also registered under group ```KafkaSourceReader.KafkaConsumer```.
For example, Kafka consumer metric "records-consumed-total" will be reported in metric:
```<some_parent_groups>.operator.KafkaSourceReader.KafkaConsumer.records-consumed-total``` .

You can configure whether to register Kafka consumer's metric by configuring option 
```register.consumer.metrics```. This option will be set as true by default. 

For metrics of Kafka consumer, you can refer to 
<a href="http://kafka.apache.org/documentation/#consumer_monitoring">Apache Kafka Documentation</a>
for more details.

### Behind the Scene
{{< hint info >}}
If you are interested in how Kafka source works under the design of new data source API, you may
want to read this part as a reference. For details about the new data source API,
[documentation of data source]({{< ref "docs/dev/datastream/sources.md" >}}) and
<a href="https://cwiki.apache.org/confluence/display/FLINK/FLIP-27%3A+Refactor+Source+Interface">FLIP-27</a>
provide more descriptive discussions.
{{< /hint >}}

Under the abstraction of the new data source API, Kafka source consists of the following components:
#### Source Split
A source split in Kafka source represents a partition of Kafka topic. A Kafka source split consists
of:
- ```TopicPartition``` the split representing
- Starting offset of the partition
- Stopping offset of the partition, only available when the source is running in bounded mode

The state of Kafka source split also stores current consuming offset of the partition, and the state
will be converted to immutable split when Kafka source reader is snapshot, assigning current offset
to the starting offset of the immutable split.

You can check class ```KafkaPartitionSplit``` and ```KafkaPartitionSplitState``` for more details.

#### Split Enumerator
The split enumerator of Kafka is responsible for discovering new splits (partitions) under the
provided topic partition subscription pattern, and assigning splits to readers, uniformly
distributed across subtasks, in round-robin style. Note that the split enumerator of Kafka source 
pushes splits eagerly to source readers, so it won't need to handle split requests from source reader.

#### Source Reader
The source reader of Kafka source extends the provided ```SourceReaderBase```, and use
single-thread-multiplexed thread model, which read multiple assigned splits (partitions) with one
KafkaConsumer driven by one ```SplitReader```. Messages are deserialized right after they are
fetched from Kafka in ```SplitReader```. The state of split, or current progress of message
consuming is updated by ```KafkaRecordEmitter``` , which is also responsible for assigning event time
when the record is emitted downstream.

## Kafka SourceFunction
{{< hint info >}}
This part describes Kafka source based on the legacy SourceFunction API.
{{< /hint >}}

Flink's Kafka consumer - `FlinkKafkaConsumer` provides access to read from one or more Kafka topics.

The constructor accepts the following arguments:

1. The topic name / list of topic names
2. A DeserializationSchema / KafkaDeserializationSchema for deserializing the data from Kafka
3. Properties for the Kafka consumer.
  The following properties are required:
  - "bootstrap.servers" (comma separated list of Kafka brokers)
  - "group.id" the id of the consumer group

{{< tabs "df271e99-0154-40bb-be51-0b3a5837c14b" >}}
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
    .addSource(new FlinkKafkaConsumer[String]("topic", new SimpleStringSchema(), properties)
```
{{< /tab >}}
{{< /tabs >}}

### The `DeserializationSchema`

The Flink Kafka Consumer needs to know how to turn the binary data in Kafka into Java/Scala objects.
The `KafkaDeserializationSchema` allows users to specify such a schema. The `T deserialize(ConsumerRecord<byte[], byte[]> record)` method gets called for each Kafka message, passing the value from Kafka.

For convenience, Flink provides the following schemas out of the box:

1. `TypeInformationSerializationSchema` (and `TypeInformationKeyValueSerializationSchema`) which creates
    a schema based on a Flink's `TypeInformation`. This is useful if the data is both written and read by Flink.
    This schema is a performant Flink-specific alternative to other generic serialization approaches.

2. `JsonDeserializationSchema` (and `JSONKeyValueDeserializationSchema`) which turns the serialized JSON
    into an ObjectNode object, from which fields can be accessed using `objectNode.get("field").as(Int/String/...)()`.
    The KeyValue objectNode contains a "key" and "value" field which contain all fields, as well as
    an optional "metadata" field that exposes the offset/partition/topic for this message.
    
3. `AvroDeserializationSchema` which reads data serialized with Avro format using a statically provided schema. It can
    infer the schema from Avro generated classes (`AvroDeserializationSchema.forSpecific(...)`) or it can work with `GenericRecords`
    with a manually provided schema (with `AvroDeserializationSchema.forGeneric(...)`). This deserialization schema expects that
    the serialized records DO NOT contain embedded schema.

    - There is also a version of this schema available that can lookup the writer's schema (schema which was used to write the record) in
      [Confluent Schema Registry](https://docs.confluent.io/current/schema-registry/docs/index.html). Using these deserialization schema
      record will be read with the schema that was retrieved from Schema Registry and transformed to a statically provided( either through 
      `ConfluentRegistryAvroDeserializationSchema.forGeneric(...)` or `ConfluentRegistryAvroDeserializationSchema.forSpecific(...)`).

    <br>To use this deserialization schema one has to add the following additional dependency:
    
{{< tabs "8c6721c7-4a48-496e-b0fe-6522cf6a5e13" >}}
{{< tab "AvroDeserializationSchema" >}}
{{< artifact flink-avro >}}
{{< /tab >}}
{{< tab "ConfluentRegistryAvroDeserializationSchema" >}}
{{< artifact flink-avro-confluent-registry >}}
{{< /tab >}}
{{< /tabs >}}

When encountering a corrupted message that cannot be deserialized for any reason the deserialization schema should return null which will result in the record being skipped.
Due to the consumer's fault tolerance (see below sections for more details), failing the job on the corrupted message will let the consumer attempt to deserialize the message again.
Therefore, if deserialization still fails, the consumer will fall into a non-stop restart and fail loop on that corrupted message.

### Kafka Consumers Start Position Configuration

The Flink Kafka Consumer allows configuring how the start positions for Kafka partitions are determined.

{{< tabs "e11f7a2e-0917-4856-afed-883fdeb2123c" >}}
{{< tab "Java" >}}
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

FlinkKafkaConsumer<String> myConsumer = new FlinkKafkaConsumer<>(...);
myConsumer.setStartFromEarliest();     // start from the earliest record possible
myConsumer.setStartFromLatest();       // start from the latest record
myConsumer.setStartFromTimestamp(...); // start from specified epoch timestamp (milliseconds)
myConsumer.setStartFromGroupOffsets(); // the default behaviour

DataStream<String> stream = env.addSource(myConsumer);
...
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val env = StreamExecutionEnvironment.getExecutionEnvironment()

val myConsumer = new FlinkKafkaConsumer[String](...)
myConsumer.setStartFromEarliest()      // start from the earliest record possible
myConsumer.setStartFromLatest()        // start from the latest record
myConsumer.setStartFromTimestamp(...)  // start from specified epoch timestamp (milliseconds)
myConsumer.setStartFromGroupOffsets()  // the default behaviour

val stream = env.addSource(myConsumer)
...
```
{{< /tab >}}
{{< /tabs >}}

All versions of the Flink Kafka Consumer have the above explicit configuration methods for start position.

 * `setStartFromGroupOffsets` (default behaviour): Start reading partitions from
 the consumer group's (`group.id` setting in the consumer properties) committed
 offsets in Kafka brokers. If offsets could not be
 found for a partition, the `auto.offset.reset` setting in the properties will be used.
 * `setStartFromEarliest()` / `setStartFromLatest()`: Start from the earliest / latest
 record. Under these modes, committed offsets in Kafka will be ignored and
 not used as starting positions. If offsets become out of range for a partition,
 the `auto.offset.reset` setting in the properties will be used.
 * `setStartFromTimestamp(long)`: Start from the specified timestamp. For each partition, the record
 whose timestamp is larger than or equal to the specified timestamp will be used as the start position.
 If a partition's latest record is earlier than the timestamp, the partition will simply be read
 from the latest record. Under this mode, committed offsets in Kafka will be ignored and not used as
 starting positions.
 
You can also specify the exact offsets the consumer should start from for each partition:

{{< tabs "c715ff08-54d1-408f-9e05-4e42d1b7d333" >}}
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

The above example configures the consumer to start from the specified offsets for
partitions 0, 1, and 2 of topic `myTopic`. The offset values should be the
next record that the consumer should read for each partition. Note that
if the consumer needs to read a partition which does not have a specified
offset within the provided offsets map, it will fallback to the default
group offsets behaviour (i.e. `setStartFromGroupOffsets()`) for that
particular partition.

Note that these start position configuration methods do not affect the start position when the job is
automatically restored from a failure or manually restored using a savepoint.
On restore, the start position of each Kafka partition is determined by the
offsets stored in the savepoint or checkpoint
(please see the next section for information about checkpointing to enable
fault tolerance for the consumer).

### Kafka Consumers and Fault Tolerance

With Flink's checkpointing enabled, the Flink Kafka Consumer will consume records from a topic and periodically checkpoint all
its Kafka offsets, together with the state of other operations. In case of a job failure, Flink will restore
the streaming program to the state of the latest checkpoint and re-consume the records from Kafka, starting from the offsets that were
stored in the checkpoint.

The interval of drawing checkpoints therefore defines how much the program may have to go back at most, in case of a failure.
To use fault tolerant Kafka Consumers, checkpointing of the topology needs to be enabled in the [job]({{< ref "docs/deployment/config" >}}#execution-checkpointing-interval).

If checkpointing is disabled, the Kafka consumer will periodically commit the offsets to Zookeeper.

### Kafka Consumers Topic and Partition Discovery

#### Partition discovery

The Flink Kafka Consumer supports discovering dynamically created Kafka partitions, and consumes them with
exactly-once guarantees. All partitions discovered after the initial retrieval of partition metadata (i.e., when the
job starts running) will be consumed from the earliest possible offset.

By default, partition discovery is disabled. To enable it, set a non-negative value
for `flink.partition-discovery.interval-millis` in the provided properties config,
representing the discovery interval in milliseconds. 

#### Topic discovery

The Kafka Consumer is also capable of discovering topics by matching topic names using regular expressions.

{{< tabs "e2d7364f-4741-4d36-ae4b-d738f1aa6800" >}}
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

In the above example, all topics with names that match the specified regular expression
(starting with `test-topic-` and ending with a single digit) will be subscribed by the consumer
when the job starts running.

To allow the consumer to discover dynamically created topics after the job started running,
set a non-negative value for `flink.partition-discovery.interval-millis`. This allows
the consumer to discover partitions of new topics with names that also match the specified
pattern.

### Kafka Consumers Offset Committing Behaviour Configuration

The Flink Kafka Consumer allows configuring the behaviour of how offsets
are committed back to Kafka brokers. Note that the
Flink Kafka Consumer does not rely on the committed offsets for fault
tolerance guarantees. The committed offsets are only a means to expose
the consumer's progress for monitoring purposes.

The way to configure offset commit behaviour is different, depending on
whether checkpointing is enabled for the job.

 - *Checkpointing disabled:* if checkpointing is disabled, the Flink Kafka
 Consumer relies on the automatic periodic offset committing capability
 of the internally used Kafka clients. Therefore, to disable or enable offset
 committing, simply set the `enable.auto.commit` / `auto.commit.interval.ms` keys to appropriate values
 in the provided `Properties` configuration.
 
 - *Checkpointing enabled:* if checkpointing is enabled, the Flink Kafka
 Consumer will commit the offsets stored in the checkpointed states when
 the checkpoints are completed. This ensures that the committed offsets
 in Kafka brokers is consistent with the offsets in the checkpointed states.
 Users can choose to disable or enable offset committing by calling the
 `setCommitOffsetsOnCheckpoints(boolean)` method on the consumer (by default,
 the behaviour is `true`).
 Note that in this scenario, the automatic periodic offset committing
 settings in `Properties` is completely ignored.

### Kafka Consumers and Timestamp Extraction/Watermark Emission

In many scenarios, the timestamp of a record is embedded in the record itself, or the metadata of the `ConsumerRecord`.
In addition, users may want to emit watermarks either periodically, or irregularly, e.g. based on
special records in the Kafka stream that contain the current event-time watermark. For these cases, the Flink Kafka
Consumer allows the specification of a [watermark strategy]({{< ref "docs/concepts/time" >}}).

You can specify your custom strategy as described
[here]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}), or use one from the
[predefined ones]({{< ref "docs/dev/datastream/event-time/built_in" >}}). 

{{< tabs "33d02541-e172-4f6e-99fc-e097c5f5f856" >}}
{{< tab "Java" >}}
```java
Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");
properties.setProperty("group.id", "test");

FlinkKafkaConsumer<String> myConsumer =
    new FlinkKafkaConsumer<>("topic", new SimpleStringSchema(), properties);
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy.
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
    new FlinkKafkaConsumer("topic", new SimpleStringSchema(), properties)
myConsumer.assignTimestampsAndWatermarks(
    WatermarkStrategy.
        .forBoundedOutOfOrderness(Duration.ofSeconds(20)))

val stream = env.addSource(myConsumer)
```
{{< /tab >}}
{{< /tabs >}}


**Note**: If a watermark assigner depends on records read from Kafka to advance its watermarks
(which is commonly the case), all topics and partitions need to have a continuous stream of records.
Otherwise, the watermarks of the whole application cannot advance and all time-based operations,
such as time windows or functions with timers, cannot make progress. A single idle Kafka partition causes this behavior.
Consider setting appropriate [idelness timeouts]({{< ref "docs/dev/datastream/event-time/generating_watermarks" >}}#dealing-with-idle-sources) to mitigate this issue.
 
## Kafka Producer

Flink’s Kafka Producer - `FlinkKafkaProducer` allows writing a stream of records to one or more Kafka topics.

The constructor accepts the following arguments:

1. A default output topic where events should be written
2. A SerializationSchema / KafkaSerializationSchema for serializing data into Kafka
3. Properties for the Kafka client. The following properties are required:
    * "bootstrap.servers" (comma separated list of Kafka brokers)
4. A fault-tolerance semantic

{{< tabs "8dbb32b3-47e8-468e-91a7-43cec0c658ac" >}}
{{< tab "Java" >}}
```java
DataStream<String> stream = ...

Properties properties = new Properties();
properties.setProperty("bootstrap.servers", "localhost:9092");

FlinkKafkaProducer<String> myProducer = new FlinkKafkaProducer<>(
        "my-topic",                  // target topic
        new SimpleStringSchema(),    // serialization schema
        properties,                  // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE); // fault-tolerance

stream.addSink(myProducer);
```
{{< /tab >}}
{{< tab "Scala" >}}
```scala
val stream: DataStream[String] = ...

val properties = new Properties
properties.setProperty("bootstrap.servers", "localhost:9092")

val myProducer = new FlinkKafkaProducer[String](
        "my-topic",                  // target topic
        new SimpleStringSchema(),    // serialization schema
        properties,                  // producer config
        FlinkKafkaProducer.Semantic.EXACTLY_ONCE) // fault-tolerance

stream.addSink(myProducer)
```
{{< /tab >}}
{{< /tabs >}}

### The `SerializationSchema`

The Flink Kafka Producer needs to know how to turn Java/Scala objects into binary data.
The `KafkaSerializationSchema` allows users to specify such a schema.
The `ProducerRecord<byte[], byte[]> serialize(T element, @Nullable Long timestamp)` method gets called for each record, generating a `ProducerRecord` that is written to Kafka.

This gives users fine-grained control over how data is written out to Kafka. 
Through the producer record you can:
* Set header values
* Define keys for each record
* Specify custom partitioning of data

### Kafka Producers and Fault Tolerance

With Flink's checkpointing enabled, the `FlinkKafkaProducer` can provide
exactly-once delivery guarantees.

Besides enabling Flink's checkpointing, you can also choose three different modes of operating
chosen by passing appropriate `semantic` parameter to the `FlinkKafkaProducer`:

 * `Semantic.NONE`: Flink will not guarantee anything. Produced records can be lost or they can
 be duplicated.
 * `Semantic.AT_LEAST_ONCE` (default setting): This guarantees that no records will be lost (although they can be duplicated).
 * `Semantic.EXACTLY_ONCE`: Kafka transactions will be used to provide exactly-once semantic. Whenever you write
 to Kafka using transactions, do not forget about setting desired `isolation.level` (`read_committed`
 or `read_uncommitted` - the latter one is the default value) for any application consuming records
 from Kafka.

##### Caveats

`Semantic.EXACTLY_ONCE` mode relies on the ability to commit transactions
that were started before taking a checkpoint, after recovering from the said checkpoint. If the time
between Flink application crash and completed restart is larger than Kafka's transaction timeout
there will be data loss (Kafka will automatically abort transactions that exceeded timeout time).
Having this in mind, please configure your transaction timeout appropriately to your expected down
times.

Kafka brokers by default have `transaction.max.timeout.ms` set to 15 minutes. This property will
not allow to set transaction timeouts for the producers larger than it's value.
`FlinkKafkaProducer` by default sets the `transaction.timeout.ms` property in producer config to
1 hour, thus `transaction.max.timeout.ms` should be increased before using the
`Semantic.EXACTLY_ONCE` mode.

In `read_committed` mode of `KafkaConsumer`, any transactions that were not finished
(neither aborted nor completed) will block all reads from the given Kafka topic past any
un-finished transaction. In other words after following sequence of events:

1. User started `transaction1` and written some records using it
2. User started `transaction2` and written some further records using it
3. User committed `transaction2`

Even if records from `transaction2` are already committed, they will not be visible to
the consumers until `transaction1` is committed or aborted. This has two implications:

 * First of all, during normal working of Flink applications, user can expect a delay in visibility
 of the records produced into Kafka topics, equal to average time between completed checkpoints.
 * Secondly in case of Flink application failure, topics into which this application was writing,
 will be blocked for the readers until the application restarts or the configured transaction 
 timeout time will pass. This remark only applies for the cases when there are multiple
 agents/applications writing to the same Kafka topic.

**Note**:  `Semantic.EXACTLY_ONCE` mode uses a fixed size pool of KafkaProducers
per each `FlinkKafkaProducer` instance. One of each of those producers is used per one
checkpoint. If the number of concurrent checkpoints exceeds the pool size, `FlinkKafkaProducer`
will throw an exception and will fail the whole application. Please configure max pool size and max
number of concurrent checkpoints accordingly.

**Note**: `Semantic.EXACTLY_ONCE` takes all possible measures to not leave any lingering transactions
that would block the consumers from reading from Kafka topic more then it is necessary. However in the
event of failure of Flink application before first checkpoint, after restarting such application there
is no information in the system about previous pool sizes. Thus it is unsafe to scale down Flink
application before first checkpoint completes, by factor larger than `FlinkKafkaProducer.SAFE_SCALE_DOWN_FACTOR`.

## Kafka Connector Metrics

Flink's Kafka connectors provide some metrics through Flink's [metrics system]({{< ref "docs/ops/metrics" >}}) to analyze
the behavior of the connector.
The producers export Kafka's internal metrics through Flink's metric system for all supported versions.
The Kafka documentation lists all exported metrics in its [documentation](http://kafka.apache.org/documentation/#selector_monitoring).

In addition to these metrics, all consumers expose the `current-offsets` and `committed-offsets` for each topic partition.
The `current-offsets` refers to the current offset in the partition. This refers to the offset of the last element that
we retrieved and emitted successfully. The `committed-offsets` is the last committed offset.

The Kafka Consumers in Flink commit the offsets back to the Kafka brokers.
If checkpointing is disabled, offsets are committed periodically.
With checkpointing, the commit happens once all operators in the streaming topology have confirmed that they've created a checkpoint of their state. 
This provides users with at-least-once semantics for the offsets committed to Zookeeper or the broker. For offsets checkpointed to Flink, the system 
provides exactly once guarantees.

The offsets committed to ZK or the broker can also be used to track the read progress of the Kafka consumer. The difference between
the committed offset and the most recent offset in each partition is called the *consumer lag*. If the Flink topology is consuming
the data slower from the topic than new data is added, the lag will increase and the consumer will fall behind.
For large production deployments we recommend monitoring that metric to avoid increasing latency.

## Enabling Kerberos Authentication

Flink provides first-class support through the Kafka connector to authenticate to a Kafka installation
configured for Kerberos. Simply configure Flink in `flink-conf.yaml` to enable Kerberos authentication for Kafka like so:

1. Configure Kerberos credentials by setting the following -
 - `security.kerberos.login.use-ticket-cache`: By default, this is `true` and Flink will attempt to use Kerberos credentials in ticket caches managed by `kinit`.
 Note that when using the Kafka connector in Flink jobs deployed on YARN, Kerberos authorization using ticket caches will not work.
 This is also the case when deploying using Mesos, as authorization using ticket cache is not supported for Mesos deployments.
 - `security.kerberos.login.keytab` and `security.kerberos.login.principal`: To use Kerberos keytabs instead, set values for both of these properties.
 
2. Append `KafkaClient` to `security.kerberos.login.contexts`: This tells Flink to provide the configured Kerberos credentials to the Kafka login context to be used for Kafka authentication.

Once Kerberos-based Flink security is enabled, you can authenticate to Kafka with either the Flink Kafka Consumer or Producer
by simply including the following two settings in the provided properties configuration that is passed to the internal Kafka client:

- Set `security.protocol` to `SASL_PLAINTEXT` (default `NONE`): The protocol used to communicate to Kafka brokers.
When using standalone Flink deployment, you can also use `SASL_SSL`; please see how to configure the Kafka client for SSL [here](https://kafka.apache.org/documentation/#security_configclients). 
- Set `sasl.kerberos.service.name` to `kafka` (default `kafka`): The value for this should match the `sasl.kerberos.service.name` used for Kafka broker configurations.
A mismatch in service name between client and server configuration will cause the authentication to fail.

For more information on Flink configuration for Kerberos security, please see [here]({{< ref "docs/deployment/config" >}}).
You can also find [here]({{< ref "docs/deployment/security/security-kerberos" >}}) further details on how Flink internally setups Kerberos-based security.

## Upgrading to the Latest Connector Version

The generic upgrade steps are outlined in [upgrading jobs and Flink versions
guide]({{< ref "docs/ops/upgrading" >}}). For Kafka, you additionally need
to follow these steps:

* Do not upgrade Flink and the Kafka Connector version at the same time.
* Make sure you have a `group.id` configured for your Consumer.
* Set `setCommitOffsetsOnCheckpoints(true)` on the consumer so that read
  offsets are committed to Kafka. It's important to do this before stopping and
  taking the savepoint. You might have to do a stop/restart cycle on the old
  connector version to enable this setting.
* Set `setStartFromGroupOffsets(true)` on the consumer so that we get read
  offsets from Kafka. This will only take effect when there is no read offset
  in Flink state, which is why the next step is very important.
* Change the assigned `uid` of your source/sink. This makes sure the new
  source/sink doesn't read state from the old source/sink operators.
* Start the new job with `--allow-non-restored-state` because we still have the
  state of the previous connector version in the savepoint.

## Troubleshooting

{{< hint info >}}
If you have a problem with Kafka when using Flink, keep in mind that Flink only wraps
<a href="https://kafka.apache.org/documentation/#consumerapi">KafkaConsumer</a> or
<a href="https://kafka.apache.org/documentation/#producerapi">KafkaProducer</a>
and your problem might be independent of Flink and sometimes can be solved by upgrading Kafka brokers,
reconfiguring Kafka brokers or reconfiguring <tt>KafkaConsumer</tt> or <tt>KafkaProducer</tt> in Flink.
Some examples of common problems are listed below.
{{< /hint >}}

### Data loss

Depending on your Kafka configuration, even after Kafka acknowledges
writes you can still experience data loss. In particular keep in mind about the following properties
in Kafka config:

- `acks`
- `log.flush.interval.messages`
- `log.flush.interval.ms`
- `log.flush.*`

Default values for the above options can easily lead to data loss.
Please refer to the Kafka documentation for more explanation.

### UnknownTopicOrPartitionException

One possible cause of this error is when a new leader election is taking place,
for example after or during restarting a Kafka broker.
This is a retriable exception, so Flink job should be able to restart and resume normal operation.
It also can be circumvented by changing `retries` property in the producer settings.
However this might cause reordering of messages,
which in turn if undesired can be circumvented by setting `max.in.flight.requests.per.connection` to 1.

{{< top >}}
