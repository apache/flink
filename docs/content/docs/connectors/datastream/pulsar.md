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

# Apache Pulsar Connector

Flink provides an [Apache Pulsar](https://pulsar.apache.org) connector for reading data from Pulsar topics with exactly-once guarantees.

## Dependency

You can use the connector with the Pulsar 2.8.1 or higher version. However, if you want a more stable
Pulsar [transactions](https://pulsar.apache.org/docs/en/txn-what/) support, 
it is recommended to use the Pulsar 2.9.2 or higher version.
For details on Pulsar compatibility, refer to the [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification).

{{< artifact flink-connector-pulsar >}}

{{< hint info >}}
Flink's streaming connectors are not part of the binary distribution.
See how to link with them [here]({{< ref "docs/dev/configuration/overview" >}}).
{{< /hint >}}

## Pulsar Source

{{< hint info >}}
This part describes the Pulsar source based on the new
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.

If you want to use the legacy `SourceFunction` interface or on Flink 1.13 or lower releases, refer to the StreamNative's [pulsar-flink](https://github.com/streamnative/pulsar-flink).
{{< /hint >}}

### Usage

Pulsar source provides a builder class for constructing a PulsarSource instance. The code snippet below shows
how to build a PulsarSource instance to consume messages from the earliest cursor of the topic 
"persistent://public/default/my-topic" in **Exclusive** subscription type (`my-subscription`) 
and deserialize the raw payload of the messages as strings.


```java
PulsarSource<String> source = PulsarSource.builder()
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

The following properties are **required** for building a PulsarSource:

- Pulsar service URL, configured by `setServiceUrl(String)`
- Pulsar service HTTP URL (aka. admin URL), configured by `setAdminUrl(String)`
- Pulsar subscription name, configured by `setSubscriptionName(String)`
- Topics / partitions to subscribe, see the following
  [topic-partition subscription](#topic-partition-subscription) for more details.
- Deserializer to parse Pulsar messages, see the following
  [deserializer](#deserializer) for more details.

It's **recommended** to set the consumer name in Pulsar Source by `setConsumerName(String)`. This would give
a unique name to the flink connector in the Pulsar statistic dashboard and a mechanism to
monitor the performance of your flink applications.

### Topic-partition Subscription

Pulsar source provide two ways of topic-partition subscription:

- Topic list, subscribing messages from all partitions in a list of topics. For example:
  ```java
  PulsarSource.builder().setTopics("some-topic1", "some-topic2")

  // Partition 0 and 2 of topic "topic-a"
  PulsarSource.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")
  ```

- Topic pattern, subscribing messages from all topics whose name matches the provided regular expression. For example:
  ```java
  PulsarSource.builder().setTopicPattern("topic-*")
  ```

#### Flexible Topic Naming

Since Pulsar 2.0, all topic names internally are in  a form of  `{persistent|non-persistent}://tenant/namespace/topic`.
Now, for partitioned topics, you can use short names in many cases (for the sake of simplicity).
The flexible naming system stems from the fact that there is now a default topic type, tenant, and namespace in a Pulsar cluster.


Topic property | Default
:------------|:-------
topic type | `persistent`
tenant | `public`
namespace | `default`

This table lists a mapping relationship between your input topic name and the translated topic name:

Input topic name | Translated topic name
:----------------|:---------------------
`my-topic` | `persistent://public/default/my-topic`
`my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic`

{{< hint warning >}}
For non-persistent topics, you need to specify the entire topic name,
as the default-based rules do not apply for non-partitioned topics.
Thus, you cannot use a short name like `non-persistent://my-topic` and would use `non-persistent://public/default/my-topic` instead.
{{< /hint >}}

#### Subscribing Pulsar Topic Partition

Internally, Pulsar divides a partitioned topic as a set of non-partitioned topics according to the partition size.

For example, if a `simple-string` topic with 3 partitions is created under the `sample` tenant with the `flink` namespace.
The topics on Pulsar would be:

Topic name | Partitioned
:--------- | :----------
`persistent://sample/flink/simple-string` | Y
`persistent://sample/flink/simple-string-partition-0` | N
`persistent://sample/flink/simple-string-partition-1` | N
`persistent://sample/flink/simple-string-partition-2` | N

You can directly consume messages from the topic partitions by using the non-partitioned topic names above.
For example, use `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")` 
would consume the partitions 1 and 2 of the `sample/flink/simple-string` topic.

#### Setting Topic Regex Pattern

Pulsar source extracts the topic type (`persistent` or `non-persistent`) from the given topic pattern.
For example, you can use the `PulsarSource.builder().setTopicPattern("non-persistent://my-topic*")` to specify 
a  `non-persistent` topic. By default, a `persistent` topic is created if you do not specify the topic type in the regular expression.

You can use `setTopicPattern("topic-*", RegexSubscriptionMode.AllTopics)` to consume both `persistent` and `non-persistent` topics based on the topic pattern.

Pulsar source would filter the available topics by the `RegexSubscriptionMode`.


### Deserializer

A deserializer (`PulsarDeserializationSchema`) is for parsing Pulsar messages from bytes.
You can configure the deserializer using `setDeserializationSchema(PulsarDeserializationSchema)`.

If only the raw payload of a message (message data in bytes) is needed,
you can use the predefined `PulsarDeserializationSchema`. Pulsar connector provides three implementation methods.

- Decode the message by using Pulsar's [Schema](https://pulsar.apache.org/docs/en/schema-understand/).
  ```java
  // Primitive types
  PulsarDeserializationSchema.pulsarSchema(Schema)

  // Struct types (JSON, Protobuf, Avro, etc.)
  PulsarDeserializationSchema.pulsarSchema(Schema, Class)

  // KeyValue type
  PulsarDeserializationSchema.pulsarSchema(Schema, Class, Class)
  ```
- Decode the message by using Flink's `DeserializationSchema`
  ```java
  PulsarDeserializationSchema.flinkSchema(DeserializationSchema)
  ```
- Decode the message by using Flink's `TypeInformation`
  ```java
  PulsarDeserializationSchema.flinkTypeInfo(TypeInformation, ExecutionConfig)
  ```
  

Pulsar `Message<byte[]>` contains some [extra properties](https://pulsar.apache.org/docs/en/concepts-messaging/#messages),
such as message key, message publish time, message time, and application-defined key/value pairs etc.
These properties could be acquired by the `Message<byte[]>` interface.

If you want to deserialize the Pulsar message by these properties, you need to implement `PulsarDeserializationSchema`.
And ensure that the `TypeInformation` from the `PulsarDeserializationSchema.getProducedType()` is correct.
Flink uses this `TypeInformation` to pass the messages to downstream operators.


### Pulsar Subscriptions

A Pulsar subscription is a named configuration rule that determines how messages are delivered to Flink readers.
The subscription name is required for consuming messages. Pulsar connector supports four subscription types:

- [Exclusive](https://pulsar.apache.org/docs/en/concepts-messaging/#exclusive)
- [Shared](https://pulsar.apache.org/docs/en/concepts-messaging/#shared)
- [Failover](https://pulsar.apache.org/docs/en/concepts-messaging/#failover)
- [Key_Shared](https://pulsar.apache.org/docs/en/concepts-messaging/#key_shared)

There is no difference between `Exclusive` and `Failover` in the Pulsar connector.
When a Flink reader crashes, all (non-acknowledged and subsequent) messages are redelivered to the available Flink readers.

By default, if no subscription type is defined, Pulsar source uses `Shared` subscription.

```java
// Shared subscription with name "my-shared"
PulsarSource.builder().setSubscriptionName("my-shared")

// Exclusive subscription with name "my-exclusive"
PulsarSource.builder().setSubscriptionName("my-exclusive").setSubscriptionType(SubscriptionType.Exclusive)
```

If you want to use the `Key_Shared` subscription type on the Pulsar connector.
Ensure that you provide a `RangeGenerator` implementation. The `RangeGenerator` generates a set of 
key hash ranges so that a respective reader subtask only dispatches messages where the hash of the
message key is contained in the specified range.

Pulsar connector uses a `UniformRangeGenerator` which divides the range by the Flink source 
parallelism if no `RangeGenerator` is provided in the `Key_Shared` subscription type.


### Starting Position

Pulsar source is able to consume messages starting from different positions by setting
the `setStartCursor(StartCursor)` option.

- Start from the earliest available message in the topic.
  ```java
  StartCursor.earliest()
  ```
- Start from the latest available message in the topic.
  ```java
  StartCursor.latest()
  ```
- Start from a specified message between the earliest and the latest.
  Pulsar connector consumes from the latest available message if the message ID does not exist.

  The start message is included in consuming result.
  ```java
  StartCursor.fromMessageId(MessageId)
  ```
- Start from a specified message between the earliest and the latest.
  Pulsar connector consumes from the latest available message if the message ID does not exist.

  Include or exclude the start message by using the second boolean parameter.
  ```java
  StartCursor.fromMessageId(MessageId, boolean)
  ```
- Start from the specified message by `Message<byte[]>.getEventTime()`.
  ```java
  StartCursor.fromMessageTime(long)
  ```

{{< hint info >}}
Each Pulsar message belongs to an ordered sequence on its topic.
The sequence ID (`MessageId`) of the message is ordered in that sequence.
The `MessageId` contains some extra information (the ledger, entry, partition) about how the message is stored,
you can create a `MessageId` by using `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)`.
{{< /hint >}}


### Boundedness

Pulsar source supports streaming and batch running modes.
By default, the `PulsarSource` runs in the streaming mode.

In streaming mode, Pulsar source never stops until a Flink job fails or is cancelled. However,
you can set Pulsar source stopping at a stop position by using  `setUnboundedStopCursor(StopCursor)`.


You can use `setBoundedStopCursor(StopCursor)` to specify a stop position to run in batch mode.


Built-in stop cursors include:

- The Pulsar source never stops consuming messages.
  ```java
  StopCursor.never()
  ```
- Stop at the latest available message when the  Pulsar source starts consuming messages.
  ```java
  StopCursor.latest()
  ```
- Stop when the connector meets a given message, or stop at a message which is produced after this given message.
  ```java
  StopCursor.atMessageId(MessageId)
  ```
- Stop but include the given message in the consuming result.
  ```java
  StopCursor.afterMessageId(MessageId)
  ```
- Stop at the specified message time by `Message<byte[]>.getEventTime()`.
  ```java
  StopCursor.atEventTime(long)
  ```


### Configurable Options

In addition to configuration options described above, you can set arbitrary options for `PulsarClient`,
`PulsarAdmin`, Pulsar `Consumer` and `PulsarSource` by using `setConfig(ConfigOption<T>, T)`,
`setConfig(Configuration)` and `setConfig(Properties)`.

#### PulsarClient Options

The Pulsar connector uses the [client API](https://pulsar.apache.org/docs/en/client-libraries-java/)
to create the `Consumer` instance. The Pulsar connector extracts most parts of Pulsar's `ClientConfigurationData`,
which is required for creating a `PulsarClient`, as Flink configuration options in `PulsarOptions`.

{{< generated/pulsar_client_configuration >}}


#### PulsarAdmin Options

The [admin API](https://pulsar.apache.org/docs/en/admin-api-overview/) is used for querying topic metadata and for discovering the desired topics when the Pulsar connector uses topic-pattern subscription. It shares most part of the
configuration options with the client API. The configuration options listed here are only used in the admin API.
They are also defined in `PulsarOptions`.


{{< generated/pulsar_admin_configuration >}}

#### Pulsar Consumer Options

In general, Pulsar provides the Reader API and Consumer API for consuming messages in different scenarios.
The Pulsar connector uses the Consumer API. It extracts most parts of Pulsar's `ConsumerConfigurationData` as Flink configuration options in `PulsarSourceOptions`.

{{< generated/pulsar_consumer_configuration >}}

#### PulsarSource Options

The configuration options below are mainly used for customizing the performance and message acknowledgement behavior.
You can just leave them alone if you do not meet any performance issues.

{{< generated/pulsar_source_configuration >}}

### Dynamic Partition Discovery

To handle scenarios like topic scaling-out or topic creation without restarting the Flink
job, Pulsar source periodically discover new partitions under a provided
topic-partition subscription pattern. To enable partition discovery, you can set a non-negative value for
the `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS` option`:

```java
// discover new partitions per 10 seconds
PulsarSource.builder()
    .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```


{{< hint warning >}}
- Partition discovery is **enabled** by default. The Pulsar connector queries the topic metadata every 30 seconds.
- To disable partition discovery, you need to set a negative partition discovery interval.
- The partition discovery is disabled in the batch mode even if you set this option with a non-negative value.
{{< /hint >}}


### Event Time and Watermarks

By default, the message uses the timestamp embedded in Pulsar `Message<byte[]>` as the event time.
You can define a `WatermarkStrategy` to extract the event time from the message,
and emit the watermark downstream:


```java
env.fromSource(pulsarSource, new CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy")
```

[This documentation]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}) describes 
details about how to define a `WatermarkStrategy`.

### Message Acknowledgement

When a subscription is created, Pulsar [retains](https://pulsar.apache.org/docs/en/concepts-architecture-overview/#persistent-storage) all messages, even if the consumer is disconnected.
The retained messages are discarded only when the connector acknowledges that all these messages are processed successfully.
The Pulsar connector supports four subscription types, which makes the acknowledgement behaviors vary among different subscriptions.

#### Acknowledgement on Exclusive and Failover Subscription Types

`Exclusive` and `Failover` subscription types support cumulative acknowledgment. In these subscription types, Flink only needs to acknowledge
the latest successfully consumed message. All the message before the given message are marked
with a consumed status.

The Pulsar source acknowledges the current consuming message when checkpoints are **completed**,
to ensure the consistency between Flink's checkpoint state and committed position on the Pulsar brokers.

If checkpointing is disabled, Pulsar source periodically acknowledges messages.
You can use the `PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL` option to set the acknowledgement period.

Pulsar source does **NOT** rely on committed positions for fault tolerance.
Acknowledging messages is only for exposing the progress of consumers and monitoring on these two subscription types.


#### Acknowledgement on Shared and Key_Shared Subscription Types

In `Shared` and `Key_Shared` subscription types, messages are acknowledged one by one. You can acknowledge
a message in a transaction and commit it to Pulsar.

You should enable transaction in the Pulsar `borker.conf` file when using these two subscription types in connector:


```text
transactionCoordinatorEnabled=true
```

Pulsar transaction is  created within 3 hours as the timeout by default. 
Ensure that that timeout is greater than checkpoint interval + maximum recovery time.
A shorter checkpoint interval indicates a better consuming performance.
You can use the `PulsarSourceOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS` option to change the transaction timeout.

If checkpointing is disabled or you can not enable the transaction on Pulsar broker, you should set
`PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE` to `true`.
The message is immediately acknowledged after consuming.
However, we can not promise the consistency in this scenario.

{{< hint info >}}
All acknowledgements in a transaction are recorded in the Pulsar broker side.
{{< /hint >}}

## Pulsar Sink

Pulsar sink supports writing records into one or more Pulsar topics or a specified list of Pulsar partitions.

{{< hint info >}}
This part describes the Pulsar sink based on the new
[data sink](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction) API.

If you still want to use the legacy `SinkFunction` or on Flink 1.14 or previous releases, just use the StreamNative's
[pulsar-flink](https://github.com/streamnative/pulsar-flink).
{{< /hint >}}

### Usage

Pulsar Sink uses a builder class to a construct `PulsarSink` instance.

This example writes a String record to a Pulsar topic with at-least-once delivery guarantee.
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

The following properties are **required** for building PulsarSink:

- Pulsar service url, configured by `setServiceUrl(String)`
- Pulsar service http url (aka. admin url), configured by `setAdminUrl(String)`
- Topics / partitions to write, see [writing targets](#writing-targets) for more details.
- Serializer to generate Pulsar messages, see [serializer](#serializer) for more details.

It is **recommended** to set the producer name in Pulsar Sink by `setProducerName(String)`. This gives
a unique name to the flink connector in the Pulsar statistic dashboard which is convenient for
performance monitoring of your flink applications.

### Specify target topics 

Setting the topics to write is similar to the [topic-partition subscription](#topic-partition-subscription)
in Pulsar source. We support a mixin style of topic setting. Therefore, you can provide a list of topics
, partitions or both of them.

```java
// Topic "some-topic1" and "some-topic2"
PulsarSink.builder().setTopics("some-topic1", "some-topic2")

// Partition 0 and 2 of topic "topic-a"
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2")

// Partition 0 and 2 of topic "topic-a" and topic "some-topic2"
PulsarSink.builder().setTopics("topic-a-partition-0", "topic-a-partition-2", "some-topic2")
```

Auto partition discovery is enabled by default. Pulsar sink query the topic metadata from
the Pulsar in a fixed interval. You can use the `PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL` 
option to change the discovery interval option.

A custom [`TopicRouter`][message routing](#message-routing) allows user implement custom topic routing strategy.
And read [flexible topic naming](#flexible-topic-naming)for understanding how to configure partitions 
on the Pulsar connector.


{{< hint warning >}}
If you build  the Pulsar sink based on both the topic and its corresponding partitions, 
Pulsar sink merges them and only use the topic.

For example, if you use the `PulsarSink.builder().setTopics("some-topic1", "some-topic1-partition-0")` 
option to build the Pulsar sink, it is simplified to `PulsarSink.builder().setTopics("some-topic1")`.

{{< /hint >}}

### Serializer

A serializer (`PulsarSerializationSchema`) is required to serialize the record instance into bytes.
Similar to `PulsarSource`, Pulsar sink supports both Flink's `SerializationSchema` and
Pulsar's `Schema`. But Pulsar's `Schema.AUTO_PRODUCE_BYTES()` is not supported in Pulsar Sink.

If you do not need the message key and other message properties in Pulsar's
[Message](https://pulsar.apache.org/api/client/2.9.0-SNAPSHOT/org/apache/pulsar/client/api/Message.html) 
interface, you can use the predefined `PulsarSerializationSchema`. Pulsar sink provides two default
implementation.


- Encode the message by using Pulsar's [Schema](https://pulsar.apache.org/docs/en/schema-understand/).
  ```java
  // Primitive types
  PulsarSerializationSchema.pulsarSchema(Schema)

  // Struct types (JSON, Protobuf, Avro, etc.)
  PulsarSerializationSchema.pulsarSchema(Schema, Class)

  // KeyValue type
  PulsarSerializationSchema.pulsarSchema(Schema, Class, Class)
  ```
- Encode the message by using Flink's `SerializationSchema`
  ```java
  PulsarSerializationSchema.flinkSchema(SerializationSchema)
  ```

[Schema evolution](https://pulsar.apache.org/docs/en/schema-evolution-compatibility/#schema-evolution)
can be enabled for users by using `PulsarSerializationSchema.pulsarSchema()` and
`PulsarSinkBuilder.enableSchemaEvolution()`, meaning that any broker schema validation will be in place.
Here is a code sample below.

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
If you use Pulsar schema without enabling schema evolution, the target topic will have a `Schema.BYTES` schema.
And consumers need to handle the deserialization (if needed) themselves.

For example, if you set  `PulsarSerializationSchema.pulsarSchema(Schema.STRING)` without enabling schema evolution,
the schema stored in Pulsar topics is `Schema.BYTES`.
{{< /hint >}}

### Message Routing

Routing in Pulsar Sink is operated on the partition level. For a list of partitioned topics,
the routing algorithm first collects all partitions from different topics, and then calculates routing within all the partitions.
By default Pulsar Sink supports two router implementation.

- `KeyHashTopicRouter`: use the hashcode of the message's key to decide the topic partition that messages are sent to.

  The message key is provided by `PulsarMessageBuiilder.key(String key)`.
  You need to implement this interface and extract the message key when you want to send the message
  with the same key to the same topic partition.

  If you do not provide the message key. A topic  partition is randomly chosen from the topic list.

  The message key can be hashed in two ways: `MessageKeyHash.JAVA_HASH` and `MessageKeyHash.MURMUR3_32_HASH`.
  You can use the `PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH` option to choose the hash method.

- `RoundRobinRouter`: Round-robin among all the partitions.

  All messages are sent to the first partition, and switch to the next partition after sending
  a fixed number of messages. The batch size can be customized by the `PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES` option.

You can configure  custom routers by using the `TopicRouter` interface. If you implement a  `TopicRouter`, 
ensure that  it is  serializable. And you can return partitions which are not
available in the pre-discovered partition list.

Thus, you do not  need to specify  topics using the `PulsarSinkBuilder.setTopics` option when you 
implement the custom topic router.


```java
@PublicEvolving
public interface TopicRouter<IN> extends Serializable {

    String route(IN in, List<String> partitions, PulsarSinkContext context);

    default void open(SinkConfiguration sinkConfiguration) {
        // Nothing to do by default.
    }
}
```

{{< hint info >}}
Internally, a Pulsar partition is implemented as a topic. The Pulsar client provides APIs to hide this
implementation detail and handles routing under the hood automatically. Pulsar Sink uses a lower client
API to implement its own routing layer to support multiple topics routing.

For details, see  [partitioned topics](https://pulsar.apache.org/docs/en/cookbooks-partitioned/).
{{< /hint >}}


### Delivery Guarantee

`PulsarSink` supports three delivery guarantee semantics.

- `NONE`: Data loss can happen even when the pipeline is running.
  Basically, we use a fire-and-forget strategy to send records to Pulsar topics in this mode. It means that this mode  has the highest throughput.
- `AT_LEAST_ONCE`: No data loss happens, but data duplication can happen after a restart from checkpoint.
- `EXACTLY_ONCE`: No data loss happens. Each record is sent to the Pulsar broker only once.
  Under the hood,  Pulsar Sink uses [Pulsar transaction](https://pulsar.apache.org/docs/en/transactions/)
  and two phase commit (2PC) to ensure records are sent only once even after pipeline restarts.


### Delayed message delivery

[Delayed message delivery](https://pulsar.apache.org/docs/en/next/concepts-messaging/#delayed-message-delivery)
enables you to consume a message later. With delayed message enabled,  the Pulsar sink sends a message to the Pulsar topic
**immediately**, but the message is delivered to a consumer once the specified delay is over.

Delayed message delivery only works in the `Shared` subscription type. In `Exclusive` and `Failover`
subscription types, the delayed message is dispatched immediately.

You can configure the `MessageDelayer` to specify when to send the message to the consumer.
The default delayer is never delay the message dispatching. You can use the `MessageDelayer.fixed(Duration)` option to
Configure delaying all messages in a fixed duration. You can also implement the `MessageDelayer`
interface to dispatch  messages at different  time.


{{< hint warning >}}
The dispatch time should be calculated by using the `PulsarSinkContext.processTime()`.
{{< /hint >}}

### Configurable Options

You can set options for `PulsarClient`, `PulsarAdmin`, Pulsar `Producer` and `PulsarSink`
by using `setConfig(ConfigOption<T>, T)`, `setConfig(Configuration)` and `setConfig(Properties)`.

#### PulsarClient and PulsarAdmin Options

For details, refer to the [PulsarClient options](#pulsarclient-options) and [PulsarAdmin options](#pulsaradmin-options) defined in source part.

#### Pulsar Producer Options

The Pulsar connector uses the Producer API to send messages. It extracts most parts of
Pulsar's `ProducerConfigurationData` as Flink configuration options in `PulsarSinkOptions`.

{{< generated/pulsar_producer_configuration >}}

#### PulsarSink Options

The configuration options below are mainly used for customizing the performance and message
sending behavior. You can just leave them alone if you do not meet any performance issues.

{{< generated/pulsar_sink_configuration >}}

### Metrics

This table lists supported metrics.
The first 6 metrics are standard Pulsar Sink metrics as described in
[FLIP-33: Standardize Connector Metrics]([https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics](https://cwiki.apache.org/confluence/display/FLINK/FLIP-33%3A+Standardize+Connector+Metrics))

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
        <td>The total number of output bytes since the sink starts.</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>numBytesOutPerSecond</td>
        <td>n/a</td>
        <td>The output bytes per second</td>
        <td>Meter</td>
    </tr>
    <tr>
        <td>numRecordsOut</td>
        <td>n/a</td>
        <td>The total number of output records since the sink starts</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>numRecordsOutPerSecond</td>
        <td>n/a</td>
        <td>The output records per second</td>
        <td>Meter</td>
    </tr>
    <tr>
        <td>numRecordsOutErrors</td>
        <td>n/a</td>
        <td>The total number of records failed to send</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>currentSendTime</td>
        <td>n/a</td>
        <td>The time it takes to send the last record, from when enqueuing the message in client buffer to its ack</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.numAcksReceived</td>
        <td>n/a</td>
        <td>The number of acks received for sent messages</td>
        <td>Counter</td>
    </tr>
    <tr>
        <td>PulsarSink.sendLatencyMax</td>
        <td>n/a</td>
        <td>The max send latency in the last refresh interval across all producers</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency50Pct</td>
        <td>ProducerName</td>
        <td>The 50th percentile of send latency in the last refresh interval for a specific producer</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency75Pct</td>
        <td>ProducerName</td>
        <td>The 75th percentile of send latency in the last refresh interval for a specific producer</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency95Pct</td>
        <td>ProducerName</td>
        <td>The 95th percentile of send latency in the last refresh interval for a specific producer</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency99Pct</td>
        <td>ProducerName</td>
        <td>The 99th percentile of send latency in the last refresh interval for a specific producer</td>
        <td>Gauge</td>
    </tr>
    <tr>
        <td>PulsarSink.producer."ProducerName".sendLatency999Pct</td>
        <td>ProducerName</td>
        <td>The 99.9th percentile of send latency in the last refresh interval for a specific producer</td>
        <td>Gauge</td>
    </tr>
  </tbody>
</table>



{{< hint info >}}
- `numBytesOut`, `numRecordsOut`, `numRecordsOutErrors` are retrieved from Pulsar client metrics.
  See how to link with them [here]({{< ref "docs/dev/configuration/overview" >}}).
- `currentSendTime` tracks the time from when the producer calls `sendAync()` to
  the time when message is acknowledged by the broker. This metric is not available in `NONE` delivery guarantee.
{{< /hint >}}


  
The Pulsar producer refreshes its stats every 60 seconds by default. And PulsarSink retrieves the Pulsar producer
stats every 500ms. Thus `numRecordsOut`, `numBytesOut`, `numAcksReceived`, and `numRecordsOutErrors` 
is updated every 60 seconds. To increase the metrics refresh frequency, you can change
the Pulsar producer stats refresh interval to a smaller value (minimum 1 second) as shown below.

```java
    builder.setConfig(PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS. 1L)
```

`numBytesOutRate` and `numRecordsOutRate` are calculated based on the `numBytesOut` and `numRecordsOut`
counter respectively. Flink internally uses a fixed 60 seconds window to calculate the rates.


### Brief Design Rationale

Pulsar sink follow the Sink API defined in 
[FLIP-191](https://cwiki.apache.org/confluence/display/FLINK/FLIP-191%3A+Extend+unified+Sink+interface+to+support+small+file+compaction).

#### Stateless SinkWriter

In `EXACTLY_ONCE` mode, the Pulsar sink does not store transaction information in checkpoint,
which means a new bunch of transactions will be created after a restart.
Therefore, any message in previous pending transactions is  either aborted or timeout
(Either way, they are not visible to the downstream Pulsar consumer).
Pulsar team is working to optimize the resource cost bought by unfinished pending transactions.
This design gurantees that SinkWriter is stateless.


#### Pulsar Schema Evolution

[Pulsar Schema Evolution](https://pulsar.apache.org/docs/en/schema-evolution-compatibility/) allows
you to reuse the same Flink job after certain "allowed" data model changes, like adding or deleting
a field in a AVRO-based Pojo class. Please note that you can specify Pulsar schema validation rules
and define an auto schema update. For details, refer to [Pulsar Schema Evolution](https://pulsar.apache.org/docs/en/schema-evolution-compatibility/).


## Known Issues
This section describes some known issues about the Pulsar connectors.

### No TransactionCoordinatorNotFound, but automatic reconnect

Pulsar  transaction is still in active development and is not stable. Pulsar 2.9.2
introduce [a breaking change](https://github.com/apache/pulsar/pull/13135) in transactions.
If you use Pulsar 2.9.2 or higher version  with an older Pulsar client, you might 
get a `TransactionCoordinatorNotFound` exception.

You can use the latest `pulsar-client-all` package to resolve this issue.

## Upgrading to the Latest Connector Version

The generic upgrade steps are outlined in [upgrading jobs and Flink versions guide]({{< ref "docs/ops/upgrading" >}}).
The Pulsar connector does not store any state on the Flink side. The Pulsar connector pushes and stores all the states on the Pulsar side.
For Pulsar, you additionally need to know these limitations:

* Do not upgrade the Pulsar connector and Pulsar broker version at the same time.
* Always use a newer Pulsar client with Pulsar connector to consume message from Pulsar.

## Troubleshooting

If you have a problem with Pulsar when using Flink, keep in mind that Flink only wraps
[PulsarClient](https://pulsar.apache.org/docs/en/client-libraries-java/) or
[PulsarAdmin](https://pulsar.apache.org/docs/en/admin-api-overview/)
and your problem might be independent of Flink and sometimes can be solved by upgrading Pulsar brokers,
reconfiguring Pulsar brokers or reconfiguring Pulsar connector in Flink.

### Messages can be delayed on low volume topics

When the Pulsar source connector reads from a low volume topic, users might observe a 10 seconds delay between messages. Pulsar buffers messages from topics by default. Before emitting to downstream
operators, the number of buffered records must be equal or larger than `PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS`. If the data volume is low, it could be that filling up the number of buffered records takes longer than `PULSAR_MAX_FETCH_TIME` (default to 10 seconds). If that's the case, it means that only after this time has passed the messages will be emitted. 

To avoid this behaviour, you need to change either the buffered records or the waiting time. 



{{< top >}}
