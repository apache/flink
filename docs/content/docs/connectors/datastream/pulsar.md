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

Flink provides an [Apache Pulsar](https://pulsar.apache.org) source connector for reading data from Pulsar topics with exactly-once guarantees.

## Dependency

You can use the connector with Pulsar 2.7.0 or higher. However, the Pulsar source connector supports
Pulsar [transactions](https://pulsar.apache.org/docs/en/txn-what/),
it is recommended to use Pulsar 2.8.0 or higher releases.
For details on Pulsar compatibility, please refer to the [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification).

{{< artifact flink-connector-pulsar >}}

Flink's streaming connectors are not part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/configuration/overview" >}}).

## Pulsar Source

{{< hint info >}}
This part describes the Pulsar source based on the new
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.

If you want to use the legacy `SourceFunction` or on Flink 1.13 or lower releases, just use the StreamNative's [pulsar-flink](https://github.com/streamnative/pulsar-flink).
{{< /hint >}}

### Usage

Pulsar source provides a builder class for constructing an instance of PulsarSource. The code snippet below shows
how to build a PulsarSource to consume messages from the earliest cursor of topic "persistent://public/default/my-topic",
with **Exclusive** subscription `my-subscription` and deserialize the raw payload of the messages as strings.

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

The following properties are **required** for building a PulsarSource:

- Pulsar service url, configured by `setServiceUrl(String)`
- Pulsar service http url (aka. admin url), configured by `setAdminUrl(String)`
- Pulsar subscription name, configured by `setSubscriptionName(String)`
- Topics / partitions to subscribe, see the following
  [Topic-partition subscription](#topic-partition-subscription) for more details.
- Deserializer to parse Pulsar messages, see the following
  [Deserializer](#deserializer) for more details.

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

Since Pulsar 2.0, all topic names internally have the form `{persistent|non-persistent}://tenant/namespace/topic`.
Now, for partitioned topics, you can use short names in many cases (for the sake of simplicity).
The flexible naming system stems from the fact that there is now a default topic type, tenant, and namespace in a Pulsar cluster.

Topic property | Default
:------------|:-------
topic type | `persistent`
tenant | `public`
namespace | `default`

This table lists a mapping relationship between your input topic name and translated topic name:

Input topic name | Translated topic name
:----------------|:---------------------
`my-topic` | `persistent://public/default/my-topic`
`my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic`

{{< hint warning >}}
For non-persistent topics, you need to continue to specify the entire topic name,
as the default-based rules do not apply for non-partitioned topics.
Thus, you cannot use a short name like `non-persistent://my-topic` and would need to use `non-persistent://public/default/my-topic` instead.
{{< /hint >}}

#### Subscribing Pulsar Topic Partition

Internally, Pulsar divides a partitioned topic as a set of non-partitioned topics according to the partition size.

For example, if a `simple-string` topic with 3 partitions is created under the `sample` tenant with `flink` namespace.
The topics on Pulsar would be:

Topic name | Partitioned
:--------- | :----------
`persistent://sample/flink/simple-string` | Y
`persistent://sample/flink/simple-string-partition-0` | N
`persistent://sample/flink/simple-string-partition-1` | N
`persistent://sample/flink/simple-string-partition-2` | N

You can directly consume messages from the topic partitions by using the non-partitioned topic names above.
For example, use `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")` would consume the partitions 1 and 2 of the `sample/flink/simple-string` topic.

#### RegexSubscriptionMode for Topic Pattern

Pulsar connector extracts the topic type (`persistent` or `non-persistent`) from the given topic pattern.
For example, `PulsarSource.builder().setTopicPattern("non-persistent://my-topic*")` would be `non-persistent`.
The topic type would be `persistent` if you do not provide the topic type in the regular expression.

To consume both `persistent` and `non-persistent` topics based on the topic pattern,
you can use `setTopicPattern("topic-*", RegexSubscriptionMode.AllTopics)`.
Pulsar connector would filter the available topics by the `RegexSubscriptionMode`.

### Deserializer

A deserializer (Deserialization schema) is required for parsing Pulsar messages. The deserializer is
configured by `setDeserializationSchema(PulsarDeserializationSchema)`.
The `PulsarDeserializationSchema` defines how to deserialize a Pulsar `Message<byte[]>`.

If only the raw payload of a message (message data in bytes) is needed,
you can use the predefined `PulsarDeserializationSchema`. Pulsar connector provides three types of implementation.

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
such as message key, message publish time, message time, application defined key/value pairs that will be attached to the message, etc.
These properties could be acquired by the `Message<byte[]>` interface.

If you want to deserialize the Pulsar message by these properties, you need to implement `PulsarDeserializationSchema`.
And ensure that the `TypeInformation` from the `PulsarDeserializationSchema.getProducedType()` must be correct.
Flink would use this `TypeInformation` for passing the messages to downstream operators.

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

If you want to use `Key_Shared` subscription type on the Pulsar connector. Ensure that you provide a `RangeGenerator` implementation.
The `RangeGenerator` generates a set of key hash ranges so that
a respective reader subtask will only dispatch messages where the hash of the message key is contained in the specified range.

Pulsar connector would use a `UniformRangeGenerator` which would divides the range by the Flink source parallelism
if no `RangeGenerator` is provided in the `Key_Shared` subscription type.

### Starting Position

Pulsar source is able to consume messages starting from different positions by `setStartCursor(StartCursor)`.
Built-in start cursors include:

- Start from the earliest available message in the topic.
  ```java
  StartCursor.earliest()
  ```
- Start from the latest available message in the topic.
  ```java
  StartCursor.latest()
  ```
- Start from a specified message between the earliest and the latest.
  Pulsar connector would consume from the latest available message if the message id doesn't exist.

  The start message is included in consuming result.
  ```java
  StartCursor.fromMessageId(MessageId)
  ```
- Start from a specified message between the earliest and the latest.
  Pulsar connector would consume from the latest available message if the message id doesn't exist.

  Include or exclude the start message by using the second boolean parameter.
  ```java
  StartCursor.fromMessageId(MessageId, boolean)
  ```
- Start from the specified message time by `Message<byte[]>.getEventTime()`.
  ```java
  StartCursor.fromMessageTime(long)
  ```

{{< hint info >}}
Each Pulsar message belongs to an ordered sequence on its topic.
The sequence ID (`MessageId`) of the message is ordered in that sequence.
`MessageId` contains some extra information (the ledger, entry, partition) on how the message is stored,
you can create a `MessageId` by using `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)`.
{{< /hint >}}

### Boundedness

Pulsar source supports streaming and batch running modes.
By default, the `PulsarSource` is set to run in the streaming mode.

In streaming mode, Pulsar source never stops until a Flink job fails or is cancelled. However,
you can set Pulsar source stopping at a stop position by using ```setUnboundedStopCursor(StopCursor)```.
The Pulsar source will finish when all partitions reach their specified stop positions.

You can use ```setBoundedStopCursor(StopCursor)``` to specify a stop position so that the Pulsar source can run in the batch mode.
When all partitions have reached their stop positions, the source will finish.

Built-in stop cursors include:

- Connector will never stop consuming.
  ```java
  StopCursor.never()
  ```
- Stop at the latest available message in Pulsar when the connector starts consuming.
  ```java
  StopCursor.latest()
  ```
- Stop when connector meet a given message, or stop at a message which is produced after this given message.
  ```java
  StopCursor.atMessageId(MessageId)
  ```
- Stop but include the given message in consuming result.
  ```java
  StopCursor.afterMessageId(MessageId)
  ```
- Stop at the specified message time by `Message<byte[]>.getEventTime()`.
  ```java
  StopCursor.atEventTime(long)
  ```

### Configurable Options

In addition to configuration options described above, you can set arbitrary options for `PulsarClient`,
`PulsarAdmin`， Pulsar `Consumer` and `PulsarSource` by using `setConfig(ConfigOption<T>, T)` and `setConfig(Configuration)`.

#### PulsarClient Options

Pulsar connector use the [client API](https://pulsar.apache.org/docs/en/client-libraries-java/)
to create the `Consumer` instance. Pulsar connector extracts most parts of Pulsar's `ClientConfigurationData`,
which is required for creating a `PulsarClient`, as Flink configuration options in `PulsarOptions`.

{{< generated/pulsar_client_configuration >}}

#### PulsarAdmin Options

The [admin API](https://pulsar.apache.org/docs/en/admin-api-overview/) is used for querying topic metadata
and for discovering the desired topics when Pulsar connector uses topic pattern subscription. It would share most part of the
configuration options with the client API. The configuration options listed here are only used in the admin API.
They are also defined in `PulsarOptions`.

{{< generated/pulsar_admin_configuration >}}

#### Pulsar Consumer Options

In general, Pulsar provides the Reader API and Consumer API for consuming messages in different scenarios.
Flink's Pulsar connector uses the Consumer API. It extracts most parts of Pulsar's `ConsumerConfigurationData` as Flink configuration options in `PulsarSourceOptions`.

{{< generated/pulsar_consumer_configuration >}}

#### PulsarSource Options

The configuration options below are mainly used for customizing the performance and message acknowledgement behavior.
You can just leave them alone if you do not meet any performance issues.

{{< generated/pulsar_source_configuration >}}

### Dynamic Partition Discovery

To handle scenarios like topic scaling-out or topic creation without restarting the Flink
job, Pulsar source can be configured to periodically discover new partitions under provided
topic-partition subscribing pattern. To enable partition discovery, set a non-negative value for
the option `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS`:

```java
// discover new partitions per 10 seconds
PulsarSource.builder()
        .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```

{{< hint warning >}}
- Partition discovery is **enabled** by default. Pulsar connector would query the topic metadata every 30 seconds.
- You need to set the partition discovery interval to a negative value to disable this feature.
- The partition discovery would be disabled in batch mode even if you set this option with a non-negative value.
{{< /hint >}}

### Event Time and Watermarks

By default, the message uses the timestamp embedded in Pulsar `Message<byte[]>` as the event time.
You can define your own `WatermarkStrategy` to extract the event time from the message,
and emit the watermark downstream:

```java
env.fromSource(pulsarSource, new CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy")
```

[This documentation]({{< ref "docs/dev/datastream/event-time/generating_watermarks.md" >}}) describes
details about how to define a `WatermarkStrategy`.

### Message Acknowledgement

When a subscription is created, Pulsar [retains](https://pulsar.apache.org/docs/en/concepts-architecture-overview/#persistent-storage) all messages, even if the consumer is disconnected.
The retained messages are discarded only when the connector acknowledges that all these messages are processed successfully.
Pulsar connector supports four subscription types, which makes the acknowledgement behaviors variety among different subscriptions.

#### Acknowledgement on Exclusive and Failover Subscription Types

`Exclusive` and `Failover` subscription types support cumulative acknowledgment. In these subscription types, Flink only needs to acknowledge
the latest successfully consumed message. All the message before the given message are marked
with a consumed status.

Pulsar source acknowledges the current consuming message when checkpoints are **completed**,
to ensure the consistency between Flink's checkpoint state and committed position on Pulsar brokers.

If checkpointing is disabled, Pulsar source periodically acknowledges messages.
You can set the acknowledgement period by using the `PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL` option.

Pulsar source does **NOT** rely on committed positions for fault tolerance.
Acknowledging messages is only for exposing the progress of consumer and monitoring on these two subscription types.

#### Acknowledgement on Shared and Key_Shared Subscription Types

`Shared` and `Key_Shared` subscription types need to acknowledge messages one by one. You can acknowledge
a message in a transaction and commit it to Pulsar.

You should enable transaction in the Pulsar `borker.conf` file when using these two subscription types in connector:

```text
transactionCoordinatorEnabled=true
```

Pulsar transaction would be created with 3 hours as the timeout by default. Make sure that timeout > checkpoint interval + maximum recovery time.
A shorter checkpoint interval would increase the consuming performance.
You can change the transaction timeout by using the `PulsarSourceOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS` option.

If checkpointing is disabled or you can not enable the transaction on Pulsar broker, you should set
`PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE` to `true`.
The message would be immediately acknowledged after consuming.
We can not promise consistency in this scenario.

{{< hint info >}}
All acknowledgements in a transaction are recorded in the Pulsar broker side.
{{< /hint >}}

## Upgrading to the Latest Connector Version

The generic upgrade steps are outlined in [upgrading jobs and Flink versions guide]({{< ref "docs/ops/upgrading" >}}).
The Pulsar connector does not store any state on the Flink side. The Pulsar connector pushes and stores all the states on the Pulsar side.
For Pulsar, you additionally need to know these limitations:

* Do not upgrade the Pulsar connector and Pulsar broker version at the same time.
* Always use a newer Pulsar client with Pulsar connector for consuming message from Pulsar.

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
