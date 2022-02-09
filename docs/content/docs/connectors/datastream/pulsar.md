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

Flink provides an [Apache Pulsar](https://pulsar.apache.org) connector for reading and writing data from and to Pulsar topics with exactly-once guarantees.

## Dependency

You can use the connector with the Pulsar 2.9.0 or higher. Because the Pulsar connector supports
Pulsar [transactions](https://pulsar.apache.org/docs/en/txn-what/), it is recommended to use the Pulsar 2.10.0 or higher.
Details on Pulsar compatibility can be found in [PIP-72](https://github.com/apache/pulsar/wiki/PIP-72%3A-Introduce-Pulsar-Interface-Taxonomy%3A-Audience-and-Stability-Classification).

{{< artifact flink-connector-pulsar withScalaVersion >}}

Flink's streaming connectors are not part of the binary distribution.
See how to link with them for cluster execution [here]({{< ref "docs/dev/datastream/project-configuration" >}}).

## Pulsar Source

{{< hint info >}}
This part describes the Pulsar source based on the new
[data source]({{< ref "docs/dev/datastream/sources.md" >}}) API.

If you want to use the legacy `SourceFunction` or on Flink 1.13 or lower releases, just use the StreamNative's [pulsar-flink](https://github.com/streamnative/pulsar-flink).
{{< /hint >}}

### Usage

The Pulsar source provides a builder class for constructing a PulsarSource instance. The code snippet below builds a PulsarSource instance. It consumes messages from the earliest cursor of the topic
"persistent://public/default/my-topic" in **Exclusive** subscription type (`my-subscription`)
and deserializes the raw payload of the messages as strings.

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
- Pulsar service HTTP URL (also known as admin URL), configured by `setAdminUrl(String)`
- Pulsar subscription name, configured by `setSubscriptionName(String)`
- Topics / partitions to subscribe, see the following
  [topic-partition subscription](#topic-partition-subscription) for more details.
- Deserializer to parse Pulsar messages, see the following
  [deserializer](#deserializer) for more details.

It is recommended to set the consumer name in Pulsar Source by `setConsumerName(String)`.
This sets a unique name for the Flink connector in the Pulsar statistic dashboard.
You can use it to monitor the performance of your Flink connector and applications.

### Topic-partition Subscription

Pulsar source provide two ways of topic-partition subscription:

- Topic list, subscribing messages from all partitions in a list of topics. For example:
  ```java
  PulsarSource.builder().setTopics("some-topic1", "some-topic2");

  // Partition 0 and 2 of topic "topic-a"
  PulsarSource.builder().setTopics("topic-a-partition-0", "topic-a-partition-2");
  ```

- Topic pattern, subscribing messages from all topics whose name matches the provided regular expression. For example:
  ```java
  PulsarSource.builder().setTopicPattern("topic-*");
  ```

#### Flexible Topic Naming

Since Pulsar 2.0, all topic names internally are in  a form of  `{persistent|non-persistent}://tenant/namespace/topic`.
Now, for partitioned topics, you can use short names in many cases (for the sake of simplicity).
The flexible naming system stems from the fact that there is now a default topic type, tenant, and namespace in a Pulsar cluster.

| Topic property | Default      |
|:---------------|:-------------|
| topic type     | `persistent` |
| tenant         | `public`     |
| namespace      | `default`    |

This table lists a mapping relationship between your input topic name and the translated topic name:

| Input topic name                  | Translated topic name                          |
|:----------------------------------|:-----------------------------------------------|
| `my-topic`                        | `persistent://public/default/my-topic`         |
| `my-tenant/my-namespace/my-topic` | `persistent://my-tenant/my-namespace/my-topic` |

{{< hint warning >}}
For non-persistent topics, you need to specify the entire topic name,
as the default-based rules do not apply for non-partitioned topics.
Thus, you cannot use a short name like `non-persistent://my-topic` and need to use `non-persistent://public/default/my-topic` instead.
{{< /hint >}}

#### Subscribing Pulsar Topic Partition

Internally, Pulsar divides a partitioned topic as a set of non-partitioned topics according to the partition size.

For example, if a `simple-string` topic with 3 partitions is created under the `sample` tenant with the `flink` namespace.
The topics on Pulsar would be:

| Topic name                                            | Partitioned |
|:------------------------------------------------------|:------------|
| `persistent://sample/flink/simple-string`             | Y           |
| `persistent://sample/flink/simple-string-partition-0` | N           |
| `persistent://sample/flink/simple-string-partition-1` | N           |
| `persistent://sample/flink/simple-string-partition-2` | N           |

You can directly consume messages from the topic partitions by using the non-partitioned topic names above.
For example, use `PulsarSource.builder().setTopics("sample/flink/simple-string-partition-1", "sample/flink/simple-string-partition-2")`
would consume the partitions 1 and 2 of the `sample/flink/simple-string` topic.

#### Setting Topic Patterns

The Pulsar source extracts the topic type (`persistent` or `non-persistent`) from the provided topic pattern.
For example, you can use the `PulsarSource.builder().setTopicPattern("non-persistent://my-topic*")` to specify a `non-persistent` topic.
By default, a `persistent` topic is created if you do not specify the topic type in the regular expression.

You can use `setTopicPattern("topic-*", RegexSubscriptionMode.AllTopics)` to consume
both `persistent` and `non-persistent` topics based on the topic pattern.
The Pulsar source would filter the available topics by the `RegexSubscriptionMode`.

### Deserializer

A deserializer (`PulsarDeserializationSchema`) is for decoding Pulsar messages from bytes.
You can configure the deserializer using `setDeserializationSchema(PulsarDeserializationSchema)`.
The `PulsarDeserializationSchema` defines how to deserialize a Pulsar `Message<byte[]>`.

If only the raw payload of a message (message data in bytes) is needed,
you can use the predefined `PulsarDeserializationSchema`. Pulsar connector provides three implementation methods.

- Decode the message by using Pulsar's [Schema](https://pulsar.apache.org/docs/en/schema-understand/).
  ```java
  // Primitive types
  PulsarDeserializationSchema.pulsarSchema(Schema);

  // Struct types (JSON, Protobuf, Avro, etc.)
  PulsarDeserializationSchema.pulsarSchema(Schema, Class);

  // KeyValue type
  PulsarDeserializationSchema.pulsarSchema(Schema, Class, Class);
  ```
- Decode the message by using Flink's `DeserializationSchema`
  ```java
  PulsarDeserializationSchema.flinkSchema(DeserializationSchema);
  ```
- Decode the message by using Flink's `TypeInformation`
  ```java
  PulsarDeserializationSchema.flinkTypeInfo(TypeInformation, ExecutionConfig);
  ```

Pulsar `Message<byte[]>` contains some [extra properties](https://pulsar.apache.org/docs/en/concepts-messaging/#messages),
such as message key, message publish time, message time, and application-defined key/value pairs etc.
These properties could be defined in the `Message<byte[]>` interface.

If you want to deserialize the Pulsar message by these properties, you need to implement `PulsarDeserializationSchema`.
Ensure that the `TypeInformation` from the `PulsarDeserializationSchema.getProducedType()` is correct.
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

By default, if no subscription type is defined, Pulsar source uses the `Shared` subscription type.

```java
// Shared subscription with name "my-shared"
PulsarSource.builder().setSubscriptionName("my-shared");

// Exclusive subscription with name "my-exclusive"
PulsarSource.builder().setSubscriptionName("my-exclusive").setSubscriptionType(SubscriptionType.Exclusive);
```

Ensure that you provide a `RangeGenerator` implementation if you want to use the `Key_Shared` subscription type on the Pulsar connector.
The `RangeGenerator` generates a set of key hash ranges so that a respective reader subtask only dispatches messages where the hash of the message key is contained in the specified range.

The Pulsar connector uses `UniformRangeGenerator` that divides the range by the Flink source
parallelism if no `RangeGenerator` is provided in the `Key_Shared` subscription type.

### Starting Position

The Pulsar source is able to consume messages starting from different positions by setting the `setStartCursor(StartCursor)` option.
Built-in start cursors include:

- Start from the earliest available message in the topic.
  ```java
  StartCursor.earliest();
  ```
- Start from the latest available message in the topic.
  ```java
  StartCursor.latest();
  ```
- Start from a specified message between the earliest and the latest.
The Pulsar connector consumes from the latest available message if the message ID does not exist.

  The start message is included in consuming result.
  ```java
  StartCursor.fromMessageId(MessageId);
  ```
- Start from a specified message between the earliest and the latest.
The Pulsar connector consumes from the latest available message if the message ID doesn't exist.

  Include or exclude the start message by using the second boolean parameter.
  ```java
  StartCursor.fromMessageId(MessageId, boolean);
  ```
- Start from the specified message publish time by `Message<byte[]>.getPublishTime()`.
This method is deprecated because the name is totally wrong which may cause confuse.
You can use `StartCursor.fromPublishTime(long)` instead.
  ```java
  StartCursor.fromMessageTime(long);
  ```
- Start from the specified message publish time by `Message<byte[]>.getPublishTime()`.
  ```java
  StartCursor.fromPublishTime(long);
  ```

{{< hint info >}}
Each Pulsar message belongs to an ordered sequence on its topic.
The sequence ID (`MessageId`) of the message is ordered in that sequence.
The `MessageId` contains some extra information (the ledger, entry, partition) about how the message is stored,
you can create a `MessageId` by using `DefaultImplementation.newMessageId(long ledgerId, long entryId, int partitionIndex)`.
{{< /hint >}}

### Boundedness

The Pulsar source supports streaming and batch execution mode.
By default, the `PulsarSource` is configured for unbounded data.

For unbounded data the Pulsar source never stops until a Flink job is stopped or failed. 
You can use the `setUnboundedStopCursor(StopCursor)` to set the Pulsar source to stop at a specific stop position.

You can use `setBoundedStopCursor(StopCursor)` to specify a stop position for bounded data. 

Built-in stop cursors include:

- The Pulsar source never stops consuming messages.
  ```java
  StopCursor.never();
  ```
- Stop at the latest available message when the  Pulsar source starts consuming messages.
  ```java
  StopCursor.latest();
  ```
- Stop when the connector meets a given message, or stop at a message which is produced after this given message.
  ```java
  StopCursor.atMessageId(MessageId);
  ```
- Stop but include the given message in the consuming result.
  ```java
  StopCursor.afterMessageId(MessageId);
  ```
- Stop at the specified event time by `Message<byte[]>.getEventTime()`. The message with the
given event time won't be included in the consuming result.
  ```java
  StopCursor.atEventTime(long);
  ```
- Stop after the specified event time by `Message<byte[]>.getEventTime()`. The message with the
given event time will be included in the consuming result.
  ```java
  StopCursor.afterEventTime(long);
  ```
- Stop at the specified publish time by `Message<byte[]>.getPublishTime()`. The message with the
given publish time won't be included in the consuming result.
  ```java
  StopCursor.atPublishTime(long);
  ```
- Stop after the specified publish time by `Message<byte[]>.getPublishTime()`. The message with the
  given publish time will be included in the consuming result.
  ```java
  StopCursor.afterPublishTime(long);
  ```

### Source Configurable Options

In addition to configuration options described above, you can set arbitrary options for `PulsarClient`,
`PulsarAdmin`, Pulsar `Consumer` and `PulsarSource` by using `setConfig(ConfigOption<T>, T)`,
`setConfig(Configuration)` and `setConfig(Properties)`.

#### PulsarClient Options

The Pulsar connector uses the [client API](https://pulsar.apache.org/docs/en/client-libraries-java/)
to create the `Consumer` instance. The Pulsar connector extracts most parts of Pulsar's `ClientConfigurationData`,
which is required for creating a `PulsarClient`, as Flink configuration options in `PulsarOptions`.

{{< generated/pulsar_client_configuration >}}

#### PulsarAdmin Options

The [admin API](https://pulsar.apache.org/docs/en/admin-api-overview/) is used for querying topic metadata
and for discovering the desired topics when the Pulsar connector uses topic-pattern subscription.
It shares most part of the configuration options with the client API.
The configuration options listed here are only used in the admin API.
They are also defined in `PulsarOptions`.

{{< generated/pulsar_admin_configuration >}}

#### Pulsar Consumer Options

In general, Pulsar provides the Reader API and Consumer API for consuming messages in different scenarios.
The Pulsar connector uses the Consumer API. It extracts most parts of Pulsar's `ConsumerConfigurationData` as Flink configuration options in `PulsarSourceOptions`.

{{< generated/pulsar_consumer_configuration >}}

#### PulsarSource Options

The configuration options below are mainly used for customizing the performance and message acknowledgement behavior.
You can ignore them if you do not have any performance issues.

{{< generated/pulsar_source_configuration >}}

### Dynamic Partition Discovery

To handle scenarios like topic scaling-out or topic creation without restarting the Flink
job, the Pulsar source periodically discover new partitions under a provided
topic-partition subscription pattern. To enable partition discovery, you can set a non-negative value for
the `PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS` option:

```java
// discover new partitions per 10 seconds
PulsarSource.builder()
    .setConfig(PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS, 10000);
```

{{< hint warning >}}
- Partition discovery is **enabled** by default. The Pulsar connector queries the topic metadata every 30 seconds.
- To disable partition discovery, you need to set a negative partition discovery interval.
- Partition discovery is disabled for bounded data even if you set this option with a non-negative value.
{{< /hint >}}

### Event Time and Watermarks

By default, the message uses the timestamp embedded in Pulsar `Message<byte[]>` as the event time.
You can define your own `WatermarkStrategy` to extract the event time from the message,
and emit the watermark downstream:

```java
env.fromSource(pulsarSource, new CustomWatermarkStrategy(), "Pulsar Source With Custom Watermark Strategy");
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

The default timeout for Pulsar transactions is 3 hours.
Make sure that that timeout is greater than checkpoint interval + maximum recovery time.
A shorter checkpoint interval indicates a better consuming performance.
You can use the `PulsarSourceOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS` option to change the transaction timeout.

If checkpointing is disabled or you can not enable the transaction on Pulsar broker, you should set
`PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE` to `true`.
The message is immediately acknowledged after consuming.
No consistency guarantees can be made in this scenario.

{{< hint info >}}
All acknowledgements in a transaction are recorded in the Pulsar broker side.
{{< /hint >}}

## Upgrading to the Latest Connector Version

The generic upgrade steps are outlined in [upgrading jobs and Flink versions guide]({{< ref "docs/ops/upgrading" >}}).
The Pulsar connector does not store any state on the Flink side. The Pulsar connector pushes and stores all the states on the Pulsar side.
For Pulsar, you additionally need to know these limitations:

* Do not upgrade the Pulsar connector and Pulsar broker version at the same time.
* Always use a newer Pulsar client with Pulsar connector to consume messages from Pulsar.

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
