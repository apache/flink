# Flink Source for Google Cloud Pub/Sub

This is a source implementation for receiving Google Cloud Pub/Sub messages in Flink with an at-least-once guarantee.

## Installation

Add this dependency entry to your pom.xml to use the Google Cloud Pub/Sub source:

```xml
<dependency>
  <groupId>org.apache.flink</groupId>
  <artifactId>flink-connector-gcp-pubsub_${scala.binary.version}</artifactId>
  <version>1.13-SNAPSHOT</version>
</dependency>
```

## Architecture

### Split Enumerator

An enumerator is supposed to discover splits and assign them to the readers so that they can do the actual reading. However, the implementation of the `PubSubSourceEnumerator` doesn't do any real work discovery because Pub/Sub doesn't expose any partitions from which splits could be constructed. Instead, the enumerator assigns a static `PubSubSplit` to every `PubSubSourceReader` that joins so that the readers can start pulling messages. The static source split doesn't contain split-specific information like partitions/offsets because this information can not be obtained from Pub/Sub. Because of the static source split, there is no state in the enumerator which would have to be snapshot when checkpointing.

### Source Reader

A `PubSubSourceReader` uses Pub/Sub's pull mechanism to read new messages from the Pub/Sub subscription specified by the user. In the case of parallel-running source readers in Flink, every source reader is passed the same source split from the enumerator. Because of this, all source readers use the same connection details and the same Pub/Sub subscription to receive messages. In this case, Pub/Sub automatically load-balances messages between all source readers which pull from the same subscription. This way, parallel processing is achieved in the source. The source reader is notified when a checkpoint completes so that it can trigger the acknowledgement of successfully received Pub/Sub messages through the split reader. As a result, when a checkpoint completes, all messages which were successfully pulled since the previous checkpoint are acknowledged to Pub/Sub to ensure they won't be redelivered.

## Delivery Guarantee

Google Cloud Pub/Sub only guarantees at-least-once message delivery. This guarantee is kept up by the source as well. To achieve this, the source makes use of Pub/Sub's expectation that a message should be acknowledged by the subscriber to signal that the message has been consumed successfully. Any message that has not been acknowledged yet will be automatically redelivered by Pub/Sub once an ack deadline has passed.

When a checkpoint completes, all messages which were successfully pulled since the previous checkpoint are acknowledged to Pub/Sub. This ensures at-least-once delivery in the source because in the case of failure, non-checkpointed messages have not yet been acknowledged and will therefore be redelivered by the Pub/Sub service.

## Usage

To keep up the Google Cloud Pub/Sub at-least-once guarantee, messages are acknowledged against Pub/Sub when checkpointing succeeds. If a message is not acknowledged within an acknowledge deadline, Pub/Sub will attempt redelivery. To avoid unnecessary redelivery of successfully received messages, the checkpointing interval should always be configured (much) *lower* than the Google Cloud Pub/Sub acknowledge deadline.

```java
import org.apache.flink.streaming.connectors.gcp.pubsub.source.PubSubSource;

StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

// Checkpointing must be enabled for the source to work so that messages can be acknowledged towards Pub/Sub
env.enableCheckpointing(1000);

// Parallelism > 1 may be set
// env.setParallelism(4);

PubSubSource<String> source =
        PubSubSource.<String>builder()
                // The deserialization schema to deserialize Pub/Sub messages
                .setDeserializationSchema(new SimpleStringSchema())
                // The name string of your Pub/Sub project
                .setProjectName(PROJECT_NAME)
                // The name string of the subscription you would like to receive messages from
                .setSubscriptionName(SUBSCRIPTION_NAME)
                // An instance of the com.google.auth.Credentials class to authenticate against Google Cloud
                .setCredentials(CREDENTIALS)
                .setPubSubSubscriberFactory(
                        // The maximum number of messages that should be pulled in one go
                        3,
                        // The timeout after which a message pull request is deemed a failure
                        Duration.ofSeconds(1),
                        // The number of times the reception of a message should be retried in case of failure
                        10)
                .build();

DataStream<String> fromPubSub =
                env.fromSource(source, WatermarkStrategy.noWatermarks(), "pubsub-source");
```
