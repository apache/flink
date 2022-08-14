/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.annotation.docs.ConfigGroup;
import org.apache.flink.annotation.docs.ConfigGroups;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.config.CursorVerification;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Duration;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.emptyMap;
import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.CONSUMER_CONFIG_PREFIX;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.SOURCE_CONFIG_PREFIX;

/**
 * Configurations for PulsarSource. All the options list here could be configured in {@link
 * PulsarSourceBuilder#setConfig(ConfigOption, Object)}. The {@link PulsarOptions} is also required
 * for pulsar source.
 *
 * @see PulsarOptions
 */
@PublicEvolving
@ConfigGroups(
        groups = {
            @ConfigGroup(name = "PulsarSource", keyPrefix = SOURCE_CONFIG_PREFIX),
            @ConfigGroup(name = "PulsarConsumer", keyPrefix = CONSUMER_CONFIG_PREFIX)
        })
@SuppressWarnings("java:S1192")
public final class PulsarSourceOptions {

    // Pulsar source connector config prefix.
    public static final String SOURCE_CONFIG_PREFIX = "pulsar.source.";
    // Pulsar consumer API config prefix.
    public static final String CONSUMER_CONFIG_PREFIX = "pulsar.consumer.";

    private PulsarSourceOptions() {
        // This is a constant class
    }

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for pulsar source part.
    // All the configuration listed below should have the pulsar.source prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<Long> PULSAR_PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "partitionDiscoveryIntervalMs")
                    .longType()
                    .defaultValue(Duration.ofSeconds(30).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The interval (in ms) for the Pulsar source to discover the new partitions.")
                                    .text(" A non-positive value disables the partition discovery.")
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "enableAutoAcknowledgeMessage")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Flink commits the consuming position with pulsar transactions on checkpoint.")
                                    .text(
                                            " However, if you have disabled the Flink checkpoint or disabled transaction for your Pulsar cluster,")
                                    .text(
                                            " ensure that you have set this option to %s.",
                                            code("true"))
                                    .linebreak()
                                    .text(
                                            "The source would use pulsar client's internal mechanism and commit cursor in two ways.")
                                    .list(
                                            text(
                                                    "For %s and %s subscription, the cursor would be committed once the message is consumed.",
                                                    code("Key_Shared"), code("Shared")),
                                            text(
                                                    "For %s and %s subscription, the cursor would be committed in a given interval.",
                                                    code("Exclusive"), code("Failover")))
                                    .build());

    public static final ConfigOption<Long> PULSAR_AUTO_COMMIT_CURSOR_INTERVAL =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "autoCommitCursorInterval")
                    .longType()
                    .defaultValue(Duration.ofSeconds(5).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This option is used only when the user disables the checkpoint and uses Exclusive or Failover subscription.")
                                    .text(
                                            " We would automatically commit the cursor using the given period (in ms).")
                                    .build());

    public static final ConfigOption<Long> PULSAR_TRANSACTION_TIMEOUT_MILLIS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "transactionTimeoutMillis")
                    .longType()
                    .defaultValue(Duration.ofHours(3).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This option is used in %s or %s subscription.",
                                            code("Shared"), code("Key_Shared"))
                                    .text(
                                            " You should configure this option when you do not enable the %s option.",
                                            code("pulsar.source.enableAutoAcknowledgeMessage"))
                                    .linebreak()
                                    .text(
                                            "The value (in ms) should be greater than the checkpoint interval.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_MAX_FETCH_TIME =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchTime")
                    .longType()
                    .defaultValue(Duration.ofSeconds(10).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text("The maximum time (in ms) to wait when fetching records.")
                                    .text(" A longer time increases throughput but also latency.")
                                    .text(
                                            " A fetch batch might be finished earlier because of %s.",
                                            code("pulsar.source.maxFetchRecords"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_FETCH_RECORDS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchRecords")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The maximum number of records to fetch to wait when polling.")
                                    .text(" A longer time increases throughput but also latency.")
                                    .text(
                                            " A fetch batch might be finished earlier because of %s.",
                                            code("pulsar.source.maxFetchTime"))
                                    .build());

    public static final ConfigOption<CursorVerification> PULSAR_VERIFY_INITIAL_OFFSETS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "verifyInitialOffsets")
                    .enumType(CursorVerification.class)
                    .defaultValue(CursorVerification.WARN_ON_MISMATCH)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Upon (re)starting the source, check whether the expected message can be read.")
                                    .text(
                                            " If failure is enabled, the application fails. Otherwise, it logs a warning.")
                                    .text(
                                            " A possible solution is to adjust the retention settings in Pulsar or ignoring the check result.")
                                    .build());

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ConsumerConfigurationData part.
    // All the configuration listed below should have the pulsar.consumer prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_SUBSCRIPTION_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Specify the subscription name for this consumer.")
                                    .text(
                                            " This argument is required when constructing the consumer.")
                                    .build());

    public static final ConfigOption<SubscriptionType> PULSAR_SUBSCRIPTION_TYPE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionType")
                    .enumType(SubscriptionType.class)
                    .defaultValue(SubscriptionType.Shared)
                    .withDescription(
                            Description.builder()
                                    .text("Subscription type.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Four subscription types are available:")
                                    .list(
                                            text("Exclusive"),
                                            text("Failover"),
                                            text("Shared"),
                                            text("Key_Shared"))
                                    .build());

    public static final ConfigOption<SubscriptionMode> PULSAR_SUBSCRIPTION_MODE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionMode")
                    .enumType(SubscriptionMode.class)
                    .defaultValue(SubscriptionMode.Durable)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Select the subscription mode to be used when subscribing to the topic.")
                                    .list(
                                            text(
                                                    "%s: Make the subscription to be backed by a durable cursor that will retain messages and persist the current position.",
                                                    code("Durable")),
                                            text(
                                                    "%s: Lightweight subscription mode that doesn't have a durable cursor associated",
                                                    code("NonDurable")))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_RECEIVER_QUEUE_SIZE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "receiverQueueSize")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            Description.builder()
                                    .text("Size of a consumer's receiver queue.")
                                    .linebreak()
                                    .text(
                                            "For example, the number of messages accumulated by a consumer before an application calls %s.",
                                            code("Receive"))
                                    .linebreak()
                                    .text(
                                            "A value higher than the default value increases consumer throughput, though at the expense of more memory utilization.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "acknowledgementsGroupTimeMicros")
                    .longType()
                    .defaultValue(TimeUnit.MILLISECONDS.toMicros(100))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Group a consumer acknowledgment for a specified time (in μs).")
                                    .text(
                                            " By default, a consumer uses %s grouping time to send out acknowledgments to a broker.",
                                            code("100μs"))
                                    .text(
                                            " If the group time is set to %s, acknowledgments are sent out immediately.",
                                            code("0"))
                                    .text(
                                            " A longer ack group time is more efficient at the expense of a slight increase in message re-deliveries after a failure.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "negativeAckRedeliveryDelayMicros")
                    .longType()
                    .defaultValue(TimeUnit.MINUTES.toMicros(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Delay (in μs) to wait before redelivering messages that failed to be processed.")
                                    .linebreak()
                                    .text(
                                            "When an application uses %s, failed messages are redelivered after a fixed timeout.",
                                            code("Consumer.negativeAcknowledge(Message)"))
                                    .build());

    public static final ConfigOption<Integer>
            PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS =
                    ConfigOptions.key(
                                    CONSUMER_CONFIG_PREFIX
                                            + "maxTotalReceiverQueueSizeAcrossPartitions")
                            .intType()
                            .defaultValue(50000)
                            .withDescription(
                                    Description.builder()
                                            .text(
                                                    "The maximum total receiver queue size across partitions.")
                                            .linebreak()
                                            .text(
                                                    "This setting reduces the receiver queue size for individual partitions if the total receiver queue size exceeds this value.")
                                            .build());

    public static final ConfigOption<String> PULSAR_CONSUMER_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "consumerName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "The consumer name is informative and it can be used to identify a particular consumer instance from the topic stats.");

    public static final ConfigOption<Long> PULSAR_ACK_TIMEOUT_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackTimeoutMillis")
                    .longType()
                    .defaultValue(0L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The timeout (in ms) for unacknowledged messages, truncated to the nearest millisecond. The timeout needs to be greater than 1 second.")
                                    .linebreak()
                                    .text(
                                            "By default, the acknowledge timeout is disabled and that means that messages delivered to a consumer will not be re-delivered unless the consumer crashes.")
                                    .linebreak()
                                    .text(
                                            "When acknowledgement timeout being enabled, if a message is not acknowledged within the specified timeout it will be re-delivered to the consumer (possibly to a different consumer in case of a shared subscription).")
                                    .build());

    public static final ConfigOption<Long> PULSAR_TICK_DURATION_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "tickDurationMillis")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            Description.builder()
                                    .text("Granularity (in ms) of the ack-timeout redelivery.")
                                    .linebreak()
                                    .text(
                                            "A greater (for example, 1 hour) %s reduces the memory overhead to track messages.",
                                            code("tickDurationMillis"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_PRIORITY_LEVEL =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "priorityLevel")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Priority level for a consumer to which a broker gives more priorities while dispatching messages in the shared subscription type.")
                                    .linebreak()
                                    .text(
                                            "The broker follows descending priorities. For example, 0=max-priority, 1, 2,...")
                                    .linebreak()
                                    .text(
                                            "In shared subscription mode, the broker first dispatches messages to the consumers on the highest priority level if they have permits.")
                                    .text(
                                            " Otherwise, the broker considers consumers on the next priority level.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example 1")
                                    .linebreak()
                                    .text(
                                            "If a subscription has consumer A with %s 0 and consumer B with %s 1, then the broker only dispatches messages to consumer A until it runs out permits and then starts dispatching messages to consumer B.",
                                            code("priorityLevel"), code("priorityLevel"))
                                    .linebreak()
                                    .text("Example 2")
                                    .linebreak()
                                    .text(
                                            "Consumer Priority, Level, Permits\n"
                                                    + "C1, 0, 2\n"
                                                    + "C2, 0, 1\n"
                                                    + "C3, 0, 1\n"
                                                    + "C4, 1, 2\n"
                                                    + "C5, 1, 1\n")
                                    .linebreak()
                                    .text(
                                            "The order in which a broker dispatches messages to consumers is: C1, C2, C3, C1, C4, C5, C4.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_PENDING_CHUNKED_MESSAGE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "maxPendingChunkedMessage")
                    .intType()
                    .defaultValue(10)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The consumer buffers chunk messages into memory until it receives all the chunks of the original message.")
                                    .text(
                                            " While consuming chunk-messages, chunks from the same message might not be contiguous in the stream and they might be mixed with other messages' chunks.")
                                    .text(
                                            " So, consumer has to maintain multiple buffers to manage chunks coming from different messages.")
                                    .text(
                                            " This mainly happens when multiple publishers are publishing messages on the topic concurrently or publishers failed to publish all chunks of the messages.")
                                    .linebreak()
                                    .text(
                                            "For example, there are M1-C1, M2-C1, M1-C2, M2-C2 messages.")
                                    .text(
                                            "Messages M1-C1 and M1-C2 belong to the M1 original message while M2-C1 and M2-C2 belong to the M2 message.")
                                    .linebreak()
                                    .text(
                                            "Buffering a large number of outstanding uncompleted chunked messages can bring memory pressure and it can be guarded by providing this %s threshold.",
                                            code("pulsar.consumer.maxPendingChunkedMessage"))
                                    .text(
                                            " Once, a consumer reaches this threshold, it drops the outstanding unchunked messages by silently acknowledging or asking the broker to redeliver messages later by marking it unacknowledged.")
                                    .text(
                                            " This behavior can be controlled by the %s option.",
                                            code(
                                                    "pulsar.consumer.autoAckOldestChunkedMessageOnQueueFull"))
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoAckOldestChunkedMessageOnQueueFull")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Buffering a large number of outstanding uncompleted chunked messages can bring memory pressure and it can be guarded by providing this %s threshold.",
                                            code("pulsar.consumer.maxPendingChunkedMessage"))
                                    .text(
                                            " Once a consumer reaches this threshold, it drops the outstanding unchunked-messages by silently acknowledging if %s is true. Otherwise, it marks them for redelivery.",
                                            code(
                                                    "pulsar.consumer.autoAckOldestChunkedMessageOnQueueFull"))
                                    .build());

    public static final ConfigOption<Long> PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "expireTimeOfIncompleteChunkedMessageMillis")
                    .longType()
                    .defaultValue(60 * 1000L)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If a producer fails to publish all the chunks of a message,")
                                    .text(
                                            " the consumer can expire incomplete chunks if the consumer cannot receive all chunks in expire times (default 1 hour, in ms).")
                                    .build());

    public static final ConfigOption<ConsumerCryptoFailureAction> PULSAR_CRYPTO_FAILURE_ACTION =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "cryptoFailureAction")
                    .enumType(ConsumerCryptoFailureAction.class)
                    .defaultValue(ConsumerCryptoFailureAction.FAIL)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The consumer should take action when it receives a message that can not be decrypted.")
                                    .list(
                                            text(
                                                    "%s: this is the default option to fail messages until crypto succeeds.",
                                                    code("FAIL")),
                                            text(
                                                    "%s: silently acknowledge but do not deliver messages to an application.",
                                                    code("DISCARD")),
                                            text(
                                                    "%s: deliver encrypted messages to applications. It is the application's responsibility to decrypt the message.",
                                                    code("CONSUME")))
                                    .linebreak()
                                    .text("Fail to decompress the messages.")
                                    .linebreak()
                                    .text(
                                            "If messages contain batch messages, a client is not be able to retrieve individual messages in batch.")
                                    .linebreak()
                                    .text(
                                            "The delivered encrypted message contains %s which contains encryption and compression information in.",
                                            code("EncryptionContext"))
                                    .text(
                                            " You can use an application to decrypt the consumed message payload.")
                                    .build());

    public static final ConfigOption<Map<String, String>> PULSAR_CONSUMER_PROPERTIES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "properties")
                    .mapType()
                    .defaultValue(emptyMap())
                    .withDescription(
                            Description.builder()
                                    .text("A name or value property of this consumer.")
                                    .text(
                                            " %s is application defined metadata attached to a consumer.",
                                            code("properties"))
                                    .text(
                                            " When getting a topic stats, associate this metadata with the consumer stats for easier identification.")
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_READ_COMPACTED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "readCompacted")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If enabling %s, a consumer reads messages from a compacted topic rather than reading a full message backlog of a topic.",
                                            code("readCompacted"))
                                    .linebreak()
                                    .text(
                                            "A consumer only sees the latest value for each key in the compacted topic, up until reaching the point in the topic message when compacting backlog. Beyond that point, send messages as normal.")
                                    .linebreak()
                                    .text(
                                            "Only enabling %s on subscriptions to persistent topics, which have a single active consumer (like failure or exclusive subscriptions).",
                                            code("readCompacted"))
                                    .linebreak()
                                    .text(
                                            "Attempting to enable it on subscriptions to non-persistent topics or on shared subscriptions leads to a subscription call throwing a %s.",
                                            code("PulsarClientException"))
                                    .build());

    /**
     * @deprecated This option would be reset by {@link StartCursor}, no need to use it anymore.
     *     Pulsar didn't support this config option before 1.10.1, so we have to remove this config
     *     option.
     */
    @Deprecated
    public static final ConfigOption<SubscriptionInitialPosition>
            PULSAR_SUBSCRIPTION_INITIAL_POSITION =
                    ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionInitialPosition")
                            .enumType(SubscriptionInitialPosition.class)
                            .defaultValue(SubscriptionInitialPosition.Latest)
                            .withDescription(
                                    "Initial position at which to set cursor when subscribing to a topic at first time.");

    // The config set for DeadLetterPolicy

    /**
     * Dead letter policy for consumers.
     *
     * <p>By default, some messages are probably redelivered many times, even to the extent that it
     * never stops.
     *
     * <p>By using the dead letter mechanism, messages have the max redelivery count. When exceeding
     * the maximum number of redeliveries, messages are sent to the Dead Letter Topic and
     * acknowledged automatically.
     *
     * <p>You can enable the dead letter mechanism by setting deadLetterPolicy.
     *
     * <p>Example <code>pulsar.consumer.deadLetterPolicy.maxRedeliverCount = 10</code> Default dead
     * letter topic name is <strong>{TopicName}-{Subscription}-DLQ</strong>.
     *
     * <p>To set a custom dead letter topic name:
     *
     * <pre><code>
     * pulsar.consumer.deadLetterPolicy.maxRedeliverCount = 10
     * pulsar.consumer.deadLetterPolicy.deadLetterTopic = your-topic-name
     * </code></pre>
     *
     * <p>When specifying the dead letter policy while not specifying ackTimeoutMillis, you can set
     * the ack timeout to 30000 millisecond.
     */
    public static final ConfigOption<Integer> PULSAR_MAX_REDELIVER_COUNT =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.maxRedeliverCount")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            "The maximum number of times that a message are redelivered before being sent to the dead letter queue.");

    public static final ConfigOption<String> PULSAR_RETRY_LETTER_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.retryLetterTopic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the retry topic where the failed messages are sent.");
    public static final ConfigOption<String> PULSAR_DEAD_LETTER_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.deadLetterTopic")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the dead topic where the failed messages are sent.");

    public static final ConfigOption<Boolean> PULSAR_RETRY_ENABLE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "retryEnable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("If enabled, the consumer will automatically retry messages.");

    public static final ConfigOption<Integer> PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoUpdatePartitionsIntervalSeconds")
                    .intType()
                    .defaultValue(60)
                    .withDescription(
                            "The interval (in seconds) of updating partitions. This only works if autoUpdatePartitions is enabled.");

    public static final ConfigOption<Boolean> PULSAR_REPLICATE_SUBSCRIPTION_STATE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "replicateSubscriptionState")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If %s is enabled, a subscription state is replicated to geo-replicated clusters.",
                                            code("replicateSubscriptionState"))
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_ACK_RECEIPT_ENABLED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackReceiptEnabled")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Acknowledgement will return a receipt but this does not mean that the message will not be resent after getting the receipt.");

    public static final ConfigOption<Boolean> PULSAR_POOL_MESSAGES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "poolMessages")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Enable pooling of messages and the underlying data buffers.");
}
