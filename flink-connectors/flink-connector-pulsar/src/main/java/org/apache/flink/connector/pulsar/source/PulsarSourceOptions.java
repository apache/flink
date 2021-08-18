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
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.connector.pulsar.common.config.PulsarOptions;
import org.apache.flink.connector.pulsar.source.config.CursorVerification;

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configurations for PulsarSource. All the options list here could be configured in {@link
 * PulsarSourceBuilder#setProperty(ConfigOption, Object)}. The {@link PulsarOptions} is also
 * required for pulsar source.
 *
 * @see PulsarOptions
 */
@PublicEvolving
public final class PulsarSourceOptions {

    // Pulsar source connector config prefix.
    private static final String SOURCE_CONFIG_PREFIX = "pulsar.source.";
    // Pulsar consumer API config prefix.
    private static final String CONSUMER_CONFIG_PREFIX = "pulsar.consumer.";

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
                            "The interval in milliseconds for the Pulsar source to discover "
                                    + "the new partitions. A non-positive value disables the partition discovery.");

    public static final ConfigOption<Boolean> PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "enableAutoAcknowledgeMessage")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Flink commits the consuming position with pulsar transactions on checkpoint.")
                                    .linebreak()
                                    .text(
                                            "However, if you have disabled the flink checkpoint or your pulsar cluster disabled the transaction,"
                                                    + " make sure you have set this option to %s.",
                                            code("true"))
                                    .text(
                                            "The source would use pulsar client's internal mechanism and commit cursor in two ways.")
                                    .list(
                                            text(
                                                    "For Key_Shared and Shared subscription: the cursor would be committed once the message is consumed."),
                                            text(
                                                    "For Exclusive and Failover subscription: the cursor would be committed in a fixed interval."))
                                    .build());

    public static final ConfigOption<Long> PULSAR_AUTO_COMMIT_CURSOR_INTERVAL =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "autoCommitCursorInterval")
                    .longType()
                    .defaultValue(Duration.ofSeconds(5).toMillis())
                    .withDescription(
                            "This option is used only when user disabled checkpoint"
                                    + " and using Exclusive or Failover subscription");

    public static final ConfigOption<Long> PULSAR_TRANSACTION_TIMEOUT_MILLIS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "transactionTimeoutMillis")
                    .longType()
                    .defaultValue(Duration.ofHours(3).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "This option is used for when using Shared or Key_Shared subscription."
                                                    + " You should set this option when you didn't enable the %s option.",
                                            code("pulsar.source.enableAutoAcknowledgeMessage"))
                                    .linebreak()
                                    .text(
                                            "This value should be greater than the checkpoint interval.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_MAX_FETCH_TIME =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchTime")
                    .longType()
                    .defaultValue(Duration.ofSeconds(10).toMillis())
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The max time to wait when fetching records. "
                                                    + "A longer time increases throughput but also latency. "
                                                    + "A fetch batch might be finished earlier because of %s.",
                                            code("pulsar.source.maxFetchRecords"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_FETCH_RECORDS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "maxFetchRecords")
                    .intType()
                    .defaultValue(100)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "The max number of records to fetch to wait when polling. "
                                                    + "A longer time increases throughput but also latency."
                                                    + "A fetch batch might be finished earlier because of %s.",
                                            code("pulsar.source.maxFetchTime"))
                                    .build());

    public static final ConfigOption<CursorVerification> PULSAR_VERIFY_INITIAL_OFFSETS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "verifyInitialOffsets")
                    .enumType(CursorVerification.class)
                    .defaultValue(CursorVerification.WARN_ON_MISMATCH)
                    .withDescription(
                            "Upon (re)starting the source checks whether the expected message can be read. "
                                    + "If failure is enabled the application fails, else it logs a warning. "
                                    + "A possible solution is to adjust the retention settings in pulsar or ignoring the check result.");

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ConsumerConfigurationData part.
    // All the configuration listed below should have the pulsar.consumer prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_TOPIC_NAMES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "topicNames")
                    .stringType()
                    .defaultValue("[]")
                    .withDescription("Topic name.");

    public static final ConfigOption<String> PULSAR_TOPICS_PATTERN =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "topicsPattern")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Topic pattern.");

    public static final ConfigOption<String> PULSAR_SUBSCRIPTION_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Subscription name.");

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
                    .defaultValue(SubscriptionMode.Durable);

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
                                    .text("Group a consumer acknowledgment for a specified time.")
                                    .linebreak()
                                    .text(
                                            "By default, a consumer uses 100ms grouping time to send out acknowledgments to a broker.")
                                    .linebreak()
                                    .text(
                                            "Setting a group time of 0 sends out acknowledgments immediately.")
                                    .linebreak()
                                    .text(
                                            "A longer ack group time is more efficient at the expense of a slight increase in message re-deliveries after a failure.")
                                    .build());

    public static final ConfigOption<Long> PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "negativeAckRedeliveryDelayMicros")
                    .longType()
                    .defaultValue(TimeUnit.MINUTES.toMicros(1))
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Delay to wait before redelivering messages that failed to be processed.")
                                    .linebreak()
                                    .text(
                                            "When an application uses %s, failed messages are redelivered after a fixed timeout.",
                                            code("Consumer#negativeAcknowledge(Message)"))
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
                                                    "The max total receiver queue size across partitions.")
                                            .linebreak()
                                            .text(
                                                    "This setting reduces the receiver queue size for individual partitions if the total receiver queue size exceeds this value.")
                                            .build());

    public static final ConfigOption<String> PULSAR_CONSUMER_NAME =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "consumerName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Consumer name.");

    public static final ConfigOption<Long> PULSAR_ACK_TIMEOUT_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackTimeoutMillis")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Timeout of unacknowledged messages.");

    public static final ConfigOption<Long> PULSAR_TICK_DURATION_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "tickDurationMillis")
                    .longType()
                    .defaultValue(1000L)
                    .withDescription(
                            Description.builder()
                                    .text("Granularity of the ack-timeout redelivery.")
                                    .linebreak()
                                    .text(
                                            "Using an higher %s reduces the memory overhead to track messages when setting ack-timeout to a bigger value (for example, 1 hour).",
                                            code("tickDurationMillis"))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_PRIORITY_LEVEL =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "priorityLevel")
                    .intType()
                    .defaultValue(0)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Priority level for a consumer to which a broker gives more priority while dispatching messages in the shared subscription mode.")
                                    .linebreak()
                                    .text(
                                            "The broker follows descending priorities. For example, 0=max-priority, 1, 2,...")
                                    .linebreak()
                                    .text(
                                            "In shared subscription mode, the broker first dispatches messages to the max priority level consumers if they have permits. Otherwise, the broker considers next priority level consumers.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example 1")
                                    .linebreak()
                                    .text(
                                            "If a subscription has consumerA with %s 0 and consumerB with %s 1, then the broker only dispatches messages to consumerA until it runs out permits and then starts dispatching messages to consumerB.",
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
                                            "Order in which a broker dispatches messages to consumers is: C1, C2, C3, C1, C4, C5, C4.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_MAX_PENDING_CHUNKED_MESSAGE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "maxPendingChunkedMessage")
                    .intType()
                    .defaultValue(10);

    public static final ConfigOption<Boolean> PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoAckOldestChunkedMessageOnQueueFull")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Long> PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "expireTimeOfIncompleteChunkedMessageMillis")
                    .longType()
                    .defaultValue(60 * 1000L);

    public static final ConfigOption<ConsumerCryptoFailureAction> PULSAR_CRYPTO_FAILURE_ACTION =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "cryptoFailureAction")
                    .enumType(ConsumerCryptoFailureAction.class)
                    .defaultValue(ConsumerCryptoFailureAction.FAIL)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Consumer should take action when it receives a message that can not be decrypted.")
                                    .list(
                                            text(
                                                    "FAIL: this is the default option to fail messages until crypto succeeds."),
                                            text(
                                                    "DISCARD: silently acknowledge and not deliver message to an application."),
                                            text(
                                                    "CONSUME: deliver encrypted messages to applications. It is the application's responsibility to decrypt the message."))
                                    .linebreak()
                                    .text("The decompression of message fails.")
                                    .linebreak()
                                    .text(
                                            "If messages contain batch messages, a client is not be able to retrieve individual messages in batch.")
                                    .linebreak()
                                    .text(
                                            "Delivered encrypted message contains %s which contains encryption and compression information in it using which application can decrypt consumed message payload.",
                                            code("EncryptionContext"))
                                    .build());

    public static final ConfigOption<String> PULSAR_CONSUMER_PROPERTIES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "properties")
                    .stringType()
                    .defaultValue("{}")
                    .withDescription(
                            Description.builder()
                                    .text("A name or value property of this consumer.")
                                    .linebreak()
                                    .text(
                                            "%s is application defined metadata attached to a consumer.",
                                            code("properties"))
                                    .linebreak()
                                    .text(
                                            "When getting a topic stats, associate this metadata with the consumer stats for easier identification.")
                                    .build());

    public static final ConfigOption<Boolean> PULSAR_READ_COMPACTED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "readCompacted") // NOSONAR
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

    public static final ConfigOption<SubscriptionInitialPosition>
            PULSAR_SUBSCRIPTION_INITIAL_POSITION =
                    ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "subscriptionInitialPosition")
                            .enumType(SubscriptionInitialPosition.class)
                            .defaultValue(SubscriptionInitialPosition.Latest)
                            .withDescription(
                                    "Initial position at which to set cursor when subscribing to a topic at first time.");

    public static final ConfigOption<Integer> PULSAR_PATTERN_AUTO_DISCOVERY_PERIOD =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "patternAutoDiscoveryPeriod")
                    .intType()
                    .defaultValue(60)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Topic auto discovery period when using a pattern for topic's consumer.")
                                    .linebreak()
                                    .text("The default and minimum value is 1 minute.")
                                    .build());

    public static final ConfigOption<RegexSubscriptionMode> PULSAR_REGEX_SUBSCRIPTION_MODE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "regexSubscriptionMode")
                    .enumType(RegexSubscriptionMode.class)
                    .defaultValue(RegexSubscriptionMode.PersistentOnly)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "When subscribing to a topic using a regular expression, you can pick a certain type of topics.")
                                    .list(
                                            text(
                                                    "PersistentOnly: only subscribe to persistent topics."),
                                            text(
                                                    "NonPersistentOnly: only subscribe to non-persistent topics."),
                                            text(
                                                    "AllTopics: subscribe to both persistent and non-persistent topics."))
                                    .build());

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
                    .defaultValue(0);

    public static final ConfigOption<String> PULSAR_RETRY_LETTER_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.retryLetterTopic")
                    .stringType()
                    .noDefaultValue();
    public static final ConfigOption<String> PULSAR_DEAD_LETTER_TOPIC =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "deadLetterPolicy.deadLetterTopic")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<Boolean> PULSAR_RETRY_ENABLE =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "retryEnable")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_AUTO_UPDATE_PARTITIONS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoUpdatePartitions")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "If %s is enabled, a consumer subscribes to partition increase automatically.",
                                            code("autoUpdatePartitions"))
                                    .linebreak()
                                    .text("Note: this is only for partitioned consumers.\t")
                                    .build());

    public static final ConfigOption<Long> PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "autoUpdatePartitionsIntervalSeconds")
                    .longType()
                    .defaultValue(60L);

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

    public static final ConfigOption<Boolean> PULSAR_RESET_INCLUDE_HEAD =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "resetIncludeHead")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_BATCH_INDEX_ACK_ENABLED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "batchIndexAckEnabled")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_ACK_RECEIPT_ENABLED =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "ackReceiptEnabled")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<Boolean> PULSAR_POOL_MESSAGES =
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "poolMessages")
                    .booleanType()
                    .defaultValue(false);
}
