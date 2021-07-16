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

import org.apache.pulsar.client.api.ConsumerCryptoFailureAction;
import org.apache.pulsar.client.api.ProxyProtocol;
import org.apache.pulsar.client.api.RegexSubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionInitialPosition;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.configuration.description.TextElement.code;
import static org.apache.flink.configuration.description.TextElement.text;

/**
 * Configurations for PulsarSource. All these options would be listed in pulsar flink connector
 * documentation.
 */
@PublicEvolving
public final class PulsarSourceOptions {

    private PulsarSourceOptions() {
        // This is a constant class
    }

    // Pulsar source connector config prefix.
    private static final String SOURCE_CONFIG_PREFIX = "pulsar.source.";

    // Pulsar client API config prefix.
    private static final String CLIENT_CONFIG_PREFIX = "pulsar.client.";
    // Pulsar admin API config prefix.
    private static final String ADMIN_CONFIG_PREFIX = "pulsar.admin.";
    // Pulsar consumer API config prefix.
    private static final String CONSUMER_CONFIG_PREFIX = "pulsar.consumer.";

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for pulsar source part.
    // All the configuration listed below should have the pulsar.source prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<Long> PARTITION_DISCOVERY_INTERVAL_MS =
            ConfigOptions.key(SOURCE_CONFIG_PREFIX + "partitionDiscoveryIntervalMs")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription(
                            "The interval in milliseconds for the Pulsar source to discover "
                                    + "the new partitions. A non-positive value disables the partition discovery.");

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for ClientConfigurationData part.
    // All the configuration listed below should have the pulsar.client prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    public static final ConfigOption<String> PULSAR_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "serviceUrl")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text("Service URL provider for Pulsar service.")
                                    .linebreak()
                                    .text(
                                            "To connect to Pulsar using client libraries, you need to specify a Pulsar protocol URL.")
                                    .linebreak()
                                    .text(
                                            "You can assign Pulsar protocol URLs to specific clusters and use the %s scheme.",
                                            code("pulsar"))
                                    .linebreak()
                                    .list(
                                            text(
                                                    "This is an example of %s: %s.",
                                                    code("localhost"),
                                                    code("pulsar://localhost:6650")),
                                            text(
                                                    "If you have multiple brokers, the URL is as: %s",
                                                    code(
                                                            "pulsar://localhost:6550,localhost:6651,localhost:6652")),
                                            text(
                                                    "A URL for a production Pulsar cluster is as: %s",
                                                    code(
                                                            "pulsar://pulsar.us-west.example.com:6650")),
                                            text(
                                                    "If you use TLS authentication, the URL is as %s",
                                                    code(
                                                            "pulsar+ssl://pulsar.us-west.example.com:6651")))
                                    .build());

    public static final ConfigOption<String> PULSAR_AUTH_PLUGIN_CLASS_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authPluginClassName")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Name of the authentication plugin.");

    public static final ConfigOption<String> PULSAR_AUTH_PARAMS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParams")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "String represents parameters for the authentication plugin.")
                                    .linebreak()
                                    .linebreak()
                                    .text("Example:")
                                    .linebreak()
                                    .add(code("key1:val1,key2:val2"))
                                    .build());

    // The real config type is Map<String, String>, you should provided a json str here.
    public static final ConfigOption<String> PULSAR_AUTH_PARAM_MAP =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "authParamMap")
                    .stringType()
                    .defaultValue("{}");

    public static final ConfigOption<Long> PULSAR_OPERATION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "operationTimeoutMs")
                    .longType()
                    .defaultValue(30000L)
                    .withDescription("Operation timeout.");

    public static final ConfigOption<Long> PULSAR_STATS_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "statsIntervalSeconds")
                    .longType()
                    .defaultValue(60L)
                    .withDescription(
                            Description.builder()
                                    .text("Interval between each stats info.")
                                    .linebreak()
                                    .list(
                                            text(
                                                    "Stats is activated with positive %s",
                                                    code("statsInterval")),
                                            text(
                                                    "Set %s to 1 second at least",
                                                    code("statsIntervalSeconds")))
                                    .build());

    public static final ConfigOption<Integer> PULSAR_NUM_IO_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numIoThreads")
                    .intType()
                    .defaultValue(1)
                    .withDescription(
                            "The number of threads used for handling connections to brokers.");

    public static final ConfigOption<Integer> PULSAR_NUM_LISTENER_THREADS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "numListenerThreads")
                    .intType()
                    .defaultValue(1)
                    .withDescription("The number of threads used for handling message listeners.");

    public static final ConfigOption<Integer> PULSAR_CONNECTIONS_PER_BROKER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionsPerBroker")
                    .intType()
                    .defaultValue(1);

    public static final ConfigOption<Boolean> PULSAR_USE_TCP_NO_DELAY =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useTcpNoDelay")
                    .booleanType()
                    .defaultValue(true)
                    .withDescription(
                            "Whether to use TCP no-delay flag on the connection to disable Nagle algorithm.");

    public static final ConfigOption<Boolean> PULSAR_USE_TLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useTls")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to use TLS encryption on the connection.");

    public static final ConfigOption<String> PULSAR_TLS_TRUST_CERTS_FILE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustCertsFilePath")
                    .stringType()
                    .defaultValue("")
                    .withDescription("Path to the trusted TLS certificate file.");

    public static final ConfigOption<Boolean> PULSAR_TLS_ALLOW_INSECURE_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsAllowInsecureConnection")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription(
                            "Whether the Pulsar client accepts untrusted TLS certificate from broker.");

    public static final ConfigOption<Boolean> PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsHostnameVerificationEnable")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Whether to enable TLS hostname verification.");

    public static final ConfigOption<Integer> PULSAR_CONCURRENT_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "concurrentLookupRequest")
                    .intType()
                    .defaultValue(5000)
                    .withDescription(
                            "The number of concurrent lookup requests allowed to send on each broker connection to prevent overload on broker.");

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REQUEST =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRequest")
                    .intType()
                    .defaultValue(50000)
                    .withDescription(
                            "The maximum number of lookup requests allowed on each broker connection to prevent overload on broker.");

    public static final ConfigOption<Integer> PULSAR_MAX_LOOKUP_REDIRECTS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxLookupRedirects")
                    .intType()
                    .defaultValue(20);

    public static final ConfigOption<Integer> PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxNumberOfRejectedRequestPerConnection")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "The maximum number of rejected requests of a broker in a certain time"
                                    + " frame (30 seconds) after the current connection is closed and"
                                    + " the client creates a new connection to connect to a different broker.");

    public static final ConfigOption<Integer> PULSAR_KEEP_ALIVE_INTERVAL_SECONDS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "keepAliveIntervalSeconds")
                    .intType()
                    .defaultValue(30)
                    .withDescription(
                            "Seconds of keeping alive interval for each client broker connection.");

    public static final ConfigOption<Integer> PULSAR_CONNECTION_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "connectionTimeoutMs")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            Description.builder()
                                    .text(
                                            "Duration of waiting for a connection to a broker to be established.")
                                    .linebreak()
                                    .text(
                                            "If the duration passes without a response from a broker, the connection attempt is dropped.")
                                    .build());

    public static final ConfigOption<Integer> PULSAR_REQUEST_TIMEOUT_MS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "requestTimeoutMs")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Maximum duration for completing a request.");

    public static final ConfigOption<Long> PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "initialBackoffIntervalNanos")
                    .longType()
                    .defaultValue(TimeUnit.MILLISECONDS.toNanos(100))
                    .withDescription("Default duration for a backoff interval.");

    public static final ConfigOption<Long> PULSAR_MAX_BACKOFF_INTERVAL_NANOS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "maxBackoffIntervalNanos")
                    .longType()
                    .defaultValue(TimeUnit.SECONDS.toNanos(60))
                    .withDescription("Maximum duration for a backoff interval.");

    public static final ConfigOption<Boolean> PULSAR_ENABLE_BUSY_WAIT =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableBusyWait")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<String> PULSAR_LISTENER_NAME =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "listenerName").stringType().noDefaultValue();

    public static final ConfigOption<Boolean> PULSAR_USE_KEY_STORE_TLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "useKeyStoreTls")
                    .booleanType()
                    .defaultValue(false);

    public static final ConfigOption<String> PULSAR_SSL_PROVIDER =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "sslProvider").stringType().noDefaultValue();

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_TYPE =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStoreType")
                    .stringType()
                    .defaultValue("JKS");

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PATH =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePath")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<String> PULSAR_TLS_TRUST_STORE_PASSWORD =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsTrustStorePassword")
                    .stringType()
                    .noDefaultValue();

    // The real config type is Set<String>, you should provided a json str here.
    public static final ConfigOption<String> PULSAR_TLS_CIPHERS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsCiphers").stringType().defaultValue("[]");

    // The real config type is Set<String>, you should provided a json str here.
    public static final ConfigOption<String> PULSAR_TLS_PROTOCOLS =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "tlsProtocols")
                    .stringType()
                    .defaultValue("[]");

    public static final ConfigOption<Long> PULSAR_MEMORY_LIMIT_BYTES =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "memoryLimitBytes")
                    .longType()
                    .defaultValue(0L);

    public static final ConfigOption<String> PULSAR_PROXY_SERVICE_URL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyServiceUrl")
                    .stringType()
                    .noDefaultValue();

    public static final ConfigOption<ProxyProtocol> PULSAR_PROXY_PROTOCOL =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "proxyProtocol")
                    .enumType(ProxyProtocol.class)
                    .noDefaultValue();

    public static final ConfigOption<Boolean> PULSAR_ENABLE_TRANSACTION =
            ConfigOptions.key(CLIENT_CONFIG_PREFIX + "enableTransaction")
                    .booleanType()
                    .defaultValue(false);

    ///////////////////////////////////////////////////////////////////////////////
    //
    // The configuration for PulsarAdmin part.
    // All the configuration listed below should have the pulsar.admin prefix.
    //
    ///////////////////////////////////////////////////////////////////////////////

    // The network connect timeout in millis.
    public static final ConfigOption<Long> PULSAR_CONNECT_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "connectTimeout")
                    .longType()
                    .defaultValue(TimeUnit.SECONDS.toMillis(60));

    // The read timeout in millis.
    public static final ConfigOption<Long> PULSAR_READ_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "readTimeout")
                    .longType()
                    .defaultValue(TimeUnit.SECONDS.toMillis(60));

    // The request timeout in millis.
    public static final ConfigOption<Long> PULSAR_REQUEST_TIMEOUT =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "requestTimeout")
                    .longType()
                    .defaultValue(TimeUnit.SECONDS.toMillis(300));

    // The auto refresh time for certification in millis.
    public static final ConfigOption<Long> PULSAR_AUTO_CERT_REFRESH_TIME =
            ConfigOptions.key(ADMIN_CONFIG_PREFIX + "autoCertRefreshTime")
                    .longType()
                    .defaultValue(TimeUnit.SECONDS.toMillis(300));

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
                    .defaultValue("flink-source-" + UUID.randomUUID())
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
            ConfigOptions.key(CONSUMER_CONFIG_PREFIX + "priorityLevel") // NOSONAR
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
                                                    "DISCARD:silently acknowledge and not deliver message to an application."),
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
