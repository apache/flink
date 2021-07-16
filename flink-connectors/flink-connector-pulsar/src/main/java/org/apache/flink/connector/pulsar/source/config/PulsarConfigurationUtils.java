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

package org.apache.flink.connector.pulsar.source.config;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.connector.pulsar.source.utils.PulsarJsonUtils;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.client.api.Authentication;
import org.apache.pulsar.client.api.AuthenticationFactory;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.impl.auth.AuthenticationDisabled;
import org.apache.pulsar.client.impl.conf.ClientConfigurationData;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACK_RECEIPT_ENABLED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACK_TIMEOUT_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTH_PLUGIN_CLASS_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_UPDATE_PARTITIONS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_BATCH_INDEX_ACK_ENABLED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONCURRENT_LOOKUP_REQUEST;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONNECTIONS_PER_BROKER;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONNECTION_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_PROPERTIES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CRYPTO_FAILURE_ACTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_DEAD_LETTER_TOPIC;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_BUSY_WAIT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_TRANSACTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_KEEP_ALIVE_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_LISTENER_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_BACKOFF_INTERVAL_NANOS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_LOOKUP_REDIRECTS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_LOOKUP_REQUEST;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_PENDING_CHUNKED_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_REDELIVER_COUNT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MEMORY_LIMIT_BYTES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_NUM_IO_THREADS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_NUM_LISTENER_THREADS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_OPERATION_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PATTERN_AUTO_DISCOVERY_PERIOD;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_POOL_MESSAGES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PRIORITY_LEVEL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PROXY_PROTOCOL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PROXY_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_READ_COMPACTED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RECEIVER_QUEUE_SIZE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REGEX_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REPLICATE_SUBSCRIPTION_STATE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REQUEST_TIMEOUT_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RESET_INCLUDE_HEAD;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RETRY_ENABLE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RETRY_LETTER_TOPIC;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SSL_PROVIDER;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_STATS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_INITIAL_POSITION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TICK_DURATION_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_ALLOW_INSECURE_CONNECTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_CIPHERS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_PROTOCOLS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_TRUST_CERTS_FILE_PATH;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_TRUST_STORE_PASSWORD;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_TRUST_STORE_PATH;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TLS_TRUST_STORE_TYPE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TOPICS_PATTERN;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TOPIC_NAMES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_USE_KEY_STORE_TLS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_USE_TCP_NO_DELAY;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_USE_TLS;

/** The util for creating pulsar configuration class from flink's {@link Configuration} */
public final class PulsarConfigurationUtils {

    private static final List<Set<ConfigOption<?>>> CONFLICT_CLIENT_OPTIONS =
            ImmutableList.<Set<ConfigOption<?>>>builder()
                    .add(ImmutableSet.of(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP))
                    .add(ImmutableSet.of(PULSAR_TOPIC_NAMES, PULSAR_TOPICS_PATTERN))
                    .build();

    private static final Set<ConfigOption<?>> REQUIRED_CLIENT_OPTIONS =
            ImmutableSet.<ConfigOption<?>>builder().add(PULSAR_SERVICE_URL).build();

    private PulsarConfigurationUtils() {
        // No need to create instance.
    }

    /**
     * Create a common {@link ClientConfigurationData} for both pulsar source and sink connector by
     * using the given {@link Configuration}. Add the configuration was listed in {@link
     * PulsarSourceOptions}, and we would parse it one by one.
     *
     * @param configuration The flink configuration
     */
    public static ClientConfigurationData createClientConfig(Configuration configuration)
            throws PulsarClientException {
        checkClientConfiguration(configuration);

        ClientConfigurationData data = new ClientConfigurationData();

        // Set the properties one by one.
        data.setServiceUrl(configuration.get(PULSAR_SERVICE_URL));
        data.setAuthPluginClassName(configuration.get(PULSAR_AUTH_PLUGIN_CLASS_NAME));
        data.setAuthParams(configuration.get(PULSAR_AUTH_PARAMS));
        data.setAuthParamMap(
                getOptionValue(
                        configuration,
                        PULSAR_AUTH_PARAM_MAP,
                        v -> PulsarJsonUtils.toMap(String.class, String.class, v)));
        data.setOperationTimeoutMs(configuration.get(PULSAR_OPERATION_TIMEOUT_MS));
        data.setStatsIntervalSeconds(configuration.get(PULSAR_STATS_INTERVAL_SECONDS));
        data.setNumIoThreads(configuration.get(PULSAR_NUM_IO_THREADS));
        data.setNumListenerThreads(configuration.get(PULSAR_NUM_LISTENER_THREADS));
        data.setConnectionsPerBroker(configuration.get(PULSAR_CONNECTIONS_PER_BROKER));
        data.setUseTcpNoDelay(configuration.get(PULSAR_USE_TCP_NO_DELAY));
        data.setUseTls(configuration.get(PULSAR_USE_TLS));
        data.setTlsTrustCertsFilePath(configuration.get(PULSAR_TLS_TRUST_CERTS_FILE_PATH));
        data.setTlsAllowInsecureConnection(configuration.get(PULSAR_TLS_ALLOW_INSECURE_CONNECTION));
        data.setTlsHostnameVerificationEnable(
                configuration.get(PULSAR_TLS_HOSTNAME_VERIFICATION_ENABLE));
        data.setConcurrentLookupRequest(configuration.get(PULSAR_CONCURRENT_LOOKUP_REQUEST));
        data.setMaxLookupRequest(configuration.get(PULSAR_MAX_LOOKUP_REQUEST));
        data.setMaxLookupRedirects(configuration.get(PULSAR_MAX_LOOKUP_REDIRECTS));
        data.setMaxNumberOfRejectedRequestPerConnection(
                configuration.get(PULSAR_MAX_NUMBER_OF_REJECTED_REQUEST_PER_CONNECTION));
        data.setKeepAliveIntervalSeconds(configuration.get(PULSAR_KEEP_ALIVE_INTERVAL_SECONDS));
        data.setConnectionTimeoutMs(configuration.get(PULSAR_CONNECTION_TIMEOUT_MS));
        data.setRequestTimeoutMs(configuration.get(PULSAR_REQUEST_TIMEOUT_MS));
        data.setInitialBackoffIntervalNanos(
                configuration.get(PULSAR_INITIAL_BACKOFF_INTERVAL_NANOS));
        data.setMaxBackoffIntervalNanos(configuration.get(PULSAR_MAX_BACKOFF_INTERVAL_NANOS));
        data.setEnableBusyWait(configuration.get(PULSAR_ENABLE_BUSY_WAIT));
        data.setListenerName(configuration.get(PULSAR_LISTENER_NAME));
        data.setUseKeyStoreTls(configuration.get(PULSAR_USE_KEY_STORE_TLS));
        data.setSslProvider(configuration.get(PULSAR_SSL_PROVIDER));
        data.setTlsTrustStoreType(configuration.get(PULSAR_TLS_TRUST_STORE_TYPE));
        data.setTlsTrustStorePath(configuration.get(PULSAR_TLS_TRUST_STORE_PATH));
        data.setTlsTrustStorePassword(configuration.get(PULSAR_TLS_TRUST_STORE_PASSWORD));
        data.setTlsCiphers(
                getOptionValue(
                        configuration,
                        PULSAR_TLS_CIPHERS,
                        v -> PulsarJsonUtils.toSet(String.class, v)));
        data.setTlsProtocols(
                getOptionValue(
                        configuration,
                        PULSAR_TLS_PROTOCOLS,
                        v -> PulsarJsonUtils.toSet(String.class, v)));
        data.setMemoryLimitBytes(configuration.get(PULSAR_MEMORY_LIMIT_BYTES));
        data.setProxyServiceUrl(configuration.get(PULSAR_PROXY_SERVICE_URL));
        data.setProxyProtocol(configuration.get(PULSAR_PROXY_PROTOCOL));
        data.setEnableTransaction(configuration.get(PULSAR_ENABLE_TRANSACTION));

        // Create Authentication instance like pulsar command line tools.
        // The token and TLS authentication should be created by the below factory method.
        data.setAuthentication(createAuthentication(configuration));

        return data;
    }

    /**
     * Create the {@link Authentication} instance for both {@code PulsarClient} and {@code
     * PulsarAdmin}. If the user didn't provide configuration, a {@link AuthenticationDisabled}
     * instance would be returned.
     */
    public static Authentication createAuthentication(Configuration configuration)
            throws PulsarClientException {
        if (configuration.contains(PULSAR_AUTH_PLUGIN_CLASS_NAME)) {
            if (configuration.contains(PULSAR_AUTH_PARAMS)) {
                return AuthenticationFactory.create(
                        configuration.get(PULSAR_AUTH_PLUGIN_CLASS_NAME),
                        configuration.get(PULSAR_AUTH_PARAMS));
            } else if (configuration.contains(PULSAR_AUTH_PARAM_MAP)) {
                Map<String, String> paramsMap =
                        getOptionValue(
                                configuration,
                                PULSAR_AUTH_PARAM_MAP,
                                v -> PulsarJsonUtils.toMap(String.class, String.class, v));
                return AuthenticationFactory.create(
                        configuration.get(PULSAR_AUTH_PLUGIN_CLASS_NAME), paramsMap);
            }
        }

        return AuthenticationDisabled.INSTANCE;
    }

    /**
     * Helper method for checking client related config options. We would validate:
     *
     * <ul>
     *   <li>If user have provided the required client config options.
     *   <li>If user have provided some conflict options.
     * </ul>
     */
    private static void checkClientConfiguration(Configuration configuration) {
        REQUIRED_CLIENT_OPTIONS.forEach(
                option ->
                        Preconditions.checkArgument(
                                configuration.contains(option),
                                "Config option %s is not provided for pulsar client.",
                                option));

        CONFLICT_CLIENT_OPTIONS.forEach(
                options -> {
                    long nums = options.stream().filter(configuration::contains).count();
                    Preconditions.checkArgument(
                            nums > 1,
                            "Conflict config options %s were provided, we only support one of them for creating pulsar client.",
                            options);
                });
    }

    /**
     * Create a {@link ConsumerConfigurationData} by using the given {@link Configuration}. Add the
     * configuration was listed in {@link PulsarSourceOptions}, and we would parse it one by one.
     *
     * @param configuration The flink configuration
     * @param <T> The type of the pulsar message, this type is ignored.
     */
    public static <T> ConsumerConfigurationData<T> createConsumerConfig(
            Configuration configuration) {
        ConsumerConfigurationData<T> data = new ConsumerConfigurationData<>();

        // Set the properties one by one.
        data.setTopicNames(
                getOptionValue(
                        configuration,
                        PULSAR_TOPIC_NAMES,
                        v -> PulsarJsonUtils.toSet(String.class, v)));
        data.setTopicsPattern(
                getOptionValue(configuration, PULSAR_TOPICS_PATTERN, Pattern::compile));
        data.setSubscriptionName(configuration.get(PULSAR_SUBSCRIPTION_NAME));
        data.setSubscriptionType(configuration.get(PULSAR_SUBSCRIPTION_TYPE));

        data.setSubscriptionMode(configuration.get(PULSAR_SUBSCRIPTION_MODE));
        data.setReceiverQueueSize(configuration.get(PULSAR_RECEIVER_QUEUE_SIZE));
        data.setAcknowledgementsGroupTimeMicros(
                configuration.get(PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS));
        data.setNegativeAckRedeliveryDelayMicros(
                configuration.get(PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS));
        data.setMaxTotalReceiverQueueSizeAcrossPartitions(
                configuration.get(PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS));
        data.setConsumerName(configuration.get(PULSAR_CONSUMER_NAME));
        data.setAckTimeoutMillis(configuration.get(PULSAR_ACK_TIMEOUT_MILLIS));
        data.setTickDurationMillis(configuration.get(PULSAR_TICK_DURATION_MILLIS));
        data.setPriorityLevel(configuration.get(PULSAR_PRIORITY_LEVEL));

        // This method is deprecated in 2.8.0, we use this method for backward compatible.
        data.setMaxPendingChunkedMessage(configuration.get(PULSAR_MAX_PENDING_CHUNKED_MESSAGE));
        data.setAutoAckOldestChunkedMessageOnQueueFull(
                configuration.get(PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL));
        data.setExpireTimeOfIncompleteChunkedMessageMillis(
                configuration.get(PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS));
        data.setCryptoFailureAction(configuration.get(PULSAR_CRYPTO_FAILURE_ACTION));
        data.setProperties(
                getOptionValue(
                        configuration,
                        PULSAR_CONSUMER_PROPERTIES,
                        v ->
                                (SortedMap<String, String>)
                                        PulsarJsonUtils.toMap(
                                                TreeMap.class, String.class, String.class, v)));
        data.setReadCompacted(configuration.get(PULSAR_READ_COMPACTED));
        data.setSubscriptionInitialPosition(
                configuration.get(PULSAR_SUBSCRIPTION_INITIAL_POSITION));
        data.setPatternAutoDiscoveryPeriod(configuration.get(PULSAR_PATTERN_AUTO_DISCOVERY_PERIOD));
        data.setRegexSubscriptionMode(configuration.get(PULSAR_REGEX_SUBSCRIPTION_MODE));

        // Set dead letter policy
        if (configuration.contains(PULSAR_MAX_REDELIVER_COUNT)
                || configuration.contains(PULSAR_RETRY_LETTER_TOPIC)
                || configuration.contains(PULSAR_DEAD_LETTER_TOPIC)) {
            DeadLetterPolicy deadLetterPolicy =
                    DeadLetterPolicy.builder()
                            .maxRedeliverCount(configuration.get(PULSAR_MAX_REDELIVER_COUNT))
                            .retryLetterTopic(configuration.get(PULSAR_RETRY_LETTER_TOPIC))
                            .deadLetterTopic(configuration.get(PULSAR_DEAD_LETTER_TOPIC))
                            .build();
            data.setDeadLetterPolicy(deadLetterPolicy);
        }

        data.setRetryEnable(configuration.get(PULSAR_RETRY_ENABLE));
        data.setAutoUpdatePartitions(configuration.get(PULSAR_AUTO_UPDATE_PARTITIONS));
        data.setAutoUpdatePartitionsIntervalSeconds(
                configuration.get(PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS));
        data.setReplicateSubscriptionState(configuration.get(PULSAR_REPLICATE_SUBSCRIPTION_STATE));
        data.setResetIncludeHead(configuration.get(PULSAR_RESET_INCLUDE_HEAD));
        data.setBatchIndexAckEnabled(configuration.get(PULSAR_BATCH_INDEX_ACK_ENABLED));
        data.setAckReceiptEnabled(configuration.get(PULSAR_ACK_RECEIPT_ENABLED));
        data.setPoolMessages(configuration.get(PULSAR_POOL_MESSAGES));

        return data;
    }

    /** Get the option value str from given config, convert it into the real value instance. */
    public static <T> T getOptionValue(
            Configuration configuration,
            ConfigOption<String> option,
            Function<String, T> convertor) {
        String value = configuration.getString(option);

        if (value != null) {
            return convertor.apply(value);
        } else {
            return null;
        }
    }
}
