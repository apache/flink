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

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.pulsar.common.config.ConfigurationDataCustomizer;
import org.apache.flink.connector.pulsar.common.utils.PulsarJsonUtils;
import org.apache.flink.connector.pulsar.source.PulsarSourceOptions;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.impl.conf.ConsumerConfigurationData;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.regex.Pattern;

import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.getOptionValue;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.common.utils.PulsarJsonUtils.configMap;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACK_RECEIPT_ENABLED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACK_TIMEOUT_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_UPDATE_PARTITIONS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_BATCH_INDEX_ACK_ENABLED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_PROPERTIES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CRYPTO_FAILURE_ACTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_DEAD_LETTER_TOPIC;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_PENDING_CHUNKED_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_REDELIVER_COUNT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PATTERN_AUTO_DISCOVERY_PERIOD;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_POOL_MESSAGES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PRIORITY_LEVEL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_READ_COMPACTED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RECEIVER_QUEUE_SIZE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REGEX_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REPLICATE_SUBSCRIPTION_STATE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RESET_INCLUDE_HEAD;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RETRY_ENABLE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RETRY_LETTER_TOPIC;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_INITIAL_POSITION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TICK_DURATION_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TOPICS_PATTERN;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TOPIC_NAMES;

/** Create source related {@link Consumer} and validate config. */
@Internal
public final class PulsarSourceConfigUtils {

    private PulsarSourceConfigUtils() {
        // No need to create instance.
    }

    private static final List<Set<ConfigOption<?>>> CONFLICT_SOURCE_OPTIONS =
            ImmutableList.<Set<ConfigOption<?>>>builder()
                    .add(ImmutableSet.of(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP))
                    .add(ImmutableSet.of(PULSAR_TOPIC_NAMES, PULSAR_TOPICS_PATTERN))
                    .build();

    private static final Set<ConfigOption<?>> REQUIRED_SOURCE_OPTIONS =
            ImmutableSet.<ConfigOption<?>>builder()
                    .add(PULSAR_SERVICE_URL)
                    .add(PULSAR_ADMIN_URL)
                    .add(PULSAR_SUBSCRIPTION_NAME)
                    .build();

    /**
     * Helper method for checking client related config options. We would validate:
     *
     * <ul>
     *   <li>If user have provided the required client config options.
     *   <li>If user have provided some conflict options.
     * </ul>
     */
    public static void checkConfigurations(Configuration configuration) {
        REQUIRED_SOURCE_OPTIONS.forEach(
                option ->
                        Preconditions.checkArgument(
                                configuration.contains(option),
                                "Config option %s is not provided for pulsar source.",
                                option));

        CONFLICT_SOURCE_OPTIONS.forEach(
                options -> {
                    long nums = options.stream().filter(configuration::contains).count();
                    Preconditions.checkArgument(
                            nums <= 1,
                            "Conflict config options %s were provided, we only support one of them for creating pulsar source.",
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
            Configuration configuration,
            @Nullable ConfigurationDataCustomizer<ConsumerConfigurationData<T>> customizer) {
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

        if (customizer != null) {
            customizer.customize(data);
        }

        return data;
    }

    /** Create a Pulsar Consumer by using the flink Configuration and the config customizer. */
    public static <T> Consumer<T> createConsumer(
            PulsarClient client,
            Schema<T> schema,
            Configuration configuration,
            ConfigurationDataCustomizer<ConsumerConfigurationData<T>> customizer)
            throws PulsarClientException {
        return createConsumer(client, schema, createConsumerConfig(configuration, customizer));
    }

    /** Create a pulsar consumer by using the given ConsumerConfigurationData and PulsarClient. */
    public static <T> Consumer<T> createConsumer(
            PulsarClient client, Schema<T> schema, ConsumerConfigurationData<T> config)
            throws PulsarClientException {
        // ConsumerBuilder don't support using the given ConsumerConfigurationData directly.
        ConsumerBuilder<T> consumerBuilder = client.newConsumer(schema).loadConf(configMap(config));

        // Set some non-serializable fields.
        if (config.getMessageListener() != null) {
            consumerBuilder.messageListener(config.getMessageListener());
        }
        if (config.getConsumerEventListener() != null) {
            consumerBuilder.consumerEventListener(config.getConsumerEventListener());
        }
        if (config.getCryptoKeyReader() != null) {
            consumerBuilder.cryptoKeyReader(config.getCryptoKeyReader());
        }
        if (config.getMessageCrypto() != null) {
            consumerBuilder.messageCrypto(config.getMessageCrypto());
        }
        if (config.getBatchReceivePolicy() != null) {
            consumerBuilder.batchReceivePolicy(config.getBatchReceivePolicy());
        }

        return consumerBuilder.subscribe();
    }
}
