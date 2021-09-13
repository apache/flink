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
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableSet;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.List;
import java.util.Optional;
import java.util.Set;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.setOptionValue;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACK_RECEIPT_ENABLED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ACK_TIMEOUT_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CONSUMER_PROPERTIES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_CRYPTO_FAILURE_ACTION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_DEAD_LETTER_TOPIC;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_PENDING_CHUNKED_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_REDELIVER_COUNT;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_POOL_MESSAGES;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PRIORITY_LEVEL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_READ_COMPACTED;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RECEIVER_QUEUE_SIZE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_REPLICATE_SUBSCRIPTION_STATE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RETRY_ENABLE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_RETRY_LETTER_TOPIC;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_INITIAL_POSITION;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TICK_DURATION_MILLIS;

/** Create source related {@link Consumer} and validate config. */
@Internal
public final class PulsarSourceConfigUtils {

    private PulsarSourceConfigUtils() {
        // No need to create instance.
    }

    private static final List<Set<ConfigOption<?>>> CONFLICT_SOURCE_OPTIONS =
            ImmutableList.<Set<ConfigOption<?>>>builder()
                    .add(ImmutableSet.of(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP))
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

    /** Create a pulsar consumer builder by using the given Configuration. */
    public static <T> ConsumerBuilder<T> createConsumerBuilder(
            PulsarClient client, Schema<T> schema, Configuration configuration) {
        ConsumerBuilder<T> builder = client.newConsumer(schema);

        setOptionValue(configuration, PULSAR_SUBSCRIPTION_NAME, builder::subscriptionName);
        setOptionValue(
                configuration, PULSAR_ACK_TIMEOUT_MILLIS, v -> builder.ackTimeout(v, MILLISECONDS));
        setOptionValue(configuration, PULSAR_ACK_RECEIPT_ENABLED, builder::isAckReceiptEnabled);
        setOptionValue(
                configuration,
                PULSAR_TICK_DURATION_MILLIS,
                v -> builder.ackTimeoutTickTime(v, MILLISECONDS));
        setOptionValue(
                configuration,
                PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS,
                v -> builder.negativeAckRedeliveryDelay(v, MICROSECONDS));
        setOptionValue(configuration, PULSAR_SUBSCRIPTION_TYPE, builder::subscriptionType);
        setOptionValue(configuration, PULSAR_SUBSCRIPTION_MODE, builder::subscriptionMode);
        setOptionValue(configuration, PULSAR_CRYPTO_FAILURE_ACTION, builder::cryptoFailureAction);
        setOptionValue(configuration, PULSAR_RECEIVER_QUEUE_SIZE, builder::receiverQueueSize);
        setOptionValue(
                configuration,
                PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS,
                v -> builder.acknowledgmentGroupTime(v, MICROSECONDS));
        setOptionValue(
                configuration,
                PULSAR_REPLICATE_SUBSCRIPTION_STATE,
                builder::replicateSubscriptionState);
        setOptionValue(
                configuration,
                PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS,
                builder::maxTotalReceiverQueueSizeAcrossPartitions);
        setOptionValue(configuration, PULSAR_CONSUMER_NAME, builder::consumerName);
        setOptionValue(configuration, PULSAR_READ_COMPACTED, builder::readCompacted);
        setOptionValue(configuration, PULSAR_PRIORITY_LEVEL, builder::priorityLevel);
        setOptionValue(configuration, PULSAR_CONSUMER_PROPERTIES, builder::properties);
        setOptionValue(
                configuration,
                PULSAR_SUBSCRIPTION_INITIAL_POSITION,
                builder::subscriptionInitialPosition);
        createDeadLetterPolicy(configuration).ifPresent(builder::deadLetterPolicy);
        setOptionValue(
                configuration,
                PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS,
                v -> builder.autoUpdatePartitionsInterval(v, SECONDS));
        setOptionValue(configuration, PULSAR_RETRY_ENABLE, builder::enableRetry);
        setOptionValue(
                configuration,
                PULSAR_MAX_PENDING_CHUNKED_MESSAGE,
                builder::maxPendingChunkedMessage);
        setOptionValue(
                configuration,
                PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL,
                builder::autoAckOldestChunkedMessageOnQueueFull);
        setOptionValue(
                configuration,
                PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS,
                v -> builder.expireTimeOfIncompleteChunkedMessage(v, MILLISECONDS));
        setOptionValue(configuration, PULSAR_POOL_MESSAGES, builder::poolMessages);

        return builder;
    }

    private static Optional<DeadLetterPolicy> createDeadLetterPolicy(Configuration configuration) {
        if (configuration.contains(PULSAR_MAX_REDELIVER_COUNT)
                || configuration.contains(PULSAR_RETRY_LETTER_TOPIC)
                || configuration.contains(PULSAR_DEAD_LETTER_TOPIC)) {
            DeadLetterPolicy.DeadLetterPolicyBuilder builder = DeadLetterPolicy.builder();

            setOptionValue(configuration, PULSAR_MAX_REDELIVER_COUNT, builder::maxRedeliverCount);
            setOptionValue(configuration, PULSAR_RETRY_LETTER_TOPIC, builder::retryLetterTopic);
            setOptionValue(configuration, PULSAR_DEAD_LETTER_TOPIC, builder::deadLetterTopic);

            return Optional.of(builder.build());
        } else {
            return Optional.empty();
        }
    }
}
