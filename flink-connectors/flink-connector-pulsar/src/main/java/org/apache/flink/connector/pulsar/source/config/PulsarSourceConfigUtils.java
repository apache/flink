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
import org.apache.flink.connector.pulsar.common.config.PulsarConfigValidator;

import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.DeadLetterPolicy;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.Map;
import java.util.Optional;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
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

    public static final PulsarConfigValidator SOURCE_CONFIG_VALIDATOR =
            PulsarConfigValidator.builder()
                    .requiredOption(PULSAR_SERVICE_URL)
                    .requiredOption(PULSAR_ADMIN_URL)
                    .requiredOption(PULSAR_SUBSCRIPTION_NAME)
                    .conflictOptions(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP)
                    .build();

    /** Create a pulsar consumer builder by using the given Configuration. */
    public static <T> ConsumerBuilder<T> createConsumerBuilder(
            PulsarClient client, Schema<T> schema, SourceConfiguration configuration) {
        ConsumerBuilder<T> builder = client.newConsumer(schema);

        configuration.useOption(PULSAR_SUBSCRIPTION_NAME, builder::subscriptionName);
        configuration.useOption(
                PULSAR_ACK_TIMEOUT_MILLIS, v -> builder.ackTimeout(v, MILLISECONDS));
        configuration.useOption(PULSAR_ACK_RECEIPT_ENABLED, builder::isAckReceiptEnabled);
        configuration.useOption(
                PULSAR_TICK_DURATION_MILLIS, v -> builder.ackTimeoutTickTime(v, MILLISECONDS));
        configuration.useOption(
                PULSAR_NEGATIVE_ACK_REDELIVERY_DELAY_MICROS,
                v -> builder.negativeAckRedeliveryDelay(v, MICROSECONDS));
        configuration.useOption(PULSAR_SUBSCRIPTION_TYPE, builder::subscriptionType);
        configuration.useOption(PULSAR_SUBSCRIPTION_MODE, builder::subscriptionMode);
        configuration.useOption(PULSAR_CRYPTO_FAILURE_ACTION, builder::cryptoFailureAction);
        configuration.useOption(PULSAR_RECEIVER_QUEUE_SIZE, builder::receiverQueueSize);
        configuration.useOption(
                PULSAR_ACKNOWLEDGEMENTS_GROUP_TIME_MICROS,
                v -> builder.acknowledgmentGroupTime(v, MICROSECONDS));
        configuration.useOption(
                PULSAR_REPLICATE_SUBSCRIPTION_STATE, builder::replicateSubscriptionState);
        configuration.useOption(
                PULSAR_MAX_TOTAL_RECEIVER_QUEUE_SIZE_ACROSS_PARTITIONS,
                builder::maxTotalReceiverQueueSizeAcrossPartitions);
        configuration.useOption(PULSAR_CONSUMER_NAME, builder::consumerName);
        configuration.useOption(PULSAR_READ_COMPACTED, builder::readCompacted);
        configuration.useOption(PULSAR_PRIORITY_LEVEL, builder::priorityLevel);
        configuration.useOption(
                PULSAR_SUBSCRIPTION_INITIAL_POSITION, builder::subscriptionInitialPosition);
        createDeadLetterPolicy(configuration).ifPresent(builder::deadLetterPolicy);
        configuration.useOption(
                PULSAR_AUTO_UPDATE_PARTITIONS_INTERVAL_SECONDS,
                v -> builder.autoUpdatePartitionsInterval(v, SECONDS));
        configuration.useOption(PULSAR_RETRY_ENABLE, builder::enableRetry);
        configuration.useOption(
                PULSAR_MAX_PENDING_CHUNKED_MESSAGE, builder::maxPendingChunkedMessage);
        configuration.useOption(
                PULSAR_AUTO_ACK_OLDEST_CHUNKED_MESSAGE_ON_QUEUE_FULL,
                builder::autoAckOldestChunkedMessageOnQueueFull);
        configuration.useOption(
                PULSAR_EXPIRE_TIME_OF_INCOMPLETE_CHUNKED_MESSAGE_MILLIS,
                v -> builder.expireTimeOfIncompleteChunkedMessage(v, MILLISECONDS));
        configuration.useOption(PULSAR_POOL_MESSAGES, builder::poolMessages);

        Map<String, String> properties = configuration.getProperties(PULSAR_CONSUMER_PROPERTIES);
        if (!properties.isEmpty()) {
            builder.properties(properties);
        }

        return builder;
    }

    private static Optional<DeadLetterPolicy> createDeadLetterPolicy(
            SourceConfiguration configuration) {
        if (configuration.contains(PULSAR_MAX_REDELIVER_COUNT)
                || configuration.contains(PULSAR_RETRY_LETTER_TOPIC)
                || configuration.contains(PULSAR_DEAD_LETTER_TOPIC)) {
            DeadLetterPolicy.DeadLetterPolicyBuilder builder = DeadLetterPolicy.builder();

            configuration.useOption(PULSAR_MAX_REDELIVER_COUNT, builder::maxRedeliverCount);
            configuration.useOption(PULSAR_RETRY_LETTER_TOPIC, builder::retryLetterTopic);
            configuration.useOption(PULSAR_DEAD_LETTER_TOPIC, builder::deadLetterTopic);

            return Optional.of(builder.build());
        } else {
            return Optional.empty();
        }
    }
}
