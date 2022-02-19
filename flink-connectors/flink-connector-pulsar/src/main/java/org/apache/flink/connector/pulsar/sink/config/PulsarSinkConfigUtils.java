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

package org.apache.flink.connector.pulsar.sink.config;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connector.pulsar.common.config.PulsarConfigValidator;

import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.ProducerBuilder;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.Schema;

import java.util.Map;

import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_ADMIN_URL;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAMS;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_AUTH_PARAM_MAP;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_SERVICE_URL;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_ENABLED;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_BYTES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_PARTITION_SWITCH_FREQUENCY_BY_PUBLISH_DELAY;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_CHUNKING_ENABLED;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_COMPRESSION_TYPE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_INITIAL_SEQUENCE_ID;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MAX_PENDING_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_PRODUCER_NAME;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_PRODUCER_PROPERTIES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_SEND_TIMEOUT_MS;
import static org.apache.pulsar.client.api.MessageRoutingMode.SinglePartition;
import static org.apache.pulsar.client.api.ProducerAccessMode.Shared;

/** Create the {@link Producer} to send message and a validator for building sink config. */
@Internal
public final class PulsarSinkConfigUtils {

    private PulsarSinkConfigUtils() {
        // No need to create instance.
    }

    public static final PulsarConfigValidator SINK_CONFIG_VALIDATOR =
            PulsarConfigValidator.builder()
                    .requiredOption(PULSAR_SERVICE_URL)
                    .requiredOption(PULSAR_ADMIN_URL)
                    .conflictOptions(PULSAR_AUTH_PARAMS, PULSAR_AUTH_PARAM_MAP)
                    .build();

    /** Create a pulsar producer builder by using the given Configuration. */
    public static <T> ProducerBuilder<T> createProducerBuilder(
            PulsarClient client, Schema<T> schema, SinkConfiguration configuration) {
        ProducerBuilder<T> builder = client.newProducer(schema);

        configuration.useOption(PULSAR_PRODUCER_NAME, builder::producerName);
        configuration.useOption(
                PULSAR_SEND_TIMEOUT_MS,
                Math::toIntExact,
                ms -> builder.sendTimeout(ms, MILLISECONDS));
        configuration.useOption(PULSAR_MAX_PENDING_MESSAGES, builder::maxPendingMessages);
        configuration.useOption(
                PULSAR_MAX_PENDING_MESSAGES_ACROSS_PARTITIONS,
                builder::maxPendingMessagesAcrossPartitions);
        configuration.useOption(
                PULSAR_BATCHING_MAX_PUBLISH_DELAY_MICROS,
                s -> builder.batchingMaxPublishDelay(s, MICROSECONDS));
        configuration.useOption(
                PULSAR_BATCHING_PARTITION_SWITCH_FREQUENCY_BY_PUBLISH_DELAY,
                builder::roundRobinRouterBatchingPartitionSwitchFrequency);
        configuration.useOption(PULSAR_BATCHING_MAX_MESSAGES, builder::batchingMaxMessages);
        configuration.useOption(PULSAR_BATCHING_MAX_BYTES, builder::batchingMaxBytes);
        configuration.useOption(PULSAR_BATCHING_ENABLED, builder::enableBatching);
        configuration.useOption(PULSAR_CHUNKING_ENABLED, builder::enableChunking);
        configuration.useOption(PULSAR_COMPRESSION_TYPE, builder::compressionType);
        configuration.useOption(PULSAR_INITIAL_SEQUENCE_ID, builder::initialSequenceId);

        // Set producer properties
        Map<String, String> properties = configuration.getProperties(PULSAR_PRODUCER_PROPERTIES);
        if (!properties.isEmpty()) {
            builder.properties(properties);
        }

        // Set the default value for current producer builder.
        // We use non-partitioned producer by default. This wouldn't be changed in the future.
        builder.blockIfQueueFull(true)
                .messageRoutingMode(SinglePartition)
                .enableMultiSchema(false)
                .autoUpdatePartitions(false)
                .accessMode(Shared)
                .enableLazyStartPartitionedProducers(false);

        return builder;
    }
}
