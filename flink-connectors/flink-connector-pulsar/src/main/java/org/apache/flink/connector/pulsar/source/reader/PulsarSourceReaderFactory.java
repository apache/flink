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

package org.apache.flink.connector.pulsar.source.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.pulsar.source.config.SourceConfiguration;
import org.apache.flink.connector.pulsar.source.reader.deserializer.PulsarDeserializationSchema;
import org.apache.flink.connector.pulsar.source.reader.message.PulsarMessage;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarOrderedSourceReader;
import org.apache.flink.connector.pulsar.source.reader.source.PulsarUnorderedSourceReader;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarOrderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.reader.split.PulsarUnorderedPartitionSplitReader;
import org.apache.flink.connector.pulsar.source.split.PulsarPartitionSplit;

import org.apache.pulsar.client.admin.PulsarAdmin;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.apache.pulsar.client.impl.PulsarClientImpl;

import java.util.function.Supplier;

import static org.apache.flink.connector.base.source.reader.SourceReaderOptions.ELEMENT_QUEUE_CAPACITY;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.createAdmin;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.createClient;

/**
 * This factory class is used for creating different types of source reader for different
 * subscription type.
 *
 * <ol>
 *   <li>Failover, Exclusive: We would create {@link PulsarOrderedSourceReader}.
 *   <li>Shared, Key_Shared: We would create {@link PulsarUnorderedSourceReader}.
 * </ol>
 */
@Internal
public final class PulsarSourceReaderFactory {

    private PulsarSourceReaderFactory() {
        // No public constructor.
    }

    @SuppressWarnings("java:S2095")
    public static <OUT> SourceReader<OUT, PulsarPartitionSplit> create(
            SourceReaderContext readerContext,
            PulsarDeserializationSchema<OUT> deserializationSchema,
            Configuration configuration,
            SourceConfiguration sourceConfiguration) {

        PulsarClient pulsarClient = createClient(configuration);
        PulsarAdmin pulsarAdmin = createAdmin(configuration);

        // Create a message queue with the predefined source option.
        int queueSize = configuration.getInteger(ELEMENT_QUEUE_CAPACITY);
        FutureCompletingBlockingQueue<RecordsWithSplitIds<PulsarMessage<OUT>>> elementsQueue =
                new FutureCompletingBlockingQueue<>(queueSize);

        // Create different pulsar source reader by subscription type.
        SubscriptionType subscriptionType = sourceConfiguration.getSubscriptionType();
        if (subscriptionType == SubscriptionType.Failover
                || subscriptionType == SubscriptionType.Exclusive) {
            // Create an ordered split reader supplier.
            Supplier<PulsarOrderedPartitionSplitReader<OUT>> splitReaderSupplier =
                    () ->
                            new PulsarOrderedPartitionSplitReader<>(
                                    pulsarClient,
                                    pulsarAdmin,
                                    configuration,
                                    sourceConfiguration,
                                    deserializationSchema);

            return new PulsarOrderedSourceReader<>(
                    elementsQueue,
                    splitReaderSupplier,
                    configuration,
                    readerContext,
                    sourceConfiguration,
                    pulsarClient,
                    pulsarAdmin);
        } else if (subscriptionType == SubscriptionType.Shared
                || subscriptionType == SubscriptionType.Key_Shared) {
            TransactionCoordinatorClient coordinatorClient =
                    ((PulsarClientImpl) pulsarClient).getTcClient();
            if (coordinatorClient == null
                    && !sourceConfiguration.isEnableAutoAcknowledgeMessage()) {
                throw new IllegalStateException("Transaction is required but didn't enabled");
            }

            Supplier<PulsarUnorderedPartitionSplitReader<OUT>> splitReaderSupplier =
                    () ->
                            new PulsarUnorderedPartitionSplitReader<>(
                                    pulsarClient,
                                    pulsarAdmin,
                                    configuration,
                                    sourceConfiguration,
                                    deserializationSchema,
                                    coordinatorClient);

            return new PulsarUnorderedSourceReader<>(
                    elementsQueue,
                    splitReaderSupplier,
                    configuration,
                    readerContext,
                    sourceConfiguration,
                    pulsarClient,
                    pulsarAdmin,
                    coordinatorClient);
        } else {
            throw new UnsupportedOperationException(
                    "This subscription type is not " + subscriptionType + " supported currently.");
        }
    }
}
