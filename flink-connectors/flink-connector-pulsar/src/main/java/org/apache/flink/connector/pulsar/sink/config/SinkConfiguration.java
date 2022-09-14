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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.sink.Sink.InitContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.common.config.PulsarConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.PulsarWriter;
import org.apache.flink.connector.pulsar.sink.writer.router.MessageKeyHash;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSchemaWrapper;

import org.apache.pulsar.client.api.Schema;

import java.util.Objects;

import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_BATCHING_MAX_MESSAGES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MAX_PENDING_MESSAGES_ON_PARALLELISM;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MAX_RECOMMIT_TIMES;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_MESSAGE_KEY_HASH;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_TOPIC_METADATA_REFRESH_INTERVAL;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_DELIVERY_GUARANTEE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_SCHEMA_EVOLUTION;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_TRANSACTION_TIMEOUT;

/** The configured class for pulsar sink. */
@PublicEvolving
public class SinkConfiguration extends PulsarConfiguration {
    private static final long serialVersionUID = 4941360605051251153L;

    private final DeliveryGuarantee deliveryGuarantee;
    private final long transactionTimeoutMillis;
    private final long topicMetadataRefreshInterval;
    private final int partitionSwitchSize;
    private final MessageKeyHash messageKeyHash;
    private final boolean enableSchemaEvolution;
    private final int maxPendingMessages;
    private final int maxRecommitTimes;

    public SinkConfiguration(Configuration configuration) {
        super(configuration);

        this.deliveryGuarantee = get(PULSAR_WRITE_DELIVERY_GUARANTEE);
        this.transactionTimeoutMillis = getLong(PULSAR_WRITE_TRANSACTION_TIMEOUT);
        this.topicMetadataRefreshInterval = getLong(PULSAR_TOPIC_METADATA_REFRESH_INTERVAL);
        this.partitionSwitchSize = getInteger(PULSAR_BATCHING_MAX_MESSAGES);
        this.messageKeyHash = get(PULSAR_MESSAGE_KEY_HASH);
        this.enableSchemaEvolution = get(PULSAR_WRITE_SCHEMA_EVOLUTION);
        this.maxPendingMessages = get(PULSAR_MAX_PENDING_MESSAGES_ON_PARALLELISM);
        this.maxRecommitTimes = get(PULSAR_MAX_RECOMMIT_TIMES);
    }

    /** The delivery guarantee changes the behavior of {@link PulsarWriter}. */
    public DeliveryGuarantee getDeliveryGuarantee() {
        return deliveryGuarantee;
    }

    /**
     * Pulsar's transactions have a timeout mechanism for the uncommitted transaction. We use
     * transactions for making sure the message could be written only once. Since the checkpoint
     * interval couldn't be acquired from {@link InitContext}, we have to expose this option. Make
     * sure this value is greater than the checkpoint interval. Create a pulsar producer builder by
     * using the given Configuration.
     */
    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    /**
     * Auto-update the topic metadata in a fixed interval (in ms). The default value is 30 minutes.
     */
    public long getTopicMetadataRefreshInterval() {
        return topicMetadataRefreshInterval;
    }

    /**
     * Switch the partition to write when we have written the given size of messages. It's used for
     * a round-robin topic router.
     */
    public int getPartitionSwitchSize() {
        return partitionSwitchSize;
    }

    /** The message key's hash logic for routing the message into one Pulsar partition. */
    public MessageKeyHash getMessageKeyHash() {
        return messageKeyHash;
    }

    /**
     * If we should serialize and send the message with a specified Pulsar {@link Schema} instead
     * the default {@link Schema#BYTES}. This switch is only used for {@link PulsarSchemaWrapper}.
     */
    public boolean isEnableSchemaEvolution() {
        return enableSchemaEvolution;
    }

    /**
     * Pulsar message is sent asynchronously. Set this option for limiting the pending messages in a
     * Pulsar writer instance.
     */
    public int getMaxPendingMessages() {
        return maxPendingMessages;
    }

    /** The maximum allowed recommitting time for a Pulsar transaction. */
    public int getMaxRecommitTimes() {
        return maxRecommitTimes;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        if (!super.equals(o)) {
            return false;
        }
        SinkConfiguration that = (SinkConfiguration) o;
        return transactionTimeoutMillis == that.transactionTimeoutMillis
                && topicMetadataRefreshInterval == that.topicMetadataRefreshInterval
                && partitionSwitchSize == that.partitionSwitchSize
                && enableSchemaEvolution == that.enableSchemaEvolution
                && messageKeyHash == that.messageKeyHash
                && maxPendingMessages == that.maxPendingMessages
                && maxRecommitTimes == that.maxRecommitTimes;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                transactionTimeoutMillis,
                topicMetadataRefreshInterval,
                partitionSwitchSize,
                messageKeyHash,
                enableSchemaEvolution,
                maxPendingMessages,
                maxRecommitTimes);
    }
}
