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

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.CursorPosition;
import org.apache.flink.connector.pulsar.source.enumerator.cursor.StartCursor;

import org.apache.pulsar.client.api.ConsumerBuilder;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;

import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;

import static org.apache.flink.connector.base.source.reader.SourceReaderOptions.ELEMENT_QUEUE_CAPACITY;
import static org.apache.flink.connector.pulsar.common.config.PulsarConfigUtils.getOptionValue;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_AUTO_COMMIT_CURSOR_INTERVAL;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_RECORDS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_MAX_FETCH_TIME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_PARTITION_DISCOVERY_INTERVAL_MS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_MODE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_NAME;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_SUBSCRIPTION_TYPE;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_TRANSACTION_TIMEOUT_MILLIS;
import static org.apache.flink.connector.pulsar.source.PulsarSourceOptions.PULSAR_VERIFY_INITIAL_OFFSETS;

/** The configure class for pulsar source. */
@PublicEvolving
public class SourceConfiguration implements Serializable {
    private static final long serialVersionUID = 8488507275800787580L;

    private final int messageQueueCapacity;
    private final long partitionDiscoveryIntervalMs;
    private final boolean enableAutoAcknowledgeMessage;
    private final long autoCommitCursorInterval;
    private final long transactionTimeoutMillis;
    private final Duration maxFetchTime;
    private final int maxFetchRecords;
    private final CursorVerification verifyInitialOffsets;
    private final String subscriptionName;
    private final SubscriptionType subscriptionType;
    private final SubscriptionMode subscriptionMode;

    public SourceConfiguration(Configuration configuration) {
        this.messageQueueCapacity = configuration.getInteger(ELEMENT_QUEUE_CAPACITY);
        this.partitionDiscoveryIntervalMs =
                configuration.get(PULSAR_PARTITION_DISCOVERY_INTERVAL_MS);
        this.enableAutoAcknowledgeMessage =
                configuration.get(PULSAR_ENABLE_AUTO_ACKNOWLEDGE_MESSAGE);
        this.autoCommitCursorInterval = configuration.get(PULSAR_AUTO_COMMIT_CURSOR_INTERVAL);
        this.transactionTimeoutMillis = configuration.get(PULSAR_TRANSACTION_TIMEOUT_MILLIS);
        this.maxFetchTime =
                getOptionValue(configuration, PULSAR_MAX_FETCH_TIME, Duration::ofMillis);
        this.maxFetchRecords = configuration.get(PULSAR_MAX_FETCH_RECORDS);
        this.verifyInitialOffsets = configuration.get(PULSAR_VERIFY_INITIAL_OFFSETS);
        this.subscriptionName = configuration.get(PULSAR_SUBSCRIPTION_NAME);
        this.subscriptionType = configuration.get(PULSAR_SUBSCRIPTION_TYPE);
        this.subscriptionMode = configuration.get(PULSAR_SUBSCRIPTION_MODE);
    }

    /** The capacity of the element queue in the source reader. */
    public int getMessageQueueCapacity() {
        return messageQueueCapacity;
    }

    /**
     * We would override the interval into a negative number when we set the connector with bounded
     * stop cursor.
     */
    public boolean isEnablePartitionDiscovery() {
        return getPartitionDiscoveryIntervalMs() > 0;
    }

    /** The interval in millis for flink querying topic partition information. */
    public long getPartitionDiscoveryIntervalMs() {
        return partitionDiscoveryIntervalMs;
    }

    /**
     * This is used for all subscription type. But the behavior may not be the same among them. If
     * you don't enable the flink checkpoint, make sure this option is set to true.
     *
     * <ul>
     *   <li>{@link SubscriptionType#Shared} and {@link SubscriptionType#Key_Shared} would
     *       immediately acknowledge the message after consuming it.
     *   <li>{@link SubscriptionType#Failover} and {@link SubscriptionType#Exclusive} would perform
     *       a incremental acknowledge in a fixed {@link #getAutoCommitCursorInterval}.
     * </ul>
     */
    public boolean isEnableAutoAcknowledgeMessage() {
        return enableAutoAcknowledgeMessage;
    }

    /**
     * The interval in millis for acknowledge message when you enable {@link
     * #isEnableAutoAcknowledgeMessage} and use {@link SubscriptionType#Failover} or {@link
     * SubscriptionType#Exclusive} as your consuming subscription type.
     */
    public long getAutoCommitCursorInterval() {
        return autoCommitCursorInterval;
    }

    /**
     * Pulsar's transaction have a timeout mechanism for uncommitted transaction. We use transaction
     * for {@link SubscriptionType#Shared} and {@link SubscriptionType#Key_Shared} when user disable
     * {@link #isEnableAutoAcknowledgeMessage} and enable flink checkpoint. Since the checkpoint
     * interval couldn't be acquired from {@link SourceReaderContext#getConfiguration()}, we have to
     * expose this option. Make sure this value is greater than the checkpoint interval.
     */
    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    /**
     * The fetch time for flink split reader polling message. We would stop polling message and
     * return the message in {@link RecordsWithSplitIds} when timeout or exceed the {@link
     * #getMaxFetchRecords}.
     */
    public Duration getMaxFetchTime() {
        return maxFetchTime;
    }

    /**
     * The fetch counts for a split reader. We would stop polling message and return the message in
     * {@link RecordsWithSplitIds} when timeout {@link #getMaxFetchTime} or exceed this value.
     */
    public int getMaxFetchRecords() {
        return maxFetchRecords;
    }

    /** Validate the {@link CursorPosition} generated by {@link StartCursor}. */
    public CursorVerification getVerifyInitialOffsets() {
        return verifyInitialOffsets;
    }

    /**
     * The pulsar's subscription name for this flink source. All the readers would share this
     * subscription name.
     *
     * @see ConsumerBuilder#subscriptionName
     */
    public String getSubscriptionName() {
        return subscriptionName;
    }

    /**
     * The pulsar's subscription type for this flink source. All the readers would share this
     * subscription type.
     *
     * @see SubscriptionType
     */
    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

    /**
     * The pulsar's subscription mode for this flink source. All the readers would share this
     * subscription mode.
     *
     * @see SubscriptionMode
     */
    public SubscriptionMode getSubscriptionMode() {
        return subscriptionMode;
    }

    /** Convert the subscription into a readable str. */
    public String getSubscriptionDesc() {
        return getSubscriptionName()
                + "("
                + getSubscriptionType()
                + ","
                + getSubscriptionMode()
                + ")";
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
        SourceConfiguration that = (SourceConfiguration) o;
        return partitionDiscoveryIntervalMs == that.partitionDiscoveryIntervalMs
                && enableAutoAcknowledgeMessage == that.enableAutoAcknowledgeMessage
                && autoCommitCursorInterval == that.autoCommitCursorInterval
                && transactionTimeoutMillis == that.transactionTimeoutMillis
                && maxFetchRecords == that.maxFetchRecords
                && Objects.equals(maxFetchTime, that.maxFetchTime)
                && verifyInitialOffsets == that.verifyInitialOffsets
                && Objects.equals(subscriptionName, that.subscriptionName)
                && subscriptionType == that.subscriptionType
                && subscriptionMode == that.subscriptionMode;
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                super.hashCode(),
                partitionDiscoveryIntervalMs,
                enableAutoAcknowledgeMessage,
                autoCommitCursorInterval,
                transactionTimeoutMillis,
                maxFetchTime,
                maxFetchRecords,
                verifyInitialOffsets,
                subscriptionName,
                subscriptionType,
                subscriptionMode);
    }
}
