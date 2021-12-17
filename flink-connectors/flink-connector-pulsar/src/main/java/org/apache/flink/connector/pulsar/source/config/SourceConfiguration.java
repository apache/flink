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

    /** The interval in millis for flink querying topic partition information. */
    private final long partitionDiscoveryIntervalMs;

    /**
     * This is used for all subscription type. But the behavior may not be the same among them. If
     * you don't enable the flink checkpoint, make sure this option is set to true.
     *
     * <ul>
     *   <li>{@link SubscriptionType#Shared} and {@link SubscriptionType#Key_Shared} would
     *       immediately acknowledge the message after consuming it.
     *   <li>{@link SubscriptionType#Failover} and {@link SubscriptionType#Exclusive} would perform
     *       a incremental acknowledge in a fixed {@link #autoCommitCursorInterval}.
     * </ul>
     */
    private final boolean enableAutoAcknowledgeMessage;

    /**
     * The interval in millis for acknowledge message when you enable {@link
     * #enableAutoAcknowledgeMessage} and use {@link SubscriptionType#Failover} or {@link
     * SubscriptionType#Exclusive} as your consuming subscription type.
     */
    private final long autoCommitCursorInterval;

    /**
     * Pulsar's transaction have a timeout mechanism for uncommitted transaction. We use transaction
     * for {@link SubscriptionType#Shared} and {@link SubscriptionType#Key_Shared} when user disable
     * {@link #enableAutoAcknowledgeMessage} and enable flink checkpoint. Since the checkpoint
     * interval couldn't be acquired from {@link SourceReaderContext#getConfiguration()}, we have to
     * expose this option. Make sure this value is greater than the checkpoint interval.
     */
    private final long transactionTimeoutMillis;

    /**
     * The fetch time for flink split reader polling message. We would stop polling message and
     * return the message in {@link RecordsWithSplitIds} when timeout or exceed the {@link
     * #maxFetchRecords}.
     */
    private final Duration maxFetchTime;

    /**
     * The fetch counts for a split reader. We would stop polling message and return the message in
     * {@link RecordsWithSplitIds} when timeout {@link #maxFetchTime} or exceed this value.
     */
    private final int maxFetchRecords;

    /** Validate the {@link CursorPosition} generated by {@link StartCursor}. */
    private final CursorVerification verifyInitialOffsets;

    /**
     * The pulsar's subscription name for this flink source. All the readers would share this
     * subscription name.
     *
     * @see ConsumerBuilder#subscriptionName
     */
    private final String subscriptionName;

    /**
     * The pulsar's subscription type for this flink source. All the readers would share this
     * subscription type.
     *
     * @see SubscriptionType
     */
    private final SubscriptionType subscriptionType;

    /**
     * The pulsar's subscription mode for this flink source. All the readers would share this
     * subscription mode.
     *
     * @see SubscriptionMode
     */
    private final SubscriptionMode subscriptionMode;

    public SourceConfiguration(Configuration configuration) {
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

    public boolean enablePartitionDiscovery() {
        return partitionDiscoveryIntervalMs > 0;
    }

    public long getPartitionDiscoveryIntervalMs() {
        return partitionDiscoveryIntervalMs;
    }

    public boolean isEnableAutoAcknowledgeMessage() {
        return enableAutoAcknowledgeMessage;
    }

    public long getAutoCommitCursorInterval() {
        return autoCommitCursorInterval;
    }

    public long getTransactionTimeoutMillis() {
        return transactionTimeoutMillis;
    }

    public Duration getMaxFetchTime() {
        return maxFetchTime;
    }

    public int getMaxFetchRecords() {
        return maxFetchRecords;
    }

    public CursorVerification getVerifyInitialOffsets() {
        return verifyInitialOffsets;
    }

    public String getSubscriptionName() {
        return subscriptionName;
    }

    public SubscriptionType getSubscriptionType() {
        return subscriptionType;
    }

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
}
