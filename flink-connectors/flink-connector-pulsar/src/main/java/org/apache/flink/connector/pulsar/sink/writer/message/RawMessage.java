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

package org.apache.flink.connector.pulsar.sink.writer.message;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The raw message which would be converted into Pulsar {@link Message} by using {@link
 * TypedMessageBuilder}. We create the class for pre-created the message before sending to Pulsar.
 *
 * <p>All the fields in this class could be nullable.
 *
 * @param <IN> The record type which would be send to Pulsar.
 */
@PublicEvolving
public final class RawMessage<IN> {

    /** Property {@link TypedMessageBuilder#orderingKey(byte[])}. */
    private byte[] orderingKey = null;

    /** Property {@link TypedMessageBuilder#key(String)}. */
    private String key = null;

    /** Property {@link TypedMessageBuilder#properties(Map)}. */
    private Map<String, String> properties = null;

    /** Property {@link TypedMessageBuilder#eventTime(long)}. */
    private Long eventTime = null;

    /** Property {@link TypedMessageBuilder#replicationClusters(List)}. */
    private List<String> replicationClusters = null;

    /** Property {@link TypedMessageBuilder#disableReplication()}. */
    private boolean disableReplication = false;

    /** Property {@link TypedMessageBuilder#value(Object)}. */
    private final IN value;

    public RawMessage(IN value) {
        this.value = checkNotNull(value);
    }

    /**
     * Sets the ordering key of the message for message dispatch in {@link
     * SubscriptionType#Key_Shared} mode. Partition key Will be used if ordering key not specified.
     *
     * @param orderingKey the ordering key for the message
     */
    public RawMessage<IN> setOrderingKey(byte[] orderingKey) {
        this.orderingKey = orderingKey;
        return this;
    }

    /**
     * Sets the key of the message for routing policy.
     *
     * @param key the partitioning key for the message
     */
    public RawMessage<IN> setKey(String key) {
        this.key = key;
        return this;
    }

    /** Add all the properties in the provided map. */
    public RawMessage<IN> setProperties(Map<String, String> properties) {
        this.properties = properties;
        return this;
    }

    /**
     * Set the event time for a given message.
     *
     * <p>Applications can retrieve the event time by calling {@link Message#getEventTime()}.
     *
     * <p>Note: currently pulsar doesn't support event-time based index. so the subscribers can't
     * seek the messages by event time.
     */
    public RawMessage<IN> setEventTime(Long eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    /**
     * Override the geo-replication clusters for this message.
     *
     * @param replicationClusters the list of clusters.
     */
    public RawMessage<IN> setReplicationClusters(List<String> replicationClusters) {
        this.replicationClusters = replicationClusters;
        return this;
    }

    /** Disable geo-replication for this message. */
    public RawMessage<IN> setDisableReplication(boolean disableReplication) {
        this.disableReplication = disableReplication;
        return this;
    }

    /** @return The record need to send to Pulsar. */
    public IN getValue() {
        return value;
    }

    /** @return The message key. */
    @Nullable
    public String getKey() {
        return key;
    }

    /**
     * Convert the raw message into a Pulsar managed TypedMessageBuilder. We can't create {@link
     * Message} directly, so we have to expose this method for sink.
     */
    @Internal
    public void supplement(TypedMessageBuilder<?> builder) {
        if (orderingKey != null && orderingKey.length > 0) {
            builder.orderingKey(orderingKey);
        }
        if (key != null) {
            builder.key(key);
        }
        if (properties != null && !properties.isEmpty()) {
            builder.properties(properties);
        }
        if (eventTime != null) {
            builder.eventTime(eventTime);
        }
        if (replicationClusters != null && !replicationClusters.isEmpty()) {
            builder.replicationClusters(replicationClusters);
        }
        if (disableReplication) {
            builder.disableReplication();
        }
    }
}
