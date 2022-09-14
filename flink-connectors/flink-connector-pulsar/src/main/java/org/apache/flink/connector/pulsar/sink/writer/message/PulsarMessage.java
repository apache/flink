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

import org.apache.flink.annotation.PublicEvolving;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Map;

/**
 * The message instance would be used for {@link TypedMessageBuilder}. We create this class because
 * the Pulsar lacks such kind of POJO class.
 */
@PublicEvolving
public class PulsarMessage<T> {

    @Nullable private final byte[] orderingKey;
    @Nullable private final String key;
    private final long eventTime;
    private final Schema<T> schema;
    @Nullable private final T value;
    @Nullable private final Map<String, String> properties;
    @Nullable private final Long sequenceId;
    @Nullable private final List<String> replicationClusters;
    private final boolean disableReplication;

    /** Package private for building this class only in {@link PulsarMessageBuilder}. */
    PulsarMessage(
            @Nullable byte[] orderingKey,
            @Nullable String key,
            long eventTime,
            Schema<T> schema,
            @Nullable T value,
            @Nullable Map<String, String> properties,
            @Nullable Long sequenceId,
            @Nullable List<String> replicationClusters,
            boolean disableReplication) {
        this.orderingKey = orderingKey;
        this.key = key;
        this.eventTime = eventTime;
        this.schema = schema;
        this.value = value;
        this.properties = properties;
        this.sequenceId = sequenceId;
        this.replicationClusters = replicationClusters;
        this.disableReplication = disableReplication;
    }

    @Nullable
    public byte[] getOrderingKey() {
        return orderingKey;
    }

    @Nullable
    public String getKey() {
        return key;
    }

    public long getEventTime() {
        return eventTime;
    }

    public Schema<T> getSchema() {
        return schema;
    }

    @Nullable
    public T getValue() {
        return value;
    }

    @Nullable
    public Map<String, String> getProperties() {
        return properties;
    }

    @Nullable
    public Long getSequenceId() {
        return sequenceId;
    }

    @Nullable
    public List<String> getReplicationClusters() {
        return replicationClusters;
    }

    public boolean isDisableReplication() {
        return disableReplication;
    }
}
