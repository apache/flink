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
import org.apache.flink.connector.pulsar.sink.writer.router.KeyHashTopicRouter;

import org.apache.pulsar.client.api.Schema;
import org.apache.pulsar.client.api.TypedMessageBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkNotNull;

/** {@link TypedMessageBuilder} wrapper for providing the required method for end-users. */
@PublicEvolving
public class PulsarMessageBuilder<T> {

    private byte[] orderingKey;
    private String key;
    private long eventTime;
    Schema<T> schema;
    private T value;
    private Map<String, String> properties = new HashMap<>();
    private Long sequenceId;
    private List<String> replicationClusters;
    private boolean disableReplication = false;

    /** Method wrapper of {@link TypedMessageBuilder#orderingKey(byte[])}. */
    public PulsarMessageBuilder<T> orderingKey(byte[] orderingKey) {
        this.orderingKey = checkNotNull(orderingKey);
        return this;
    }

    /**
     * Property {@link TypedMessageBuilder#key(String)}. This property would also be used in {@link
     * KeyHashTopicRouter}.
     */
    public PulsarMessageBuilder<T> key(String key) {
        this.key = checkNotNull(key);
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#eventTime(long)}. */
    public PulsarMessageBuilder<T> eventTime(long eventTime) {
        this.eventTime = eventTime;
        return this;
    }

    /**
     * Method wrapper of {@link TypedMessageBuilder#value(Object)}. You can pass any schema for
     * validating it on Pulsar. This is called schema evolution. But the topic on Pulsar should bind
     * to a fixed {@link Schema}. You can't have multiple schemas on the same topic unless it's
     * compatible with each other.
     *
     * @param value The value could be null, which is called tombstones message in Pulsar. (It will
     *     be skipped and considered deleted.)
     */
    public PulsarMessageBuilder<T> value(Schema<T> schema, T value) {
        this.schema = checkNotNull(schema);
        this.value = value;
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#property(String, String)}. */
    public PulsarMessageBuilder<T> property(String key, String value) {
        this.properties.put(checkNotNull(key), checkNotNull(value));
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#properties(Map)}. */
    public PulsarMessageBuilder<T> properties(Map<String, String> properties) {
        this.properties.putAll(checkNotNull(properties));
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#sequenceId(long)}. */
    public PulsarMessageBuilder<T> sequenceId(long sequenceId) {
        this.sequenceId = sequenceId;
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#replicationClusters(List)}. */
    public PulsarMessageBuilder<T> replicationClusters(List<String> replicationClusters) {
        this.replicationClusters = checkNotNull(replicationClusters);
        return this;
    }

    /** Method wrapper of {@link TypedMessageBuilder#disableReplication()}. */
    public PulsarMessageBuilder<T> disableReplication() {
        this.disableReplication = true;
        return this;
    }

    public PulsarMessage<T> build() {
        checkNotNull(schema, "Schema should be provided.");

        return new PulsarMessage<>(
                orderingKey,
                key,
                eventTime,
                schema,
                value,
                properties,
                sequenceId,
                replicationClusters,
                disableReplication);
    }
}
