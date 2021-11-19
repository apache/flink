/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.InstantiationUtil;

import org.apache.http.HttpHost;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct a {@link ElasticsearchSink}.
 *
 * <p>The following example shows the minimal setup to create a ElasticsearchSink that submits
 * actions on checkpoint or the default number of actions was buffered (1000).
 *
 * <pre>{@code
 * Elasticsearch<String> sink = Elasticsearch
 *     .builder()
 *     .setHosts(MY_ELASTICSEARCH_HOSTS)
 *     .setEmitter(MY_ELASTICSEARCH_EMITTER)
 *     .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records converted to Elasticsearch actions
 */
@PublicEvolving
public class ElasticsearchSinkBuilder<IN> {

    private int bulkFlushMaxActions = 1000;
    private int bulkFlushMaxMb = -1;
    private long bulkFlushInterval = -1;
    private FlushBackoffType bulkFlushBackoffType = FlushBackoffType.NONE;
    private int bulkFlushBackoffRetries = -1;
    private long bulkFlushBackOffDelay = -1;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private List<HttpHost> hosts;
    private ElasticsearchEmitter<? super IN> emitter;
    private String username;
    private String password;
    private String connectionPathPrefix;

    ElasticsearchSinkBuilder() {}

    /**
     * Sets the hosts where the Elasticsearch cluster nodes are reachable.
     *
     * @param hosts http addresses describing the node locations
     */
    public ElasticsearchSinkBuilder<IN> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty.");
        this.hosts = Arrays.asList(hosts);
        return this;
    }

    /**
     * Sets the emitter which is invoked on every record to convert it to Elasticsearch actions.
     *
     * @param emitter to process records into Elasticsearch actions.
     * @return {@link ElasticsearchSinkBuilder}
     */
    public <T extends IN> ElasticsearchSinkBuilder<T> setEmitter(
            ElasticsearchEmitter<? super T> emitter) {
        checkNotNull(emitter);
        checkState(
                InstantiationUtil.isSerializable(emitter),
                "The elasticsearch emitter must be serializable.");
        final ElasticsearchSinkBuilder<T> self = self();
        self.emitter = emitter;
        return self;
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#NONE}
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        checkState(
                deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                "Elasticsearch sink does not support the EXACTLY_ONCE guarantee.");
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        return this;
    }

    /**
     * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
     * disable it. The default flush size 1000.
     *
     * @param numMaxActions the maximum number of actions to buffer per bulk request.
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setBulkFlushMaxActions(int numMaxActions) {
        checkState(
                numMaxActions == -1 || numMaxActions > 0,
                "Max number of buffered actions must be larger than 0.");
        this.bulkFlushMaxActions = numMaxActions;
        return this;
    }

    /**
     * Sets the maximum size of buffered actions, in mb, per bulk request. You can pass -1 to
     * disable it.
     *
     * @param maxSizeMb the maximum size of buffered actions, in mb.
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setBulkFlushMaxSizeMb(int maxSizeMb) {
        checkState(
                maxSizeMb == -1 || maxSizeMb > 0,
                "Max size of buffered actions must be larger than 0.");
        this.bulkFlushMaxMb = maxSizeMb;
        return this;
    }

    /**
     * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * @param intervalMillis the bulk flush interval, in milliseconds.
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setBulkFlushInterval(long intervalMillis) {
        checkState(
                intervalMillis == -1 || intervalMillis >= 0,
                "Interval (in milliseconds) between each flush must be larger than "
                        + "or equal to 0.");
        this.bulkFlushInterval = intervalMillis;
        return this;
    }

    /**
     * Sets the type of back off to use when flushing bulk requests. The default bulk flush back off
     * type is {@link FlushBackoffType#NONE}.
     *
     * <p>Sets the amount of delay between each backoff attempt when flushing bulk requests, in
     * milliseconds.
     *
     * <p>Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
     *
     * @param flushBackoffType the backoff type to use.
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setBulkFlushBackoffStrategy(
            FlushBackoffType flushBackoffType, int maxRetries, long delayMillis) {
        this.bulkFlushBackoffType = checkNotNull(flushBackoffType);
        checkState(
                flushBackoffType != FlushBackoffType.NONE,
                "FlushBackoffType#NONE does not require a configuration it is the default, retries and delay are ignored.");
        checkState(maxRetries > 0, "Max number of backoff attempts must be larger than 0.");
        this.bulkFlushBackoffRetries = maxRetries;
        checkState(
                delayMillis >= 0,
                "Delay (in milliseconds) between each backoff attempt must be larger "
                        + "than or equal to 0.");
        this.bulkFlushBackOffDelay = delayMillis;
        return this;
    }

    /**
     * Sets the username used to authenticate the connection with the Elasticsearch cluster.
     *
     * @param username of the Elasticsearch cluster user
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setConnectionUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return this;
    }

    /**
     * Sets the password used to authenticate the conection with the Elasticsearch cluster.
     *
     * @param password of the Elasticsearch cluster user
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setConnectionPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return this;
    }

    /**
     * Sets a prefix which used for every REST communication to the Elasticsearch cluster.
     *
     * @param prefix for the communication
     * @return {@link ElasticsearchSinkBuilder}
     */
    public ElasticsearchSinkBuilder<IN> setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return this;
    }

    /** @return {@link ElasticsearchSink} */
    public ElasticsearchSink<IN> build() {
        checkNotNull(hosts);
        checkNotNull(emitter);
        return new ElasticsearchSink<IN>(
                hosts,
                emitter,
                deliveryGuarantee,
                buildBulkProcessorConfig(),
                new NetworkClientConfig(username, password, connectionPathPrefix));
    }

    @SuppressWarnings("unchecked")
    private <T extends IN> ElasticsearchSinkBuilder<T> self() {
        return (ElasticsearchSinkBuilder<T>) this;
    }

    private BulkProcessorConfig buildBulkProcessorConfig() {
        return new BulkProcessorConfig(
                bulkFlushMaxActions,
                bulkFlushMaxMb,
                bulkFlushInterval,
                bulkFlushBackoffType,
                bulkFlushBackoffRetries,
                bulkFlushBackOffDelay);
    }
}
