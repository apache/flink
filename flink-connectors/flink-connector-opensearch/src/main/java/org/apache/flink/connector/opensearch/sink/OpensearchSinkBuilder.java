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

package org.apache.flink.connector.opensearch.sink;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.util.InstantiationUtil;

import org.apache.http.HttpHost;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;

import java.util.Arrays;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * Builder to construct an Opensearch compatible {@link OpensearchSink}.
 *
 * <p>The following example shows the minimal setup to create a OpensearchSink that submits actions
 * on checkpoint or the default number of actions was buffered (1000).
 *
 * <pre>{@code
 * OpensearchSink<String> sink = new OpensearchSinkBuilder<String>()
 *     .setHosts(new HttpHost("localhost:9200")
 *     .setEmitter((element, context, indexer) -> {
 *          indexer.add(
 *              new IndexRequest("my-index")
 *              .id(element.f0.toString())
 *              .source(element.f1)
 *          );
 *      })
 *     .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
 *     .build();
 * }</pre>
 *
 * @param <IN> type of the records converted to Opensearch actions
 */
@PublicEvolving
public class OpensearchSinkBuilder<IN> {

    private int bulkFlushMaxActions = 1000;
    private int bulkFlushMaxMb = -1;
    private long bulkFlushInterval = -1;
    private FlushBackoffType bulkFlushBackoffType = FlushBackoffType.NONE;
    private int bulkFlushBackoffRetries = -1;
    private long bulkFlushBackOffDelay = -1;
    private DeliveryGuarantee deliveryGuarantee = DeliveryGuarantee.NONE;
    private List<HttpHost> hosts;
    protected OpensearchEmitter<? super IN> emitter;
    private String username;
    private String password;
    private String connectionPathPrefix;
    private Integer connectionTimeout;
    private Integer connectionRequestTimeout;
    private Integer socketTimeout;
    private Boolean allowInsecure;

    public OpensearchSinkBuilder() {}

    @SuppressWarnings("unchecked")
    protected <S extends OpensearchSinkBuilder<?>> S self() {
        return (S) this;
    }

    /**
     * Sets the emitter which is invoked on every record to convert it to Opensearch actions.
     *
     * @param emitter to process records into Opensearch actions.
     * @return this builder
     */
    public <T extends IN> OpensearchSinkBuilder<T> setEmitter(
            OpensearchEmitter<? super T> emitter) {
        checkNotNull(emitter);
        checkState(
                InstantiationUtil.isSerializable(emitter),
                "The Opensearch emitter must be serializable.");

        final OpensearchSinkBuilder<T> self = self();
        self.emitter = emitter;
        return self;
    }

    /**
     * Sets the hosts where the Opensearch cluster nodes are reachable.
     *
     * @param hosts http addresses describing the node locations
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setHosts(HttpHost... hosts) {
        checkNotNull(hosts);
        checkState(hosts.length > 0, "Hosts cannot be empty.");
        this.hosts = Arrays.asList(hosts);
        return self();
    }

    /**
     * Sets the wanted {@link DeliveryGuarantee}. The default delivery guarantee is {@link
     * DeliveryGuarantee#NONE}
     *
     * @param deliveryGuarantee which describes the record emission behaviour
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setDeliveryGuarantee(DeliveryGuarantee deliveryGuarantee) {
        checkState(
                deliveryGuarantee != DeliveryGuarantee.EXACTLY_ONCE,
                "Opensearch sink does not support the EXACTLY_ONCE guarantee.");
        this.deliveryGuarantee = checkNotNull(deliveryGuarantee);
        return self();
    }

    /**
     * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
     * disable it. The default flush size 1000.
     *
     * @param numMaxActions the maximum number of actions to buffer per bulk request.
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setBulkFlushMaxActions(int numMaxActions) {
        checkState(
                numMaxActions == -1 || numMaxActions > 0,
                "Max number of buffered actions must be larger than 0.");
        this.bulkFlushMaxActions = numMaxActions;
        return self();
    }

    /**
     * Sets the maximum size of buffered actions, in mb, per bulk request. You can pass -1 to
     * disable it.
     *
     * @param maxSizeMb the maximum size of buffered actions, in mb.
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setBulkFlushMaxSizeMb(int maxSizeMb) {
        checkState(
                maxSizeMb == -1 || maxSizeMb > 0,
                "Max size of buffered actions must be larger than 0.");
        this.bulkFlushMaxMb = maxSizeMb;
        return self();
    }

    /**
     * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
     *
     * @param intervalMillis the bulk flush interval, in milliseconds.
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setBulkFlushInterval(long intervalMillis) {
        checkState(
                intervalMillis == -1 || intervalMillis >= 0,
                "Interval (in milliseconds) between each flush must be larger than "
                        + "or equal to 0.");
        this.bulkFlushInterval = intervalMillis;
        return self();
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
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setBulkFlushBackoffStrategy(
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
        return self();
    }

    /**
     * Sets the username used to authenticate the connection with the Opensearch cluster.
     *
     * @param username of the Opensearch cluster user
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setConnectionUsername(String username) {
        checkNotNull(username);
        this.username = username;
        return self();
    }

    /**
     * Sets the password used to authenticate the conection with the Opensearch cluster.
     *
     * @param password of the Opensearch cluster user
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setConnectionPassword(String password) {
        checkNotNull(password);
        this.password = password;
        return self();
    }

    /**
     * Sets a prefix which used for every REST communication to the Opensearch cluster.
     *
     * @param prefix for the communication
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setConnectionPathPrefix(String prefix) {
        checkNotNull(prefix);
        this.connectionPathPrefix = prefix;
        return self();
    }

    /**
     * Sets the timeout for requesting the connection of the Opensearch cluster from the connection
     * manager.
     *
     * @param timeout for the connection request
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setConnectionRequestTimeout(int timeout) {
        checkState(timeout >= 0, "Connection request timeout must be larger than or equal to 0.");
        this.connectionRequestTimeout = timeout;
        return self();
    }

    /**
     * Sets the timeout for establishing a connection of the Opensearch cluster.
     *
     * @param timeout for the connection
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setConnectionTimeout(int timeout) {
        checkState(timeout >= 0, "Connection timeout must be larger than or equal to 0.");
        this.connectionTimeout = timeout;
        return self();
    }

    /**
     * Sets the timeout for waiting for data or, put differently, a maximum period inactivity
     * between two consecutive data packets.
     *
     * @param timeout for the socket
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setSocketTimeout(int timeout) {
        checkState(timeout >= 0, "Socket timeout must be larger than or equal to 0.");
        this.socketTimeout = timeout;
        return self();
    }

    /**
     * Allows to bypass the certificates chain validation and connect to insecure network endpoints
     * (for example, servers which use self-signed certificates).
     *
     * @param allowInsecure allow or not to insecure network endpoints
     * @return this builder
     */
    public OpensearchSinkBuilder<IN> setAllowInsecure(boolean allowInsecure) {
        this.allowInsecure = allowInsecure;
        return self();
    }

    protected BulkProcessorBuilderFactory getBulkProcessorBuilderFactory() {
        return new BulkProcessorBuilderFactory() {
            @Override
            public BulkProcessor.Builder apply(
                    RestHighLevelClient client,
                    BulkProcessorConfig bulkProcessorConfig,
                    BulkProcessor.Listener listener) {
                BulkProcessor.Builder builder =
                        BulkProcessor.builder(
                                new BulkRequestConsumerFactory() { // This cannot be inlined as a
                                    // lambda because then
                                    // deserialization fails
                                    @Override
                                    public void accept(
                                            BulkRequest bulkRequest,
                                            ActionListener<BulkResponse>
                                                    bulkResponseActionListener) {
                                        client.bulkAsync(
                                                bulkRequest,
                                                RequestOptions.DEFAULT,
                                                bulkResponseActionListener);
                                    }
                                },
                                listener);

                if (bulkProcessorConfig.getBulkFlushMaxActions() != -1) {
                    builder.setBulkActions(bulkProcessorConfig.getBulkFlushMaxActions());
                }

                if (bulkProcessorConfig.getBulkFlushMaxMb() != -1) {
                    builder.setBulkSize(
                            new ByteSizeValue(
                                    bulkProcessorConfig.getBulkFlushMaxMb(), ByteSizeUnit.MB));
                }

                if (bulkProcessorConfig.getBulkFlushInterval() != -1) {
                    builder.setFlushInterval(
                            new TimeValue(bulkProcessorConfig.getBulkFlushInterval()));
                }

                BackoffPolicy backoffPolicy;
                final TimeValue backoffDelay =
                        new TimeValue(bulkProcessorConfig.getBulkFlushBackOffDelay());
                final int maxRetryCount = bulkProcessorConfig.getBulkFlushBackoffRetries();
                switch (bulkProcessorConfig.getFlushBackoffType()) {
                    case CONSTANT:
                        backoffPolicy = BackoffPolicy.constantBackoff(backoffDelay, maxRetryCount);
                        break;
                    case EXPONENTIAL:
                        backoffPolicy =
                                BackoffPolicy.exponentialBackoff(backoffDelay, maxRetryCount);
                        break;
                    case NONE:
                        backoffPolicy = BackoffPolicy.noBackoff();
                        break;
                    default:
                        throw new IllegalArgumentException(
                                "Received unknown backoff policy type "
                                        + bulkProcessorConfig.getFlushBackoffType());
                }
                builder.setBackoffPolicy(backoffPolicy);
                return builder;
            }
        };
    }

    /**
     * Constructs the {@link OpensearchSink} with the properties configured this builder.
     *
     * @return {@link OpensearchSink}
     */
    public OpensearchSink<IN> build() {
        checkNotNull(emitter);
        checkNotNull(hosts);

        NetworkClientConfig networkClientConfig = buildNetworkClientConfig();
        BulkProcessorConfig bulkProcessorConfig = buildBulkProcessorConfig();

        BulkProcessorBuilderFactory bulkProcessorBuilderFactory = getBulkProcessorBuilderFactory();
        ClosureCleaner.clean(
                bulkProcessorBuilderFactory, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);

        return new OpensearchSink<>(
                hosts,
                emitter,
                deliveryGuarantee,
                bulkProcessorBuilderFactory,
                bulkProcessorConfig,
                networkClientConfig);
    }

    private NetworkClientConfig buildNetworkClientConfig() {
        checkArgument(!hosts.isEmpty(), "Hosts cannot be empty.");

        return new NetworkClientConfig(
                username,
                password,
                connectionPathPrefix,
                connectionRequestTimeout,
                connectionTimeout,
                socketTimeout,
                allowInsecure);
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

    @Override
    public String toString() {
        return "OpensearchSinkBuilder{"
                + "bulkFlushMaxActions="
                + bulkFlushMaxActions
                + ", bulkFlushMaxMb="
                + bulkFlushMaxMb
                + ", bulkFlushInterval="
                + bulkFlushInterval
                + ", bulkFlushBackoffType="
                + bulkFlushBackoffType
                + ", bulkFlushBackoffRetries="
                + bulkFlushBackoffRetries
                + ", bulkFlushBackOffDelay="
                + bulkFlushBackOffDelay
                + ", deliveryGuarantee="
                + deliveryGuarantee
                + ", hosts="
                + hosts
                + ", emitter="
                + emitter
                + ", username='"
                + username
                + '\''
                + ", password='"
                + password
                + '\''
                + ", connectionPathPrefix='"
                + connectionPathPrefix
                + '\''
                + ", allowInsecure='"
                + allowInsecure
                + '\''
                + '}';
    }
}
