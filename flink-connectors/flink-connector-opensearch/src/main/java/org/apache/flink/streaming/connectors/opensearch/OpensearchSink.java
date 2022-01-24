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

package org.apache.flink.streaming.connectors.opensearch;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.connectors.opensearch.util.NoOpFailureHandler;
import org.apache.flink.util.InstantiationUtil;
import org.apache.flink.util.Preconditions;

import org.apache.http.HttpHost;
import org.opensearch.action.ActionRequest;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.bulk.BulkItemResponse;
import org.opensearch.action.bulk.BulkProcessor;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.RestClient;
import org.opensearch.client.RestClientBuilder;
import org.opensearch.client.RestHighLevelClient;
import org.opensearch.common.unit.ByteSizeUnit;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for all Flink Opensearch Sinks.
 *
 * <p>This class implements the common behaviour across Opensearch versions, such as the use of an
 * internal {@link BulkProcessor} to buffer multiple {@link ActionRequest}s before sending the
 * requests to the cluster, as well as passing input records to the user provided {@link
 * OpensearchSinkFunction} for processing.
 *
 * @param <T> Type of the elements handled by this sink
 */
@Internal
public class OpensearchSink<T> extends RichSinkFunction<T> implements CheckpointedFunction {
    private static final long serialVersionUID = -1007596293618451942L;
    private static final Logger LOG = LoggerFactory.getLogger(OpensearchSink.class);

    // ------------------------------------------------------------------------
    //  Internal bulk processor configuration
    // ------------------------------------------------------------------------

    public static final String CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS = "bulk.flush.max.actions";
    public static final String CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB = "bulk.flush.max.size.mb";
    public static final String CONFIG_KEY_BULK_FLUSH_INTERVAL_MS = "bulk.flush.interval.ms";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE = "bulk.flush.backoff.enable";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE = "bulk.flush.backoff.type";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES = "bulk.flush.backoff.retries";
    public static final String CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY = "bulk.flush.backoff.delay";

    /** Used to control whether the retry delay should increase exponentially or remain constant. */
    @PublicEvolving
    public enum FlushBackoffType {
        CONSTANT,
        EXPONENTIAL
    }

    /**
     * Provides a backoff policy for bulk requests. Whenever a bulk request is rejected due to
     * resource constraints (i.e. the client's internal thread pool is full), the backoff policy
     * decides how long the bulk processor will wait before the operation is retried internally.
     *
     * <p>This is a proxy for version specific backoff policies.
     */
    public static class BulkFlushBackoffPolicy implements Serializable {

        private static final long serialVersionUID = -6022851996101826049L;

        // the default values follow the Opensearch default settings for BulkProcessor
        private FlushBackoffType backoffType = FlushBackoffType.EXPONENTIAL;
        private int maxRetryCount = 8;
        private long delayMillis = 50;

        public FlushBackoffType getBackoffType() {
            return backoffType;
        }

        public int getMaxRetryCount() {
            return maxRetryCount;
        }

        public long getDelayMillis() {
            return delayMillis;
        }

        public void setBackoffType(FlushBackoffType backoffType) {
            this.backoffType = checkNotNull(backoffType);
        }

        public void setMaxRetryCount(int maxRetryCount) {
            checkArgument(maxRetryCount >= 0);
            this.maxRetryCount = maxRetryCount;
        }

        public void setDelayMillis(long delayMillis) {
            checkArgument(delayMillis >= 0);
            this.delayMillis = delayMillis;
        }
    }

    private final Integer bulkProcessorFlushMaxActions;
    private final Integer bulkProcessorFlushMaxSizeMb;
    private final Long bulkProcessorFlushIntervalMillis;
    private final BulkFlushBackoffPolicy bulkProcessorFlushBackoffPolicy;

    // ------------------------------------------------------------------------
    //  User-facing API and configuration
    // ------------------------------------------------------------------------

    /**
     * The config map that contains configuration for the bulk flushing behaviours.
     *
     * <p>For {@link org.opensearch.client.transport.TransportClient} based implementations, this
     * config map would also contain Opensearch-shipped configuration, and therefore this config map
     * would also be forwarded when creating the Opensearch client.
     */
    private final Map<String, String> userConfig;

    /**
     * The function that is used to construct multiple {@link ActionRequest ActionRequests} from
     * each incoming element.
     */
    private final OpensearchSinkFunction<T> opensearchSinkFunction;

    /** User-provided handler for failed {@link ActionRequest ActionRequests}. */
    private final ActionRequestFailureHandler failureHandler;

    /**
     * If true, the producer will wait until all outstanding action requests have been sent to
     * Opensearch.
     */
    private boolean flushOnCheckpoint = true;

    /**
     * Provided to the user via the {@link OpensearchSinkFunction} to add {@link ActionRequest
     * ActionRequests}.
     */
    private transient RequestIndexer requestIndexer;

    /**
     * Provided to the {@link ActionRequestFailureHandler} to allow users to re-index failed
     * requests.
     */
    private transient BufferingNoOpRequestIndexer failureRequestIndexer;

    // ------------------------------------------------------------------------
    //  Internals for the Flink Opensearch Sink
    // ------------------------------------------------------------------------

    /** Opensearch client instance. */
    private transient RestHighLevelClient client;

    /**
     * Number of pending action requests not yet acknowledged by Opensearch. This value is
     * maintained only if {@link OpensearchSinkBase#flushOnCheckpoint} is {@code true}.
     *
     * <p>This is incremented whenever the user adds (or re-adds through the {@link
     * ActionRequestFailureHandler}) requests to the {@link RequestIndexer}. It is decremented for
     * each completed request of a bulk request, in {@link BulkProcessor.Listener#afterBulk(long,
     * BulkRequest, BulkResponse)} and {@link BulkProcessor.Listener#afterBulk(long, BulkRequest,
     * Throwable)}.
     */
    private AtomicLong numPendingRequests = new AtomicLong(0);

    /** User-provided HTTP Host. */
    private final List<HttpHost> httpHosts;

    /** The factory to configure the rest client. */
    private final RestClientFactory restClientFactory;

    /** Bulk processor to buffer and send requests to Opensearch, created using the client. */
    private transient BulkProcessor bulkProcessor;

    /**
     * This is set from inside the {@link BulkProcessor.Listener} if a {@link Throwable} was thrown
     * in callbacks and the user considered it should fail the sink via the {@link
     * ActionRequestFailureHandler#onFailure(ActionRequest, Throwable, int, RequestIndexer)} method.
     *
     * <p>Errors will be checked and rethrown before processing each input element, and when the
     * sink is closed.
     */
    private final AtomicReference<Throwable> failureThrowable = new AtomicReference<>();

    private OpensearchSink(
            Map<String, String> userConfig,
            List<HttpHost> httpHosts,
            OpensearchSinkFunction<T> opensearchSinkFunction,
            ActionRequestFailureHandler failureHandler,
            RestClientFactory restClientFactory) {
        checkArgument(httpHosts != null && !httpHosts.isEmpty());
        this.httpHosts = httpHosts;
        this.restClientFactory = checkNotNull(restClientFactory);
        this.opensearchSinkFunction = checkNotNull(opensearchSinkFunction);
        this.failureHandler = checkNotNull(failureHandler);
        // we eagerly check if the user-provided sink function and failure handler is serializable;
        // otherwise, if they aren't serializable, users will merely get a non-informative error
        // message
        // "OpensearchSinkBase is not serializable"

        checkArgument(
                InstantiationUtil.isSerializable(opensearchSinkFunction),
                "The implementation of the provided OpensearchSinkFunction is not serializable. "
                        + "The object probably contains or references non-serializable fields.");

        checkArgument(
                InstantiationUtil.isSerializable(failureHandler),
                "The implementation of the provided ActionRequestFailureHandler is not serializable. "
                        + "The object probably contains or references non-serializable fields.");

        // extract and remove bulk processor related configuration from the user-provided config,
        // so that the resulting user config only contains configuration related to the
        // Opensearch client.

        checkNotNull(userConfig);

        // copy config so we can remove entries without side-effects
        userConfig = new HashMap<>(userConfig);

        ParameterTool params = ParameterTool.fromMap(userConfig);

        if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS)) {
            bulkProcessorFlushMaxActions = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS);
        } else {
            bulkProcessorFlushMaxActions = null;
        }

        if (params.has(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB)) {
            bulkProcessorFlushMaxSizeMb = params.getInt(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB);
        } else {
            bulkProcessorFlushMaxSizeMb = null;
        }

        if (params.has(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS)) {
            bulkProcessorFlushIntervalMillis = params.getLong(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
            userConfig.remove(CONFIG_KEY_BULK_FLUSH_INTERVAL_MS);
        } else {
            bulkProcessorFlushIntervalMillis = null;
        }

        boolean bulkProcessorFlushBackoffEnable =
                params.getBoolean(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, true);
        userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE);

        if (bulkProcessorFlushBackoffEnable) {
            this.bulkProcessorFlushBackoffPolicy = new BulkFlushBackoffPolicy();

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)) {
                bulkProcessorFlushBackoffPolicy.setBackoffType(
                        FlushBackoffType.valueOf(params.get(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE)));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE);
            }

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES)) {
                bulkProcessorFlushBackoffPolicy.setMaxRetryCount(
                        params.getInt(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES);
            }

            if (params.has(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY)) {
                bulkProcessorFlushBackoffPolicy.setDelayMillis(
                        params.getLong(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY));
                userConfig.remove(CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY);
            }

        } else {
            bulkProcessorFlushBackoffPolicy = null;
        }

        this.userConfig = userConfig;
    }

    /**
     * Disable flushing on checkpoint. When disabled, the sink will not wait for all pending action
     * requests to be acknowledged by Opensearch on checkpoints.
     *
     * <p>NOTE: If flushing on checkpoint is disabled, the Flink Opensearch Sink does NOT provide
     * any strong guarantees for at-least-once delivery of action requests.
     */
    public void disableFlushOnCheckpoint() {
        this.flushOnCheckpoint = false;
    }

    @Override
    public void open(Configuration parameters) throws Exception {
        client = createClient(userConfig);
        bulkProcessor = buildBulkProcessor(new BulkProcessorListener());
        requestIndexer =
                new OpensearchBulkProcessorIndexer(
                        bulkProcessor, flushOnCheckpoint, numPendingRequests);
        failureRequestIndexer = new BufferingNoOpRequestIndexer();
        opensearchSinkFunction.open();
    }

    @Override
    public void invoke(T value, Context context) throws Exception {
        checkAsyncErrorsAndRequests();
        opensearchSinkFunction.process(value, getRuntimeContext(), requestIndexer);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        // no initialization needed
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        checkAsyncErrorsAndRequests();

        if (flushOnCheckpoint) {
            while (numPendingRequests.get() != 0) {
                bulkProcessor.flush();
                checkAsyncErrorsAndRequests();
            }
        }
    }

    @Override
    public void close() throws Exception {
        opensearchSinkFunction.close();
        if (bulkProcessor != null) {
            bulkProcessor.close();
            bulkProcessor = null;
        }

        if (client != null) {
            client.close();
            client = null;
        }

        // make sure any errors from callbacks are rethrown
        checkErrorAndRethrow();
    }

    /** Build the {@link BulkProcessor}. */
    protected BulkProcessor buildBulkProcessor(BulkProcessor.Listener listener) {
        checkNotNull(listener);

        BulkProcessor.Builder bulkProcessorBuilder =
                BulkProcessor.builder(
                        (request, bulkListener) ->
                                client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener),
                        listener);

        // This makes flush() blocking
        bulkProcessorBuilder.setConcurrentRequests(0);

        if (bulkProcessorFlushMaxActions != null) {
            bulkProcessorBuilder.setBulkActions(bulkProcessorFlushMaxActions);
        }

        if (bulkProcessorFlushMaxSizeMb != null) {
            configureBulkSize(bulkProcessorBuilder);
        }

        if (bulkProcessorFlushIntervalMillis != null) {
            configureFlushInterval(bulkProcessorBuilder);
        }

        // if backoff retrying is disabled, bulkProcessorFlushBackoffPolicy will be null
        configureBulkProcessorBackoff(bulkProcessorBuilder, bulkProcessorFlushBackoffPolicy);

        return bulkProcessorBuilder.build();
    }

    /**
     * Creates an Opensearch client implementing {@link AutoCloseable}.
     *
     * @param clientConfig The configuration to use when constructing the client.
     * @return The created client.
     * @throws IOException IOException
     */
    private RestHighLevelClient createClient(Map<String, String> clientConfig) throws IOException {
        RestClientBuilder builder =
                RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]));
        restClientFactory.configureRestClientBuilder(builder);

        RestHighLevelClient rhlClient = new RestHighLevelClient(builder);
        verifyClientConnection(rhlClient);

        return rhlClient;
    }

    /**
     * Verify the client connection by making a test request/ping to the Opensearch cluster.
     *
     * <p>Called by {@link OpensearchSinkBase#open(org.apache.flink.configuration.Configuration)}
     * after creating the client. This makes sure the underlying client is closed if the connection
     * is not successful and preventing thread leak.
     *
     * @param client the Opensearch client.
     */
    private void verifyClientConnection(RestHighLevelClient client) throws IOException {
        if (LOG.isInfoEnabled()) {
            LOG.info("Pinging Opensearch cluster via hosts {} ...", httpHosts);
        }

        if (!client.ping(RequestOptions.DEFAULT)) {
            throw new RuntimeException("There are no reachable Opensearch nodes!");
        }

        if (LOG.isInfoEnabled()) {
            LOG.info("Opensearch RestHighLevelClient is connected to {}", httpHosts.toString());
        }
    }

    /**
     * Set backoff-related configurations on the provided {@link BulkProcessor.Builder}. The builder
     * will be later on used to instantiate the actual {@link BulkProcessor}.
     *
     * @param builder the {@link BulkProcessor.Builder} to configure.
     * @param flushBackoffPolicy user-provided backoff retry settings ({@code null} if the user
     *     disabled backoff retries).
     */
    private static void configureBulkProcessorBackoff(
            BulkProcessor.Builder builder,
            @Nullable OpensearchSink.BulkFlushBackoffPolicy flushBackoffPolicy) {

        BackoffPolicy backoffPolicy;
        if (flushBackoffPolicy != null) {
            switch (flushBackoffPolicy.getBackoffType()) {
                case CONSTANT:
                    backoffPolicy =
                            BackoffPolicy.constantBackoff(
                                    new TimeValue(flushBackoffPolicy.getDelayMillis()),
                                    flushBackoffPolicy.getMaxRetryCount());
                    break;
                case EXPONENTIAL:
                default:
                    backoffPolicy =
                            BackoffPolicy.exponentialBackoff(
                                    new TimeValue(flushBackoffPolicy.getDelayMillis()),
                                    flushBackoffPolicy.getMaxRetryCount());
            }
        } else {
            backoffPolicy = BackoffPolicy.noBackoff();
        }

        builder.setBackoffPolicy(backoffPolicy);
    }

    private void configureBulkSize(BulkProcessor.Builder bulkProcessorBuilder) {
        final ByteSizeUnit sizeUnit;
        if (bulkProcessorFlushMaxSizeMb == -1) {
            // bulk size can be disabled with -1, however the ByteSizeValue constructor accepts -1
            // only with BYTES as the size unit
            sizeUnit = ByteSizeUnit.BYTES;
        } else {
            sizeUnit = ByteSizeUnit.MB;
        }
        bulkProcessorBuilder.setBulkSize(new ByteSizeValue(bulkProcessorFlushMaxSizeMb, sizeUnit));
    }

    private void configureFlushInterval(BulkProcessor.Builder bulkProcessorBuilder) {
        if (bulkProcessorFlushIntervalMillis == -1) {
            bulkProcessorBuilder.setFlushInterval(null);
        } else {
            bulkProcessorBuilder.setFlushInterval(
                    TimeValue.timeValueMillis(bulkProcessorFlushIntervalMillis));
        }
    }

    private void checkErrorAndRethrow() {
        Throwable cause = failureThrowable.get();
        if (cause != null) {
            throw new RuntimeException("An error occurred in OpensearchSink.", cause);
        }
    }

    private void checkAsyncErrorsAndRequests() {
        checkErrorAndRethrow();
        failureRequestIndexer.processBufferedRequests(requestIndexer);
    }

    private class BulkProcessorListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {}

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            if (response.hasFailures()) {
                BulkItemResponse itemResponse;
                Throwable failure;
                RestStatus restStatus;
                DocWriteRequest actionRequest;

                try {
                    for (int i = 0; i < response.getItems().length; i++) {
                        itemResponse = response.getItems()[i];
                        failure = extractFailureCauseFromBulkItemResponse(itemResponse);
                        if (failure != null) {
                            restStatus = itemResponse.getFailure().getStatus();
                            actionRequest = request.requests().get(i);
                            if (restStatus == null) {
                                if (actionRequest instanceof ActionRequest) {
                                    failureHandler.onFailure(
                                            (ActionRequest) actionRequest,
                                            failure,
                                            -1,
                                            failureRequestIndexer);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "The sink currently only supports ActionRequests");
                                }
                            } else {
                                if (actionRequest instanceof ActionRequest) {
                                    failureHandler.onFailure(
                                            (ActionRequest) actionRequest,
                                            failure,
                                            restStatus.getStatus(),
                                            failureRequestIndexer);
                                } else {
                                    throw new UnsupportedOperationException(
                                            "The sink currently only supports ActionRequests");
                                }
                            }
                        }
                    }
                } catch (Throwable t) {
                    // fail the sink and skip the rest of the items
                    // if the failure handler decides to throw an exception
                    failureThrowable.compareAndSet(null, t);
                }
            }

            if (flushOnCheckpoint) {
                numPendingRequests.getAndAdd(-request.numberOfActions());
            }
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            try {
                for (DocWriteRequest writeRequest : request.requests()) {
                    if (writeRequest instanceof ActionRequest) {
                        failureHandler.onFailure(
                                (ActionRequest) writeRequest, failure, -1, failureRequestIndexer);
                    } else {
                        throw new UnsupportedOperationException(
                                "The sink currently only supports ActionRequests");
                    }
                }
            } catch (Throwable t) {
                // fail the sink and skip the rest of the items
                // if the failure handler decides to throw an exception
                failureThrowable.compareAndSet(null, t);
            }

            if (flushOnCheckpoint) {
                numPendingRequests.getAndAdd(-request.numberOfActions());
            }
        }
    }

    /**
     * Extracts the cause of failure of a bulk item action.
     *
     * @param bulkItemResponse the bulk item response to extract cause of failure
     * @return the extracted {@link Throwable} from the response ({@code null} is the response is
     *     successful).
     */
    private static Throwable extractFailureCauseFromBulkItemResponse(
            BulkItemResponse bulkItemResponse) {
        if (!bulkItemResponse.isFailed()) {
            return null;
        } else {
            return bulkItemResponse.getFailure().getCause();
        }
    }

    long getNumPendingRequests() {
        if (flushOnCheckpoint) {
            return numPendingRequests.get();
        } else {
            throw new UnsupportedOperationException(
                    "The number of pending requests is not maintained when flushing on checkpoint is disabled.");
        }
    }

    /**
     * A builder for creating an {@link OpensearchSink}.
     *
     * @param <T> Type of the elements handled by the sink this builder creates.
     * @deprecated This has been deprecated, please use {@link
     *     org.apache.flink.connector.opensearch.sink.OpensearchSinkBuilder}.
     */
    @Deprecated
    @PublicEvolving
    public static class Builder<T> {

        private final List<HttpHost> httpHosts;
        private final OpensearchSinkFunction<T> opensearchSinkFunction;

        private Map<String, String> bulkRequestsConfig = new HashMap<>();
        private ActionRequestFailureHandler failureHandler = new NoOpFailureHandler();
        private RestClientFactory restClientFactory = restClientBuilder -> {};

        /**
         * Creates a new {@code OpensearchSink} that connects to the cluster using a {@link
         * RestHighLevelClient}.
         *
         * @param httpHosts The list of {@link HttpHost} to which the {@link RestHighLevelClient}
         *     connects to.
         * @param opensearchSinkFunction This is used to generate multiple {@link ActionRequest}
         *     from the incoming element.
         */
        public Builder(List<HttpHost> httpHosts, OpensearchSinkFunction<T> opensearchSinkFunction) {
            this.httpHosts = Preconditions.checkNotNull(httpHosts);
            this.opensearchSinkFunction = Preconditions.checkNotNull(opensearchSinkFunction);
        }

        /**
         * Sets the maximum number of actions to buffer for each bulk request. You can pass -1 to
         * disable it.
         *
         * @param numMaxActions the maximum number of actions to buffer per bulk request.
         */
        public void setBulkFlushMaxActions(int numMaxActions) {
            Preconditions.checkArgument(
                    numMaxActions == -1 || numMaxActions > 0,
                    "Max number of buffered actions must be larger than 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_MAX_ACTIONS, String.valueOf(numMaxActions));
        }

        /**
         * Sets the maximum size of buffered actions, in mb, per bulk request. You can pass -1 to
         * disable it.
         *
         * @param maxSizeMb the maximum size of buffered actions, in mb.
         */
        public void setBulkFlushMaxSizeMb(int maxSizeMb) {
            Preconditions.checkArgument(
                    maxSizeMb == -1 || maxSizeMb > 0,
                    "Max size of buffered actions must be larger than 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_MAX_SIZE_MB, String.valueOf(maxSizeMb));
        }

        /**
         * Sets the bulk flush interval, in milliseconds. You can pass -1 to disable it.
         *
         * @param intervalMillis the bulk flush interval, in milliseconds.
         */
        public void setBulkFlushInterval(long intervalMillis) {
            Preconditions.checkArgument(
                    intervalMillis == -1 || intervalMillis >= 0,
                    "Interval (in milliseconds) between each flush must be larger than or equal to 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_INTERVAL_MS, String.valueOf(intervalMillis));
        }

        /**
         * Sets whether or not to enable bulk flush backoff behaviour.
         *
         * @param enabled whether or not to enable backoffs.
         */
        public void setBulkFlushBackoff(boolean enabled) {
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_ENABLE, String.valueOf(enabled));
        }

        /**
         * Sets the type of back of to use when flushing bulk requests.
         *
         * @param flushBackoffType the backoff type to use.
         */
        public void setBulkFlushBackoffType(FlushBackoffType flushBackoffType) {
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_TYPE,
                    Preconditions.checkNotNull(flushBackoffType).toString());
        }

        /**
         * Sets the maximum number of retries for a backoff attempt when flushing bulk requests.
         *
         * @param maxRetries the maximum number of retries for a backoff attempt when flushing bulk
         *     requests
         */
        public void setBulkFlushBackoffRetries(int maxRetries) {
            Preconditions.checkArgument(
                    maxRetries > 0, "Max number of backoff attempts must be larger than 0.");

            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_RETRIES, String.valueOf(maxRetries));
        }

        /**
         * Sets the amount of delay between each backoff attempt when flushing bulk requests, in
         * milliseconds.
         *
         * @param delayMillis the amount of delay between each backoff attempt when flushing bulk
         *     requests, in milliseconds.
         */
        public void setBulkFlushBackoffDelay(long delayMillis) {
            Preconditions.checkArgument(
                    delayMillis >= 0,
                    "Delay (in milliseconds) between each backoff attempt must be larger than or equal to 0.");
            this.bulkRequestsConfig.put(
                    CONFIG_KEY_BULK_FLUSH_BACKOFF_DELAY, String.valueOf(delayMillis));
        }

        /**
         * Sets a failure handler for action requests.
         *
         * @param failureHandler This is used to handle failed {@link ActionRequest}.
         */
        public void setFailureHandler(ActionRequestFailureHandler failureHandler) {
            this.failureHandler = Preconditions.checkNotNull(failureHandler);
        }

        /**
         * Sets a REST client factory for custom client configuration.
         *
         * @param restClientFactory the factory that configures the rest client.
         */
        public void setRestClientFactory(RestClientFactory restClientFactory) {
            this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
        }

        /**
         * Creates the Opensearch sink.
         *
         * @return the created Opensearch sink.
         */
        public OpensearchSink<T> build() {
            return new OpensearchSink<>(
                    bulkRequestsConfig,
                    httpHosts,
                    opensearchSinkFunction,
                    failureHandler,
                    restClientFactory);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Builder<?> builder = (Builder<?>) o;
            return Objects.equals(httpHosts, builder.httpHosts)
                    && Objects.equals(opensearchSinkFunction, builder.opensearchSinkFunction)
                    && Objects.equals(bulkRequestsConfig, builder.bulkRequestsConfig)
                    && Objects.equals(failureHandler, builder.failureHandler)
                    && Objects.equals(restClientFactory, builder.restClientFactory);
        }

        @Override
        public int hashCode() {
            return Objects.hash(
                    httpHosts,
                    opensearchSinkFunction,
                    bulkRequestsConfig,
                    failureHandler,
                    restClientFactory);
        }
    }
}
