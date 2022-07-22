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

package org.apache.flink.connector.elasticsearch.sink;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.action.DocWriteRequest;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.update.UpdateRequest;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.rest.RestStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;

import static org.apache.flink.util.ExceptionUtils.firstOrSuppressed;
import static org.apache.flink.util.Preconditions.checkNotNull;

class ElasticsearchWriter<IN> implements SinkWriter<IN> {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriter.class);

    private final ElasticsearchEmitter<? super IN> emitter;
    private final MailboxExecutor mailboxExecutor;
    private final boolean flushOnCheckpoint;
    private final BulkProcessor bulkProcessor;
    private final RestHighLevelClient client;
    private final RequestIndexer requestIndexer;
    private final Counter numBytesOutCounter;

    private long pendingActions = 0;
    private boolean checkpointInProgress = false;
    private volatile long lastSendTime = 0;
    private volatile long ackTime = Long.MAX_VALUE;
    private volatile boolean closed = false;

    /**
     * Constructor creating an elasticsearch writer.
     *
     * @param hosts the reachable elasticsearch cluster nodes
     * @param emitter converting incoming records to elasticsearch actions
     * @param flushOnCheckpoint if true all until now received records are flushed after every
     *     checkpoint
     * @param bulkProcessorConfig describing the flushing and failure handling of the used {@link
     *     BulkProcessor}
     * @param bulkProcessorBuilderFactory configuring the {@link BulkProcessor}'s builder
     * @param networkClientConfig describing properties of the network connection used to connect to
     *     the elasticsearch cluster
     * @param metricGroup for the sink writer
     * @param mailboxExecutor Flink's mailbox executor
     */
    ElasticsearchWriter(
            List<HttpHost> hosts,
            ElasticsearchEmitter<? super IN> emitter,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            BulkProcessorBuilderFactory bulkProcessorBuilderFactory,
            NetworkClientConfig networkClientConfig,
            SinkWriterMetricGroup metricGroup,
            MailboxExecutor mailboxExecutor) {
        this.emitter = checkNotNull(emitter);
        this.flushOnCheckpoint = flushOnCheckpoint;
        this.mailboxExecutor = checkNotNull(mailboxExecutor);
        this.client =
                new RestHighLevelClient(
                        configureRestClientBuilder(
                                RestClient.builder(hosts.toArray(new HttpHost[0])),
                                networkClientConfig));
        this.bulkProcessor = createBulkProcessor(bulkProcessorBuilderFactory, bulkProcessorConfig);
        this.requestIndexer = new DefaultRequestIndexer(metricGroup.getNumRecordsSendCounter());
        checkNotNull(metricGroup);
        metricGroup.setCurrentSendTimeGauge(() -> ackTime - lastSendTime);
        this.numBytesOutCounter = metricGroup.getIOMetricGroup().getNumBytesOutCounter();
        try {
            emitter.open();
        } catch (Exception e) {
            throw new FlinkRuntimeException("Failed to open the ElasticsearchEmitter", e);
        }
    }

    @Override
    public void write(IN element, Context context) throws IOException, InterruptedException {
        // do not allow new bulk writes until all actions are flushed
        while (checkpointInProgress) {
            mailboxExecutor.yield();
        }
        emitter.emit(element, context, requestIndexer);
    }

    @Override
    public void flush(boolean endOfInput) throws IOException, InterruptedException {
        checkpointInProgress = true;
        while (pendingActions != 0 && (flushOnCheckpoint || endOfInput)) {
            bulkProcessor.flush();
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
        checkpointInProgress = false;
    }

    @VisibleForTesting
    void blockingFlushAllActions() throws InterruptedException {
        while (pendingActions != 0) {
            bulkProcessor.flush();
            LOG.info("Waiting for the response of {} pending actions.", pendingActions);
            mailboxExecutor.yield();
        }
    }

    @Override
    public void close() throws Exception {
        closed = true;
        emitter.close();
        bulkProcessor.close();
        client.close();
    }

    private static RestClientBuilder configureRestClientBuilder(
            RestClientBuilder builder, NetworkClientConfig networkClientConfig) {
        if (networkClientConfig.getConnectionPathPrefix() != null) {
            builder.setPathPrefix(networkClientConfig.getConnectionPathPrefix());
        }
        if (networkClientConfig.getPassword() != null
                && networkClientConfig.getUsername() != null) {
            final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(
                    AuthScope.ANY,
                    new UsernamePasswordCredentials(
                            networkClientConfig.getUsername(), networkClientConfig.getPassword()));
            builder.setHttpClientConfigCallback(
                    httpClientBuilder ->
                            httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider));
        }
        if (networkClientConfig.getConnectionRequestTimeout() != null
                || networkClientConfig.getConnectionTimeout() != null
                || networkClientConfig.getSocketTimeout() != null) {
            builder.setRequestConfigCallback(
                    requestConfigBuilder -> {
                        if (networkClientConfig.getConnectionRequestTimeout() != null) {
                            requestConfigBuilder.setConnectionRequestTimeout(
                                    networkClientConfig.getConnectionRequestTimeout());
                        }
                        if (networkClientConfig.getConnectionTimeout() != null) {
                            requestConfigBuilder.setConnectTimeout(
                                    networkClientConfig.getConnectionTimeout());
                        }
                        if (networkClientConfig.getSocketTimeout() != null) {
                            requestConfigBuilder.setSocketTimeout(
                                    networkClientConfig.getSocketTimeout());
                        }
                        return requestConfigBuilder;
                    });
        }
        return builder;
    }

    private BulkProcessor createBulkProcessor(
            BulkProcessorBuilderFactory bulkProcessorBuilderFactory,
            BulkProcessorConfig bulkProcessorConfig) {

        BulkProcessor.Builder builder =
                bulkProcessorBuilderFactory.apply(client, bulkProcessorConfig, new BulkListener());

        // This makes flush() blocking
        builder.setConcurrentRequests(0);

        return builder.build();
    }

    private class BulkListener implements BulkProcessor.Listener {

        @Override
        public void beforeBulk(long executionId, BulkRequest request) {
            LOG.info("Sending bulk of {} actions to Elasticsearch.", request.numberOfActions());
            lastSendTime = System.currentTimeMillis();
            numBytesOutCounter.inc(request.estimatedSizeInBytes());
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, BulkResponse response) {
            ackTime = System.currentTimeMillis();
            enqueueActionInMailbox(
                    () -> extractFailures(request, response), "elasticsearchSuccessCallback");
        }

        @Override
        public void afterBulk(long executionId, BulkRequest request, Throwable failure) {
            enqueueActionInMailbox(
                    () -> {
                        throw new FlinkRuntimeException("Complete bulk has failed.", failure);
                    },
                    "elasticsearchErrorCallback");
        }
    }

    private void enqueueActionInMailbox(
            ThrowingRunnable<? extends Exception> action, String actionName) {
        // If the writer is cancelled before the last bulk response (i.e. no flush on checkpoint
        // configured or shutdown without a final
        // checkpoint) the mailbox might already be shutdown, so we should not enqueue any
        // actions.
        if (isClosed()) {
            return;
        }
        mailboxExecutor.execute(action, actionName);
    }

    private void extractFailures(BulkRequest request, BulkResponse response) {
        if (!response.hasFailures()) {
            pendingActions -= request.numberOfActions();
            return;
        }

        Throwable chainedFailures = null;
        for (int i = 0; i < response.getItems().length; i++) {
            final BulkItemResponse itemResponse = response.getItems()[i];
            if (!itemResponse.isFailed()) {
                continue;
            }
            final Throwable failure = itemResponse.getFailure().getCause();
            if (failure == null) {
                continue;
            }
            final RestStatus restStatus = itemResponse.getFailure().getStatus();
            final DocWriteRequest<?> actionRequest = request.requests().get(i);

            chainedFailures =
                    firstOrSuppressed(
                            wrapException(restStatus, failure, actionRequest), chainedFailures);
        }
        if (chainedFailures == null) {
            return;
        }
        throw new FlinkRuntimeException(chainedFailures);
    }

    private static Throwable wrapException(
            RestStatus restStatus, Throwable rootFailure, DocWriteRequest<?> actionRequest) {
        if (restStatus == null) {
            return new FlinkRuntimeException(
                    String.format("Single action %s of bulk request failed.", actionRequest),
                    rootFailure);
        } else {
            return new FlinkRuntimeException(
                    String.format(
                            "Single action %s of bulk request failed with status %s.",
                            actionRequest, restStatus.getStatus()),
                    rootFailure);
        }
    }

    private boolean isClosed() {
        if (closed) {
            LOG.warn("Writer was closed before all records were acknowledged by Elasticsearch.");
        }
        return closed;
    }

    private class DefaultRequestIndexer implements RequestIndexer {

        private final Counter numRecordsSendCounter;

        public DefaultRequestIndexer(Counter numRecordsSendCounter) {
            this.numRecordsSendCounter = checkNotNull(numRecordsSendCounter);
        }

        @Override
        public void add(DeleteRequest... deleteRequests) {
            for (final DeleteRequest deleteRequest : deleteRequests) {
                numRecordsSendCounter.inc();
                pendingActions++;
                bulkProcessor.add(deleteRequest);
            }
        }

        @Override
        public void add(IndexRequest... indexRequests) {
            for (final IndexRequest indexRequest : indexRequests) {
                numRecordsSendCounter.inc();
                pendingActions++;
                bulkProcessor.add(indexRequest);
            }
        }

        @Override
        public void add(UpdateRequest... updateRequests) {
            for (final UpdateRequest updateRequest : updateRequests) {
                numRecordsSendCounter.inc();
                pendingActions++;
                bulkProcessor.add(updateRequest);
            }
        }
    }
}
