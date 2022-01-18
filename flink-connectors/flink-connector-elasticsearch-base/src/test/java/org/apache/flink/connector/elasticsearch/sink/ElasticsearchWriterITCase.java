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

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.elasticsearch.ElasticsearchUtil;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.DockerImageVersions;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.function.ThrowingRunnable;

import org.apache.http.HttpHost;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.action.bulk.BulkRequest;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.action.get.GetRequest;
import org.elasticsearch.action.get.GetResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.ByteSizeUnit;
import org.elasticsearch.common.unit.ByteSizeValue;
import org.elasticsearch.core.TimeValue;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

import static org.apache.flink.connector.elasticsearch.sink.TestClientBase.buildMessage;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.greaterThan;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Tests for {@link ElasticsearchWriter}. */
@Testcontainers
@ExtendWith(TestLoggerExtension.class)
class ElasticsearchWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(ElasticsearchWriterITCase.class);

    @Container
    private static final ElasticsearchContainer ES_CONTAINER =
            ElasticsearchUtil.createElasticsearchContainer(
                    DockerImageVersions.ELASTICSEARCH_7, LOG);

    private RestHighLevelClient client;
    private TestClientBase context;
    private MetricListener metricListener;

    @BeforeEach
    void setUp() {
        metricListener = new MetricListener();
        client =
                new RestHighLevelClient(
                        RestClient.builder(HttpHost.create(ES_CONTAINER.getHttpHostAddress())));
        context = new TestClient(client);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Test
    void testWriteOnBulkFlush() throws Exception {
        final String index = "test-bulk-flush-without-checkpoint";
        final int flushAfterNActions = 5;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final ElasticsearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);

            // Ignore flush on checkpoint
            writer.prepareCommit(false);

            context.assertThatIdsAreNotWritten(index, 1, 2, 3, 4);

            // Trigger flush
            writer.write(Tuple2.of(5, "test-5"), null);
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5);

            writer.write(Tuple2.of(6, "test-6"), null);
            context.assertThatIdsAreNotWritten(index, 6);

            // Force flush
            writer.blockingFlushAllActions();
            context.assertThatIdsAreWritten(index, 1, 2, 3, 4, 5, 6);
        }
    }

    @Test
    void testWriteOnBulkIntervalFlush() throws Exception {
        final String index = "test-bulk-flush-with-interval";

        // Configure bulk processor to flush every 1s;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(-1, -1, 1000, FlushBackoffType.NONE, 0, 0);

        try (final ElasticsearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);
            writer.write(Tuple2.of(4, buildMessage(4)), null);
            writer.blockingFlushAllActions();
        }

        context.assertThatIdsAreWritten(index, 1, 2, 3, 4);
    }

    @Test
    void testWriteOnCheckpoint() throws Exception {
        final String index = "test-bulk-flush-with-checkpoint";
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(-1, -1, -1, FlushBackoffType.NONE, 0, 0);

        // Enable flush on checkpoint
        try (final ElasticsearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, true, bulkProcessorConfig)) {
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);
            writer.write(Tuple2.of(3, buildMessage(3)), null);

            context.assertThatIdsAreNotWritten(index, 1, 2, 3);

            // Trigger flush
            writer.prepareCommit(false);

            context.assertThatIdsAreWritten(index, 1, 2, 3);
        }
    }

    @Test
    void testIncrementByteOutMetric() throws Exception {
        final String index = "test-inc-byte-out";
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final ElasticsearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig, metricGroup)) {
            final Counter numBytesOut = operatorIOMetricGroup.getNumBytesOutCounter();
            assertEquals(numBytesOut.getCount(), 0);
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();
            long first = numBytesOut.getCount();

            assertTrue(first > 0);

            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();
            assertTrue(numBytesOut.getCount() > first);
        }
    }

    @Test
    void testCurrentSendTime() throws Exception {
        final String index = "test-current-send-time";
        final int flushAfterNActions = 2;
        final BulkProcessorConfig bulkProcessorConfig =
                new BulkProcessorConfig(flushAfterNActions, -1, -1, FlushBackoffType.NONE, 0, 0);

        try (final ElasticsearchWriter<Tuple2<Integer, String>> writer =
                createWriter(index, false, bulkProcessorConfig)) {
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");
            writer.write(Tuple2.of(1, buildMessage(1)), null);
            writer.write(Tuple2.of(2, buildMessage(2)), null);

            writer.blockingFlushAllActions();

            assertTrue(currentSendTime.isPresent());
            assertThat(currentSendTime.get().getValue(), greaterThan(0L));
        }
    }

    private ElasticsearchWriter<Tuple2<Integer, String>> createWriter(
            String index, boolean flushOnCheckpoint, BulkProcessorConfig bulkProcessorConfig) {
        return createWriter(
                index,
                flushOnCheckpoint,
                bulkProcessorConfig,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()));
    }

    private ElasticsearchWriter<Tuple2<Integer, String>> createWriter(
            String index,
            boolean flushOnCheckpoint,
            BulkProcessorConfig bulkProcessorConfig,
            SinkWriterMetricGroup metricGroup) {
        return new ElasticsearchWriter<Tuple2<Integer, String>>(
                Collections.singletonList(HttpHost.create(ES_CONTAINER.getHttpHostAddress())),
                TestEmitter.jsonEmitter(index, context.getDataFieldName()),
                flushOnCheckpoint,
                bulkProcessorConfig,
                new TestBulkProcessorBuilderFactory(),
                new NetworkClientConfig(null, null, null, null, null, null),
                metricGroup,
                new TestMailbox());
    }

    private static class TestBulkProcessorBuilderFactory implements BulkProcessorBuilderFactory {
        @Override
        public BulkProcessor.Builder apply(
                RestHighLevelClient client,
                BulkProcessorConfig bulkProcessorConfig,
                BulkProcessor.Listener listener) {
            BulkProcessor.Builder builder =
                    BulkProcessor.builder(
                            new BulkRequestConsumerFactory() { // This cannot be inlined as a lambda
                                // because then deserialization fails
                                @Override
                                public void accept(
                                        BulkRequest bulkRequest,
                                        ActionListener<BulkResponse> bulkResponseActionListener) {
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
                builder.setFlushInterval(new TimeValue(bulkProcessorConfig.getBulkFlushInterval()));
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
                    backoffPolicy = BackoffPolicy.exponentialBackoff(backoffDelay, maxRetryCount);
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
    }

    private static class TestClient extends TestClientBase {

        TestClient(RestHighLevelClient client) {
            super(client);
        }

        @Override
        GetResponse getResponse(String index, int id) throws IOException {
            return client.get(new GetRequest(index, Integer.toString(id)), RequestOptions.DEFAULT);
        }
    }

    private static class TestMailbox implements MailboxExecutor {

        @Override
        public void execute(
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            try {
                command.run();
            } catch (Exception e) {
                throw new RuntimeException("Unexpected error", e);
            }
        }

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {
            Thread.sleep(100);
        }

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }
    }
}
