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

package org.apache.flink.connector.pulsar.sink.writer;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.operators.ProcessingTimeService;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink2.Sink.InitContext;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.sink.committer.PulsarCommittable;
import org.apache.flink.connector.pulsar.sink.config.SinkConfiguration;
import org.apache.flink.connector.pulsar.sink.writer.delayer.FixedMessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.delayer.MessageDelayer;
import org.apache.flink.connector.pulsar.sink.writer.metrics.PulsarSinkWriterMetric;
import org.apache.flink.connector.pulsar.sink.writer.router.RoundRobinTopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicMetadataListener;
import org.apache.flink.connector.pulsar.source.enumerator.topic.TopicNameUtils;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.streaming.runtime.tasks.TestProcessingTimeService;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.pulsar.client.api.transaction.TransactionCoordinatorClient;
import org.assertj.core.data.Offset;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collection;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.base.DeliveryGuarantee.AT_LEAST_ONCE;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.connector.pulsar.common.config.PulsarOptions.PULSAR_STATS_INTERVAL_SECONDS;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_PRODUCER_NAME;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_SCHEMA_EVOLUTION;
import static org.apache.flink.connector.pulsar.sink.writer.metrics.PulsarProducerMetricsRegister.PRODUCER_UPDATE_POLLING_INTERVAL_MILLIS;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.pulsarSchema;
import static org.apache.flink.shaded.guava30.com.google.common.util.concurrent.Uninterruptibles.sleepUninterruptibly;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarWriter}. */
class PulsarWriterTest extends PulsarTestSuiteBase {
    private static final SinkWriter.Context CONTEXT = new MockSinkWriterContext();
    private static final String DEFAULT_TEST_PRODUCER_NAME = "test-producer";
    private static final String EXPECTED_ALL_PRODUCER_METRIC_NAME = "PulsarSink.numAcksReceived";
    private static final String EXPECTED_PER_PRODUCER_METRIC_PREFIX = "PulsarSink.producer";
    private static final String EXPECTED_PER_PRODUCER_METRIC_NAME = "sendLatency99Pct";
    private static final String GLOBAL_MAX_LATENCY_METRIC_NAME = "PulsarSink.sendLatencyMax";
    private static final String CURRENT_SEND_TIME_METRIC_NAME = "currentSendTime";
    private static final String NUM_ACKS_RECEIVED_METRIC_NAME = "PulsarSink.numAcksReceived";
    private static final long TEST_PULSAR_STATS_INTERVAL_SECONDS = 1L;

    private MetricListener metricListener;
    private TestProcessingTimeService timeService;
    private MockInitContext initContext;

    @BeforeEach
    void setup() throws Exception {
        metricListener = new MetricListener();
        timeService = new TestProcessingTimeService();
        timeService.setCurrentTime(0L);
        initContext = new MockInitContext(metricListener, timeService);
    }

    @Test
    void metricsPresentAfterWriterCreated() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        try (final PulsarWriter<String> ignored = createWriter(topic, initContext)) {
            assertThat(metricListener.getCounter(EXPECTED_ALL_PRODUCER_METRIC_NAME)).isPresent();
        }
    }

    @Test
    void perProducerMetricsPresentAfterMessageWritten() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        try (final PulsarWriter<String> writer = createWriter(topic, initContext)) {
            String message = randomAlphabetic(10);
            sendMessages(writer, 1);
            // advance timer to update the producers
            timeService.advance(PRODUCER_UPDATE_POLLING_INTERVAL_MILLIS + 1000);
            assertThat(metricListener.getGauge(perProducerMetricName(topic, 0))).isPresent();

            // send another message and advance timer
            sendMessages(writer, 1000);
            timeService.advance(PRODUCER_UPDATE_POLLING_INTERVAL_MILLIS + 1000);

            assertThat(metricListener.getGauge(perProducerMetricName(topic, 1))).isPresent();
        }
    }

    @Test
    void maxSendLatencyGauge() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        try (final PulsarWriter<String> writer = createWriter(topic, initContext)) {
            String message = randomAlphabetic(10);
            // No producer is present, max latency should be INVALID_LATENCY
            assertThat(metricListener.getGauge(GLOBAL_MAX_LATENCY_METRIC_NAME)).isPresent();
            Double latencyBeforeProducerCreation =
                    metricListener
                            .<Double>getGauge(GLOBAL_MAX_LATENCY_METRIC_NAME)
                            .get()
                            .getValue();
            assertThat(latencyBeforeProducerCreation)
                    .isCloseTo(PulsarSinkWriterMetric.INVALID_LATENCY, Offset.offset(0.01));
            // Write a message
            sendMessages(writer, 1);
            // Advance Timer to trigger the metrics update
            timeService.advance(
                    TimeUnit.SECONDS.toMillis(TEST_PULSAR_STATS_INTERVAL_SECONDS) + 100);

            Double latencyAfterProducerCreation =
                    metricListener
                            .<Double>getGauge(GLOBAL_MAX_LATENCY_METRIC_NAME)
                            .get()
                            .getValue();
            assertThat(latencyAfterProducerCreation).isGreaterThanOrEqualTo(0.0);
        }
    }

    @Test
    void currentSendTimeGauge() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        try (final PulsarWriter<String> writer = createWriter(topic, initContext)) {
            String message = randomAlphabetic(10);
            // No producer is present, currentSendTime should be INVALID_LAST_SEND_TIME
            assertThat(metricListener.getGauge(CURRENT_SEND_TIME_METRIC_NAME)).isPresent();
            Long sendTimeBeforeProducerCreation =
                    metricListener.<Long>getGauge(CURRENT_SEND_TIME_METRIC_NAME).get().getValue();
            assertThat(sendTimeBeforeProducerCreation)
                    .isEqualTo(PulsarSinkWriterMetric.INVALID_LAST_SEND_TIME);

            // Write a message
            sendMessages(writer, 1);

            // After send success, send time should be updated
            Long sendTimeAfterProducerCreation =
                    metricListener.<Long>getGauge(CURRENT_SEND_TIME_METRIC_NAME).get().getValue();
            assertThat(sendTimeAfterProducerCreation).isEqualTo(0);
        }
    }

    @Test
    void numAcksReceived() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        try (final PulsarWriter<String> writer = createWriter(topic, initContext)) {
            String message = randomAlphabetic(10);

            // Write and flush a  message
            sendMessages(writer, 1);
            // Advance Timer to reflect the time change after flush
            sleepUninterruptibly(TEST_PULSAR_STATS_INTERVAL_SECONDS + 1, TimeUnit.SECONDS);
            timeService.advance(
                    TimeUnit.SECONDS.toMillis(TEST_PULSAR_STATS_INTERVAL_SECONDS) + 100);

            Long numAcks =
                    metricListener.<Long>getCounter(NUM_ACKS_RECEIVED_METRIC_NAME).get().getCount();
            assertThat(numAcks).isEqualTo(1);
        }
    }

    @Test
    void standardMetricsFromIOMetricsGroup() throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        try (final PulsarWriter<String> writer = createWriter(topic, initContext)) {
            String message = randomAlphabetic(10);
            Counter recordsOutCounter =
                    initContext.metricGroup().getIOMetricGroup().getNumRecordsOutCounter();
            Counter bytesOutCounter =
                    initContext.metricGroup().getIOMetricGroup().getNumBytesOutCounter();
            // Write and flush a  message
            sendMessages(writer, 1);
            // Advance Timer to update counter metrics
            sleepUninterruptibly(TEST_PULSAR_STATS_INTERVAL_SECONDS + 1, TimeUnit.SECONDS);
            timeService.advance(
                    TimeUnit.SECONDS.toMillis(TEST_PULSAR_STATS_INTERVAL_SECONDS) + 100);

            // Validate the counter is updated
            assertThat(recordsOutCounter.getCount()).isEqualTo(1);
            assertThat(bytesOutCounter.getCount()).isGreaterThanOrEqualTo(10);

            // Write and flush another message
            sendMessages(writer, 1);
            // Advance Timer and system time to update counter metrics
            sleepUninterruptibly(TEST_PULSAR_STATS_INTERVAL_SECONDS + 1, TimeUnit.SECONDS);
            timeService.advance(
                    TimeUnit.SECONDS.toMillis(TEST_PULSAR_STATS_INTERVAL_SECONDS) + 100);
            assertThat(recordsOutCounter.getCount()).isEqualTo(2);
            assertThat(bytesOutCounter.getCount()).isGreaterThanOrEqualTo(20);
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void writeMessages(DeliveryGuarantee guarantee) throws Exception {
        String topic = randomAlphabetic(10);
        operator().createTopic(topic, 8);

        SinkConfiguration configuration = sinkConfiguration(guarantee);
        PulsarSerializationSchema<String> schema = pulsarSchema(STRING);
        TopicMetadataListener listener = new TopicMetadataListener(singletonList(topic));
        RoundRobinTopicRouter<String> router = new RoundRobinTopicRouter<>(configuration);
        FixedMessageDelayer<String> delayer = MessageDelayer.never();
        MockInitContext initContext = new MockInitContext();

        PulsarWriter<String> writer =
                new PulsarWriter<>(configuration, schema, listener, router, delayer, initContext);

        writer.flush(false);
        writer.prepareCommit();
        writer.flush(false);
        writer.prepareCommit();

        String message = randomAlphabetic(10);
        writer.write(message, CONTEXT);
        writer.flush(false);

        Collection<PulsarCommittable> committables = writer.prepareCommit();
        if (guarantee != EXACTLY_ONCE) {
            assertThat(committables).isEmpty();
        } else {
            assertThat(committables).hasSize(1);
            PulsarCommittable committable =
                    committables.stream().findFirst().orElseThrow(IllegalArgumentException::new);
            TransactionCoordinatorClient coordinatorClient = operator().coordinatorClient();
            coordinatorClient.commit(committable.getTxnID());
        }

        String consumedMessage = operator().receiveMessage(topic, STRING).getValue();
        assertEquals(consumedMessage, message);
    }

    private SinkConfiguration sinkConfiguration(DeliveryGuarantee deliveryGuarantee) {
        Configuration configuration = operator().sinkConfig(deliveryGuarantee);
        configuration.set(PULSAR_WRITE_SCHEMA_EVOLUTION, true);
        configuration.set(PULSAR_STATS_INTERVAL_SECONDS, TEST_PULSAR_STATS_INTERVAL_SECONDS);
        configuration.set(PULSAR_PRODUCER_NAME, DEFAULT_TEST_PRODUCER_NAME);

        return new SinkConfiguration(configuration);
    }

    private PulsarWriter<String> createWriter(String topic, MockInitContext initContext) {
        SinkConfiguration configuration = sinkConfiguration(AT_LEAST_ONCE);
        PulsarSerializationSchema<String> schema = pulsarSchema(STRING);
        TopicMetadataListener listener = new TopicMetadataListener(singletonList(topic));
        RoundRobinTopicRouter<String> router = new RoundRobinTopicRouter<>(configuration);
        FixedMessageDelayer<String> delayer = MessageDelayer.never();

        return new PulsarWriter<>(configuration, schema, listener, router, delayer, initContext);
    }

    private static class MockInitContext implements InitContext {

        private final MetricListener metricListener;
        private final OperatorIOMetricGroup ioMetricGroup;
        private final SinkWriterMetricGroup metricGroup;
        private final ProcessingTimeService timeService;

        private MockInitContext() {
            this.metricListener = new MetricListener();
            this.ioMetricGroup =
                    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()
                            .getIOMetricGroup();
            MetricGroup metricGroup = metricListener.getMetricGroup();
            this.metricGroup = InternalSinkWriterMetricGroup.mock(metricGroup, ioMetricGroup);
            this.timeService = new TestProcessingTimeService();
        }

        private MockInitContext(MetricListener metricListener, ProcessingTimeService timeService) {
            this.metricListener = metricListener;
            this.ioMetricGroup =
                    UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup()
                            .getIOMetricGroup();
            MetricGroup metricGroup = metricListener.getMetricGroup();
            this.metricGroup = InternalSinkWriterMetricGroup.mock(metricGroup, ioMetricGroup);
            this.timeService = timeService;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            return new SyncMailboxExecutor();
        }

        @Override
        public ProcessingTimeService getProcessingTimeService() {
            return timeService;
        }

        @Override
        public int getSubtaskId() {
            return 0;
        }

        @Override
        public int getNumberOfParallelSubtasks() {
            return 1;
        }

        @Override
        public SinkWriterMetricGroup metricGroup() {
            return metricGroup;
        }

        @Override
        public OptionalLong getRestoredCheckpointId() {
            return OptionalLong.empty();
        }

        @Override
        public SerializationSchema.InitializationContext
                asSerializationSchemaInitializationContext() {
            return new SerializationSchema.InitializationContext() {
                @Override
                public MetricGroup getMetricGroup() {
                    return metricGroup;
                }

                @Override
                public UserCodeClassLoader getUserCodeClassLoader() {
                    return null;
                }
            };
        }
    }

    private static class MockSinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    private String perProducerMetricName(String topic, int partitionId) {
        return EXPECTED_PER_PRODUCER_METRIC_PREFIX
                + "."
                + DEFAULT_TEST_PRODUCER_NAME
                + TopicNameUtils.topicNameWithPartition(topic, partitionId)
                + "."
                + EXPECTED_PER_PRODUCER_METRIC_NAME;
    }

    private void sendMessages(PulsarWriter<String> writer, int numMessages) throws Exception {
        for (int i = 0; i < numMessages; i++) {
            String message = randomAlphabetic(10);
            writer.write(message, CONTEXT);
        }
        writer.flush(false);
    }
}
