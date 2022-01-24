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

package org.apache.flink.connector.kafka.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.testutils.annotations.Kafka;
import org.apache.flink.connector.kafka.testutils.annotations.KafkaKit;
import org.apache.flink.connector.kafka.testutils.extension.KafkaClientKit;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.OperatorIOMetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.runtime.metrics.groups.UnregisteredMetricGroups;
import org.apache.flink.util.TestLoggerExtension;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.PriorityQueue;
import java.util.Properties;
import java.util.stream.IntStream;

import static org.apache.flink.connector.kafka.testutils.KafkaUtil.drainAllRecordsFromTopic;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for the standalone KafkaWriter. */
@ExtendWith(TestLoggerExtension.class)
@Kafka
class KafkaWriterITCase {

    @KafkaKit static KafkaClientKit client;

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWriterITCase.class);
    private static final String KAFKA_METRIC_WITH_GROUP_NAME = "KafkaProducer.incoming-byte-total";
    private static final SinkWriter.Context SINK_WRITER_CONTEXT = new DummySinkWriterContext();

    private String topic;
    private MetricListener metricListener;
    private TriggerTimeService timeService;

    @BeforeEach
    public void setUp(TestInfo testInfo) {
        metricListener = new MetricListener();
        timeService = new TriggerTimeService();
        topic = testInfo.getDisplayName().replaceAll("\\W", "");
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        try (final KafkaWriter<Integer> ignored =
                createWriterWithConfiguration(getKafkaClientConfiguration(), guarantee)) {
            assertThat(metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME)).isPresent();
        }
    }

    @ParameterizedTest
    @EnumSource(DeliveryGuarantee.class)
    void testNotRegisterMetrics(DeliveryGuarantee guarantee) throws Exception {
        assertKafkaMetricNotPresent(guarantee, "flink.disable-metrics", "true");
        assertKafkaMetricNotPresent(guarantee, "register.producer.metrics", "false");
    }

    @Test
    void testIncreasingRecordBasedCounters() throws Exception {
        final OperatorIOMetricGroup operatorIOMetricGroup =
                UnregisteredMetricGroups.createUnregisteredOperatorMetricGroup().getIOMetricGroup();
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(
                        metricListener.getMetricGroup(), operatorIOMetricGroup);
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.NONE, metricGroup)) {
            final Counter numBytesOut = operatorIOMetricGroup.getNumBytesOutCounter();
            final Counter numRecordsOut = operatorIOMetricGroup.getNumRecordsOutCounter();
            assertThat(numBytesOut.getCount()).isEqualTo(0L);
            writer.write(1, SINK_WRITER_CONTEXT);
            timeService.trigger();
            assertThat(numRecordsOut.getCount()).isEqualTo(1);
            assertThat(numBytesOut.getCount()).isGreaterThan(0L);
        }
    }

    @Test
    void testCurrentSendTimeMetric() throws Exception {
        final InternalSinkWriterMetricGroup metricGroup =
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup());
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(),
                        DeliveryGuarantee.AT_LEAST_ONCE,
                        metricGroup)) {
            final Optional<Gauge<Long>> currentSendTime =
                    metricListener.getGauge("currentSendTime");
            assertThat(currentSendTime.isPresent()).isTrue();
            assertThat(0L).isEqualTo(currentSendTime.get().getValue());
            IntStream.range(0, 100)
                    .forEach(
                            (run) -> {
                                try {
                                    writer.write(1, SINK_WRITER_CONTEXT);
                                    // Manually flush the records to generate a sendTime
                                    if (run % 10 == 0) {
                                        writer.prepareCommit(false);
                                    }
                                } catch (IOException e) {
                                    throw new RuntimeException("Failed writing Kafka record.");
                                }
                            });
            assertThat(currentSendTime.get().getValue()).isGreaterThan(0L);
        }
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @Test
    void testLingeringTransaction() throws Exception {
        final KafkaWriter<Integer> failedWriter =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE);

        // create two lingering transactions
        failedWriter.prepareCommit(false);
        failedWriter.snapshotState(1);
        failedWriter.prepareCommit(false);
        failedWriter.snapshotState(2);

        try (final KafkaWriter<Integer> recoveredWriter =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
            recoveredWriter.write(1, SINK_WRITER_CONTEXT);

            List<KafkaCommittable> committables = recoveredWriter.prepareCommit(false);
            recoveredWriter.snapshotState(1);
            assertThat(committables).hasSize(1);
            assertThat(committables.get(0).getProducer()).isPresent();
            committables.get(0).getProducer().get().getObject().commitTransaction();

            List<ConsumerRecord<byte[], byte[]>> records =
                    drainAllRecordsFromTopic(topic, getKafkaClientConfiguration(), true);
            assertThat(records).hasSize(1);
        }

        failedWriter.close();
    }

    /** Test that producer is not accidentally recreated or pool is used. */
    @ParameterizedTest
    @EnumSource(
            value = DeliveryGuarantee.class,
            names = "EXACTLY_ONCE",
            mode = EnumSource.Mode.EXCLUDE)
    void useSameProducerForNonTransactional(DeliveryGuarantee guarantee) throws Exception {
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(getKafkaClientConfiguration(), guarantee)) {
            assertThat(writer.getProducerPool()).isEmpty();
            FlinkKafkaInternalProducer<byte[], byte[]> firstProducer = writer.getCurrentProducer();
            List<KafkaCommittable> committables = writer.prepareCommit(false);
            writer.snapshotState(0);
            assertThat(committables).isEmpty();
            assertThat(writer.getCurrentProducer())
                    .as("Expected same producer")
                    .isSameAs(firstProducer);
            assertThat(writer.getProducerPool()).hasSize(0);
        }
    }

    /** Test that producers are reused when committed. */
    @Test
    void usePoolForTransactional() throws Exception {
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(
                        getKafkaClientConfiguration(), DeliveryGuarantee.EXACTLY_ONCE)) {
            assertThat(writer.getProducerPool()).isEmpty();

            List<KafkaCommittable> committables0 = writer.prepareCommit(false);
            writer.snapshotState(1);
            assertThat(committables0).hasSize(1);
            assertThat(committables0.get(0).getProducer()).isPresent();
            FlinkKafkaInternalProducer<?, ?> firstProducer =
                    committables0.get(0).getProducer().get().getObject();
            assertThat(firstProducer)
                    .as("Expected different producer")
                    .isNotSameAs(writer.getCurrentProducer());

            // recycle first producer, KafkaCommitter would commit it and then return it
            assertThat(writer.getProducerPool()).isEmpty();
            firstProducer.commitTransaction();
            committables0.get(0).getProducer().get().close();
            assertThat(writer.getProducerPool()).hasSize(1);

            List<KafkaCommittable> committables1 = writer.prepareCommit(false);
            writer.snapshotState(2);
            assertThat(committables1).hasSize(1);
            assertThat(committables1.get(0).getProducer()).isPresent();

            assertThat(firstProducer)
                    .as("Expected recycled producer")
                    .isSameAs(writer.getCurrentProducer());
        }
    }

    /**
     * Tests that open transactions are automatically aborted on close such that successive writes
     * succeed.
     */
    @Test
    void testAbortOnClose() throws Exception {
        Properties properties = getKafkaClientConfiguration();
        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(properties, DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(1, SINK_WRITER_CONTEXT);
            assertThat(drainAllRecordsFromTopic(topic, properties, true)).isEmpty();
        }

        try (final KafkaWriter<Integer> writer =
                createWriterWithConfiguration(properties, DeliveryGuarantee.EXACTLY_ONCE)) {
            writer.write(2, SINK_WRITER_CONTEXT);
            List<KafkaCommittable> committables = writer.prepareCommit(false);
            writer.snapshotState(1L);

            // manually commit here, which would only succeed if the first transaction was aborted
            assertThat(committables).hasSize(1);
            String transactionalId = committables.get(0).getTransactionalId();
            try (FlinkKafkaInternalProducer<byte[], byte[]> producer =
                    new FlinkKafkaInternalProducer<>(properties, transactionalId)) {
                producer.resumeTransaction(
                        committables.get(0).getProducerId(), committables.get(0).getEpoch());
                producer.commitTransaction();
            }

            assertThat(drainAllRecordsFromTopic(topic, properties, true)).hasSize(1);
        }
    }

    private void assertKafkaMetricNotPresent(
            DeliveryGuarantee guarantee, String configKey, String configValue) throws Exception {
        final Properties config = getKafkaClientConfiguration();
        config.put(configKey, configValue);
        try (final KafkaWriter<Integer> ignored =
                createWriterWithConfiguration(config, guarantee)) {
            assertThat(metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME)).isNotPresent();
        }
    }

    private KafkaWriter<Integer> createWriterWithConfiguration(
            Properties config, DeliveryGuarantee guarantee) {
        return createWriterWithConfiguration(
                config,
                guarantee,
                InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup()));
    }

    private KafkaWriter<Integer> createWriterWithConfiguration(
            Properties config,
            DeliveryGuarantee guarantee,
            SinkWriterMetricGroup sinkWriterMetricGroup) {
        return new KafkaWriter<>(
                guarantee,
                config,
                "test-prefix",
                new SinkInitContext(sinkWriterMetricGroup, timeService),
                new DummyRecordSerializer(),
                new DummySchemaContext(),
                ImmutableList.of());
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", client.getBootstrapServers());
        standardProps.put("group.id", "kafkaWriter-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        return standardProps;
    }

    private static class SinkInitContext implements Sink.InitContext {

        private final SinkWriterMetricGroup metricGroup;
        private final Sink.ProcessingTimeService timeService;

        SinkInitContext(SinkWriterMetricGroup metricGroup, Sink.ProcessingTimeService timeService) {
            this.metricGroup = metricGroup;
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
        public Sink.ProcessingTimeService getProcessingTimeService() {
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
    }

    private class DummyRecordSerializer implements KafkaRecordSerializationSchema<Integer> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Integer element, KafkaSinkContext context, Long timestamp) {
            return new ProducerRecord<>(topic, ByteBuffer.allocate(4).putInt(element).array());
        }
    }

    private static class DummySchemaContext implements SerializationSchema.InitializationContext {

        @Override
        public MetricGroup getMetricGroup() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }
    }

    private static class DummySinkWriterContext implements SinkWriter.Context {
        @Override
        public long currentWatermark() {
            return 0;
        }

        @Override
        public Long timestamp() {
            return null;
        }
    }

    private static class TriggerTimeService implements Sink.ProcessingTimeService {

        private final PriorityQueue<Tuple2<Long, ProcessingTimeCallback>> registeredCallbacks =
                new PriorityQueue<>(Comparator.comparingLong(o -> o.f0));

        @Override
        public long getCurrentProcessingTime() {
            return 0;
        }

        @Override
        public void registerProcessingTimer(
                long time, ProcessingTimeCallback processingTimerCallback) {
            registeredCallbacks.add(new Tuple2<>(time, processingTimerCallback));
        }

        public void trigger() throws IOException, InterruptedException {
            final Tuple2<Long, ProcessingTimeCallback> registered = registeredCallbacks.poll();
            if (registered == null) {
                LOG.warn("Triggered time service but no callback was registered.");
                return;
            }
            registered.f1.onProcessingTime(registered.f0);
        }
    }
}
