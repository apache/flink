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

package org.apache.flink.streaming.connectors.kafka.sink;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.groups.SinkWriterMetricGroup;
import org.apache.flink.metrics.testutils.MetricListener;
import org.apache.flink.runtime.metrics.groups.InternalSinkWriterMetricGroup;
import org.apache.flink.util.UserCodeClassLoader;

import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableList;
import org.apache.flink.shaded.guava30.com.google.common.collect.ImmutableMap;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

import static org.junit.Assert.assertThrows;

/** Tests for the standalone KafkaWriter. */
@RunWith(Parameterized.class)
public class KafkaWriterITCase {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaWriterITCase.class);
    private static final Slf4jLogConsumer LOG_CONSUMER = new Slf4jLogConsumer(LOG);
    private static final String INTER_CONTAINER_KAFKA_ALIAS = "kafka";
    private static final Network NETWORK = Network.newNetwork();
    private static final String KAFKA_METRIC_WITH_GROUP_NAME = "KafkaProducer.incoming-byte-total";

    private final DeliveryGuarantee guarantee;

    private MetricListener metricListener;

    @ClassRule
    public static final KafkaContainer KAFKA_CONTAINER =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:5.5.2"))
                    .withEmbeddedZookeeper()
                    .withEnv(
                            ImmutableMap.of(
                                    "KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR",
                                    "1",
                                    "KAFKA_TRANSACTION_MAX_TIMEOUT_MS",
                                    String.valueOf(Duration.ofHours(2).toMillis()),
                                    "KAFKA_TRANSACTION_STATE_LOG_MIN_ISR",
                                    "1",
                                    "KAFKA_MIN_INSYNC_REPLICAS",
                                    "1"))
                    .withNetwork(NETWORK)
                    .withLogConsumer(LOG_CONSUMER)
                    .withNetworkAliases(INTER_CONTAINER_KAFKA_ALIAS);

    @Before
    public void setUp() {
        metricListener = new MetricListener();
    }

    @Parameterized.Parameters
    public static List<DeliveryGuarantee> guarantees() {
        return ImmutableList.of(
                DeliveryGuarantee.NONE,
                DeliveryGuarantee.AT_LEAST_ONCE,
                DeliveryGuarantee.EXACTLY_ONCE);
    }

    public KafkaWriterITCase(DeliveryGuarantee guarantee) {
        this.guarantee = guarantee;
    }

    @Test
    public void testRegisterMetrics() throws Exception {
        try (final KafkaWriter<Integer> ignored =
                createWriterWithConfiguration(getKafkaClientConfiguration())) {
            metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME);
        }
    }

    @Test
    public void testNotRegisterMetrics() throws Exception {
        final Properties config = getKafkaClientConfiguration();
        config.put("flink.disable-metrics", "true");
        try (final KafkaWriter<Integer> ignored = createWriterWithConfiguration(config)) {
            assertThrows(
                    IllegalArgumentException.class,
                    () -> metricListener.getGauge(KAFKA_METRIC_WITH_GROUP_NAME));
        }
    }

    private KafkaWriter<Integer> createWriterWithConfiguration(Properties config) {
        return new KafkaWriter<>(
                guarantee,
                config,
                "test-prefix",
                new SinkInitContext(
                        InternalSinkWriterMetricGroup.mock(metricListener.getMetricGroup())),
                new DummyRecordSerializer(),
                new DummySchemaContext(),
                ImmutableList.of());
    }

    private static Properties getKafkaClientConfiguration() {
        final Properties standardProps = new Properties();
        standardProps.put("bootstrap.servers", KAFKA_CONTAINER.getBootstrapServers());
        standardProps.put("group.id", "kafkaWriter-tests");
        standardProps.put("enable.auto.commit", false);
        standardProps.put("key.serializer", ByteArraySerializer.class.getName());
        standardProps.put("value.serializer", ByteArraySerializer.class.getName());
        standardProps.put("auto.offset.reset", "earliest");
        return standardProps;
    }

    private static class SinkInitContext implements Sink.InitContext {

        private final SinkWriterMetricGroup metricGroup;

        SinkInitContext(SinkWriterMetricGroup metricGroup) {
            this.metricGroup = metricGroup;
        }

        @Override
        public UserCodeClassLoader getUserCodeClassLoader() {
            throw new UnsupportedOperationException("Not implemented.");
        }

        @Override
        public MailboxExecutor getMailboxExecutor() {
            throw new UnsupportedOperationException("Not implemented");
        }

        @Override
        public Sink.ProcessingTimeService getProcessingTimeService() {
            throw new UnsupportedOperationException("Not implemented.");
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
    }

    private static class DummyRecordSerializer implements KafkaRecordSerializationSchema<Integer> {
        @Override
        public ProducerRecord<byte[], byte[]> serialize(
                Integer element, KafkaSinkContext context, Long timestamp) {
            throw new UnsupportedOperationException("Not implemented.");
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
}
