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
import org.apache.flink.connector.pulsar.sink.writer.router.RoundRobinTopicRouter;
import org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema;
import org.apache.flink.connector.pulsar.sink.writer.topic.TopicMetadataListener;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
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
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.util.Collection;
import java.util.OptionalLong;

import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.base.DeliveryGuarantee.EXACTLY_ONCE;
import static org.apache.flink.connector.pulsar.sink.PulsarSinkOptions.PULSAR_WRITE_SCHEMA_EVOLUTION;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.pulsarSchema;
import static org.apache.pulsar.client.api.Schema.STRING;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

/** Unit tests for {@link PulsarWriter}. */
class PulsarWriterTest extends PulsarTestSuiteBase {

    private static final SinkWriter.Context CONTEXT = new MockSinkWriterContext();

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

        return new SinkConfiguration(configuration);
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
}
