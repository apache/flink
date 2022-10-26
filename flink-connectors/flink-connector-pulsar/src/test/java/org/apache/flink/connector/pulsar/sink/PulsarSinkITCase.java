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

package org.apache.flink.connector.pulsar.sink;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.pulsar.testutils.PulsarTestContextFactory;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.PulsarTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.function.ControlSource;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.sink.PulsarSinkTestContext;
import org.apache.flink.connector.pulsar.testutils.sink.PulsarSinkTestSuiteBase;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.runtime.minicluster.RpcServiceSharing;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.flink.connector.pulsar.sink.writer.serializer.PulsarSerializationSchema.flinkSchema;
import static org.assertj.core.api.Assertions.assertThat;

/** Tests for using PulsarSink writing to a Pulsar cluster. */
@Tag("org.apache.flink.testutils.junit.FailsOnJava11")
class PulsarSinkITCase {

    /** Integration test based on connector testing framework. */
    @Nested
    class IntegrationTest extends PulsarSinkTestSuiteBase {

        @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

        @TestExternalSystem
        PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.mock());

        @TestSemantics
        CheckpointingMode[] semantics =
                new CheckpointingMode[] {
                    CheckpointingMode.EXACTLY_ONCE, CheckpointingMode.AT_LEAST_ONCE
                };

        @TestContext
        PulsarTestContextFactory<String, PulsarSinkTestContext> sinkContext =
                new PulsarTestContextFactory<>(pulsar, PulsarSinkTestContext::new);
    }

    /** Tests for using PulsarSink writing to a Pulsar cluster. */
    @Nested
    class DeliveryGuaranteeTest extends PulsarTestSuiteBase {

        private static final int PARALLELISM = 1;

        @RegisterExtension
        private final MiniClusterExtension clusterExtension =
                new MiniClusterExtension(
                        new MiniClusterResourceConfiguration.Builder()
                                .setNumberTaskManagers(1)
                                .setNumberSlotsPerTaskManager(PARALLELISM)
                                .setRpcServiceSharing(RpcServiceSharing.DEDICATED)
                                .withHaLeadershipControl()
                                .build());

        // Using this extension for creating shared reference which would be used in source
        // function.
        @RegisterExtension
        final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

        @ParameterizedTest
        @EnumSource(DeliveryGuarantee.class)
        void writeRecordsToPulsar(DeliveryGuarantee guarantee) throws Exception {
            // A random topic with partition 4.
            String topic = randomAlphabetic(8);
            operator().createTopic(topic, 4);
            int counts = ThreadLocalRandom.current().nextInt(100, 200);

            ControlSource source =
                    new ControlSource(
                            sharedObjects,
                            operator(),
                            topic,
                            guarantee,
                            counts,
                            Duration.ofMillis(50),
                            Duration.ofMinutes(5));
            PulsarSink<String> sink =
                    PulsarSink.builder()
                            .setServiceUrl(operator().serviceUrl())
                            .setAdminUrl(operator().adminUrl())
                            .setDeliveryGuarantee(guarantee)
                            .setTopics(topic)
                            .setSerializationSchema(flinkSchema(new SimpleStringSchema()))
                            .build();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

            env.setParallelism(PARALLELISM);
            if (guarantee != DeliveryGuarantee.NONE) {
                env.enableCheckpointing(500L);
            }
            env.addSource(source).sinkTo(sink);
            env.execute();

            List<String> expectedRecords = source.getExpectedRecords();
            List<String> consumedRecords = source.getConsumedRecords();

            assertThat(consumedRecords)
                    .hasSameSizeAs(expectedRecords)
                    .containsExactlyInAnyOrderElementsOf(expectedRecords);
        }
    }
}
