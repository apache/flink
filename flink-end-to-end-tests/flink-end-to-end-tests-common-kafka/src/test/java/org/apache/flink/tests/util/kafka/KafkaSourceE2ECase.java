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

package org.apache.flink.tests.util.kafka;

import org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContextFactory;
import org.apache.flink.connector.kafka.testutils.cluster.KafkaContainers;
import org.apache.flink.connector.kafka.testutils.extension.KafkaExtension;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment;

import org.junit.jupiter.api.extension.RegisterExtension;

import java.util.Arrays;
import java.util.stream.Collectors;

import static org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContext.SplitMappingMode.PARTITION;
import static org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContext.SplitMappingMode.TOPIC;

/** Kafka E2E test based on connector testing framework. */
public class KafkaSourceE2ECase extends SourceTestSuiteBase<String> {
    private static final String KAFKA_HOSTNAME_PREFIX = "kafka";
    private static final int KAFKA_INTERNAL_PORT = 9092;

    // Defines TestEnvironment
    @TestEnv
    static final FlinkContainerTestEnvironment FLINK = new FlinkContainerTestEnvironment(1, 6);

    @RegisterExtension
    static final KafkaExtension KAFKA =
            new KafkaExtension(
                    KafkaContainers.builder()
                            .setNetwork(FLINK.getFlinkContainers().getJobManager().getNetwork())
                            .setNetworkAliasPrefix(KAFKA_HOSTNAME_PREFIX)
                            .build());

    // Defines 2 External context Factories, so test cases will be invoked twice using these two
    // kinds of external contexts.
    @SuppressWarnings("unused")
    @TestContext
    KafkaSourceExternalContextFactory singleTopic =
            new KafkaSourceExternalContextFactory(
                    this::getBootstrapServer,
                    Arrays.asList(
                            TestUtils.getResource("kafka-connector.jar").toUri().toURL(),
                            TestUtils.getResource("kafka-clients.jar").toUri().toURL()),
                    PARTITION);

    @SuppressWarnings("unused")
    @TestContext
    KafkaSourceExternalContextFactory multipleTopic =
            new KafkaSourceExternalContextFactory(
                    KAFKA::getBootstrapServers,
                    Arrays.asList(
                            TestUtils.getResource("kafka-connector.jar").toUri().toURL(),
                            TestUtils.getResource("kafka-clients.jar").toUri().toURL()),
                    TOPIC);

    private String getBootstrapServer() {
        // Internal endpoint for connecting from FlinkContainers
        String internalBootstrapServers =
                KAFKA.getKafkaContainers().getKafkaContainer(0).getNetworkAliases().stream()
                        .map(host -> host + ":" + KAFKA_INTERNAL_PORT)
                        .collect(Collectors.joining(","));
        // Endpoint on host for connecting from external context
        String hostBootstrapServers = KAFKA.getBootstrapServers();
        // Combine two endpoints as the final bootstrap server string
        return internalBootstrapServers + "," + hostBootstrapServers;
    }

    public KafkaSourceE2ECase() throws Exception {}
}
