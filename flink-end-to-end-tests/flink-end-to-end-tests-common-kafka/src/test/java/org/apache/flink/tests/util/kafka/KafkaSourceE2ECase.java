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

import org.apache.flink.connector.kafka.source.testutils.KafkaMultipleTopicExternalContext;
import org.apache.flink.connector.kafka.source.testutils.KafkaSingleTopicExternalContext;
import org.apache.flink.connectors.test.common.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalContextFactory;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.TestEnv;
import org.apache.flink.connectors.test.common.testsuites.SourceTestSuiteBase;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/** Kafka E2E test based on connector testing framework. */
public class KafkaSourceE2ECase extends SourceTestSuiteBase<String> {
    private static final String KAFKA_HOSTNAME = "kafka";
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:5.5.2";

    // Defines TestEnvironment
    @TestEnv
    FlinkContainerTestEnvironment flink =
            new FlinkContainerTestEnvironment(
                    1,
                    6,
                    TestUtils.getResource("kafka-connector.jar").toAbsolutePath().toString(),
                    TestUtils.getResource("kafka-clients.jar").toAbsolutePath().toString());

    // Defines ConnectorExternalSystem
    @ExternalSystem
    DefaultContainerizedExternalSystem<KafkaContainer> kafka =
            DefaultContainerizedExternalSystem.builder()
                    .fromContainer(
                            new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                                    .withNetworkAliases(KAFKA_HOSTNAME))
                    .bindWithFlinkContainer(flink.getFlinkContainer())
                    .build();

    // Defines 2 External context Factories, so test cases will be invoked twice using these two
    // kinds of external contexts.
    @SuppressWarnings("unused")
    @ExternalContextFactory
    KafkaSingleTopicExternalContext.Factory singleTopic =
            new KafkaSingleTopicExternalContext.Factory(kafka.getContainer());

    @SuppressWarnings("unused")
    @ExternalContextFactory
    KafkaMultipleTopicExternalContext.Factory multipleTopic =
            new KafkaMultipleTopicExternalContext.Factory(kafka.getContainer());
}
