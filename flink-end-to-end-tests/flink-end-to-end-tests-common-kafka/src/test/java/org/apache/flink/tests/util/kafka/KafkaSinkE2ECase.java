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

import org.apache.flink.connector.kafka.sink.testutils.KafkaSinkExternalContextFactory;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SinkTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment;
import org.apache.flink.util.DockerImageVersions;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;

/** Kafka sink E2E test based on connector testing framework. */
@SuppressWarnings("unused")
public class KafkaSinkE2ECase extends SinkTestSuiteBase<String> {
    private static final String KAFKA_HOSTNAME = "kafka";

    @TestSemantics
    CheckpointingMode[] semantics =
            new CheckpointingMode[] {
                CheckpointingMode.EXACTLY_ONCE, CheckpointingMode.AT_LEAST_ONCE
            };

    // Defines TestEnvironment
    @TestEnv FlinkContainerTestEnvironment flink = new FlinkContainerTestEnvironment(1, 6);

    // Defines ConnectorExternalSystem
    @TestExternalSystem
    DefaultContainerizedExternalSystem<KafkaContainer> kafka =
            DefaultContainerizedExternalSystem.builder()
                    .fromContainer(
                            new KafkaContainer(DockerImageName.parse(DockerImageVersions.KAFKA))
                                    .withNetworkAliases(KAFKA_HOSTNAME))
                    .bindWithFlinkContainer(flink.getFlinkContainers().getJobManager())
                    .build();

    // Defines 2 External context Factories, so test cases will be invoked twice using these two
    // kinds of external contexts.
    @TestContext
    KafkaSinkExternalContextFactory contextFactory =
            new KafkaSinkExternalContextFactory(
                    kafka.getContainer(),
                    Arrays.asList(
                            TestUtils.getResource("kafka-connector.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL(),
                            TestUtils.getResource("kafka-clients.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL(),
                            TestUtils.getResource("flink-connector-testing.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL()));

    public KafkaSinkE2ECase() throws Exception {}
}
