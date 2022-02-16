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

import org.apache.flink.connector.kafka.source.testutils.table.KafkaTableSourceExternalContextFactory;
import org.apache.flink.connector.kafka.testutils.KafkaSourceExternalContext;
import org.apache.flink.connector.testframe.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.TableSourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment;
import org.apache.flink.util.DockerImageVersions;

import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.util.Arrays;
import java.util.List;

/** Kafka table source E2E test based on connector testing framework. */
@SuppressWarnings("unused")
public class KafkaTableSourceE2ECase extends TableSourceTestSuiteBase {
    private static final String KAFKA_HOSTNAME = "kafka";

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

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
    KafkaTableSourceExternalContextFactory topicSplitContext =
            new KafkaTableSourceExternalContextFactory(
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
                            TestUtils.getResource("table-java-bridge.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL()),
                    KafkaSourceExternalContext.SplitMappingMode.TOPIC);

    @TestContext
    KafkaTableSourceExternalContextFactory partitionSplitContext =
            new KafkaTableSourceExternalContextFactory(
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
                            TestUtils.getResource("table-java-bridge.jar")
                                    .toAbsolutePath()
                                    .toUri()
                                    .toURL()),
                    KafkaSourceExternalContext.SplitMappingMode.PARTITION);

    public KafkaTableSourceE2ECase() throws Exception {}

    @Override
    public List<DataType> supportTypes() {
        return Arrays.asList(
                DataTypes.CHAR(2),
                DataTypes.STRING(),
                DataTypes.VARCHAR(3),
                DataTypes.INT(),
                DataTypes.BIGINT(),
                DataTypes.SMALLINT(),
                DataTypes.TINYINT(),
                DataTypes.DOUBLE(),
                DataTypes.FLOAT(),
                DataTypes.BOOLEAN(),
                DataTypes.DATE(),
                DataTypes.TIME(),
                DataTypes.TIMESTAMP(),
                DataTypes.TIMESTAMP_WITH_LOCAL_TIME_ZONE());
    }
}
