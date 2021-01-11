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

package org.apache.flink.connectors.test.kafka;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.test.common.environment.FlinkContainersTestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.testsuites.BasicTestSuite;
import org.apache.flink.connectors.test.common.utils.FlinkContainers;
import org.apache.flink.connectors.test.common.utils.FlinkJarHelper;
import org.apache.flink.connectors.test.kafka.external.KafkaContainerizedExternalSystem;
import org.apache.flink.connectors.test.kafka.external.KafkaExternalContext;

import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;

import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;

/** End-to-end tests for Kafka connector. */
public class KafkaConnectorE2ETestCase {

    @ClassRule
    public static FlinkContainers flink =
            FlinkContainers.builder("source-sink-combined-test", 1).build();

    @Rule
    public KafkaContainerizedExternalSystem kafka =
            new KafkaContainerizedExternalSystem().withFlinkContainers(flink);

    @Test
    public void testBasicFunctionalityOnContainers() throws Exception {
        // Configure Kafka test context
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty("bootstrap.servers", KafkaContainerizedExternalSystem.ENTRY);
        KafkaExternalContext kafkaTestContext = new KafkaExternalContext(kafkaProperties);

        // Configure Flink test environment
        TestEnvironment containerTestEnvironment =
                new FlinkContainersTestEnvironment(
                        flink, FlinkJarHelper.searchJar().getAbsolutePath());

        // Create required test configurations
        Configuration config = new Configuration();
        config.set(BasicTestSuite.TestConfiguration.RMI_HOST, "localhost");
        List<String> potentialPorts =
                flink.getTaskManagerRMIPorts().stream()
                        .map(Object::toString)
                        .collect(Collectors.toList());
        config.set(
                BasicTestSuite.TestConfiguration.RMI_POTENTIAL_PORTS,
                String.join(",", potentialPorts));
        config.set(
                BasicTestSuite.TestConfiguration.RECORD_FILE_PATH_FOR_JOB,
                Paths.get(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "record.txt")
                        .toString());
        config.set(
                BasicTestSuite.TestConfiguration.OUTPUT_FILE_PATH_FOR_JOB,
                Paths.get(FlinkContainers.getWorkspaceDirInside().getAbsolutePath(), "output.txt")
                        .toString());
        config.set(
                BasicTestSuite.TestConfiguration.RECORD_FILE_PATH_FOR_VALIDATION,
                Paths.get(flink.getWorkspaceFolderOutside().getAbsolutePath(), "record.txt")
                        .toString());
        config.set(
                BasicTestSuite.TestConfiguration.OUTPUT_FILE_PATH_FOR_VALIDATION,
                Paths.get(flink.getWorkspaceFolderOutside().getAbsolutePath(), "output.txt")
                        .toString());

        // Run test case in test suite
        BasicTestSuite.testBasicFunctionality(kafkaTestContext, containerTestEnvironment, config);
    }
}
