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
import org.apache.flink.connectors.test.common.environment.MiniClusterTestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.environment.TestEnvironmentConfigs;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.source.ControllableSource;
import org.apache.flink.connectors.test.common.testsuites.BasicTestSuite;
import org.apache.flink.connectors.test.kafka.external.KafkaContainerizedExternalSystem;
import org.apache.flink.connectors.test.kafka.external.KafkaExternalContext;

import org.junit.Rule;
import org.junit.Test;

import java.util.Properties;

/** IT for Kafka connector. */
public class KafkaConnectorIntegratedTestCase {

    @Rule public KafkaContainerizedExternalSystem kafka = new KafkaContainerizedExternalSystem();

    @Test
    public void testBasicFunctionality() throws Exception {

        // Configure Kafka test context
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(
                "bootstrap.servers", kafka.getBootstrapServer().split("://")[1]);
        ExternalContext<String> externalContext = new KafkaExternalContext(kafkaProperties);

        // Configure Flink test environment
        TestEnvironment miniClusterTestEnvironment = new MiniClusterTestEnvironment();
        Configuration config = miniClusterTestEnvironment.getConfiguration();
        config.set(TestEnvironmentConfigs.RMI_HOST, "localhost");
        config.set(
                TestEnvironmentConfigs.RMI_POTENTIAL_PORTS,
                String.valueOf(ControllableSource.RMI_PORT));
        config.set(TestEnvironmentConfigs.RECORD_FILE_PATH_FOR_JOB, "/tmp/record.txt");
        config.set(TestEnvironmentConfigs.OUTPUT_FILE_PATH_FOR_JOB, "/tmp/output.txt");
        config.set(TestEnvironmentConfigs.RECORD_FILE_PATH_FOR_VALIDATION, "/tmp/record.txt");
        config.set(TestEnvironmentConfigs.OUTPUT_FILE_PATH_FOR_VALIDATION, "/tmp/record.txt");

        // Run test case in test suite
        BasicTestSuite.testBasicFunctionality(externalContext, miniClusterTestEnvironment);
    }
}
