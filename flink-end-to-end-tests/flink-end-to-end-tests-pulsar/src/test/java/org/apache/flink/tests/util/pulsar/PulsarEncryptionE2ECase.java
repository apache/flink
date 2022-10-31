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

package org.apache.flink.tests.util.pulsar;

import org.apache.flink.connector.pulsar.testutils.PulsarTestContextFactory;
import org.apache.flink.connector.pulsar.testutils.sink.PulsarSinkTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.sink.cases.PulsarEncryptSinkContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.ConsumeEncryptMessagesContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.tests.util.pulsar.common.FlinkContainerWithPulsarEnvironment;
import org.apache.flink.tests.util.pulsar.common.PulsarContainerTestEnvironment;

import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;

/**
 * The test will produce and consume messages by Pulsar connector with the End-to-end encryption
 * support.
 */
@SuppressWarnings("unused")
@Tag("org.apache.flink.testutils.junit.FailsOnJava11")
public class PulsarEncryptionE2ECase {

    @Nested
    class EncryptionSource extends SourceTestSuiteBase<String> {

        // Defines the Semantic.
        @TestSemantics
        CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

        // Defines TestEnvironment.
        @TestEnv
        FlinkContainerWithPulsarEnvironment flink = new FlinkContainerWithPulsarEnvironment(1, 6);

        // Defines ConnectorExternalSystem.
        @TestExternalSystem
        PulsarContainerTestEnvironment pulsar = new PulsarContainerTestEnvironment(flink);

        @TestContext
        PulsarTestContextFactory<String, ConsumeEncryptMessagesContext> encryptMessages =
                new PulsarTestContextFactory<>(pulsar, ConsumeEncryptMessagesContext::new);
    }

    @Nested
    class EncryptionSink extends PulsarSinkTestSuiteBase {

        // Defines the Semantic.
        @TestSemantics
        CheckpointingMode[] semantics =
                new CheckpointingMode[] {
                    CheckpointingMode.EXACTLY_ONCE, CheckpointingMode.AT_LEAST_ONCE
                };

        // Defines TestEnvironment
        @TestEnv
        FlinkContainerWithPulsarEnvironment flink = new FlinkContainerWithPulsarEnvironment(1, 6);

        // Defines ConnectorExternalSystem.
        @TestExternalSystem
        PulsarContainerTestEnvironment pulsar = new PulsarContainerTestEnvironment(flink);

        @TestContext
        PulsarTestContextFactory<String, PulsarEncryptSinkContext> sinkContext =
                new PulsarTestContextFactory<>(pulsar, PulsarEncryptSinkContext::new);
    }
}
