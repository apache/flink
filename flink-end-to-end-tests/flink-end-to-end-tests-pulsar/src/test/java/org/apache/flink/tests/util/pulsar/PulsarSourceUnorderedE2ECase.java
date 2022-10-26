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
import org.apache.flink.connector.pulsar.testutils.source.UnorderedSourceTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.source.cases.KeySharedSubscriptionContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.SharedSubscriptionContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.tests.util.pulsar.common.FlinkContainerWithPulsarEnvironment;
import org.apache.flink.tests.util.pulsar.common.PulsarContainerTestEnvironment;

import org.junit.jupiter.api.Tag;

/**
 * Pulsar E2E test based on connector testing framework. It's used for Shared & Key_Shared
 * subscription.
 */
@SuppressWarnings("unused")
@Tag("org.apache.flink.testutils.junit.FailsOnJava11")
public class PulsarSourceUnorderedE2ECase extends UnorderedSourceTestSuiteBase<String> {

    // Defines the Semantic.
    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    // Defines TestEnvironment.
    @TestEnv
    FlinkContainerWithPulsarEnvironment flink = new FlinkContainerWithPulsarEnvironment(1, 8);

    // Defines ConnectorExternalSystem.
    @TestExternalSystem
    PulsarContainerTestEnvironment pulsar = new PulsarContainerTestEnvironment(flink);

    // Defines a set of external context Factories for different test cases.
    @TestContext
    PulsarTestContextFactory<String, SharedSubscriptionContext> shared =
            new PulsarTestContextFactory<>(pulsar, SharedSubscriptionContext::new);

    @TestContext
    PulsarTestContextFactory<String, KeySharedSubscriptionContext> keyShared =
            new PulsarTestContextFactory<>(pulsar, KeySharedSubscriptionContext::new);
}
