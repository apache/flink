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
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.tests.util.pulsar.common.FlinkContainerWithPulsarEnvironment;
import org.apache.flink.tests.util.pulsar.common.PulsarContainerTestEnvironment;
import org.apache.flink.tests.util.pulsar.source.ExclusiveSubscriptionContext;
import org.apache.flink.tests.util.pulsar.source.FailoverSubscriptionContext;

import org.junit.jupiter.api.Tag;

/**
 * Pulsar E2E test based on connector testing framework. It's used for Failover & Exclusive
 * subscription.
 */
@SuppressWarnings("unused")
@Tag("org.apache.flink.testutils.junit.FailsOnJava11")
public class PulsarSourceOrderedE2ECase extends SourceTestSuiteBase<String> {

    // Defines the Semantic.
    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    // Defines TestEnvironment.
    @TestEnv
    FlinkContainerWithPulsarEnvironment flink = new FlinkContainerWithPulsarEnvironment(1, 6);

    // Defines ConnectorExternalSystem.
    @TestExternalSystem
    PulsarContainerTestEnvironment pulsar = new PulsarContainerTestEnvironment(flink);

    // Defines a set of external context Factories for different test cases.
    @TestContext
    PulsarTestContextFactory<String, ExclusiveSubscriptionContext> exclusive =
            new PulsarTestContextFactory<>(pulsar, ExclusiveSubscriptionContext::new);

    @TestContext
    PulsarTestContextFactory<String, FailoverSubscriptionContext> failover =
            new PulsarTestContextFactory<>(pulsar, FailoverSubscriptionContext::new);
}
