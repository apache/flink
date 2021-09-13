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
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalContextFactory;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.TestEnv;
import org.apache.flink.connectors.test.common.testsuites.SourceTestSuiteBase;
import org.apache.flink.tests.util.pulsar.cases.ExclusiveSubscriptionContext;
import org.apache.flink.tests.util.pulsar.cases.FailoverSubscriptionContext;
import org.apache.flink.tests.util.pulsar.common.FlinkContainerWithPulsarEnvironment;

import static org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime.container;

/**
 * Pulsar E2E test based on connector testing framework. It's used for Failover & Exclusive
 * subscription.
 */
public class PulsarSourceOrderedE2ECase extends SourceTestSuiteBase<String> {

    // Defines TestEnvironment.
    @TestEnv
    FlinkContainerWithPulsarEnvironment flink = new FlinkContainerWithPulsarEnvironment(1, 6);

    // Defines ConnectorExternalSystem.
    @ExternalSystem
    PulsarTestEnvironment pulsar = new PulsarTestEnvironment(container(flink.getFlinkContainer()));

    // Defines a set of external context Factories for different test cases.
    @ExternalContextFactory
    PulsarTestContextFactory<String, ExclusiveSubscriptionContext> exclusive =
            new PulsarTestContextFactory<>(pulsar, ExclusiveSubscriptionContext::new);

    @ExternalContextFactory
    PulsarTestContextFactory<String, FailoverSubscriptionContext> failover =
            new PulsarTestContextFactory<>(pulsar, FailoverSubscriptionContext::new);
}
