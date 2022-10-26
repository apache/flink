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

package org.apache.flink.connector.pulsar.source;

import org.apache.flink.connector.pulsar.testutils.PulsarTestContextFactory;
import org.apache.flink.connector.pulsar.testutils.PulsarTestEnvironment;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.pulsar.testutils.source.UnorderedSourceTestSuiteBase;
import org.apache.flink.connector.pulsar.testutils.source.cases.KeySharedSubscriptionContext;
import org.apache.flink.connector.pulsar.testutils.source.cases.SharedSubscriptionContext;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Tag;

/**
 * Unit test class for {@link PulsarSource}. Used for {@link SubscriptionType#Shared} subscription.
 */
@Tag("org.apache.flink.testutils.junit.FailsOnJava11")
public class PulsarUnorderedSourceITCase extends UnorderedSourceTestSuiteBase<String> {

    // Defines test environment on Flink MiniCluster
    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines pulsar running environment
    @TestExternalSystem
    PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.mock());

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    @TestContext
    PulsarTestContextFactory<String, SharedSubscriptionContext> sharedSubscription =
            new PulsarTestContextFactory<>(pulsar, SharedSubscriptionContext::new);

    @TestContext
    PulsarTestContextFactory<String, KeySharedSubscriptionContext> keySharedSubscription =
            new PulsarTestContextFactory<>(pulsar, KeySharedSubscriptionContext::new);
}
