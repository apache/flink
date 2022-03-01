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
import org.apache.flink.connector.pulsar.testutils.cases.MultipleTopicConsumingContext;
import org.apache.flink.connector.pulsar.testutils.cases.SingleTopicConsumingContext;
import org.apache.flink.connector.pulsar.testutils.runtime.PulsarRuntime;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;

import org.junit.jupiter.api.Disabled;

/** Unite test class for {@link PulsarSource}. */
@SuppressWarnings("unused")
class PulsarSourceITCase extends SourceTestSuiteBase<String> {

    @Disabled // TODO: remove override after FLINK-26177 is fixed
    @Override
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<String> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        super.testScaleUp(testEnv, externalContext, semantic);
    }

    @Disabled // TODO: remove override after FLINK-26177 is fixed
    @Override
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<String> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        super.testScaleDown(testEnv, externalContext, semantic);
    }

    // Defines test environment on Flink MiniCluster
    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines pulsar running environment
    @TestExternalSystem
    PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.embedded());

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    // Defines an external context Factories,
    // so test cases will be invoked using this external contexts.
    @TestContext
    PulsarTestContextFactory<String, SingleTopicConsumingContext> singleTopic =
            new PulsarTestContextFactory<>(pulsar, SingleTopicConsumingContext::new);

    @TestContext
    PulsarTestContextFactory<String, MultipleTopicConsumingContext> multipleTopic =
            new PulsarTestContextFactory<>(pulsar, MultipleTopicConsumingContext::new);
}
