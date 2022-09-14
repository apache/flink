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
import org.apache.flink.connector.pulsar.testutils.cases.SharedSubscriptionConsumingContext;
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
import org.apache.flink.testutils.junit.FailsOnJava11;
import org.apache.flink.util.CloseableIterator;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.Disabled;

import java.util.List;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.flink.connector.testframe.utils.CollectIteratorAssertions.assertUnordered;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Unit test class for {@link PulsarSource}. Used for {@link SubscriptionType#Shared} subscription.
 */
@Category(value = {FailsOnJava11.class})
public class PulsarUnorderedSourceITCase extends SourceTestSuiteBase<String> {
    // Defines test environment on Flink MiniCluster
    @TestEnv MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();

    // Defines pulsar running environment
    @TestExternalSystem
    PulsarTestEnvironment pulsar = new PulsarTestEnvironment(PulsarRuntime.mock());

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    @TestContext
    PulsarTestContextFactory<String, SharedSubscriptionConsumingContext> singleTopic =
            new PulsarTestContextFactory<>(pulsar, SharedSubscriptionConsumingContext::new);

    @Override
    protected void checkResultWithSemantic(
            CloseableIterator<String> resultIterator,
            List<List<String>> testData,
            CheckpointingMode semantic,
            Integer limit) {
        Runnable runnable =
                () ->
                        assertUnordered(resultIterator)
                                .withNumRecordsLimit(getExpectedSize(testData, limit))
                                .matchesRecordsFromSource(testData, semantic);

        assertThat(runAsync(runnable)).succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);
    }

    /**
     * Shared subscription will have multiple readers on same partition, this would make hard to
     * automatically stop like a bounded source.
     */
    private static int getExpectedSize(List<List<String>> testData, Integer limit) {
        if (limit == null) {
            return testData.stream().mapToInt(List::size).sum();
        } else {
            return limit;
        }
    }

    @Override
    @Disabled("We don't have any idle readers in Pulsar's shared subscription.")
    public void testIdleReader(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<String> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        super.testIdleReader(testEnv, externalContext, semantic);
    }
}
