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

package org.apache.flink.connector.pulsar.testutils.source;

import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.CloseableIterator;

import org.apache.pulsar.client.api.SubscriptionType;
import org.junit.jupiter.api.Disabled;

import java.util.List;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.flink.connector.testframe.utils.CollectIteratorAssertions.assertUnordered;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * Pulsar with {@link SubscriptionType#Key_Shared} and {@link SubscriptionType#Shared} consumes the
 * message out of order. So we have to override the default connector test tool.
 */
public abstract class UnorderedSourceTestSuiteBase<T> extends SourceTestSuiteBase<T> {

    @Override
    protected void checkResultWithSemantic(
            CloseableIterator<T> resultIterator,
            List<List<T>> testData,
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
    private static <T> int getExpectedSize(List<List<T>> testData, Integer limit) {
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
            DataStreamSourceExternalContext<T> externalContext,
            CheckpointingMode semantic) {}
}
