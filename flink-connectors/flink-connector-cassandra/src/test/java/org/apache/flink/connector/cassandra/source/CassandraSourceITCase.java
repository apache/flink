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

package org.apache.flink.connector.cassandra.source;

import org.apache.flink.connector.testframe.environment.ClusterControllable;
import org.apache.flink.connector.testframe.environment.MiniClusterTestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.junit.annotations.TestContext;
import org.apache.flink.connector.testframe.junit.annotations.TestEnv;
import org.apache.flink.connector.testframe.junit.annotations.TestExternalSystem;
import org.apache.flink.connector.testframe.junit.annotations.TestSemantics;
import org.apache.flink.connector.testframe.testsuites.SourceTestSuiteBase;
import org.apache.flink.connector.testframe.utils.CollectIteratorAssertions;
import org.apache.flink.connectors.cassandra.utils.Pojo;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Disabled;

import java.util.List;

import static java.util.concurrent.CompletableFuture.runAsync;
import static org.apache.flink.connector.cassandra.source.CassandraTestContext.CassandraTestContextFactory;
import static org.apache.flink.connector.testframe.utils.ConnectorTestConstants.DEFAULT_COLLECT_DATA_TIMEOUT;
import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for the Cassandra source. */
public class CassandraSourceITCase extends SourceTestSuiteBase<Pojo> {
    @TestEnv MiniClusterTestEnvironment flinkTestEnvironment = new MiniClusterTestEnvironment();

    @TestExternalSystem
    CassandraTestEnvironment cassandraTestEnvironment = new CassandraTestEnvironment();

    @TestSemantics
    CheckpointingMode[] semantics = new CheckpointingMode[] {CheckpointingMode.EXACTLY_ONCE};

    @TestContext
    CassandraTestContextFactory contextFactory =
            new CassandraTestContextFactory(cassandraTestEnvironment);

    // overridden to use unordered checks
    @Override
    protected void checkResultWithSemantic(
            CloseableIterator<Pojo> resultIterator,
            List<List<Pojo>> testData,
            CheckpointingMode semantic,
            Integer limit) {
        if (limit != null) {
            Runnable runnable =
                    () ->
                            CollectIteratorAssertions.assertUnordered(resultIterator)
                                    .withNumRecordsLimit(limit)
                                    .matchesRecordsFromSource(testData, semantic);

            assertThat(runAsync(runnable)).succeedsWithin(DEFAULT_COLLECT_DATA_TIMEOUT);
        } else {
            CollectIteratorAssertions.assertUnordered(resultIterator)
                    .matchesRecordsFromSource(testData, semantic);
        }
    }

    @Disabled("Not a unbounded source")
    @Override
    public void testSavepoint(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic)
            throws Exception {}

    @Disabled("Not a unbounded source")
    @Override
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic)
            throws Exception {}

    @Disabled("Not a unbounded source")
    @Override
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            CheckpointingMode semantic)
            throws Exception {}

    @Disabled("Not a unbounded source")
    @Override
    public void testTaskManagerFailure(
            TestEnvironment testEnv,
            DataStreamSourceExternalContext<Pojo> externalContext,
            ClusterControllable controller,
            CheckpointingMode semantic)
            throws Exception {}
}
