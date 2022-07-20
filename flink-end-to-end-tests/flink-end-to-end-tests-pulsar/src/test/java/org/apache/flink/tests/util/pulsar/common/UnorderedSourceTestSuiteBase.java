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

package org.apache.flink.tests.util.pulsar.common;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.connector.testframe.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connector.testframe.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;

/** A source test template for testing the messages which could be consumed in a unordered way. */
@ExtendWith({
    ConnectorTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public abstract class UnorderedSourceTestSuiteBase<T> {

    @TestTemplate
    @DisplayName("Test source with one split and four consumers")
    public void testOneSplitWithMultipleConsumers(
            TestEnvironment testEnv, DataStreamSourceExternalContext<T> externalContext)
            throws Exception {
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        List<T> testData =
                externalContext.generateTestData(
                        sourceSettings, 0, ThreadLocalRandom.current().nextLong());
        ExternalSystemSplitDataWriter<T> writer =
                externalContext.createSourceSplitDataWriter(sourceSettings);
        writer.writeRecords(testData);

        Source<T, ?, ?> source = externalContext.createSource(sourceSettings);
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        List<T> results =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar source")
                        .setParallelism(4)
                        .executeAndCollect(
                                "Source single split with four readers.", testData.size());

        assertThat(results, containsInAnyOrder(testData.toArray()));
    }
}
