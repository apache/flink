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
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;
import org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connectors.test.common.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Collection;
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
            TestEnvironment testEnv, ExternalContext<T> externalContext) throws Exception {
        Collection<T> testData =
                externalContext.generateTestData(0, ThreadLocalRandom.current().nextLong());
        SourceSplitDataWriter<T> writer = externalContext.createSourceSplitDataWriter();
        writer.writeRecords(testData);

        Source<T, ?, ?> source = externalContext.createSource(Boundedness.BOUNDED);
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment();
        List<T> results =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Pulsar source")
                        .setParallelism(4)
                        .executeAndCollect(
                                "Source single split with four readers.", testData.size());

        assertThat(results, containsInAnyOrder(testData.toArray()));
    }
}
