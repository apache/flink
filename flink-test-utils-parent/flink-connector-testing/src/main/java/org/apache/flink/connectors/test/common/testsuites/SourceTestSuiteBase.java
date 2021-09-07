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

package org.apache.flink.connectors.test.common.testsuites;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.external.SourceSplitDataWriter;
import org.apache.flink.connectors.test.common.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connectors.test.common.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connectors.test.common.junit.extensions.TestLoggerExtension;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.flink.connectors.test.common.utils.TestDataMatchers.matchesMultipleSplitTestData;
import static org.apache.flink.connectors.test.common.utils.TestDataMatchers.matchesSplitTestData;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Base class for all test suites.
 *
 * <p>All cases should have well-descriptive JavaDoc, including:
 *
 * <ul>
 *   <li>What's the purpose of this case
 *   <li>Simple description of how this case works
 *   <li>Condition to fulfill in order to pass this case
 *   <li>Requirement of running this case
 * </ul>
 */
@ExtendWith({
    ConnectorTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@Experimental
public abstract class SourceTestSuiteBase<T> {

    private static final Logger LOG = LoggerFactory.getLogger(SourceTestSuiteBase.class);

    // ----------------------------- Basic test cases ---------------------------------

    /**
     * Test connector source with only one split in the external system.
     *
     * <p>This test will create one split in the external system, write test data into it, and
     * consume back via a Flink job with 1 parallelism.
     *
     * <p>The number and order of records consumed by Flink need to be identical to the test data
     * written to the external system in order to pass this test.
     *
     * <p>A bounded source is required for this test.
     */
    @TestTemplate
    @DisplayName("Test source with single split")
    public void testSourceSingleSplit(TestEnvironment testEnv, ExternalContext<T> externalContext)
            throws Exception {

        // Write test data to external system
        LOG.info("Writing test data to split 0");
        final List<T> testRecords = generateAndWriteTestData(0, externalContext);

        // Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment();

        LOG.info("Submitting Flink job to test environment");
        try (CloseableIterator<T> resultIterator =
                execEnv.fromSource(
                                externalContext.createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(1)
                        .executeAndCollect("Source Single Split Test")) {
            // Check test result
            LOG.info("Checking test results");
            assertThat(resultIterator, matchesSplitTestData(testRecords));
        }
    }

    /**
     * Test connector source with multiple splits in the external system
     *
     * <p>This test will create 4 splits in the external system, write test data to all splits, and
     * consume back via a Flink job with 4 parallelism.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     *
     * <p>A bounded source is required for this test.
     */
    @TestTemplate
    @DisplayName("Test source with multiple splits")
    public void testMultipleSplits(TestEnvironment testEnv, ExternalContext<T> externalContext)
            throws Exception {

        final int splitNumber = 4;
        final List<List<T>> testRecordsLists = new ArrayList<>();
        LOG.info("Writing test data to split 0 to 3...");
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext));
        }

        LOG.info("Submitting Flink job to test environment");
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment();

        try (final CloseableIterator<T> resultIterator =
                execEnv.fromSource(
                                externalContext.createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(splitNumber)
                        .executeAndCollect("Source Multiple Split Test")) {
            // Check test result
            LOG.info("Checking test results");
            assertThat(resultIterator, matchesMultipleSplitTestData(testRecordsLists));
        }
    }

    /**
     * Test connector source with an idle reader.
     *
     * <p>This test will create 4 split in the external system, write test data to all splits, and
     * consume back via a Flink job with 5 parallelism, so at least one parallelism / source reader
     * will be idle (assigned with no splits). If the split enumerator of the source doesn't signal
     * NoMoreSplitsEvent to the idle source reader, the Flink job will never spin to FINISHED state.
     *
     * <p>The number and order of records in each split consumed by Flink need to be identical to
     * the test data written into the external system to pass this test. There's no requirement for
     * record order across splits.
     *
     * <p>A bounded source is required for this test.
     */
    @TestTemplate
    @DisplayName("Test source with at least one idle parallelism")
    public void testIdleReader(TestEnvironment testEnv, ExternalContext<T> externalContext)
            throws Exception {

        final int splitNumber = 4;
        final List<List<T>> testRecordsLists = new ArrayList<>();
        LOG.info("Writing test data to split 0 to 3");
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext));
        }

        LOG.info("Submitting Flink job to test environment");
        try (CloseableIterator<T> resultIterator =
                testEnv.createExecutionEnvironment()
                        .fromSource(
                                externalContext.createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(splitNumber + 1)
                        .executeAndCollect("Idle Reader Test")) {
            LOG.info("Checking test results");
            assertThat(resultIterator, matchesMultipleSplitTestData(testRecordsLists));
        }
    }

    /**
     * Test connector source with task manager failover.
     *
     * <p>This test will create 1 split in the external system, write test record set A into the
     * split, restart task manager to trigger job failover, write test record set B into the split,
     * and terminate the Flink job finally.
     *
     * <p>The number and order of records consumed by Flink should be identical to A before the
     * failover and B after the failover in order to pass the test.
     *
     * <p>An unbounded source is required for this test, since TaskManager failover will be
     * triggered in the middle of the test.
     */
    @TestTemplate
    @DisplayName("Test TaskManager failure")
    public void testTaskManagerFailure(
            TestEnvironment testEnv,
            ExternalContext<T> externalContext,
            ClusterControllable controller)
            throws Exception {
        int splitIndex = 0;

        LOG.info("Writing test data to split {}", splitIndex);
        final List<T> testRecordsBeforeFailure =
                externalContext.generateTestData(
                        splitIndex, ThreadLocalRandom.current().nextLong());
        final SourceSplitDataWriter<T> sourceSplitDataWriter =
                externalContext.createSourceSplitDataWriter();
        sourceSplitDataWriter.writeRecords(testRecordsBeforeFailure);

        final StreamExecutionEnvironment env = testEnv.createExecutionEnvironment();

        env.enableCheckpointing(50);
        final DataStreamSource<T> dataStreamSource =
                env.fromSource(
                                externalContext.createSource(Boundedness.CONTINUOUS_UNBOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .setParallelism(1);

        // Since DataStream API doesn't expose job client for executeAndCollect(), we have
        // to reuse these part of code to get both job client and result iterator :-(
        // ------------------------------------ START ---------------------------------------------
        TypeSerializer<T> serializer = dataStreamSource.getType().createSerializer(env.getConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectResultIterator<T> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig());
        CollectStreamSink<T> sink = new CollectStreamSink<>(dataStreamSource, factory);
        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());

        LOG.info("Submitting Flink job to test environment");
        final JobClient jobClient = env.executeAsync("TaskManager Failover Test");
        iterator.setJobClient(jobClient);
        // -------------------------------------- END ---------------------------------------------

        LOG.info("Checking records before killing TaskManagers");
        assertThat(
                iterator,
                matchesSplitTestData(testRecordsBeforeFailure, testRecordsBeforeFailure.size()));

        // -------------------------------- Trigger failover ---------------------------------------
        LOG.info("Trigger TaskManager failover");
        controller.triggerTaskManagerFailover(jobClient, () -> {});

        LOG.info("Waiting for job recovering from failure");
        CommonTestUtils.waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.RUNNING),
                Deadline.fromNow(Duration.ofSeconds(30)));

        LOG.info("Writing test data to split {}", splitIndex);
        final List<T> testRecordsAfterFailure =
                externalContext.generateTestData(
                        splitIndex, ThreadLocalRandom.current().nextLong());
        sourceSplitDataWriter.writeRecords(testRecordsAfterFailure);

        LOG.info("Checking records after job failover");
        assertThat(
                iterator,
                matchesSplitTestData(testRecordsAfterFailure, testRecordsAfterFailure.size()));

        // Clean up
        iterator.close();
        CommonTestUtils.terminateJob(jobClient, Duration.ofSeconds(30));
        CommonTestUtils.waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records and write it to the given split writer.
     *
     * @param externalContext External context
     * @return List of generated test records
     */
    protected List<T> generateAndWriteTestData(int splitIndex, ExternalContext<T> externalContext) {
        final List<T> testRecords =
                externalContext.generateTestData(
                        splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.debug("Writing {} records to external system", testRecords.size());
        externalContext.createSourceSplitDataWriter().writeRecords(testRecords);
        return testRecords;
    }
}
