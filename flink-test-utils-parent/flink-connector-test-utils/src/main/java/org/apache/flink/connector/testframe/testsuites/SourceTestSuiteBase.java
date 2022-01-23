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

package org.apache.flink.connector.testframe.testsuites;

import org.apache.flink.annotation.Experimental;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.connector.testframe.environment.ClusterControllable;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.ExternalSystemSplitDataWriter;
import org.apache.flink.connector.testframe.external.source.DataStreamSourceExternalContext;
import org.apache.flink.connector.testframe.external.source.TestingSourceSettings;
import org.apache.flink.connector.testframe.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connector.testframe.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connector.testframe.utils.TestDataMatchers;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.TestLoggerExtension;

import org.hamcrest.MatcherAssert;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

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
    public void testSourceSingleSplit(
            TestEnvironment testEnv, DataStreamSourceExternalContext<T> externalContext)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .build();
        TestEnvironmentSettings envSettings =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        List<T> testRecords = generateAndWriteTestData(0, externalContext, sourceSettings);

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envSettings);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(1);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "Source Single Split Test");

        // Step 4: Validate test data
        try (CloseableIterator<T> resultIterator = iteratorBuilder.build(jobClient)) {
            LOG.info("Checking test results");
            MatcherAssert.assertThat(
                    resultIterator, TestDataMatchers.matchesSplitTestData(testRecords));
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
    public void testMultipleSplits(
            TestEnvironment testEnv, DataStreamSourceExternalContext<T> externalContext)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitNumber = 4;
        List<List<T>> testRecordsLists = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(splitNumber);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "Source Multiple Split Test");

        // Step 4: Validate test data
        try (CloseableIterator<T> resultIterator = iteratorBuilder.build(jobClient)) {
            // Check test result
            LOG.info("Checking test results");
            MatcherAssert.assertThat(
                    resultIterator,
                    TestDataMatchers.matchesMultipleSplitTestData(testRecordsLists));
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
    public void testIdleReader(
            TestEnvironment testEnv, DataStreamSourceExternalContext<T> externalContext)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.BOUNDED)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitNumber = 4;
        List<List<T>> testRecordsLists = new ArrayList<>();
        for (int i = 0; i < splitNumber; i++) {
            testRecordsLists.add(generateAndWriteTestData(i, externalContext, sourceSettings));
        }

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(splitNumber + 1);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "Idle Reader Test");

        // Step 4: Validate test data
        try (CloseableIterator<T> resultIterator = iteratorBuilder.build(jobClient)) {
            LOG.info("Checking test results");
            MatcherAssert.assertThat(
                    resultIterator,
                    TestDataMatchers.matchesMultipleSplitTestData(testRecordsLists));
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
            DataStreamSourceExternalContext<T> externalContext,
            ClusterControllable controller)
            throws Exception {
        // Step 1: Preparation
        TestingSourceSettings sourceSettings =
                TestingSourceSettings.builder()
                        .setBoundedness(Boundedness.CONTINUOUS_UNBOUNDED)
                        .setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
                        .build();
        TestEnvironmentSettings envOptions =
                TestEnvironmentSettings.builder()
                        .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                        .build();
        Source<T, ?, ?> source = tryCreateSource(externalContext, sourceSettings);

        // Step 2: Write test data to external system
        int splitIndex = 0;
        List<T> testRecordsBeforeFailure =
                externalContext.generateTestData(
                        sourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        ExternalSystemSplitDataWriter<T> externalSystemSplitDataWriter =
                externalContext.createSourceSplitDataWriter(sourceSettings);
        LOG.info(
                "Writing {} records for split {} to external system",
                testRecordsBeforeFailure.size(),
                splitIndex);
        externalSystemSplitDataWriter.writeRecords(testRecordsBeforeFailure);

        // Step 3: Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment(envOptions);
        execEnv.enableCheckpointing(50);
        DataStreamSource<T> stream =
                execEnv.fromSource(source, WatermarkStrategy.noWatermarks(), "Tested Source")
                        .setParallelism(1);
        CollectIteratorBuilder<T> iteratorBuilder = addCollectSink(stream);
        JobClient jobClient = submitJob(execEnv, "TaskManager Failover Test");

        // Step 4: Validate records before killing TaskManagers
        CloseableIterator<T> iterator = iteratorBuilder.build(jobClient);
        LOG.info("Checking records before killing TaskManagers");
        MatcherAssert.assertThat(
                iterator,
                TestDataMatchers.matchesSplitTestData(
                        testRecordsBeforeFailure, testRecordsBeforeFailure.size()));

        // Step 5: Trigger TaskManager failover
        LOG.info("Trigger TaskManager failover");
        controller.triggerTaskManagerFailover(jobClient, () -> {});

        LOG.info("Waiting for job recovering from failure");
        CommonTestUtils.waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.RUNNING),
                Deadline.fromNow(Duration.ofSeconds(30)));

        // Step 6: Write test data again to external system
        List<T> testRecordsAfterFailure =
                externalContext.generateTestData(
                        sourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.info(
                "Writing {} records for split {} to external system",
                testRecordsAfterFailure.size(),
                splitIndex);
        externalSystemSplitDataWriter.writeRecords(testRecordsAfterFailure);

        // Step 7: Validate test result
        LOG.info("Checking records after job failover");
        MatcherAssert.assertThat(
                iterator,
                TestDataMatchers.matchesSplitTestData(
                        testRecordsAfterFailure, testRecordsAfterFailure.size()));

        // Step 8: Clean up
        CommonTestUtils.terminateJob(jobClient, Duration.ofSeconds(30));
        CommonTestUtils.waitForJobStatus(
                jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));
        iterator.close();
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records and write it to the given split writer.
     *
     * @param externalContext External context
     * @return List of generated test records
     */
    protected List<T> generateAndWriteTestData(
            int splitIndex,
            DataStreamSourceExternalContext<T> externalContext,
            TestingSourceSettings testingSourceSettings) {
        List<T> testRecords =
                externalContext.generateTestData(
                        testingSourceSettings, splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.info(
                "Writing {} records for split {} to external system",
                testRecords.size(),
                splitIndex);
        externalContext
                .createSourceSplitDataWriter(testingSourceSettings)
                .writeRecords(testRecords);
        return testRecords;
    }

    protected Source<T, ?, ?> tryCreateSource(
            DataStreamSourceExternalContext<T> externalContext,
            TestingSourceSettings sourceOptions) {
        try {
            return externalContext.createSource(sourceOptions);
        } catch (UnsupportedOperationException e) {
            throw new TestAbortedException("Cannot create source satisfying given options", e);
        }
    }

    protected JobClient submitJob(StreamExecutionEnvironment env, String jobName) throws Exception {
        LOG.info("Submitting Flink job to test environment");
        return env.executeAsync(jobName);
    }

    protected CollectIteratorBuilder<T> addCollectSink(DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getType().createSerializer(stream.getExecutionConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);
        CollectSinkOperator<T> operator = (CollectSinkOperator<T>) factory.getOperator();
        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        sink.name("Data stream collect sink");
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        return new CollectIteratorBuilder<>(
                operator,
                serializer,
                accumulatorName,
                stream.getExecutionEnvironment().getCheckpointConfig());
    }

    /** Builder class for constructing {@link CollectResultIterator} of collect sink. */
    protected static class CollectIteratorBuilder<T> {

        private final CollectSinkOperator<T> operator;
        private final TypeSerializer<T> serializer;
        private final String accumulatorName;
        private final CheckpointConfig checkpointConfig;

        protected CollectIteratorBuilder(
                CollectSinkOperator<T> operator,
                TypeSerializer<T> serializer,
                String accumulatorName,
                CheckpointConfig checkpointConfig) {
            this.operator = operator;
            this.serializer = serializer;
            this.accumulatorName = accumulatorName;
            this.checkpointConfig = checkpointConfig;
        }

        protected CollectResultIterator<T> build(JobClient jobClient) {
            CollectResultIterator<T> iterator =
                    new CollectResultIterator<>(
                            operator.getOperatorIdFuture(),
                            serializer,
                            accumulatorName,
                            checkpointConfig);
            iterator.setJobClient(jobClient);
            return iterator;
        }
    }
}
