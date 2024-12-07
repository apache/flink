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
import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RpcOptions;
import org.apache.flink.connector.testframe.environment.TestEnvironment;
import org.apache.flink.connector.testframe.environment.TestEnvironmentSettings;
import org.apache.flink.connector.testframe.external.ExternalSystemDataReader;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkExternalContext;
import org.apache.flink.connector.testframe.external.sink.DataStreamSinkV2ExternalContext;
import org.apache.flink.connector.testframe.external.sink.TestingSinkSettings;
import org.apache.flink.connector.testframe.junit.extensions.ConnectorTestingExtension;
import org.apache.flink.connector.testframe.junit.extensions.TestCaseInvocationContextProvider;
import org.apache.flink.connector.testframe.source.FromElementsSource;
import org.apache.flink.connector.testframe.utils.CollectIteratorAssertions;
import org.apache.flink.connector.testframe.utils.MetricQuerier;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.metrics.MetricNames;
import org.apache.flink.runtime.rest.RestClient;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.operators.collect.CollectResultIterator;
import org.apache.flink.streaming.api.operators.collect.CollectSinkOperatorFactory;
import org.apache.flink.streaming.api.operators.collect.CollectStreamSink;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.util.TestLoggerExtension;

import org.apache.commons.math3.util.Precision;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opentest4j.TestAbortedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.connector.testframe.utils.MetricQuerier.getJobDetails;
import static org.apache.flink.core.execution.CheckpointingMode.AT_LEAST_ONCE;
import static org.apache.flink.core.execution.CheckpointingMode.EXACTLY_ONCE;
import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.testutils.CommonTestUtils.terminateJob;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.apache.flink.streaming.api.CheckpointingMode.convertToCheckpointingMode;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Base class for sink test suite.
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
public abstract class SinkTestSuiteBase<T extends Comparable<T>> {
    private static final Logger LOG = LoggerFactory.getLogger(SinkTestSuiteBase.class);

    // ----------------------------- Basic test cases ---------------------------------

    /**
     * Test DataStream connector sink.
     *
     * <p>The following tests will create a sink in the external system, generate a collection of
     * test data and write them to this sink by the Flink Job.
     *
     * <p>In order to pass these tests, the number of records produced by Flink need to be equals to
     * the generated test data. And the records in the sink will be compared to the test data by the
     * different semantics. There's no requirement for records order.
     */
    @TestTemplate
    @DisplayName("Test data stream sink")
    public void testBasicSink(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        TestingSinkSettings sinkSettings = getTestingSinkSettings(semantic);
        final List<T> testRecords = generateTestData(sinkSettings, externalContext);

        // Build and execute Flink job
        StreamExecutionEnvironment execEnv =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        execEnv.enableCheckpointing(50);
        DataStream<T> dataStream =
                execEnv.fromData(testRecords)
                        .name("sourceInSinkTest")
                        .setParallelism(1)
                        .returns(externalContext.getProducedType());
        tryCreateSink(dataStream, externalContext, sinkSettings)
                .setParallelism(1)
                .name("sinkInSinkTest");
        final JobClient jobClient = execEnv.executeAsync("DataStream Sink Test");

        waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));

        // Check test result
        checkResultWithSemantic(
                externalContext.createSinkDataReader(sinkSettings), testRecords, semantic);
    }

    /**
     * Test connector sink restart from a completed savepoint with the same parallelism.
     *
     * <p>This test will create a sink in the external system, generate a collection of test data
     * and write a half part of them to this sink by the Flink Job with parallelism 2 at first. Then
     * stop the job, restart the same job from the completed savepoint. After the job has been
     * running, write the other part to the sink and compare the result.
     *
     * <p>In order to pass this test, the number of records produced by Flink need to be equals to
     * the generated test data. And the records in the sink will be compared to the test data by the
     * different semantic. There's no requirement for record order.
     */
    @TestTemplate
    @DisplayName("Test sink restarting from a savepoint")
    public void testStartFromSavepoint(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 2, 2);
    }

    /**
     * Test connector sink restart from a completed savepoint with a higher parallelism.
     *
     * <p>This test will create a sink in the external system, generate a collection of test data
     * and write a half part of them to this sink by the Flink Job with parallelism 2 at first. Then
     * stop the job, restart the same job from the completed savepoint with a higher parallelism 4.
     * After the job has been running, write the other part to the sink and compare the result.
     *
     * <p>In order to pass this test, the number of records produced by Flink need to be equals to
     * the generated test data. And the records in the sink will be compared to the test data by the
     * different semantic. There's no requirement for record order.
     */
    @TestTemplate
    @DisplayName("Test sink restarting with a higher parallelism")
    public void testScaleUp(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 2, 4);
    }

    /**
     * Test connector sink restart from a completed savepoint with a lower parallelism.
     *
     * <p>This test will create a sink in the external system, generate a collection of test data
     * and write a half part of them to this sink by the Flink Job with parallelism 4 at first. Then
     * stop the job, restart the same job from the completed savepoint with a lower parallelism 2.
     * After the job has been running, write the other part to the sink and compare the result.
     *
     * <p>In order to pass this test, the number of records produced by Flink need to be equals to
     * the generated test data. And the records in the sink will be compared to the test data by the
     * different semantic. There's no requirement for record order.
     */
    @TestTemplate
    @DisplayName("Test sink restarting with a lower parallelism")
    public void testScaleDown(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        restartFromSavepoint(testEnv, externalContext, semantic, 4, 2);
    }

    private void restartFromSavepoint(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            CheckpointingMode semantic,
            final int beforeParallelism,
            final int afterParallelism)
            throws Exception {
        // Step 1: Preparation
        TestingSinkSettings sinkSettings = getTestingSinkSettings(semantic);
        final StreamExecutionEnvironment execEnv =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        RestartStrategyUtils.configureNoRestartStrategy(execEnv);

        // Step 2: Generate test data
        final List<T> testRecords = generateTestData(sinkSettings, externalContext);

        // Step 3: Build and execute Flink job
        int numBeforeSuccess = testRecords.size() / 2;
        DataStreamSource<T> source =
                execEnv.fromSource(
                                new FromElementsSource<>(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        testRecords,
                                        numBeforeSuccess),
                                WatermarkStrategy.noWatermarks(),
                                "beforeRestartSource")
                        .setParallelism(1);

        DataStream<T> dataStream = source.returns(externalContext.getProducedType());
        tryCreateSink(dataStream, externalContext, sinkSettings)
                .name("Sink restart test")
                .setParallelism(beforeParallelism);

        /**
         * The job should stop after consume a specified number of records. In order to know when
         * the specified number of records have been consumed, a collect sink is need to be watched.
         */
        CollectResultIterator<T> iterator = addCollectSink(source);
        final JobClient jobClient = execEnv.executeAsync("Restart Test");
        iterator.setJobClient(jobClient);

        // Step 4: Wait for the expected result and stop Flink job with a savepoint
        final ExecutorService executorService = Executors.newCachedThreadPool();
        String savepointPath;
        try {
            waitForAllTaskRunning(
                    () ->
                            getJobDetails(
                                    new RestClient(new Configuration(), executorService),
                                    testEnv.getRestEndpoint(),
                                    jobClient.getJobID()));

            waitExpectedSizeData(iterator, numBeforeSuccess);

            savepointPath =
                    jobClient
                            .stopWithSavepoint(
                                    true, testEnv.getCheckpointUri(), SavepointFormatType.CANONICAL)
                            .get(30, TimeUnit.SECONDS);
            waitForJobStatus(jobClient, Collections.singletonList(JobStatus.FINISHED));
        } catch (Exception e) {
            executorService.shutdown();
            killJob(jobClient);
            throw e;
        }

        List<T> target = testRecords.subList(0, numBeforeSuccess);
        checkResultWithSemantic(
                externalContext.createSinkDataReader(sinkSettings), target, semantic);

        // Step 4: restart the Flink job with the savepoint
        final StreamExecutionEnvironment restartEnv =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .setSavepointRestorePath(savepointPath)
                                .build());
        restartEnv.enableCheckpointing(50);

        DataStreamSource<T> restartSource =
                restartEnv
                        .fromSource(
                                new FromElementsSource<>(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        testRecords,
                                        testRecords.size()),
                                WatermarkStrategy.noWatermarks(),
                                "restartSource")
                        .setParallelism(1);

        DataStream<T> sinkStream = restartSource.returns(externalContext.getProducedType());
        tryCreateSink(sinkStream, externalContext, sinkSettings).setParallelism(afterParallelism);
        addCollectSink(restartSource);
        final JobClient restartJobClient = restartEnv.executeAsync("Restart Test");

        try {
            // Check the result
            checkResultWithSemantic(
                    externalContext.createSinkDataReader(sinkSettings), testRecords, semantic);
        } finally {
            executorService.shutdown();
            killJob(restartJobClient);
            iterator.close();
        }
    }

    /**
     * Test connector sink metrics.
     *
     * <p>This test will create a sink in the external system, generate test data and write them to
     * the sink via a Flink job. Then read and compare the metrics.
     *
     * <p>Now test: numRecordsOut
     */
    @TestTemplate
    @DisplayName("Test sink metrics")
    public void testMetrics(
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> externalContext,
            CheckpointingMode semantic)
            throws Exception {
        TestingSinkSettings sinkSettings = getTestingSinkSettings(semantic);
        int parallelism = 1;
        final List<T> testRecords = generateTestData(sinkSettings, externalContext);

        // make sure use different names when executes multi times
        String sinkName = "metricTestSink" + testRecords.hashCode();
        final StreamExecutionEnvironment env =
                testEnv.createExecutionEnvironment(
                        TestEnvironmentSettings.builder()
                                .setConnectorJarPaths(externalContext.getConnectorJarPaths())
                                .build());
        env.enableCheckpointing(50);

        DataStreamSource<T> source =
                env.fromSource(
                                new FromElementsSource<>(
                                        Boundedness.CONTINUOUS_UNBOUNDED,
                                        testRecords,
                                        testRecords.size()),
                                WatermarkStrategy.noWatermarks(),
                                "metricTestSource")
                        .setParallelism(1);

        DataStream<T> dataStream = source.returns(externalContext.getProducedType());
        tryCreateSink(dataStream, externalContext, sinkSettings)
                .name(sinkName)
                .setParallelism(parallelism);
        final JobClient jobClient = env.executeAsync("Metrics Test");
        final MetricQuerier queryRestClient = new MetricQuerier(new Configuration());
        final ExecutorService executorService = Executors.newCachedThreadPool();
        try {
            waitForAllTaskRunning(
                    () ->
                            getJobDetails(
                                    new RestClient(new Configuration(), executorService),
                                    testEnv.getRestEndpoint(),
                                    jobClient.getJobID()));

            waitUntilCondition(
                    () -> {
                        // test metrics
                        try {
                            return compareSinkMetrics(
                                    queryRestClient,
                                    testEnv,
                                    externalContext,
                                    jobClient.getJobID(),
                                    sinkName,
                                    MetricNames.NUM_RECORDS_SEND,
                                    testRecords.size());
                        } catch (Exception e) {
                            // skip failed assert try
                            return false;
                        }
                    });
        } finally {
            // Clean up
            executorService.shutdown();
            killJob(jobClient);
        }
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records.
     *
     * @param testingSinkSettings sink settings
     * @param externalContext External context
     * @return Collection of generated test records
     */
    protected List<T> generateTestData(
            TestingSinkSettings testingSinkSettings,
            DataStreamSinkExternalContext<T> externalContext) {
        return externalContext.generateTestData(
                testingSinkSettings, ThreadLocalRandom.current().nextLong());
    }

    /**
     * Poll records from the sink.
     *
     * @param result Append records to which list
     * @param reader The sink reader
     * @param expected The expected list which help to stop polling
     * @param retryTimes The retry times
     * @param semantic The semantic
     * @return Collection of records in the Sink
     */
    private List<T> pollAndAppendResultData(
            List<T> result,
            ExternalSystemDataReader<T> reader,
            List<T> expected,
            int retryTimes,
            CheckpointingMode semantic) {
        long timeoutMs = 1000L;
        int retryIndex = 0;

        while (retryIndex++ < retryTimes
                && !checkGetEnoughRecordsWithSemantic(expected, result, semantic)) {
            result.addAll(reader.poll(Duration.ofMillis(timeoutMs)));
        }
        return result;
    }

    /**
     * Check whether the polling should stop.
     *
     * @param expected The expected list which help to stop polling
     * @param result The records that have been read
     * @param semantic The semantic
     * @return Whether the polling should stop
     */
    private boolean checkGetEnoughRecordsWithSemantic(
            List<T> expected, List<T> result, CheckpointingMode semantic) {
        checkNotNull(expected);
        checkNotNull(result);
        if (EXACTLY_ONCE.equals(semantic)) {
            return expected.size() <= result.size();
        } else if (AT_LEAST_ONCE.equals(semantic)) {
            Set<Integer> matchedIndex = new HashSet<>();
            for (T record : expected) {
                int before = matchedIndex.size();
                for (int i = 0; i < result.size(); i++) {
                    if (matchedIndex.contains(i)) {
                        continue;
                    }
                    if (record.equals(result.get(i))) {
                        matchedIndex.add(i);
                        break;
                    }
                }
                // if not find the record in the result
                if (before == matchedIndex.size()) {
                    return false;
                }
            }
            return true;
        }
        throw new IllegalStateException(
                String.format("%s delivery guarantee doesn't support test.", semantic.name()));
    }

    /**
     * Compare the test data with actual data in given semantic.
     *
     * @param reader the data reader for the sink
     * @param testData the test data
     * @param semantic the supported semantic, see {@link CheckpointingMode}
     */
    protected void checkResultWithSemantic(
            ExternalSystemDataReader<T> reader, List<T> testData, CheckpointingMode semantic)
            throws Exception {
        final ArrayList<T> result = new ArrayList<>();
        waitUntilCondition(
                () -> {
                    pollAndAppendResultData(result, reader, testData, 30, semantic);
                    try {
                        CollectIteratorAssertions.assertThat(sort(result).iterator())
                                .matchesRecordsFromSource(Arrays.asList(sort(testData)), semantic);
                        return true;
                    } catch (Throwable t) {
                        return false;
                    }
                });
    }

    /**
     * This method is required for downstream projects e.g. Flink connectors extending this test for
     * the case when there should be supported Flink versions below 1.20. Could be removed together
     * with dropping support for Flink 1.19.
     */
    @Deprecated
    protected void checkResultWithSemantic(
            ExternalSystemDataReader<T> reader,
            List<T> testData,
            org.apache.flink.streaming.api.CheckpointingMode semantic)
            throws Exception {
        checkResultWithSemantic(reader, testData, convertToCheckpointingMode(semantic));
    }

    /** Compare the metrics. */
    private boolean compareSinkMetrics(
            MetricQuerier metricQuerier,
            TestEnvironment testEnv,
            DataStreamSinkExternalContext<T> context,
            JobID jobId,
            String sinkName,
            String metricsName,
            long expectedSize)
            throws Exception {
        double sumNumRecordsOut =
                metricQuerier.getAggregatedMetricsByRestAPI(
                        testEnv.getRestEndpoint(),
                        jobId,
                        sinkName,
                        metricsName,
                        getSinkMetricFilter(context));

        if (Precision.equals(expectedSize, sumNumRecordsOut)) {
            return true;
        } else {
            LOG.info("expected:<{}> but was <{}>({})", expectedSize, sumNumRecordsOut, metricsName);
            return false;
        }
    }

    /** Sort the list. */
    private List<T> sort(List<T> list) {
        return list.stream().sorted().collect(Collectors.toList());
    }

    private TestingSinkSettings getTestingSinkSettings(CheckpointingMode checkpointingMode) {
        return TestingSinkSettings.builder().setCheckpointingMode(checkpointingMode).build();
    }

    private void killJob(JobClient jobClient) throws Exception {
        terminateJob(jobClient);
        waitForJobStatus(jobClient, Collections.singletonList(JobStatus.CANCELED));
    }

    private DataStreamSink<T> tryCreateSink(
            DataStream<T> dataStream,
            DataStreamSinkExternalContext<T> context,
            TestingSinkSettings sinkSettings) {
        try {
            if (context instanceof DataStreamSinkV2ExternalContext) {
                Sink<T> sinkV2 =
                        ((DataStreamSinkV2ExternalContext<T>) context).createSink(sinkSettings);
                return dataStream.sinkTo(sinkV2);
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "The supported context are DataStreamSinkV1ExternalContext and DataStreamSinkV2ExternalContext, but actual is %s.",
                                context.getClass()));
            }
        } catch (UnsupportedOperationException e) {
            // abort the test
            throw new TestAbortedException("Cannot create a sink satisfying given options.", e);
        }
    }

    /**
     * Return the filter used to filter the sink metric.
     *
     * <ul>
     *   <li>Sink v1: return null.
     *   <li>Sink v2: return the "Writer" prefix in the `SinkTransformationTranslator`.
     * </ul>
     */
    private String getSinkMetricFilter(DataStreamSinkExternalContext<T> context) {
        if (context instanceof DataStreamSinkV2ExternalContext) {
            // See class `SinkTransformationTranslator`
            return "Writer";
        } else {
            throw new IllegalArgumentException(
                    String.format("Get unexpected sink context: %s", context.getClass()));
        }
    }

    protected CollectResultIterator<T> addCollectSink(DataStream<T> stream) {
        TypeSerializer<T> serializer =
                stream.getType()
                        .createSerializer(stream.getExecutionConfig().getSerializerConfig());
        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();
        CollectSinkOperatorFactory<T> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);

        CollectStreamSink<T> sink = new CollectStreamSink<>(stream, factory);
        String operatorUid = "dataStreamCollect";
        sink.name("Data stream collect sink");
        sink.uid(operatorUid);
        stream.getExecutionEnvironment().addOperator(sink.getTransformation());
        return new CollectResultIterator<>(
                operatorUid,
                serializer,
                accumulatorName,
                stream.getExecutionEnvironment().getCheckpointConfig(),
                RpcOptions.ASK_TIMEOUT_DURATION.defaultValue().toMillis());
    }

    private void waitExpectedSizeData(CollectResultIterator<T> iterator, int targetNum) {
        assertThatFuture(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    int count = 0;
                                    while (count < targetNum && iterator.hasNext()) {
                                        iterator.next();
                                        count++;
                                    }
                                    if (count < targetNum) {
                                        throw new IllegalStateException(
                                                String.format(
                                                        "Fail to get %d records.", targetNum));
                                    }
                                    return true;
                                }))
                .eventuallySucceeds();
    }
}
