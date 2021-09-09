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
package org.apache.flink.tests.util.kafka;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.connector.kafka.source.testutils.hybrid.HybridKafkaAndFileExternalContext;
import org.apache.flink.connector.kafka.source.testutils.hybrid.HybridKafkaAndFileExternalContext.Destination;
import org.apache.flink.connectors.test.common.environment.ClusterControllable;
import org.apache.flink.connectors.test.common.environment.TestEnvironment;
import org.apache.flink.connectors.test.common.external.DefaultContainerizedExternalSystem;
import org.apache.flink.connectors.test.common.external.ExternalContext;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalContextFactory;
import org.apache.flink.connectors.test.common.junit.annotations.ExternalSystem;
import org.apache.flink.connectors.test.common.junit.annotations.TestEnv;
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
import org.apache.flink.tests.util.TestUtils;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment;
import org.apache.flink.tests.util.flink.FlinkContainerTestEnvironment.EnvironmentBuilder;
import org.apache.flink.tests.util.flink.Mount;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.extension.ExtendWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import static org.apache.flink.connectors.test.common.utils.TestDataMatchers.matchesSplitTestData;
import static org.hamcrest.MatcherAssert.assertThat;

@ExtendWith({
    ConnectorTestingExtension.class,
    TestLoggerExtension.class,
    TestCaseInvocationContextProvider.class
})
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HybridSourceE2ETest {

    private static final Logger LOG = LoggerFactory.getLogger(HybridSourceE2ETest.class);

    Path tmpDir = Files.createTempDirectory("file_source_test_");

    public HybridSourceE2ETest() throws IOException {}

    @TestEnv
    //    MiniClusterTestEnvironment flink = new MiniClusterTestEnvironment();
    FlinkContainerTestEnvironment flink =
            new EnvironmentBuilder(1, 6)
                    .withJarPath(
                            TestUtils.getResource("kafka-connector.jar")
                                    .toAbsolutePath()
                                    .toString(),
                            TestUtils.getResource("kafka-clients.jar").toAbsolutePath().toString())
                    .withMount(Mount.of(tmpDir.toString(), tmpDir.toString()))
                    // Use for speedup during local development
                    //                    .deleteOnExit(false)
                    .build();

    // Defines ConnectorExternalSystem
    private static final String KAFKA_HOSTNAME = "kafka";
    private static final String KAFKA_IMAGE_NAME = "confluentinc/cp-kafka:5.5.2";

    // Defines ConnectorExternalSystem
    @ExternalSystem
    DefaultContainerizedExternalSystem<KafkaContainer> kafka =
            DefaultContainerizedExternalSystem.builder()
                    .fromContainer(
                            new KafkaContainer(DockerImageName.parse(KAFKA_IMAGE_NAME))
                                    .withNetworkAliases(KAFKA_HOSTNAME))
                    .bindWithFlinkContainer(flink.getFlinkContainer())
                    .build();

    @SuppressWarnings("unused")
    @ExternalContextFactory
    HybridKafkaAndFileExternalContext.Factory singleTopic =
            new HybridKafkaAndFileExternalContext.Factory(kafka.getContainer(), tmpDir);

    // ----------------------------- Basic test cases ---------------------------------

    @TestTemplate
    @DisplayName("Test HybridSource")
    public void testSourceSingleSplit(
            TestEnvironment testEnv, ExternalContext<String> externalContext) throws Exception {

        // Write test data to external system
        final Collection<String> testRecords = generateAndWriteTestData(0, externalContext, 0.5);

        // Build and execute Flink job
        StreamExecutionEnvironment execEnv = testEnv.createExecutionEnvironment();

        try (CloseableIterator<String> resultIterator =
                execEnv.fromSource(
                                externalContext.createSource(Boundedness.BOUNDED),
                                WatermarkStrategy.noWatermarks(),
                                "Tested Source")
                        .returns(String.class)
                        .setParallelism(1)
                        .executeAndCollect("Source Single Split Test")) {
            // Check test result
            assertThat(resultIterator, matchesSplitTestData(testRecords));
        }
    }

    @TestTemplate
    @DisplayName("Test HybridSource with TaskManager failover post-switch")
    public void testTaskManagerFailurePreSwitch(
            TestEnvironment testEnv,
            ExternalContext<String> externalContext,
            ClusterControllable controller)
            throws Exception {
        double divideFraction = 0.5; // test data split proportion between two sources
        int failurePositionShift = +1; // fail post-switch
        testFailover(testEnv, externalContext, controller, divideFraction, failurePositionShift);
    }

    @TestTemplate
    @DisplayName("Test HybridSource with TaskManager failover pre-switch")
    public void testTaskManagerFailurePostSwitch(
            TestEnvironment testEnv,
            ExternalContext<String> externalContext,
            ClusterControllable controller)
            throws Exception {

        double divideFraction = 0.5; // test data split proportion between two sources
        int failurePositionShift = -1; // fail pre-switch
        testFailover(testEnv, externalContext, controller, divideFraction, failurePositionShift);
    }

    private void testFailover(
            TestEnvironment testEnv,
            ExternalContext<String> externalContext,
            ClusterControllable controller,
            double divideFraction,
            int failurePositionShift)
            throws Exception {
        TestExecutionContext<String> testContext = setup(testEnv, externalContext, divideFraction);
        int divideIndex = divideIndex(testContext.testRecords, divideFraction);

        List<Collection<String>> failureSlices =
                slice(testContext.testRecords, divideIndex + failurePositionShift);
        Collection<String> slicePreFailure = failureSlices.get(0);

        assertThat(
                testContext.iterator,
                matchesSplitTestData(slicePreFailure, slicePreFailure.size()));

        Collection<String> slicePostFailure = failureSlices.get(1);

        controller.triggerTaskManagerFailover(testContext.jobClient, () -> {});
        assertThat(
                testContext.iterator,
                matchesSplitTestData(slicePostFailure, slicePostFailure.size()));

        cleanUp(testContext);
    }

    private void cleanUp(TestExecutionContext<String> testContext) throws Exception {
        // TODO: Fix. Immediate cleanup sometimes causes a job-not-found exception.
        Thread.sleep(5000);

        testContext.iterator.close();
        CommonTestUtils.terminateJob(testContext.jobClient, Duration.ofSeconds(30));
        CommonTestUtils.waitForJobStatus(
                testContext.jobClient,
                Collections.singletonList(JobStatus.CANCELED),
                Deadline.fromNow(Duration.ofSeconds(30)));

        for (File file : tmpDir.toFile().listFiles()) {
            if (!file.isDirectory()) {
                file.delete();
            }
        }
    }

    private TestExecutionContext<String> setup(
            TestEnvironment testEnv, ExternalContext<String> externalContext, double divideFraction)
            throws Exception {
        int splitIndex = 0;
        final Collection<String> testRecords =
                generateAndWriteTestData(splitIndex, externalContext, divideFraction);

        final StreamExecutionEnvironment env = testEnv.createExecutionEnvironment();

        env.enableCheckpointing(50);

        // CONTINUOUS_UNBOUNDED has to be used, otherwise even when not all data was yet read from
        // the File source, the job is in FINISHED state when failover is triggered.
        final DataStreamSource<String> dataStreamSource =
                (DataStreamSource<String>)
                        env.fromSource(
                                        externalContext.createSource(
                                                Boundedness.CONTINUOUS_UNBOUNDED),
                                        WatermarkStrategy.noWatermarks(),
                                        "Tested Source")
                                .returns(String.class)
                                .setParallelism(1);

        final CollectResultIterator<String> iterator = getResultsIterator(env, dataStreamSource);
        final JobClient jobClient = env.executeAsync("TaskManager Failover Test");
        iterator.setJobClient(jobClient);

        return new TestExecutionContext<>(testRecords, jobClient, iterator);
    }

    private static class TestExecutionContext<T> {
        final Collection<T> testRecords;
        final JobClient jobClient;
        final CollectResultIterator<T> iterator;

        public TestExecutionContext(
                Collection<T> testRecords, JobClient jobClient, CollectResultIterator<T> iterator) {
            this.testRecords = testRecords;
            this.jobClient = jobClient;
            this.iterator = iterator;
        }
    }

    // ----------------------------- Helper Functions ---------------------------------

    /**
     * Generate a set of test records and write it to the given split writer.
     *
     * @param externalContext External context
     * @param fraction Determines how to divide test data between the FileSource and KafkaSource
     *     parts of the HybridSource under test
     * @return Collection of generated test records
     */
    private static Collection<String> generateAndWriteTestData(
            int splitIndex, ExternalContext<String> externalContext, double fraction) {
        final Collection<String> testRecordCollection =
                externalContext.generateTestData(
                        splitIndex, ThreadLocalRandom.current().nextLong());
        LOG.debug("Writing {} records to external system", testRecordCollection.size());

        List<Collection<String>> parts = divide(testRecordCollection, fraction);

        // This is currently just a sanity check, without any proper data generation for sources
        // "handover"
        externalContext
                .createSourceSplitDataWriter(Destination.FILE.toString())
                .writeRecords(parts.get(0));

        externalContext
                .createSourceSplitDataWriter(Destination.KAFKA.toString())
                .writeRecords(parts.get(1));

        return testRecordCollection;
    }

    /**
     * Divides a collection into two parts.
     *
     * @param input collection to be divided.
     * @param fraction indicating which portion of the input elements should be placed into the
     *     first part of the division. The second part will be assigned the remaining elements (1 -
     *     {@code fraction}).
     * @param <T> type of the collection.
     * @return resulting parts of the division stored separately at index 0 and 1 of the returned
     *     list.
     */
    private static <T> List<Collection<T>> divide(Collection<T> input, double fraction) {
        int divideIndex = divideIndex(input, fraction);
        return slice(input, divideIndex);
    }

    /**
     * Calculates the index of an element in the {@code input} collection that denotes the division
     * according to the specified {@code fraction} relation between the parts.
     *
     * @param input collection to be divided.
     * @param fraction indicating which portion of the input elements should be placed into the
     *     first part of the division. The second part will be assigned the remaining elements
     *     {@code (1 - fraction)}.
     * @param <T> type of the collection.
     * @return the index of the first element in the {@code input} collection that belongs to the
     *     second part of the division.
     */
    private static <T> int divideIndex(Collection<T> input, double fraction) {
        return (int) (input.size() * fraction);
    }

    private static <T> List<Collection<T>> slice(Collection<T> input, int numRecords) {
        final List<T> list = new ArrayList<>(input);
        final AtomicInteger counter = new AtomicInteger();

        Map<Boolean, List<T>> map =
                list.stream()
                        .collect(
                                Collectors.partitioningBy(
                                        e -> counter.incrementAndGet() > numRecords));

        return new ArrayList<>(map.values());
    }

    // This approach is used since DataStream API doesn't expose job client for executeAndCollect()
    private CollectResultIterator<String> getResultsIterator(
            StreamExecutionEnvironment env, DataStreamSource<String> dataStreamSource) {

        TypeSerializer<String> serializer =
                dataStreamSource.getType().createSerializer(env.getConfig());

        String accumulatorName = "dataStreamCollect_" + UUID.randomUUID();

        CollectSinkOperatorFactory<String> factory =
                new CollectSinkOperatorFactory<>(serializer, accumulatorName);

        CollectSinkOperator<String> operator = (CollectSinkOperator<String>) factory.getOperator();

        CollectResultIterator<String> iterator =
                new CollectResultIterator<>(
                        operator.getOperatorIdFuture(),
                        serializer,
                        accumulatorName,
                        env.getCheckpointConfig());

        CollectStreamSink<String> sink = new CollectStreamSink<>(dataStreamSource, factory);

        sink.name("Data stream collect sink");
        env.addOperator(sink.getTransformation());
        return iterator;
    }
}
