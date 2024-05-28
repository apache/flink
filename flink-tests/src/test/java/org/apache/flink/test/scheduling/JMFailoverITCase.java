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

package org.apache.flink.test.scheduling;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TupleTypeInfo;
import org.apache.flink.configuration.BatchExecutionOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.ExecutionOptions;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.RestartStrategyOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.reader.TextLineInputFormat;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.execution.Environment;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.highavailability.HighAvailabilityServices;
import org.apache.flink.runtime.highavailability.nonha.embedded.EmbeddedHaServicesWithLeadershipControl;
import org.apache.flink.runtime.io.network.partition.PartitionedFile;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobmaster.JobResult;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniCluster;
import org.apache.flink.runtime.minicluster.TestingMiniClusterConfiguration;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.GlobalStreamExchangeMode;
import org.apache.flink.streaming.api.graph.StreamConfig;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.graph.StreamingJobGraphGenerator;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.Output;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.StreamTask;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorExtension;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.function.SupplierWithException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;
import static org.assertj.core.api.Assertions.assertThat;

/** ITCase for JM failover. */
class JMFailoverITCase {

    // to speed up recovery
    private final Duration previousWorkerRecoveryTimeout = Duration.ofSeconds(3);

    @RegisterExtension
    static final TestExecutorExtension<ScheduledExecutorService> EXECUTOR_EXTENSION =
            TestingUtils.defaultExecutorExtension();

    private static final int DEFAULT_MAX_PARALLELISM = 4;
    private static final int SOURCE_PARALLELISM = 8;

    private static final int NUMBER_KEYS = 10000;
    private static final int NUMBER_OF_EACH_KEY = 4;

    private EmbeddedHaServicesWithLeadershipControl highAvailabilityServices;

    private String methodName;

    @TempDir java.nio.file.Path temporaryFolder;

    protected int numTaskManagers = 4;

    protected int numSlotsPerTaskManager = 4;

    protected Configuration flinkConfiguration = new Configuration();

    protected MiniCluster flinkCluster;

    protected Supplier<HighAvailabilityServices> highAvailabilityServicesSupplier = null;

    @BeforeEach
    void before(TestInfo testInfo) throws Exception {
        flinkConfiguration = new Configuration();
        SourceTail.clear();
        StubMapFunction.clear();
        StubRecordSink.clear();
        testInfo.getTestMethod().ifPresent(method -> methodName = method.getName());
    }

    @AfterEach
    void after() {
        Throwable exception = null;

        try {
            if (flinkCluster != null) {
                flinkCluster.close();
            }
        } catch (Throwable throwable) {
            exception = throwable;
        }

        if (exception != null) {
            ExceptionUtils.rethrow(exception);
        }
    }

    public void setup() throws Exception {
        SourceTail.clear();
        StubMapFunction.clear();
        StubRecordSink.clear();
    }

    @Test
    void testRecoverFromJMFailover() throws Exception {
        JobGraph jobGraph = prepareEnvAndGetJobGraph();

        // blocking all sink
        StubRecordSink.blockSubTasks(0, 1, 2, 3);

        JobID jobId = flinkCluster.submitJob(jobGraph).get().getJobID();

        // wait until sink is running.
        tryWaitUntilCondition(() -> StubRecordSink.attemptIds.size() > 0);

        triggerJMFailover(jobId);

        // unblock all sink.
        StubRecordSink.unblockSubTasks(0, 1, 2, 3);

        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        checkCountResults();
    }

    @Test
    void testSourceNotAllFinished() throws Exception {
        JobGraph jobGraph = prepareEnvAndGetJobGraph();

        // blocking source 0
        SourceTail.blockSubTasks(0);

        JobID jobId = flinkCluster.submitJob(jobGraph).get().getJobID();

        // wait until source is running.
        tryWaitUntilCondition(() -> SourceTail.attemptIds.size() == SOURCE_PARALLELISM);

        JobVertex source = jobGraph.getVerticesSortedTopologicallyFromSources().get(0);
        while (true) {
            AccessExecutionGraph executionGraph = flinkCluster.getExecutionGraph(jobId).get();
            long finishedTasks =
                    Arrays.stream(executionGraph.getJobVertex(source.getID()).getTaskVertices())
                            .filter(task -> task.getExecutionState() == ExecutionState.FINISHED)
                            .count();
            if (finishedTasks == SOURCE_PARALLELISM - 1) {
                break;
            }

            Thread.sleep(100L);
        }

        triggerJMFailover(jobId);

        // unblock source 0.
        SourceTail.unblockSubTasks(0);

        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        checkCountResults();
    }

    @Test
    void testTaskExecutorNotRegisterOnTime() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                BatchExecutionOptions.JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT, Duration.ZERO);
        JobGraph jobGraph = prepareEnvAndGetJobGraph(configuration);

        // blocking all sink
        StubRecordSink.blockSubTasks(0, 1, 2, 3);

        JobID jobId = flinkCluster.submitJob(jobGraph).get().getJobID();

        // wait until sink is running.
        tryWaitUntilCondition(() -> StubRecordSink.attemptIds.size() > 0);

        triggerJMFailover(jobId);

        // unblock all sink.
        StubRecordSink.unblockSubTasks(0, 1, 2, 3);

        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        checkCountResults();
    }

    @Test
    void testPartitionNotFoundTwice() throws Exception {
        JobGraph jobGraph = prepareEnvAndGetJobGraph();

        // blocking map 0 and map 1.
        StubMapFunction.blockSubTasks(0, 1);

        JobID jobId = flinkCluster.submitJob(jobGraph).get().getJobID();

        // wait until map deploying, which indicates all source finished.
        tryWaitUntilCondition(() -> StubMapFunction.attemptIds.size() > 0);

        triggerJMFailover(jobId);

        // trigger partition not found.
        releaseResultPartitionOfSource();

        // map 0 unblock.
        StubMapFunction.unblockSubTasks(0);

        // wait until map 0 restart, which indicates all source finished again.
        tryWaitUntilCondition(() -> StubMapFunction.attemptIds.get(0) == 1);

        // trigger partition not found.
        releaseResultPartitionOfSource();

        // map 1 unblock.
        StubMapFunction.unblockSubTasks(1);

        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        checkCountResults();
    }

    @Test
    void testPartitionNotFoundAndOperatorCoordinatorNotSupportBatchSnapshot() throws Exception {
        JobGraph jobGraph = prepareEnvAndGetJobGraph(false);

        // blocking all map task
        StubMapFunction2.blockSubTasks(0, 1, 2, 3);

        JobID jobId = flinkCluster.submitJob(jobGraph).get().getJobID();

        // wait until map deploying, which indicates all source finished.
        tryWaitUntilCondition(() -> StubMapFunction2.attemptIds.size() > 0);

        triggerJMFailover(jobId);

        // trigger partition not found.
        releaseResultPartitionOfSource();

        // map tasks unblock.
        StubMapFunction2.unblockSubTasks(0, 1, 2, 3);

        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        checkCountResults();
    }

    @Test
    void testPartitionNotFoundAndOperatorCoordinatorSupportBatchSnapshot() throws Exception {
        JobGraph jobGraph = prepareEnvAndGetJobGraph();

        // blocking map 0.
        StubMapFunction.blockSubTasks(0);

        JobID jobId = flinkCluster.submitJob(jobGraph).get().getJobID();

        // wait until map deploying, which indicates all source finished.
        tryWaitUntilCondition(() -> StubMapFunction.attemptIds.size() > 0);

        triggerJMFailover(jobId);

        // trigger partition not found.
        releaseResultPartitionOfSource();

        // map 0 unblock.
        StubMapFunction.unblockSubTasks(0);

        JobResult jobResult = flinkCluster.requestJobResult(jobId).get();
        assertThat(jobResult.getSerializedThrowable()).isEmpty();

        checkCountResults();
    }

    private JobGraph prepareEnvAndGetJobGraph() throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                BatchExecutionOptions.JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT,
                previousWorkerRecoveryTimeout);
        return prepareEnvAndGetJobGraph(configuration, true);
    }

    private JobGraph prepareEnvAndGetJobGraph(Configuration config) throws Exception {
        return prepareEnvAndGetJobGraph(config, true);
    }

    private JobGraph prepareEnvAndGetJobGraph(boolean operatorCoordinatorsSupportsBatchSnapshot)
            throws Exception {
        Configuration configuration = new Configuration();
        configuration.set(
                BatchExecutionOptions.JOB_RECOVERY_PREVIOUS_WORKER_RECOVERY_TIMEOUT,
                previousWorkerRecoveryTimeout);
        return prepareEnvAndGetJobGraph(configuration, operatorCoordinatorsSupportsBatchSnapshot);
    }

    private JobGraph prepareEnvAndGetJobGraph(
            Configuration config, boolean operatorCoordinatorsSupportsBatchSnapshot)
            throws Exception {
        flinkCluster =
                TestingMiniCluster.newBuilder(getMiniClusterConfiguration(config))
                        .setHighAvailabilityServicesSupplier(highAvailabilityServicesSupplier)
                        .build();
        flinkCluster.start();

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(-1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);

        return operatorCoordinatorsSupportsBatchSnapshot
                ? createJobGraph(env, methodName)
                : createJobGraphWithUnsupportedBatchSnapshotOperatorCoordinator(env, methodName);
    }

    private TestingMiniClusterConfiguration getMiniClusterConfiguration(Configuration config)
            throws IOException {
        // flink basic configuration.
        NetUtils.Port jobManagerRpcPort = NetUtils.getAvailablePort();
        flinkConfiguration.set(ExecutionOptions.RUNTIME_MODE, RuntimeExecutionMode.BATCH);
        flinkConfiguration.set(JobManagerOptions.PORT, jobManagerRpcPort.getPort());
        flinkConfiguration.set(JobManagerOptions.SLOT_REQUEST_TIMEOUT, Duration.ofMillis(5000L));
        flinkConfiguration.set(RestOptions.BIND_PORT, "0");
        flinkConfiguration.set(TaskManagerOptions.TOTAL_PROCESS_MEMORY, MemorySize.parse("1g"));
        flinkConfiguration.set(TaskManagerOptions.NETWORK_MEMORY_FRACTION, 0.4F);

        // adaptive batch job scheduler config.
        flinkConfiguration.set(
                JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.AdaptiveBatch);
        flinkConfiguration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_MAX_PARALLELISM,
                DEFAULT_MAX_PARALLELISM);
        flinkConfiguration.set(
                BatchExecutionOptions.ADAPTIVE_AUTO_PARALLELISM_AVG_DATA_VOLUME_PER_TASK,
                MemorySize.parse("256K"));

        // enable jm failover.
        flinkConfiguration.set(BatchExecutionOptions.JOB_RECOVERY_ENABLED, true);
        flinkConfiguration.set(
                BatchExecutionOptions.JOB_RECOVERY_SNAPSHOT_MIN_PAUSE, Duration.ZERO);

        // region failover config.
        flinkConfiguration.set(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "region");
        flinkConfiguration.set(RestartStrategyOptions.RESTART_STRATEGY, "fixed-delay");
        flinkConfiguration.set(RestartStrategyOptions.RESTART_STRATEGY_FIXED_DELAY_ATTEMPTS, 10);

        // ha config, which helps to trigger jm failover.
        flinkConfiguration.set(HighAvailabilityOptions.HA_STORAGE_PATH, temporaryFolder.toString());
        highAvailabilityServices =
                new EmbeddedHaServicesWithLeadershipControl(EXECUTOR_EXTENSION.getExecutor());
        highAvailabilityServicesSupplier = () -> highAvailabilityServices;

        // shuffle dir, to help trigger partitionNotFoundException
        flinkConfiguration.set(CoreOptions.TMP_DIRS, temporaryFolder.toString());

        // add user defined config
        flinkConfiguration.addAll(config);

        return TestingMiniClusterConfiguration.newBuilder()
                .setConfiguration(flinkConfiguration)
                .setNumTaskManagers(numTaskManagers)
                .setNumSlotsPerTaskManager(numSlotsPerTaskManager)
                .build();
    }

    private void triggerJMFailover(JobID jobId) throws Exception {
        highAvailabilityServices.revokeJobMasterLeadership(jobId).get();
        highAvailabilityServices.grantJobMasterLeadership(jobId);
    }

    private static void checkCountResults() {
        Map<Integer, Integer> countResults = StubRecordSink.countResults;
        assertThat(countResults.size()).isEqualTo(NUMBER_KEYS);

        Map<Integer, Integer> expectedResult =
                IntStream.range(0, NUMBER_KEYS)
                        .boxed()
                        .collect(Collectors.toMap(Function.identity(), i -> NUMBER_OF_EACH_KEY));
        assertThat(countResults).isEqualTo(expectedResult);
    }

    private void releaseResultPartitionOfSource() {
        deleteOldestFileInShuffleNettyDirectory(
                new File(flinkConfiguration.get(CoreOptions.TMP_DIRS)));
    }

    private JobGraph createJobGraph(StreamExecutionEnvironment env, String jobName) {
        TupleTypeInfo<Tuple2<Integer, Integer>> typeInfo =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        env.fromSequence(0, NUMBER_KEYS * NUMBER_OF_EACH_KEY - 1)
                .setParallelism(SOURCE_PARALLELISM)
                .slotSharingGroup("group1")
                .transform("SourceTail", TypeInformation.of(Long.class), new SourceTail())
                .setParallelism(SOURCE_PARALLELISM)
                .slotSharingGroup("group1")
                .transform("Map", typeInfo, new StubMapFunction())
                .slotSharingGroup("group2")
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .slotSharingGroup("group3")
                .transform("Sink", TypeInformation.of(Void.class), new StubRecordSink())
                .slotSharingGroup("group4");

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        streamGraph.setJobType(JobType.BATCH);
        streamGraph.setJobName(jobName);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private JobGraph createJobGraphWithUnsupportedBatchSnapshotOperatorCoordinator(
            StreamExecutionEnvironment env, String jobName) throws Exception {

        TupleTypeInfo<Tuple2<Integer, Integer>> typeInfo =
                new TupleTypeInfo<>(BasicTypeInfo.INT_TYPE_INFO, BasicTypeInfo.INT_TYPE_INFO);

        File file = new File(temporaryFolder.getParent().toFile(), "data.tmp-" + UUID.randomUUID());
        prepareTestData(file);

        FileSource<String> source =
                FileSource.forRecordStreamFormat(
                                new TextLineInputFormat(), new Path(file.getPath()))
                        .build();

        env.fromSource(source, WatermarkStrategy.noWatermarks(), "source")
                .setParallelism(SOURCE_PARALLELISM)
                .slotSharingGroup("group1")
                .transform("Map", typeInfo, new StubMapFunction2())
                .slotSharingGroup("group2")
                .keyBy(tuple2 -> tuple2.f0)
                .sum(1)
                .slotSharingGroup("group3")
                .transform("Sink", TypeInformation.of(Void.class), new StubRecordSink())
                .slotSharingGroup("group4");

        StreamGraph streamGraph = env.getStreamGraph();
        streamGraph.setGlobalStreamExchangeMode(GlobalStreamExchangeMode.ALL_EDGES_BLOCKING);
        streamGraph.setJobType(JobType.BATCH);
        streamGraph.setJobName(jobName);
        return StreamingJobGraphGenerator.createJobGraph(streamGraph);
    }

    private static void setSubtaskBlocked(
            List<Integer> indices, boolean block, Map<Integer, Boolean> subtaskBlocked) {
        indices.forEach(index -> subtaskBlocked.put(index, block));
    }

    /**
     * A stub which helps to:
     *
     * <p>1. Get source tasks' information. (Such as {@link ResultPartitionID}).
     *
     * <p>2. Manually control the execution of source task. Helps to block and unblock execution of
     * source task.
     *
     * <p>This operator should be chained with source operator.
     */
    private static class SourceTail extends AbstractStreamOperator<Long>
            implements OneInputStreamOperator<Long, Long> {

        public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
        public static Map<Integer, ResultPartitionID> resultPartitions = new ConcurrentHashMap<>();
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();

        public SourceTail() {
            super();
            // chain with source.
            setChainingStrategy(ChainingStrategy.ALWAYS);
        }

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Long>> output) {
            super.setup(containingTask, config, output);

            int subIdx = getRuntimeContext().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });

            // record result partition id.
            Environment environment = getContainingTask().getEnvironment();
            checkState(environment.getAllWriters().length == 1);
            resultPartitions.put(subIdx, environment.getAllWriters()[0].getPartitionId());

            // wait until unblocked.
            if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
                tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
            }
        }

        @Override
        public void processElement(StreamRecord<Long> streamRecord) throws Exception {
            output.collect(streamRecord);
        }

        public static void clear() {
            subtaskBlocked.clear();
            attemptIds.clear();
            resultPartitions.clear();
        }

        public static void blockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
        }

        public static void unblockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
        }
    }

    /**
     * A special map function which can get tasks' information (Such as {@link ResultPartitionID})
     * and manually control the task's execution.
     */
    private static class StubMapFunction extends AbstractStreamOperator<Tuple2<Integer, Integer>>
            implements OneInputStreamOperator<Long, Tuple2<Integer, Integer>> {

        public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Tuple2<Integer, Integer>>> output) {
            super.setup(containingTask, config, output);

            int subIdx = getRuntimeContext().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });

            // wait until unblocked.
            if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
                tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
            }
        }

        @Override
        public void processElement(StreamRecord<Long> streamRecord) throws Exception {
            int number = streamRecord.getValue().intValue();
            output.collect(new StreamRecord<>(new Tuple2<>(number % NUMBER_KEYS, 1)));
        }

        public static void clear() {
            subtaskBlocked.clear();
            attemptIds.clear();
        }

        public static void blockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
        }

        public static void unblockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
        }
    }

    private static class StubMapFunction2 extends AbstractStreamOperator<Tuple2<Integer, Integer>>
            implements OneInputStreamOperator<String, Tuple2<Integer, Integer>> {

        public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Tuple2<Integer, Integer>>> output) {
            super.setup(containingTask, config, output);

            int subIdx = getRuntimeContext().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });

            // wait until unblocked.
            if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
                tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
            }
        }

        @Override
        public void processElement(StreamRecord<String> streamRecord) throws Exception {
            int number = Integer.parseInt(streamRecord.getValue());

            output.collect(new StreamRecord<>(new Tuple2<>(number % NUMBER_KEYS, 1)));
        }

        public static void clear() {
            subtaskBlocked.clear();
            attemptIds.clear();
        }

        public static void blockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
        }

        public static void unblockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
        }
    }

    /** A special sink function which can control the task's execution. */
    private static class StubRecordSink extends AbstractStreamOperator<Void>
            implements OneInputStreamOperator<Tuple2<Integer, Integer>, Void> {

        public static Map<Integer, Boolean> subtaskBlocked = new ConcurrentHashMap<>();
        public static Map<Integer, Integer> attemptIds = new ConcurrentHashMap<>();
        public static Map<Integer, Integer> countResults = new ConcurrentHashMap<>();

        @Override
        public void setup(
                StreamTask<?, ?> containingTask,
                StreamConfig config,
                Output<StreamRecord<Void>> output) {
            super.setup(containingTask, config, output);

            int subIdx = getRuntimeContext().getIndexOfThisSubtask();

            // attempt id ++
            attemptIds.compute(
                    subIdx,
                    (ignored, value) -> {
                        if (value == null) {
                            value = 0;
                        } else {
                            value += 1;
                        }
                        return value;
                    });

            // wait until unblocked.
            if (subtaskBlocked.containsKey(subIdx) && subtaskBlocked.get(subIdx)) {
                tryWaitUntilCondition(() -> !subtaskBlocked.get(subIdx));
            }
        }

        @Override
        public void processElement(StreamRecord<Tuple2<Integer, Integer>> streamRecord)
                throws Exception {
            Tuple2<Integer, Integer> value = streamRecord.getValue();
            countResults.put(value.f0, value.f1);
        }

        public static void clear() {
            subtaskBlocked.clear();
            attemptIds.clear();
            countResults.clear();
        }

        public static void blockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), true, subtaskBlocked);
        }

        public static void unblockSubTasks(Integer... subIndices) {
            setSubtaskBlocked(Arrays.asList(subIndices), false, subtaskBlocked);
        }
    }

    private static void tryWaitUntilCondition(SupplierWithException<Boolean, Exception> condition) {
        try {
            CommonTestUtils.waitUntilCondition(condition);
        } catch (Exception exception) {
        }
    }

    private File prepareTestData(File datafile) throws IOException {
        try (FileWriter writer = new FileWriter(datafile)) {
            for (int i = 0; i < NUMBER_KEYS * NUMBER_OF_EACH_KEY; i++) {
                writer.write(i + "\n");
            }
        }
        return datafile;
    }

    private void deleteOldestFileInShuffleNettyDirectory(File directory) {
        if (directory == null || !directory.exists() || !directory.isDirectory()) {
            return;
        }

        File[] matchingDirectories =
                directory.listFiles(
                        file ->
                                file.isDirectory()
                                        && file.getName().startsWith("flink-netty-shuffle"));

        if (matchingDirectories == null) {
            return;
        }

        List<File> files = new ArrayList<>();
        for (File subdirectory : matchingDirectories) {
            Arrays.stream(subdirectory.listFiles())
                    .filter(file -> file.getName().endsWith(PartitionedFile.DATA_FILE_SUFFIX))
                    .forEach(files::add);
        }

        if (!files.isEmpty()) {
            files.sort(Comparator.comparing(this::getFileCreationTime));
            files.get(0).delete();
        }
    }

    private long getFileCreationTime(File file) {
        try {
            BasicFileAttributes attrs =
                    Files.readAttributes(file.toPath(), BasicFileAttributes.class);
            return attrs.creationTime().toMillis();
        } catch (NoSuchFileException e) {
            // TaskExecutor will delete unfinished partition file asynchronously when jom failover.
            return Long.MAX_VALUE;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
