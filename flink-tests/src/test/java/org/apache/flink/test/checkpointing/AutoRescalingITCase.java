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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.configuration.StateRecoveryOptions;
import org.apache.flink.configuration.WebOptions;
import org.apache.flink.core.execution.CheckpointingMode;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobResourceRequirements;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.rocksdb.RocksDBConfigurableOptions;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.legacy.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.legacy.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.legacy.SourceFunction;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.time.Duration;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForNewCheckpoint;
import static org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase.waitForAvailableSlots;
import static org.apache.flink.test.scheduling.UpdateJobResourceRequirementsITCase.waitForRunningTasks;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Test checkpoint rescaling under changing resource requirements. This test is mostly a variant of
 * {@link RescalingITCase} with two main differences: (1) We rescale from checkpoints instead of
 * savepoints and (2) rescaling without cancel/restart but triggered by changing resource
 * requirements.
 */
@RunWith(Parameterized.class)
public class AutoRescalingITCase extends TestLogger {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static final int numTaskManagers = 2;
    private static final int slotsPerTaskManager = 2;
    private static final int totalSlots = numTaskManagers * slotsPerTaskManager;

    @Parameterized.Parameters(name = "backend = {0}, useIngestDB = {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {"rocksdb", false},
                    {"rocksdb", true},
                    {"hashmap", false}
                });
    }

    public AutoRescalingITCase(String backend, boolean useIngestDB) {
        this.backend = backend;
        this.useIngestDB = useIngestDB;
    }

    private final String backend;

    private final boolean useIngestDB;

    private String currentBackend = null;

    enum OperatorCheckpointMethod {
        NON_PARTITIONED,
        CHECKPOINTED_FUNCTION,
        CHECKPOINTED_FUNCTION_BROADCAST,
        LIST_CHECKPOINTED
    }

    private static MiniClusterWithClientResource cluster;
    private static RestClusterClient<?> restClusterClient;

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        // detect parameter change
        if (!Objects.equals(currentBackend, backend)) {
            shutDownExistingCluster();

            currentBackend = backend;

            Configuration config = new Configuration();

            final File checkpointDir = temporaryFolder.newFolder();
            final File savepointDir = temporaryFolder.newFolder();

            config.set(StateBackendOptions.STATE_BACKEND, currentBackend);
            config.set(RocksDBConfigurableOptions.USE_INGEST_DB_RESTORE_MODE, useIngestDB);
            config.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);
            config.set(StateRecoveryOptions.LOCAL_RECOVERY, true);
            config.set(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
            config.set(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());

            config.set(JobManagerOptions.SCHEDULER, JobManagerOptions.SchedulerType.Adaptive);
            // Disable the scaling cooldown to speed up the test
            config.set(
                    JobManagerOptions.SCHEDULER_EXECUTING_COOLDOWN_AFTER_RESCALING,
                    Duration.ofMillis(0));

            // speed the test suite up
            // - lower refresh interval -> controls how fast we invalidate ExecutionGraphCache
            // - lower slot idle timeout -> controls how fast we return idle slots to TM
            config.set(WebOptions.REFRESH_INTERVAL, Duration.ofMillis(50L));
            config.set(JobManagerOptions.SLOT_IDLE_TIMEOUT, Duration.ofMillis(50L));

            cluster =
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(config)
                                    .setNumberTaskManagers(numTaskManagers)
                                    .setNumberSlotsPerTaskManager(slotsPerTaskManager)
                                    .build());
            cluster.before();
            restClusterClient = cluster.getRestClusterClient();
        }
    }

    @AfterClass
    public static void shutDownExistingCluster() {
        if (cluster != null) {
            cluster.after();
            cluster = null;
        }
    }

    @Test
    public void testCheckpointRescalingInKeyedState() throws Exception {
        testCheckpointRescalingKeyedState(false);
    }

    @Test
    public void testCheckpointRescalingOutKeyedState() throws Exception {
        testCheckpointRescalingKeyedState(true);
    }

    /**
     * Tests that a job with purely keyed state can be restarted from a checkpoint with a different
     * parallelism.
     */
    public void testCheckpointRescalingKeyedState(boolean scaleOut) throws Exception {
        final int numberKeys = 42;
        final int numberElements = 1000;
        final int parallelism = scaleOut ? totalSlots / 2 : totalSlots;
        final int parallelism2 = scaleOut ? totalSlots : totalSlots / 2;
        final int maxParallelism = 13;

        Duration timeout = Duration.ofMinutes(3);
        Deadline deadline = Deadline.now().plus(timeout);

        ClusterClient<?> client = cluster.getClusterClient();

        try {

            JobGraph jobGraph =
                    createJobGraphWithKeyedState(
                            cluster.getMiniCluster().getConfiguration().clone(),
                            parallelism,
                            maxParallelism,
                            numberKeys,
                            numberElements);

            final JobID jobID = jobGraph.getJobID();

            client.submitJob(jobGraph).get();

            SubtaskIndexSource.SOURCE_LATCH.trigger();

            // wait til the sources have emitted numberElements for each key and completed a
            // checkpoint
            assertTrue(
                    SubtaskIndexFlatMapper.workCompletedLatch.await(
                            deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

            // verify the current state

            Set<Tuple2<Integer, Integer>> actualResult = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);

                expectedResult.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, parallelism, keyGroupIndex),
                                numberElements * key));
            }

            assertEquals(expectedResult, actualResult);

            // clear the CollectionSink set for the restarted job
            CollectionSink.clearElementsSet();

            waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);

            // We need to wait for a checkpoint to be completed that was triggered after all the
            // data was processed. That ensures the entire data being flushed out of the Operator's
            // network buffers to avoid reprocessing test data twice after the restore (see
            // FLINK-34200).
            waitForNewCheckpoint(jobID, cluster.getMiniCluster());

            SubtaskIndexSource.SOURCE_LATCH.reset();

            JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
            for (JobVertex vertex : jobGraph.getVertices()) {
                builder.setParallelismForJobVertex(vertex.getID(), parallelism2, parallelism2);
            }

            restClusterClient.updateJobResourceRequirements(jobID, builder.build()).join();

            waitForRunningTasks(restClusterClient, jobID, 2 * parallelism2);
            waitForAvailableSlots(restClusterClient, totalSlots - parallelism2);

            SubtaskIndexSource.SOURCE_LATCH.trigger();

            client.requestJobResult(jobID).get();

            Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                expectedResult2.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, parallelism2, keyGroupIndex),
                                key * 2 * numberElements));
            }

            assertEquals(expectedResult2, actualResult2);

        } finally {
            // clear the CollectionSink set for the restarted job
            CollectionSink.clearElementsSet();
        }
    }

    /**
     * Tests that a job cannot be restarted from a checkpoint with a different parallelism if the
     * rescaled operator has non-partitioned state.
     */
    @Test
    public void testCheckpointRescalingNonPartitionedStateCausesException() throws Exception {
        final int parallelism = totalSlots / 2;
        final int parallelism2 = totalSlots;
        final int maxParallelism = 13;

        ClusterClient<?> client = cluster.getClusterClient();

        try {
            JobGraph jobGraph =
                    createJobGraphWithOperatorState(
                            parallelism, maxParallelism, OperatorCheckpointMethod.NON_PARTITIONED);
            // make sure the job does not finish before we take a checkpoint
            StateSourceBase.canFinishLatch = new CountDownLatch(1);

            final JobID jobID = jobGraph.getJobID();

            client.submitJob(jobGraph).get();

            // wait until the operator is started
            waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);
            // wait until the operator handles some data
            StateSourceBase.workStartedLatch.await();

            waitForNewCheckpoint(jobID, cluster.getMiniCluster());

            JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
            for (JobVertex vertex : jobGraph.getVertices()) {
                builder.setParallelismForJobVertex(vertex.getID(), parallelism2, parallelism2);
            }

            restClusterClient.updateJobResourceRequirements(jobID, builder.build()).join();

            waitForRunningTasks(restClusterClient, jobID, 2 * parallelism2);
            waitForAvailableSlots(restClusterClient, totalSlots - parallelism2);

            StateSourceBase.canFinishLatch.countDown();

            client.requestJobResult(jobID).get();
        } catch (JobExecutionException exception) {
            if (!(exception.getCause() instanceof IllegalStateException)) {
                throw exception;
            }
        }
    }

    /**
     * Tests that a job with non partitioned state can be restarted from a checkpoint with a
     * different parallelism if the operator with non-partitioned state are not rescaled.
     */
    @Test
    public void testCheckpointRescalingWithKeyedAndNonPartitionedState() throws Exception {
        int numberKeys = 42;
        int numberElements = 1000;
        int parallelism = totalSlots / 2;
        int parallelism2 = totalSlots;
        int maxParallelism = 13;

        Duration timeout = Duration.ofMinutes(3);
        Deadline deadline = Deadline.now().plus(timeout);

        ClusterClient<?> client = cluster.getClusterClient();

        try {

            JobGraph jobGraph =
                    createJobGraphWithKeyedAndNonPartitionedOperatorState(
                            parallelism,
                            maxParallelism,
                            parallelism,
                            numberKeys,
                            numberElements,
                            numberElements);

            final JobID jobID = jobGraph.getJobID();

            client.submitJob(jobGraph).get();

            SubtaskIndexSource.SOURCE_LATCH.trigger();

            // wait til the sources have emitted numberElements for each key and completed a
            // checkpoint
            assertTrue(
                    SubtaskIndexFlatMapper.workCompletedLatch.await(
                            deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS));

            // verify the current state

            Set<Tuple2<Integer, Integer>> actualResult = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);

                expectedResult.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, parallelism, keyGroupIndex),
                                numberElements * key));
            }

            assertEquals(expectedResult, actualResult);

            // clear the CollectionSink set for the restarted job
            CollectionSink.clearElementsSet();

            waitForNewCheckpoint(jobID, cluster.getMiniCluster());

            SubtaskIndexSource.SOURCE_LATCH.reset();

            JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
            for (JobVertex vertex : jobGraph.getVertices()) {
                if (vertex.getMaxParallelism() >= parallelism2) {
                    builder.setParallelismForJobVertex(vertex.getID(), parallelism2, parallelism2);
                } else {
                    builder.setParallelismForJobVertex(
                            vertex.getID(), vertex.getMaxParallelism(), vertex.getMaxParallelism());
                }
            }

            restClusterClient.updateJobResourceRequirements(jobID, builder.build()).join();

            // Source is parallelism, the flatMapper & Sink is parallelism2
            waitForRunningTasks(restClusterClient, jobID, parallelism + parallelism2);
            waitForAvailableSlots(restClusterClient, totalSlots - parallelism2);

            SubtaskIndexSource.SOURCE_LATCH.trigger();

            client.requestJobResult(jobID).get();

            Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                expectedResult2.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, parallelism2, keyGroupIndex),
                                key * 2 * numberElements));
            }

            assertEquals(expectedResult2, actualResult2);

        } finally {
            // clear the CollectionSink set for the restarted job
            CollectionSink.clearElementsSet();
        }
    }

    @Test
    public void testCheckpointRescalingInPartitionedOperatorState() throws Exception {
        testCheckpointRescalingPartitionedOperatorState(
                false, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION);
    }

    @Test
    public void testCheckpointRescalingOutPartitionedOperatorState() throws Exception {
        testCheckpointRescalingPartitionedOperatorState(
                true, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION);
    }

    @Test
    public void testCheckpointRescalingInBroadcastOperatorState() throws Exception {
        testCheckpointRescalingPartitionedOperatorState(
                false, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST);
    }

    @Test
    public void testCheckpointRescalingOutBroadcastOperatorState() throws Exception {
        testCheckpointRescalingPartitionedOperatorState(
                true, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST);
    }

    /** Tests rescaling of partitioned operator state. */
    public void testCheckpointRescalingPartitionedOperatorState(
            boolean scaleOut, OperatorCheckpointMethod checkpointMethod) throws Exception {
        final int parallelism = scaleOut ? totalSlots : totalSlots / 2;
        final int parallelism2 = scaleOut ? totalSlots / 2 : totalSlots;
        final int maxParallelism = 13;

        ClusterClient<?> client = cluster.getClusterClient();

        int counterSize = Math.max(parallelism, parallelism2);

        if (checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION
                || checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST) {
            PartitionedStateSource.checkCorrectSnapshot = new int[counterSize];
            PartitionedStateSource.checkCorrectRestore = new int[counterSize];
            PartitionedStateSource.checkCorrectSnapshots.clear();
        } else {
            throw new UnsupportedOperationException("Unsupported method:" + checkpointMethod);
        }

        JobGraph jobGraph =
                createJobGraphWithOperatorState(parallelism, maxParallelism, checkpointMethod);
        // make sure the job does not finish before we take the checkpoint
        StateSourceBase.canFinishLatch = new CountDownLatch(1);

        final JobID jobID = jobGraph.getJobID();

        client.submitJob(jobGraph).get();

        // wait until the operator is started
        waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);
        // wait until the operator handles some data
        StateSourceBase.workStartedLatch.await();

        waitForNewCheckpoint(jobID, cluster.getMiniCluster());

        JobResourceRequirements.Builder builder = JobResourceRequirements.newBuilder();
        for (JobVertex vertex : jobGraph.getVertices()) {
            builder.setParallelismForJobVertex(vertex.getID(), parallelism2, parallelism2);
        }

        restClusterClient.updateJobResourceRequirements(jobID, builder.build()).join();

        waitForRunningTasks(restClusterClient, jobID, 2 * parallelism2);
        waitForAvailableSlots(restClusterClient, totalSlots - parallelism2);

        StateSourceBase.canFinishLatch.countDown();

        client.requestJobResult(jobID).get();

        int sumExp = 0;
        int sumAct = 0;

        if (checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION) {
            for (int c : PartitionedStateSource.checkCorrectSnapshot) {
                sumExp += c;
            }

            for (int c : PartitionedStateSource.checkCorrectRestore) {
                sumAct += c;
            }
        } else {
            for (int c : PartitionedStateSource.checkCorrectSnapshot) {
                sumExp += c;
            }

            for (int c : PartitionedStateSource.checkCorrectRestore) {
                sumAct += c;
            }

            sumExp *= parallelism2;
        }

        assertEquals(sumExp, sumAct);
    }

    // ------------------------------------------------------------------------------------------------------------------

    private static void configureCheckpointing(CheckpointConfig config) {
        config.setCheckpointInterval(100);
        config.setCheckpointingConsistencyMode(CheckpointingMode.EXACTLY_ONCE);
        config.enableUnalignedCheckpoints(true);
    }

    private static JobGraph createJobGraphWithOperatorState(
            int parallelism, int maxParallelism, OperatorCheckpointMethod checkpointMethod) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        configureCheckpointing(env.getCheckpointConfig());
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(maxParallelism);
        RestartStrategyUtils.configureNoRestartStrategy(env);

        StateSourceBase.workStartedLatch = new CountDownLatch(parallelism);

        SourceFunction<Integer> src;

        switch (checkpointMethod) {
            case CHECKPOINTED_FUNCTION:
                src = new PartitionedStateSource(false);
                break;
            case CHECKPOINTED_FUNCTION_BROADCAST:
                src = new PartitionedStateSource(true);
                break;
            case NON_PARTITIONED:
                src = new NonPartitionedStateSource();
                break;
            default:
                throw new IllegalArgumentException(checkpointMethod.name());
        }

        DataStream<Integer> input = env.addSource(src);

        input.sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    public static JobGraph createJobGraphWithKeyedState(
            Configuration configuration,
            int parallelism,
            int maxParallelism,
            int numberKeys,
            int numberElements) {
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        env.setParallelism(parallelism);
        if (0 < maxParallelism) {
            env.getConfig().setMaxParallelism(maxParallelism);
        }

        configureCheckpointing(env.getCheckpointConfig());
        RestartStrategyUtils.configureNoRestartStrategy(env);
        env.getConfig().setUseSnapshotCompression(true);

        DataStream<Integer> input =
                env.addSource(new SubtaskIndexSource(numberKeys, numberElements, parallelism))
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID =
                                            -7952298871120320940L;

                                    @Override
                                    public Integer getKey(Integer value) {
                                        return value;
                                    }
                                });

        SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new SubtaskIndexFlatMapper(numberElements));

        result.addSink(new CollectionSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static JobGraph createJobGraphWithKeyedAndNonPartitionedOperatorState(
            int parallelism,
            int maxParallelism,
            int fixedParallelism,
            int numberKeys,
            int numberElements,
            int numberElementsAfterRestart) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(maxParallelism);
        configureCheckpointing(env.getCheckpointConfig());
        RestartStrategyUtils.configureNoRestartStrategy(env);

        DataStream<Integer> input =
                env.addSource(
                                new SubtaskIndexNonPartitionedStateSource(
                                        numberKeys,
                                        numberElements,
                                        numberElementsAfterRestart,
                                        parallelism))
                        .setParallelism(fixedParallelism)
                        .setMaxParallelism(fixedParallelism)
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID =
                                            -7952298871120320940L;

                                    @Override
                                    public Integer getKey(Integer value) {
                                        return value;
                                    }
                                });

        SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new SubtaskIndexFlatMapper(numberElements));

        result.addSink(new CollectionSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class SubtaskIndexSource extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = -400066323594122516L;

        private final int numberKeys;

        private final int originalParallelism;
        protected int numberElements;

        protected int counter = 0;

        private boolean running = true;

        private static final OneShotLatch SOURCE_LATCH = new OneShotLatch();

        SubtaskIndexSource(int numberKeys, int numberElements, int originalParallelism) {
            this.numberKeys = numberKeys;
            this.numberElements = numberElements;
            this.originalParallelism = originalParallelism;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            RuntimeContext runtimeContext = getRuntimeContext();
            final int subtaskIndex = runtimeContext.getTaskInfo().getIndexOfThisSubtask();

            boolean isRestartedOrRescaled =
                    runtimeContext.getTaskInfo().getNumberOfParallelSubtasks()
                                    != originalParallelism
                            || runtimeContext.getTaskInfo().getAttemptNumber() > 0;
            while (running) {
                SOURCE_LATCH.await();
                if (counter < numberElements) {
                    synchronized (ctx.getCheckpointLock()) {
                        for (int value = subtaskIndex;
                                value < numberKeys;
                                value +=
                                        runtimeContext
                                                .getTaskInfo()
                                                .getNumberOfParallelSubtasks()) {
                            ctx.collect(value);
                        }

                        counter++;
                    }
                } else {
                    if (isRestartedOrRescaled) {
                        running = false;
                    } else {
                        Thread.sleep(100);
                    }
                }
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class SubtaskIndexNonPartitionedStateSource extends SubtaskIndexSource
            implements ListCheckpointed<Integer> {

        private static final long serialVersionUID = 8388073059042040203L;
        private final int numElementsAfterRestart;

        SubtaskIndexNonPartitionedStateSource(
                int numberKeys,
                int numberElements,
                int numElementsAfterRestart,
                int originalParallelism) {
            super(numberKeys, numberElements, originalParallelism);
            this.numElementsAfterRestart = numElementsAfterRestart;
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(this.counter);
        }

        @Override
        public void restoreState(List<Integer> state) {
            if (state.size() != 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.counter = state.get(0);
            this.numberElements += numElementsAfterRestart;
        }
    }

    private static class SubtaskIndexFlatMapper
            extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 5273172591283191348L;

        private static CountDownLatch workCompletedLatch = new CountDownLatch(1);

        private transient ValueState<Integer> counter;
        private transient ValueState<Integer> sum;

        private final int numberElements;

        SubtaskIndexFlatMapper(int numberElements) {
            this.numberElements = numberElements;
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {

            int count = counter.value() + 1;
            counter.update(count);

            int s = sum.value() + value;
            sum.update(s);

            if (count % numberElements == 0) {
                out.collect(
                        Tuple2.of(getRuntimeContext().getTaskInfo().getIndexOfThisSubtask(), s));
                workCompletedLatch.countDown();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) {
            // all managed, nothing to do.
        }

        @Override
        public void initializeState(FunctionInitializationContext context) {
            counter =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("counter", Integer.class, 0));
            sum =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("sum", Integer.class, 0));
        }
    }

    private static class CollectionSink<IN> implements SinkFunction<IN> {

        private static final Set<Object> elements =
                Collections.newSetFromMap(new ConcurrentHashMap<>());

        private static final long serialVersionUID = -1652452958040267745L;

        public static <IN> Set<IN> getElementsSet() {
            return (Set<IN>) elements;
        }

        public static void clearElementsSet() {
            elements.clear();
        }

        @Override
        public void invoke(IN value) {
            elements.add(value);
        }
    }

    private static class StateSourceBase extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 7512206069681177940L;
        private static CountDownLatch workStartedLatch = new CountDownLatch(1);
        private static CountDownLatch canFinishLatch = new CountDownLatch(0);

        protected volatile int counter = 0;
        protected volatile boolean running = true;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    ++counter;
                    ctx.collect(1);
                }

                Thread.sleep(2);

                if (counter == 10) {
                    workStartedLatch.countDown();
                }

                if (counter >= 500) {
                    break;
                }
            }

            canFinishLatch.await();
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class NonPartitionedStateSource extends StateSourceBase
            implements ListCheckpointed<Integer> {

        private static final long serialVersionUID = -8108185918123186841L;

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) {
            return Collections.singletonList(this.counter);
        }

        @Override
        public void restoreState(List<Integer> state) {
            if (!state.isEmpty()) {
                this.counter = state.get(0);
            }
        }
    }

    private static class PartitionedStateSource extends StateSourceBase
            implements CheckpointedFunction {

        private static final long serialVersionUID = -359715965103593462L;
        private static final int NUM_PARTITIONS = 7;

        private transient ListState<Integer> counterPartitions;
        private final boolean broadcast;

        private static final ConcurrentHashMap<Long, int[]> checkCorrectSnapshots =
                new ConcurrentHashMap<>();
        private static int[] checkCorrectSnapshot;
        private static int[] checkCorrectRestore;

        public PartitionedStateSource(boolean broadcast) {
            this.broadcast = broadcast;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            if (getRuntimeContext().getTaskInfo().getAttemptNumber() == 0) {
                int[] snapshot =
                        checkCorrectSnapshots.computeIfAbsent(
                                context.getCheckpointId(),
                                (x) -> new int[checkCorrectRestore.length]);
                snapshot[getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()] = counter;
            }

            counterPartitions.clear();

            int div = counter / NUM_PARTITIONS;
            int mod = counter % NUM_PARTITIONS;

            for (int i = 0; i < NUM_PARTITIONS; ++i) {
                int partitionValue = div;
                if (mod > 0) {
                    --mod;
                    ++partitionValue;
                }
                counterPartitions.add(partitionValue);
            }
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            if (broadcast) {
                this.counterPartitions =
                        context.getOperatorStateStore()
                                .getUnionListState(
                                        new ListStateDescriptor<>(
                                                "counter_partitions", IntSerializer.INSTANCE));
            } else {
                this.counterPartitions =
                        context.getOperatorStateStore()
                                .getListState(
                                        new ListStateDescriptor<>(
                                                "counter_partitions", IntSerializer.INSTANCE));
            }

            if (context.isRestored()) {
                for (int v : counterPartitions.get()) {
                    counter += v;
                }
                checkCorrectRestore[getRuntimeContext().getTaskInfo().getIndexOfThisSubtask()] =
                        counter;
                context.getRestoredCheckpointId()
                        .ifPresent((id) -> checkCorrectSnapshot = checkCorrectSnapshots.get(id));
            }
        }
    }
}
