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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.NettyShuffleEnvironmentOptions;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.client.JobExecutionException;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.TestingUtils;
import org.apache.flink.testutils.executor.TestExecutorResource;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;
import org.apache.flink.util.concurrent.FixedRetryStrategy;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutorServiceAdapter;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/** Test savepoint rescaling. */
@RunWith(Parameterized.class)
public class RescalingITCase extends TestLogger {

    @ClassRule
    public static final TestExecutorResource<ScheduledExecutorService> EXECUTOR_RESOURCE =
            TestingUtils.defaultExecutorResource();

    private static final int numTaskManagers = 2;
    private static final int slotsPerTaskManager = 2;
    private static final int numSlots = numTaskManagers * slotsPerTaskManager;

    @Parameterized.Parameters(name = "backend = {0}, buffersPerChannel = {1}")
    public static Collection<Object[]> data() {
        return Arrays.asList(
                new Object[][] {
                    {"filesystem", 2}, {"rocksdb", 0}, {"filesystem", 0}, {"rocksdb", 2}
                });
    }

    public RescalingITCase(String backend, int buffersPerChannel) {
        this.backend = backend;
        this.buffersPerChannel = buffersPerChannel;
    }

    private final String backend;

    private final int buffersPerChannel;

    private String currentBackend = null;

    enum OperatorCheckpointMethod {
        NON_PARTITIONED,
        CHECKPOINTED_FUNCTION,
        CHECKPOINTED_FUNCTION_BROADCAST,
        LIST_CHECKPOINTED
    }

    private static MiniClusterWithClientResource cluster;

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        // detect parameter change
        if (currentBackend != backend) {
            shutDownExistingCluster();

            currentBackend = backend;

            Configuration config = new Configuration();

            final File checkpointDir = temporaryFolder.newFolder();
            final File savepointDir = temporaryFolder.newFolder();

            config.setString(StateBackendOptions.STATE_BACKEND, currentBackend);
            config.setString(
                    CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
            config.setString(
                    CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI().toString());
            config.setInteger(
                    NettyShuffleEnvironmentOptions.NETWORK_BUFFERS_PER_CHANNEL, buffersPerChannel);

            cluster =
                    new MiniClusterWithClientResource(
                            new MiniClusterResourceConfiguration.Builder()
                                    .setConfiguration(config)
                                    .setNumberTaskManagers(numTaskManagers)
                                    .setNumberSlotsPerTaskManager(numSlots)
                                    .build());
            cluster.before();
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
    public void testSavepointRescalingInKeyedState() throws Exception {
        testSavepointRescalingKeyedState(false, false);
    }

    @Test
    public void testSavepointRescalingOutKeyedState() throws Exception {
        testSavepointRescalingKeyedState(true, false);
    }

    @Test
    public void testSavepointRescalingInKeyedStateDerivedMaxParallelism() throws Exception {
        testSavepointRescalingKeyedState(false, true);
    }

    @Test
    public void testSavepointRescalingOutKeyedStateDerivedMaxParallelism() throws Exception {
        testSavepointRescalingKeyedState(true, true);
    }

    /**
     * Tests that a job with purely keyed state can be restarted from a savepoint with a different
     * parallelism.
     */
    public void testSavepointRescalingKeyedState(boolean scaleOut, boolean deriveMaxParallelism)
            throws Exception {
        final int numberKeys = 42;
        final int numberElements = 1000;
        final int numberElements2 = 500;
        final int parallelism = scaleOut ? numSlots / 2 : numSlots;
        final int parallelism2 = scaleOut ? numSlots : numSlots / 2;
        final int maxParallelism = 13;

        Duration timeout = Duration.ofMinutes(3);
        Deadline deadline = Deadline.now().plus(timeout);

        ClusterClient<?> client = cluster.getClusterClient();

        try {
            JobGraph jobGraph =
                    createJobGraphWithKeyedState(
                            parallelism, maxParallelism, numberKeys, numberElements, false, 100);

            final JobID jobID = jobGraph.getJobID();

            client.submitJob(jobGraph).get();

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
            CompletableFuture<String> savepointPathFuture =
                    client.triggerSavepoint(jobID, null, SavepointFormatType.CANONICAL);

            final String savepointPath =
                    savepointPathFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

            client.cancel(jobID).get();

            while (!getRunningJobs(client).isEmpty()) {
                Thread.sleep(50);
            }

            int restoreMaxParallelism =
                    deriveMaxParallelism ? JobVertex.MAX_PARALLELISM_DEFAULT : maxParallelism;

            JobGraph scaledJobGraph =
                    createJobGraphWithKeyedState(
                            parallelism2,
                            restoreMaxParallelism,
                            numberKeys,
                            numberElements2,
                            true,
                            100);

            scaledJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath));

            submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());

            Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                expectedResult2.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, parallelism2, keyGroupIndex),
                                key * (numberElements + numberElements2)));
            }

            assertEquals(expectedResult2, actualResult2);

        } finally {
            // clear the CollectionSink set for the restarted job
            CollectionSink.clearElementsSet();
        }
    }

    /**
     * Tests that a job cannot be restarted from a savepoint with a different parallelism if the
     * rescaled operator has non-partitioned state.
     *
     * @throws Exception
     */
    @Test
    public void testSavepointRescalingNonPartitionedStateCausesException() throws Exception {
        final int parallelism = numSlots / 2;
        final int parallelism2 = numSlots;
        final int maxParallelism = 13;

        Duration timeout = Duration.ofMinutes(3);
        Deadline deadline = Deadline.now().plus(timeout);

        ClusterClient<?> client = cluster.getClusterClient();

        try {
            JobGraph jobGraph =
                    createJobGraphWithOperatorState(
                            parallelism, maxParallelism, OperatorCheckpointMethod.NON_PARTITIONED);
            // make sure the job does not finish before we take the savepoint
            StateSourceBase.canFinishLatch = new CountDownLatch(1);

            final JobID jobID = jobGraph.getJobID();

            client.submitJob(jobGraph).get();

            // wait until the operator is started
            waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);
            // wait until the operator handles some data
            StateSourceBase.workStartedLatch.await();

            CompletableFuture<String> savepointPathFuture =
                    client.triggerSavepoint(jobID, null, SavepointFormatType.CANONICAL);

            final String savepointPath =
                    savepointPathFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

            // we took a savepoint, the job can finish now
            StateSourceBase.canFinishLatch.countDown();
            client.cancel(jobID).get();
            while (!getRunningJobs(client).isEmpty()) {
                Thread.sleep(50);
            }

            // job successfully removed
            JobGraph scaledJobGraph =
                    createJobGraphWithOperatorState(
                            parallelism2, maxParallelism, OperatorCheckpointMethod.NON_PARTITIONED);

            scaledJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath));

            submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());
        } catch (JobExecutionException exception) {
            if (exception.getCause() instanceof IllegalStateException) {
                // we expect a IllegalStateException wrapped
                // in a JobExecutionException, because the job containing non-partitioned state
                // is being rescaled
            } else {
                throw exception;
            }
        }
    }

    /**
     * Tests that a job with non partitioned state can be restarted from a savepoint with a
     * different parallelism if the operator with non-partitioned state are not rescaled.
     *
     * @throws Exception
     */
    @Test
    public void testSavepointRescalingWithKeyedAndNonPartitionedState() throws Exception {
        int numberKeys = 42;
        int numberElements = 1000;
        int numberElements2 = 500;
        int parallelism = numSlots / 2;
        int parallelism2 = numSlots;
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
                            false,
                            100);

            final JobID jobID = jobGraph.getJobID();

            // make sure the job does not finish before we take the savepoint
            StateSourceBase.canFinishLatch = new CountDownLatch(1);
            client.submitJob(jobGraph).get();

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
            CompletableFuture<String> savepointPathFuture =
                    client.triggerSavepoint(jobID, null, SavepointFormatType.CANONICAL);

            final String savepointPath =
                    savepointPathFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

            // we took a savepoint, the job can finish now
            StateSourceBase.canFinishLatch.countDown();
            client.cancel(jobID).get();

            while (!getRunningJobs(client).isEmpty()) {
                Thread.sleep(50);
            }

            JobGraph scaledJobGraph =
                    createJobGraphWithKeyedAndNonPartitionedOperatorState(
                            parallelism2,
                            maxParallelism,
                            parallelism,
                            numberKeys,
                            numberElements + numberElements2,
                            true,
                            100);

            scaledJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath));

            submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());

            Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                expectedResult2.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, parallelism2, keyGroupIndex),
                                key * (numberElements + numberElements2)));
            }

            assertEquals(expectedResult2, actualResult2);

        } finally {
            // clear the CollectionSink set for the restarted job
            CollectionSink.clearElementsSet();
        }
    }

    @Test
    public void testSavepointRescalingInPartitionedOperatorState() throws Exception {
        testSavepointRescalingPartitionedOperatorState(
                false, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION);
    }

    @Test
    public void testSavepointRescalingOutPartitionedOperatorState() throws Exception {
        testSavepointRescalingPartitionedOperatorState(
                true, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION);
    }

    @Test
    public void testSavepointRescalingInBroadcastOperatorState() throws Exception {
        testSavepointRescalingPartitionedOperatorState(
                false, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST);
    }

    @Test
    public void testSavepointRescalingOutBroadcastOperatorState() throws Exception {
        testSavepointRescalingPartitionedOperatorState(
                true, OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST);
    }

    @Test
    public void testSavepointRescalingInPartitionedOperatorStateList() throws Exception {
        testSavepointRescalingPartitionedOperatorState(
                false, OperatorCheckpointMethod.LIST_CHECKPOINTED);
    }

    @Test
    public void testSavepointRescalingOutPartitionedOperatorStateList() throws Exception {
        testSavepointRescalingPartitionedOperatorState(
                true, OperatorCheckpointMethod.LIST_CHECKPOINTED);
    }

    /**
     * Tests rescaling of partitioned operator state. More specific, we test the mechanism with
     * {@link ListCheckpointed} as it subsumes {@link
     * org.apache.flink.streaming.api.checkpoint.CheckpointedFunction}.
     */
    public void testSavepointRescalingPartitionedOperatorState(
            boolean scaleOut, OperatorCheckpointMethod checkpointMethod) throws Exception {
        final int parallelism = scaleOut ? numSlots : numSlots / 2;
        final int parallelism2 = scaleOut ? numSlots / 2 : numSlots;
        final int maxParallelism = 13;

        Duration timeout = Duration.ofMinutes(3);
        Deadline deadline = Deadline.now().plus(timeout);

        ClusterClient<?> client = cluster.getClusterClient();

        int counterSize = Math.max(parallelism, parallelism2);

        if (checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION
                || checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST) {
            PartitionedStateSource.checkCorrectSnapshot = new int[counterSize];
            PartitionedStateSource.checkCorrectRestore = new int[counterSize];
        } else {
            PartitionedStateSourceListCheckpointed.checkCorrectSnapshot = new int[counterSize];
            PartitionedStateSourceListCheckpointed.checkCorrectRestore = new int[counterSize];
        }

        try {
            JobGraph jobGraph =
                    createJobGraphWithOperatorState(parallelism, maxParallelism, checkpointMethod);
            // make sure the job does not finish before we take the savepoint
            StateSourceBase.canFinishLatch = new CountDownLatch(1);

            final JobID jobID = jobGraph.getJobID();

            client.submitJob(jobGraph).get();

            // wait until the operator is started
            waitForAllTaskRunning(cluster.getMiniCluster(), jobGraph.getJobID(), false);
            // wait until the operator handles some data
            StateSourceBase.workStartedLatch.await();

            CompletableFuture<String> savepointPathFuture =
                    FutureUtils.retryWithDelay(
                            () ->
                                    client.triggerSavepoint(
                                            jobID, null, SavepointFormatType.CANONICAL),
                            new FixedRetryStrategy(
                                    (int) deadline.timeLeft().getSeconds() / 10,
                                    Duration.ofSeconds(10)),
                            (throwable) -> true,
                            new ScheduledExecutorServiceAdapter(EXECUTOR_RESOURCE.getExecutor()));

            final String savepointPath =
                    savepointPathFuture.get(deadline.timeLeft().toMillis(), TimeUnit.MILLISECONDS);

            // we took a savepoint, the job can finish now
            StateSourceBase.canFinishLatch.countDown();
            client.cancel(jobID).get();
            while (!getRunningJobs(client).isEmpty()) {
                Thread.sleep(50);
            }

            JobGraph scaledJobGraph =
                    createJobGraphWithOperatorState(parallelism2, maxParallelism, checkpointMethod);

            scaledJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath));

            submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());

            int sumExp = 0;
            int sumAct = 0;

            if (checkpointMethod == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION) {
                for (int c : PartitionedStateSource.checkCorrectSnapshot) {
                    sumExp += c;
                }

                for (int c : PartitionedStateSource.checkCorrectRestore) {
                    sumAct += c;
                }
            } else if (checkpointMethod
                    == OperatorCheckpointMethod.CHECKPOINTED_FUNCTION_BROADCAST) {
                for (int c : PartitionedStateSource.checkCorrectSnapshot) {
                    sumExp += c;
                }

                for (int c : PartitionedStateSource.checkCorrectRestore) {
                    sumAct += c;
                }

                sumExp *= parallelism2;
            } else {
                for (int c : PartitionedStateSourceListCheckpointed.checkCorrectSnapshot) {
                    sumExp += c;
                }

                for (int c : PartitionedStateSourceListCheckpointed.checkCorrectRestore) {
                    sumAct += c;
                }
            }

            assertEquals(sumExp, sumAct);
        } finally {
        }
    }

    // ------------------------------------------------------------------------------------------------------------------

    private static JobGraph createJobGraphWithOperatorState(
            int parallelism, int maxParallelism, OperatorCheckpointMethod checkpointMethod) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(maxParallelism);
        env.setRestartStrategy(RestartStrategies.noRestart());

        StateSourceBase.workStartedLatch = new CountDownLatch(parallelism);

        SourceFunction<Integer> src;

        switch (checkpointMethod) {
            case CHECKPOINTED_FUNCTION:
                src = new PartitionedStateSource(false);
                break;
            case CHECKPOINTED_FUNCTION_BROADCAST:
                src = new PartitionedStateSource(true);
                break;
            case LIST_CHECKPOINTED:
                src = new PartitionedStateSourceListCheckpointed();
                break;
            case NON_PARTITIONED:
                src = new NonPartitionedStateSource();
                break;
            default:
                throw new IllegalArgumentException();
        }

        DataStream<Integer> input = env.addSource(src);

        input.sinkTo(new DiscardingSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static JobGraph createJobGraphWithKeyedState(
            int parallelism,
            int maxParallelism,
            int numberKeys,
            int numberElements,
            boolean terminateAfterEmission,
            int checkpointingInterval) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (0 < maxParallelism) {
            env.getConfig().setMaxParallelism(maxParallelism);
        }
        env.enableCheckpointing(checkpointingInterval);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setUseSnapshotCompression(true);

        DataStream<Integer> input =
                env.addSource(
                                new SubtaskIndexSource(
                                        numberKeys, numberElements, terminateAfterEmission))
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID =
                                            -7952298871120320940L;

                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                });

        SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new SubtaskIndexFlatMapper(numberElements));

        result.addSink(new CollectionSink<Tuple2<Integer, Integer>>());

        return env.getStreamGraph().getJobGraph();
    }

    private static JobGraph createJobGraphWithKeyedAndNonPartitionedOperatorState(
            int parallelism,
            int maxParallelism,
            int fixedParallelism,
            int numberKeys,
            int numberElements,
            boolean terminateAfterEmission,
            int checkpointingInterval) {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        env.getConfig().setMaxParallelism(maxParallelism);
        env.enableCheckpointing(checkpointingInterval);
        env.setRestartStrategy(RestartStrategies.noRestart());

        DataStream<Integer> input =
                env.addSource(
                                new SubtaskIndexNonPartitionedStateSource(
                                        numberKeys, numberElements, terminateAfterEmission))
                        .setParallelism(fixedParallelism)
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID =
                                            -7952298871120320940L;

                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                });

        SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new SubtaskIndexFlatMapper(numberElements));

        result.addSink(new CollectionSink<Tuple2<Integer, Integer>>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class SubtaskIndexSource extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = -400066323594122516L;

        private final int numberKeys;
        private final int numberElements;
        private final boolean terminateAfterEmission;

        protected int counter = 0;

        private boolean running = true;

        SubtaskIndexSource(int numberKeys, int numberElements, boolean terminateAfterEmission) {

            this.numberKeys = numberKeys;
            this.numberElements = numberElements;
            this.terminateAfterEmission = terminateAfterEmission;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            final Object lock = ctx.getCheckpointLock();
            final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();

            while (running) {

                if (counter < numberElements) {
                    synchronized (lock) {
                        for (int value = subtaskIndex;
                                value < numberKeys;
                                value += getRuntimeContext().getNumberOfParallelSubtasks()) {

                            ctx.collect(value);
                        }

                        counter++;
                    }
                } else {
                    if (terminateAfterEmission) {
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

        SubtaskIndexNonPartitionedStateSource(
                int numberKeys, int numberElements, boolean terminateAfterEmission) {
            super(numberKeys, numberElements, terminateAfterEmission);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.counter);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (state.isEmpty() || state.size() > 1) {
                throw new RuntimeException(
                        "Test failed due to unexpected recovered state size " + state.size());
            }
            this.counter = state.get(0);
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
                out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), s));
                workCompletedLatch.countDown();
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            // all managed, nothing to do.
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            counter =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("counter", Integer.class, 0));
            sum =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("sum", Integer.class, 0));
        }
    }

    private static class CollectionSink<IN> implements SinkFunction<IN> {

        private static Set<Object> elements =
                Collections.newSetFromMap(new ConcurrentHashMap<Object, Boolean>());

        private static final long serialVersionUID = -1652452958040267745L;

        public static <IN> Set<IN> getElementsSet() {
            return (Set<IN>) elements;
        }

        public static void clearElementsSet() {
            elements.clear();
        }

        @Override
        public void invoke(IN value) throws Exception {
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
            final Object lock = ctx.getCheckpointLock();

            while (running) {
                synchronized (lock) {
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
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
            return Collections.singletonList(this.counter);
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            if (!state.isEmpty()) {
                this.counter = state.get(0);
            }
        }
    }

    private static class PartitionedStateSourceListCheckpointed extends StateSourceBase
            implements ListCheckpointed<Integer> {

        private static final long serialVersionUID = -4357864582992546L;
        private static final int NUM_PARTITIONS = 7;

        private static int[] checkCorrectSnapshot;
        private static int[] checkCorrectRestore;

        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {

            checkCorrectSnapshot[getRuntimeContext().getIndexOfThisSubtask()] = counter;

            int div = counter / NUM_PARTITIONS;
            int mod = counter % NUM_PARTITIONS;

            List<Integer> split = new ArrayList<>();
            for (int i = 0; i < NUM_PARTITIONS; ++i) {
                int partitionValue = div;
                if (mod > 0) {
                    --mod;
                    ++partitionValue;
                }
                split.add(partitionValue);
            }
            return split;
        }

        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer v : state) {
                counter += v;
            }
            checkCorrectRestore[getRuntimeContext().getIndexOfThisSubtask()] = counter;
        }
    }

    private static class PartitionedStateSource extends StateSourceBase
            implements CheckpointedFunction {

        private static final long serialVersionUID = -359715965103593462L;
        private static final int NUM_PARTITIONS = 7;

        private transient ListState<Integer> counterPartitions;
        private boolean broadcast;

        private static int[] checkCorrectSnapshot;
        private static int[] checkCorrectRestore;

        public PartitionedStateSource(boolean broadcast) {
            this.broadcast = broadcast;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {

            counterPartitions.clear();

            checkCorrectSnapshot[getRuntimeContext().getIndexOfThisSubtask()] = counter;

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
                checkCorrectRestore[getRuntimeContext().getIndexOfThisSubtask()] = counter;
            }
        }
    }

    private static List<JobID> getRunningJobs(ClusterClient<?> client) throws Exception {
        Collection<JobStatusMessage> statusMessages = client.listJobs().get();
        return statusMessages.stream()
                .filter(status -> !status.getJobState().isGloballyTerminalState())
                .map(JobStatusMessage::getJobId)
                .collect(Collectors.toList());
    }
}
