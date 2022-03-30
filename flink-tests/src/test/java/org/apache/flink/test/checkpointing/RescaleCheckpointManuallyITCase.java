/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.time.Deadline;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.time.Duration;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Test checkpoint rescaling for incremental rocksdb. The implementations of
 * NotifyingDefiniteKeySource, SubtaskIndexFlatMapper and CollectionSink refer to RescalingITCase,
 * because the static fields in these classes can not be shared.
 */
public class RescaleCheckpointManuallyITCase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 2;
    private static final int SLOTS_PER_TASK_MANAGER = 2;

    private static MiniClusterWithClientResource cluster;
    private File checkpointDir;

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Configuration config = new Configuration();

        checkpointDir = temporaryFolder.newFolder();

        config.setString(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.setString(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI().toString());
        config.setBoolean(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, true);

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(config)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(SLOTS_PER_TASK_MANAGER)
                                .build());
        cluster.before();
    }

    @After
    public void shutDownExistingCluster() {
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
        final int numberElements2 = 500;
        final int parallelism = scaleOut ? 3 : 4;
        final int parallelism2 = scaleOut ? 4 : 3;
        final int maxParallelism = 13;

        ClusterClient<?> client = cluster.getClusterClient();
        String checkpointPath =
                runJobAndGetCheckpoint(
                        numberKeys,
                        numberElements,
                        parallelism,
                        maxParallelism,
                        client,
                        checkpointDir);

        assertNotNull(checkpointPath);

        restoreAndAssert(
                parallelism2,
                maxParallelism,
                numberKeys,
                numberElements2,
                numberElements + numberElements2,
                client,
                checkpointPath);
    }

    private static String runJobAndGetCheckpoint(
            int numberKeys,
            int numberElements,
            int parallelism,
            int maxParallelism,
            ClusterClient<?> client,
            File checkpointDir)
            throws Exception {
        try {
            Duration timeout = Duration.ofMinutes(5);
            Deadline deadline = Deadline.now().plus(timeout);

            JobGraph jobGraph =
                    createJobGraphWithKeyedState(
                            parallelism, maxParallelism, numberKeys, numberElements, false, 100);
            client.submitJob(jobGraph).get();

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

            // ensure contents of state within SubtaskIndexFlatMapper are all included
            // in the last checkpoint (refer to FLINK-26882 for more details).
            cluster.getMiniCluster().triggerCheckpoint(jobGraph.getJobID()).get();

            client.cancel(jobGraph.getJobID()).get();
            TestUtils.waitUntilJobCanceled(jobGraph.getJobID(), client);
            return TestUtils.getMostRecentCompletedCheckpoint(checkpointDir).getAbsolutePath();
        } finally {
            CollectionSink.clearElementsSet();
        }
    }

    private void restoreAndAssert(
            int restoreParallelism,
            int maxParallelism,
            int numberKeys,
            int numberElements,
            int numberElementsExpect,
            ClusterClient<?> client,
            String restorePath)
            throws Exception {
        try {

            JobGraph scaledJobGraph =
                    createJobGraphWithKeyedState(
                            restoreParallelism,
                            maxParallelism,
                            numberKeys,
                            numberElements,
                            true,
                            100);

            scaledJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(restorePath));

            submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());

            Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                expectedResult2.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, restoreParallelism, keyGroupIndex),
                                key * numberElementsExpect));
            }
            assertEquals(expectedResult2, actualResult2);
        } finally {
            CollectionSink.clearElementsSet();
        }
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
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setUseSnapshotCompression(true);

        DataStream<Integer> input =
                env.addSource(
                                new NotifyingDefiniteKeySource(
                                        numberKeys, numberElements, terminateAfterEmission))
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                });
        SubtaskIndexFlatMapper.workCompletedLatch = new CountDownLatch(numberKeys);

        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new SubtaskIndexFlatMapper(numberElements));

        result.addSink(new CollectionSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class NotifyingDefiniteKeySource extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 1L;

        private final int numberKeys;
        private final int numberElements;
        private final boolean terminateAfterEmission;

        protected int counter = 0;

        private boolean running = true;

        public NotifyingDefiniteKeySource(
                int numberKeys, int numberElements, boolean terminateAfterEmission) {
            this.numberKeys = numberKeys;
            this.numberElements = numberElements;
            this.terminateAfterEmission = terminateAfterEmission;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            final int subtaskIndex = getRuntimeContext().getIndexOfThisSubtask();
            while (running) {

                if (counter < numberElements) {
                    synchronized (ctx.getCheckpointLock()) {
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

    private static class SubtaskIndexFlatMapper
            extends RichFlatMapFunction<Integer, Tuple2<Integer, Integer>>
            implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        public static CountDownLatch workCompletedLatch = new CountDownLatch(1);

        private transient ValueState<Integer> counter;
        private transient ValueState<Integer> sum;

        private final int numberElements;

        public SubtaskIndexFlatMapper(int numberElements) {
            this.numberElements = numberElements;
        }

        @Override
        public void flatMap(Integer value, Collector<Tuple2<Integer, Integer>> out)
                throws Exception {
            Integer counterValue = counter.value();
            int count = counterValue == null ? 1 : counterValue + 1;
            counter.update(count);

            Integer sumValue = sum.value();
            int s = sumValue == null ? value : sumValue + value;
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
                            .getState(new ValueStateDescriptor<>("counter", Integer.class));
            sum =
                    context.getKeyedStateStore()
                            .getState(new ValueStateDescriptor<>("sum", Integer.class));
        }
    }

    private static class CollectionSink<IN> implements SinkFunction<IN> {

        private static final Set<Object> elements =
                Collections.newSetFromMap(new ConcurrentHashMap<>());

        private static final long serialVersionUID = 1L;

        @SuppressWarnings("unchecked")
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
}
