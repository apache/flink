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

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
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
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.flink.runtime.jobgraph.SavepointRestoreSettings.forPath;
import static org.apache.flink.runtime.testutils.CommonTestUtils.getLatestCompletedCheckpointPath;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Test checkpoint rescaling for incremental rocksdb. The implementations of
 * NotifyingDefiniteKeySource, SubtaskIndexFlatMapper and CollectionSink refer to RescalingITCase,
 * because the static fields in these classes can not be shared.
 */
public class RescaleCheckpointManuallyITCase extends TestLogger {

    private static final int NUM_TASK_MANAGERS = 2;
    private static final int SLOTS_PER_TASK_MANAGER = 2;

    private static MiniClusterWithClientResource cluster;
    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @ClassRule public static TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Before
    public void setup() throws Exception {
        Configuration config = new Configuration();
        config.setString(StateBackendOptions.STATE_BACKEND, "rocksdb");
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

        MiniCluster miniCluster = cluster.getMiniCluster();
        String checkpointPath =
                runJobAndGetCheckpoint(
                        numberKeys, numberElements, parallelism, maxParallelism, miniCluster);

        assertNotNull(checkpointPath);

        restoreAndAssert(
                parallelism2,
                maxParallelism,
                numberKeys,
                numberElements2,
                numberElements + numberElements2,
                miniCluster,
                checkpointPath);
    }

    private String runJobAndGetCheckpoint(
            int numberKeys,
            int numberElements,
            int parallelism,
            int maxParallelism,
            MiniCluster miniCluster)
            throws Exception {
        try {
            JobGraph jobGraph =
                    createJobGraphWithKeyedState(
                            parallelism,
                            maxParallelism,
                            numberKeys,
                            numberElements,
                            numberElements,
                            true,
                            100,
                            miniCluster);
            miniCluster.submitJob(jobGraph).get();
            miniCluster.requestJobResult(jobGraph.getJobID()).get();
            // The elements may not all be sent to sink when unaligned checkpoints enabled(refer to
            // FLINK-26882 for more details).
            // Don't verify current state here.
            return getLatestCompletedCheckpointPath(jobGraph.getJobID(), miniCluster)
                    .orElseThrow(
                            () ->
                                    new IllegalStateException(
                                            "Cannot get completed checkpoint, job failed before completing checkpoint"));
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
            MiniCluster miniCluster,
            String restorePath)
            throws Exception {
        try {
            JobGraph scaledJobGraph =
                    createJobGraphWithKeyedState(
                            restoreParallelism,
                            maxParallelism,
                            numberKeys,
                            numberElements,
                            numberElementsExpect,
                            false,
                            100,
                            miniCluster);

            scaledJobGraph.setSavepointRestoreSettings(forPath(restorePath));

            miniCluster.submitJob(scaledJobGraph).get();
            miniCluster.requestJobResult(scaledJobGraph.getJobID()).get();

            Set<Tuple2<Integer, Integer>> actualResult = CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
                expectedResult.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelism, restoreParallelism, keyGroupIndex),
                                key * numberElementsExpect));
            }
            assertEquals(expectedResult, actualResult);
        } finally {
            CollectionSink.clearElementsSet();
        }
    }

    private JobGraph createJobGraphWithKeyedState(
            int parallelism,
            int maxParallelism,
            int numberKeys,
            int numberElements,
            int numberElementsExpect,
            boolean failAfterEmission,
            int checkpointingInterval,
            MiniCluster miniCluster)
            throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(parallelism);
        if (0 < maxParallelism) {
            env.getConfig().setMaxParallelism(maxParallelism);
        }
        env.enableCheckpointing(checkpointingInterval);
        env.getCheckpointConfig().setCheckpointStorage(temporaryFolder.newFolder().toURI());
        env.getCheckpointConfig()
                .setExternalizedCheckpointCleanup(
                        CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.getConfig().setUseSnapshotCompression(true);

        SharedReference<JobID> jobID = sharedObjects.add(new JobID());
        SharedReference<MiniCluster> miniClusterRef = sharedObjects.add(miniCluster);
        DataStream<Integer> input =
                env.addSource(
                                new NotifyingDefiniteKeySource(
                                        numberKeys, numberElements, failAfterEmission) {
                                    @Override
                                    public void waitCheckpointCompleted() throws Exception {
                                        Optional<String> mostRecentCompletedCheckpointPath =
                                                getLatestCompletedCheckpointPath(
                                                        jobID.get(), miniClusterRef.get());
                                        while (!mostRecentCompletedCheckpointPath.isPresent()) {
                                            Thread.sleep(50);
                                            mostRecentCompletedCheckpointPath =
                                                    getLatestCompletedCheckpointPath(
                                                            jobID.get(), miniClusterRef.get());
                                        }
                                    }
                                })
                        .keyBy(
                                new KeySelector<Integer, Integer>() {
                                    private static final long serialVersionUID = 1L;

                                    @Override
                                    public Integer getKey(Integer value) {
                                        return value;
                                    }
                                });
        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new SubtaskIndexFlatMapper(numberElementsExpect));

        result.addSink(new CollectionSink<>());

        return env.getStreamGraph().getJobGraph(jobID.get());
    }

    private static class NotifyingDefiniteKeySource extends RichParallelSourceFunction<Integer> {

        private static final long serialVersionUID = 1L;

        private final int numberKeys;
        protected final int numberElements;
        private final boolean failAfterEmission;
        protected int counter = 0;
        private boolean running = true;

        public NotifyingDefiniteKeySource(
                int numberKeys, int numberElements, boolean failAfterEmission) {
            Preconditions.checkState(numberElements > 0);
            this.numberKeys = numberKeys;
            this.numberElements = numberElements;
            this.failAfterEmission = failAfterEmission;
        }

        public void waitCheckpointCompleted() throws Exception {}

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
                    waitCheckpointCompleted();
                    if (failAfterEmission) {
                        throw new FlinkRuntimeException(
                                "Make job fail artificially, to retain completed checkpoint.");
                    } else {
                        running = false;
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

            if (count == numberElements) {
                out.collect(Tuple2.of(getRuntimeContext().getIndexOfThisSubtask(), s));
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
