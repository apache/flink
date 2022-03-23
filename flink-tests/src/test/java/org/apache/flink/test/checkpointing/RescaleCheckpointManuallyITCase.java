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

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.KeyGroupRangeAssignment;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.checkpointing.utils.RescalingTestUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

import static org.apache.flink.test.util.TestUtils.submitJobAndWaitForResult;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/** Test checkpoint rescaling for incremental rocksdb. */
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
        StreamExecutionEnvironment env = createEnvironment(parallelism, maxParallelism, 100);

        String checkpointPath =
                runJobAndGetCheckpoint(
                        numberKeys, numberElements, parallelism, env, client, checkpointDir);

        assertNotNull(checkpointPath);
        env.setParallelism(parallelism2);
        JobGraph scaledJobGraph =
                createJobGraphWithKeyedState(numberKeys, numberElements2, true, env);

        scaledJobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(checkpointPath));
        submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());

        Set<Tuple2<Integer, Integer>> actualResult2 = CollectionSink.getElementsSet();
        assertEquals(
                getExpectedResult(
                        numberKeys, maxParallelism, parallelism2, numberElements + numberElements2),
                actualResult2);
    }

    private static String runJobAndGetCheckpoint(
            int numberKeys,
            int numberElements,
            int parallelism,
            StreamExecutionEnvironment env,
            ClusterClient<?> client,
            File checkpointDir)
            throws Exception {
        try {
            JobGraph jobGraph =
                    createJobGraphWithKeyedState(numberKeys, numberElements, false, env);
            NotifyingDefiniteKeySource.sourceLatch = new CountDownLatch(parallelism);
            client.submitJob(jobGraph).get();
            NotifyingDefiniteKeySource.sourceLatch.await();

            while (CollectionSink.getElementsSet().size() < numberKeys) {
                Thread.sleep(50);
            }

            TestUtils.waitUntilExternalizedCheckpointCreated(checkpointDir);
            client.cancel(jobGraph.getJobID()).get();
            TestUtils.waitUntilJobCanceled(jobGraph.getJobID(), client);
            return TestUtils.getMostRecentCompletedCheckpoint(checkpointDir).getAbsolutePath();
        } finally {
            CollectionSink.clearElementsSet();
        }
    }

    private static JobGraph createJobGraphWithKeyedState(
            int numberKeys,
            int numberElements,
            boolean terminateAfterEmission,
            StreamExecutionEnvironment env) {
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
        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new RescalingTestUtils.SubtaskIndexFlatMapper(numberElements));

        result.addSink(new CollectionSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static StreamExecutionEnvironment createEnvironment(
            int parallelism, int maxParallelism, int checkpointingInterval) {
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
        return env;
    }

    private static Set getExpectedResult(
            int numberKeys, int maxParallelism, int parallelism, int numberElements) {
        Set<Tuple2<Integer, Integer>> expectedResult = new HashSet<>();

        for (int key = 0; key < numberKeys; key++) {
            int keyGroupIndex = KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelism);
            expectedResult.add(
                    Tuple2.of(
                            KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                    maxParallelism, parallelism, keyGroupIndex),
                            key * numberElements));
        }
        return expectedResult;
    }

    private static class NotifyingDefiniteKeySource extends RescalingTestUtils.DefiniteKeySource {
        private static final long serialVersionUID = 1L;

        private static CountDownLatch sourceLatch;

        public NotifyingDefiniteKeySource(
                int numberKeys, int numberElements, boolean terminateAfterEmission) {
            super(numberKeys, numberElements, terminateAfterEmission);
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            if (sourceLatch != null) {
                sourceLatch.countDown();
            }
            super.run(ctx);
        }
    }

    /**
     * A duplicate sink from RescalingITCase.CollectionSink, because the static elements in this
     * class can not be shared.
     */
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
        public void invoke(IN value) throws Exception {
            elements.add(value);
        }
    }
}
