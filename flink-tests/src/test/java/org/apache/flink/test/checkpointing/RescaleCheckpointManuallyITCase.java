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
import org.apache.flink.test.checkpointing.utils.RescalingTestUtils;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.test.util.TestUtils;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.HashSet;
import java.util.Set;
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

        cluster.before();

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
            JobGraph jobGraph =
                    createJobGraphWithKeyedState(
                            parallelism, maxParallelism, numberKeys, numberElements, false, 100);
            NotifyingDefiniteKeySource.sourceLatch = new CountDownLatch(parallelism);
            client.submitJob(jobGraph).get();
            NotifyingDefiniteKeySource.sourceLatch.await();

            RescalingTestUtils.SubtaskIndexFlatMapper.workCompletedLatch.await();

            // verify the current state
            Set<Tuple2<Integer, Integer>> actualResult =
                    RescalingTestUtils.CollectionSink.getElementsSet();

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
            NotifyingDefiniteKeySource.sourceLatch.await();

            TestUtils.waitUntilExternalizedCheckpointCreated(checkpointDir);
            client.cancel(jobGraph.getJobID()).get();
            TestUtils.waitUntilJobCanceled(jobGraph.getJobID(), client);
            return TestUtils.getMostRecentCompletedCheckpoint(checkpointDir).getAbsolutePath();
        } finally {
            RescalingTestUtils.CollectionSink.clearElementsSet();
        }
    }

    private void restoreAndAssert(
            int restoreParallelism,
            int restoreMaxParallelism,
            int maxParallelismBefore,
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
                            restoreMaxParallelism,
                            numberKeys,
                            numberElements,
                            true,
                            100);

            scaledJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(restorePath));

            submitJobAndWaitForResult(client, scaledJobGraph, getClass().getClassLoader());

            Set<Tuple2<Integer, Integer>> actualResult2 =
                    RescalingTestUtils.CollectionSink.getElementsSet();

            Set<Tuple2<Integer, Integer>> expectedResult2 = new HashSet<>();

            for (int key = 0; key < numberKeys; key++) {
                int keyGroupIndex =
                        KeyGroupRangeAssignment.assignToKeyGroup(key, maxParallelismBefore);
                expectedResult2.add(
                        Tuple2.of(
                                KeyGroupRangeAssignment.computeOperatorIndexForKeyGroup(
                                        maxParallelismBefore, restoreParallelism, keyGroupIndex),
                                key * numberElementsExpect));
            }
            assertEquals(expectedResult2, actualResult2);
        } finally {
            RescalingTestUtils.CollectionSink.clearElementsSet();
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
                                    private static final long serialVersionUID =
                                            -7952298871120320940L;

                                    @Override
                                    public Integer getKey(Integer value) throws Exception {
                                        return value;
                                    }
                                });
        RescalingTestUtils.SubtaskIndexFlatMapper.workCompletedLatch =
                new CountDownLatch(numberKeys);

        DataStream<Tuple2<Integer, Integer>> result =
                input.flatMap(new RescalingTestUtils.SubtaskIndexFlatMapper(numberElements));

        result.addSink(new RescalingTestUtils.CollectionSink<>());

        return env.getStreamGraph().getJobGraph();
    }

    private static class NotifyingDefiniteKeySource extends RescalingTestUtils.DefiniteKeySource {
        private static final long serialVersionUID = 8120981235081181746L;

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
}
