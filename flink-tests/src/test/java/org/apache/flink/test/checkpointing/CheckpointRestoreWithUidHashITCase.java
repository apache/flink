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
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.runtime.OperatorIDPair;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.stream.IntStream;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;

/**
 * Verifies that with {@code uidHash} a job could restore state from an existing savepoint, and the
 * job would still be able to restored from the checkpoints taken after restarted correctly without
 * losing states due to mismatched operator id.
 */
public class CheckpointRestoreWithUidHashITCase {

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private SharedReference<CountDownLatch> startWaitingForCheckpointLatch;

    private SharedReference<List<Integer>> result;

    @Before
    public void setup() {
        startWaitingForCheckpointLatch = sharedObjects.add(new CountDownLatch(1));
        result = sharedObjects.add(new ArrayList<>());
    }

    @Test
    public void testRestoreFromSavepointBySetUidHash() throws Exception {
        final int maxNumber = 100;

        try (MiniCluster miniCluster = new MiniCluster(createMiniClusterConfig())) {
            miniCluster.start();

            StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
            JobGraph firstJob =
                    createJobGraph(
                            env,
                            StatefulSourceBehavior.HOLD_AFTER_CHECKPOINT_ON_FIRST_RUN,
                            maxNumber,
                            "test-uid",
                            null,
                            null);
            JobID jobId = miniCluster.submitJob(firstJob).get().getJobID();
            waitForAllTaskRunning(miniCluster, jobId, false);

            // The source would emit some records and start waiting for the checkpoint to happen.
            // With this latch we ensures the savepoint happens in a fixed position and no following
            // records are emitted after savepoint is triggered.
            startWaitingForCheckpointLatch.get().await();
            String savepointPath =
                    miniCluster
                            .triggerSavepoint(jobId, TMP_FOLDER.newFolder().getAbsolutePath(), true)
                            .get();

            // Get the operator id
            List<OperatorIDPair> operatorIds =
                    firstJob.getVerticesSortedTopologicallyFromSources().get(0).getOperatorIDs();
            OperatorIDPair sourceOperatorIds = operatorIds.get(operatorIds.size() - 1);

            JobGraph secondJob =
                    createJobGraph(
                            env,
                            StatefulSourceBehavior.PROCESS_ONLY,
                            maxNumber,
                            null,
                            sourceOperatorIds.getGeneratedOperatorID().toHexString(),
                            savepointPath);
            miniCluster.executeJobBlocking(secondJob);
        }
        assertThat(result.get(), contains(IntStream.range(0, maxNumber).boxed().toArray()));
    }

    @Test
    public void testRestoreCheckpointAfterFailoverWithUidHashSet() throws Exception {
        final int maxNumber = 100;

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(2, 500));
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        JobGraph jobGraph =
                createJobGraph(
                        env,
                        StatefulSourceBehavior.FAIL_AFTER_CHECKPOINT_ON_FIRST_RUN,
                        maxNumber,
                        null,
                        new OperatorID().toHexString(),
                        null);

        try (MiniCluster miniCluster = new MiniCluster(createMiniClusterConfig())) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
        assertThat(result.get(), contains(IntStream.range(0, maxNumber).boxed().toArray()));
    }

    private MiniClusterConfiguration createMiniClusterConfig() {
        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        return new MiniClusterConfiguration.Builder()
                .setNumTaskManagers(1)
                .setNumSlotsPerTaskManager(1)
                .setConfiguration(config)
                .build();
    }

    private JobGraph createJobGraph(
            StreamExecutionEnvironment env,
            StatefulSourceBehavior behavior,
            int maxNumber,
            @Nullable String uid,
            @Nullable String uidHash,
            @Nullable String savepointPath) {

        SingleOutputStreamOperator<Integer> source =
                env.addSource(
                                new StatefulSource(
                                        behavior, maxNumber, startWaitingForCheckpointLatch))
                        .setParallelism(1);
        if (uid != null) {
            source = source.uid(uid);
        }

        if (uidHash != null) {
            source = source.setUidHash(uidHash);
        }

        source.addSink(new CollectSink(result)).setParallelism(1);
        JobGraph jobGraph = env.getStreamGraph().getJobGraph();
        if (savepointPath != null) {
            jobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath, false));
        }

        return jobGraph;
    }

    private enum StatefulSourceBehavior {
        PROCESS_ONLY(false),
        HOLD_AFTER_CHECKPOINT_ON_FIRST_RUN(true),
        FAIL_AFTER_CHECKPOINT_ON_FIRST_RUN(true);

        boolean waitForCheckpointOnFirstRun;

        StatefulSourceBehavior(boolean waitForCheckpointOnFirstRun) {
            this.waitForCheckpointOnFirstRun = waitForCheckpointOnFirstRun;
        }
    }

    private static class StatefulSource extends RichSourceFunction<Integer>
            implements CheckpointedFunction, CheckpointListener {

        private final StatefulSourceBehavior behavior;

        private final int maxNumber;

        private final SharedReference<CountDownLatch> startWaitingForCheckpointLatch;

        private ListState<Integer> nextNumberState;

        private int nextNumber;

        private volatile boolean isCanceled;

        private volatile boolean isWaiting;

        private volatile long firstCheckpointIdAfterWaiting;

        private volatile boolean checkpointCompletedAfterWaiting;

        public StatefulSource(
                StatefulSourceBehavior behavior,
                int maxNumber,
                SharedReference<CountDownLatch> startWaitingForCheckpointLatch) {
            this.behavior = behavior;
            this.maxNumber = maxNumber;
            this.startWaitingForCheckpointLatch = startWaitingForCheckpointLatch;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            nextNumberState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("next", Integer.class));
            if (nextNumberState.get().iterator().hasNext()) {
                nextNumber = nextNumberState.get().iterator().next();
            }
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            emitRecordsTill(maxNumber / 3, ctx);

            if (behavior.waitForCheckpointOnFirstRun
                    && getRuntimeContext().getAttemptNumber() == 0) {
                // Wait till one checkpoint is triggered and completed
                isWaiting = true;
                startWaitingForCheckpointLatch.get().countDown();
                while (!checkpointCompletedAfterWaiting) {
                    Thread.sleep(200);
                }

                if (behavior == StatefulSourceBehavior.FAIL_AFTER_CHECKPOINT_ON_FIRST_RUN) {
                    throw new RuntimeException("Artificial Exception");
                } else if (behavior == StatefulSourceBehavior.HOLD_AFTER_CHECKPOINT_ON_FIRST_RUN) {
                    while (!isCanceled) {
                        Thread.sleep(200);
                    }
                }
            } else {
                emitRecordsTill(maxNumber, ctx);
            }
        }

        private void emitRecordsTill(int endExclusive, SourceContext<Integer> ctx) {
            while (!isCanceled && nextNumber < endExclusive) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextNumber);
                    nextNumber++;
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            nextNumberState.update(Collections.singletonList(nextNumber));

            if (isWaiting && firstCheckpointIdAfterWaiting <= 0) {
                firstCheckpointIdAfterWaiting = context.getCheckpointId();
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (firstCheckpointIdAfterWaiting > 0
                    && checkpointId >= firstCheckpointIdAfterWaiting) {
                checkpointCompletedAfterWaiting = true;
            }
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }

    private static class CollectSink implements SinkFunction<Integer> {

        private final SharedReference<List<Integer>> result;

        public CollectSink(SharedReference<List<Integer>> result) {
            this.result = result;
        }

        @Override
        public void invoke(Integer value, Context context) throws Exception {
            result.get().add(value);
        }
    }
}
