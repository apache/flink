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

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.testutils.CommonTestUtils;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.test.util.AbstractTestBase;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Tests an immediate checkpoint should be triggered right after all tasks reached the end of data.
 */
public class CheckpointAfterAllTasksFinishedITCase extends AbstractTestBase {
    private static final int SMALL_SOURCE_NUM_RECORDS = 20;
    private static final int BIG_SOURCE_NUM_RECORDS = 100;

    private StreamExecutionEnvironment env;
    private SharedReference<List<Integer>> smallResult;
    private SharedReference<List<Integer>> bigResult;

    @TempDir private java.nio.file.Path tmpDir;

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    @BeforeEach
    public void setUp() {
        env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(4);
        smallResult = sharedObjects.add(new CopyOnWriteArrayList<>());
        bigResult = sharedObjects.add(new CopyOnWriteArrayList<>());
        IntegerStreamSource.failedBefore = false;
        IntegerStreamSource.latch = new CountDownLatch(1);
    }

    @Test
    public void testImmediateCheckpointing() throws Exception {
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.enableCheckpointing(Long.MAX_VALUE - 1);
        StreamGraph streamGraph = getStreamGraph(env, false, false);
        env.execute(streamGraph);
        assertThat(smallResult.get().size()).isEqualTo(SMALL_SOURCE_NUM_RECORDS);
        assertThat(bigResult.get().size()).isEqualTo(BIG_SOURCE_NUM_RECORDS);
    }

    @Test
    public void testRestoreAfterSomeTasksFinished() throws Exception {
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .build();
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            env.setRestartStrategy(RestartStrategies.noRestart());
            env.enableCheckpointing(100);
            JobGraph jobGraph = getStreamGraph(env, true, false).getJobGraph();
            miniCluster.submitJob(jobGraph).get();

            CommonTestUtils.waitForSubtasksToFinish(
                    miniCluster,
                    jobGraph.getJobID(),
                    findVertexByName(jobGraph, "passA -> Sink: sinkA").getID(),
                    false);

            String savepointPath =
                    miniCluster
                            .triggerSavepoint(
                                    jobGraph.getJobID(),
                                    tmpDir.toFile().getAbsolutePath(),
                                    true,
                                    SavepointFormatType.CANONICAL)
                            .get();
            bigResult.get().clear();

            env.enableCheckpointing(Long.MAX_VALUE - 1);
            JobGraph restoredJobGraph = getStreamGraph(env, true, false).getJobGraph();
            restoredJobGraph.setSavepointRestoreSettings(
                    SavepointRestoreSettings.forPath(savepointPath, false));
            miniCluster.submitJob(restoredJobGraph).get();

            IntegerStreamSource.latch.countDown();

            miniCluster.requestJobResult(restoredJobGraph.getJobID()).get();
            assertThat(smallResult.get().size()).isEqualTo(SMALL_SOURCE_NUM_RECORDS);
            assertThat(bigResult.get().size()).isEqualTo(BIG_SOURCE_NUM_RECORDS);
        }
    }

    /**
     * Tests the behaviors of the following two scenarios at which the subtasks that have finished
     * should be restored to {@link org.apache.flink.runtime.scheduler.VertexEndOfDataListener}.
     *
     * <p>Some subtasks in the job have reached the end of data but failed due to their final
     * checkpoint throwing exceptions.
     *
     * <p>Some pipelines of the job have been finished but the next checkpoint is not triggered, at
     * this time a full-strategy failover happens.
     */
    @Test
    public void testFailoverAfterSomeTasksFinished() throws Exception {
        final Configuration config = new Configuration();
        config.setString(JobManagerOptions.EXECUTION_FAILOVER_STRATEGY, "full");

        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .withRandomPorts()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();

            env.enableCheckpointing(100);
            JobGraph jobGraph = getStreamGraph(env, true, true).getJobGraph();
            miniCluster.submitJob(jobGraph).get();

            CommonTestUtils.waitForSubtasksToFinish(
                    miniCluster,
                    jobGraph.getJobID(),
                    findVertexByName(jobGraph, "passA -> Sink: sinkA").getID(),
                    true);
            bigResult.get().clear();
            IntegerStreamSource.latch.countDown();

            miniCluster.requestJobResult(jobGraph.getJobID()).get();

            // We are expecting the source to be finished and then restart during failover, so the
            // sink should receive records as much as double SMALL_SOURCE_NUM_RECORDS.
            // However, in a few cases, a checkpoint happens to be triggered before failover, and
            // the source would not restart so the sink will only receive SMALL_SOURCE_NUM_RECORDS
            // records.
            assertThat(smallResult.get().size())
                    .isIn(SMALL_SOURCE_NUM_RECORDS, SMALL_SOURCE_NUM_RECORDS * 2);
            assertThat(bigResult.get().size()).isEqualTo(BIG_SOURCE_NUM_RECORDS);
        }
    }

    private StreamGraph getStreamGraph(
            StreamExecutionEnvironment env, boolean block, boolean needFailover) {

        env.addSource(new IntegerStreamSource(SMALL_SOURCE_NUM_RECORDS, false, false))
                .transform("passA", Types.INT, new PassThroughOperator())
                .addSink(new CollectSink(smallResult))
                .name("sinkA");

        env.addSource(new IntegerStreamSource(BIG_SOURCE_NUM_RECORDS, block, needFailover))
                .transform("passB", Types.INT, new PassThroughOperator())
                .addSink(new CollectSink(bigResult))
                .name("sinkB");
        return env.getStreamGraph();
    }

    private JobVertex findVertexByName(JobGraph jobGraph, String vertexName) {
        for (JobVertex vertex : jobGraph.getVerticesAsArray()) {
            if (vertex.getName().equals(vertexName)) {
                return vertex;
            }
        }
        return null;
    }

    private static final class IntegerStreamSource extends RichSourceFunction<Integer> {

        private static final long serialVersionUID = 1L;
        private static CountDownLatch latch;
        private static boolean failedBefore;

        private final int numRecords;
        private boolean block;
        private volatile boolean running;
        private int emittedCount;
        private boolean needFailover;

        public IntegerStreamSource(int numRecords, boolean block, boolean needFailover) {
            this.numRecords = numRecords;
            this.running = true;
            this.block = block;
            this.needFailover = needFailover;
            this.emittedCount = 0;
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running && emittedCount < numRecords) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(emittedCount);
                }
                ++emittedCount;
            }
            if (block && latch != null) {
                latch.await();
            }
            if (needFailover && !failedBefore) {
                failedBefore = true;
                throw new RuntimeException("forced failure");
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }

    private static class PassThroughOperator extends AbstractStreamOperator<Integer>
            implements OneInputStreamOperator<Integer, Integer> {

        private long checkpointID;

        @Override
        public void snapshotState(StateSnapshotContext context) throws Exception {
            super.snapshotState(context);
            checkpointID = context.getCheckpointId();
        }

        @Override
        public void processElement(StreamRecord<Integer> element) throws Exception {
            output.collect(element);
        }

        @Override
        public void close() throws Exception {
            super.close();
            assertThat(checkpointID).isGreaterThan(0);
        }
    }

    private static class CollectSink extends RichSinkFunction<Integer> {
        private final SharedReference<List<Integer>> result;

        public CollectSink(SharedReference<List<Integer>> result) {
            this.result = result;
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
        }

        @Override
        public void invoke(Integer value, Context context) throws Exception {
            result.get().add(value);
        }
    }
}
