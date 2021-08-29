/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.sink.writer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.JobSubmissionResult;
import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.FileSink;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLogger;

import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.junit.Assert.assertEquals;

/**
 * Tests migrating from {@link StreamingFileSink} to {@link FileSink}. It trigger a savepoint for
 * the {@link StreamingFileSink} job and restore the {@link FileSink} job from the savepoint taken.
 */
public class FileSinkMigrationITCase extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    private static final String SOURCE_UID = "source";

    private static final String SINK_UID = "sink";

    private static final int NUM_SOURCES = 4;

    private static final int NUM_SINKS = 3;

    private static final int NUM_RECORDS = 10000;

    private static final int NUM_BUCKETS = 4;

    private SharedReference<CountDownLatch> finalCheckpointLatch;

    @Before
    public void setup() {
        // We wait for two successful checkpoints in sources before shutting down. This ensures that
        // the sink can commit its data.
        // We need to keep a "static" latch here because all sources need to be kept running
        // while we're waiting for the required number of checkpoints. Otherwise, we would lock up
        // because we can only do checkpoints while all operators are running.
        finalCheckpointLatch = sharedObjects.add(new CountDownLatch(NUM_SOURCES * 2));
    }

    @Test
    public void test() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SharedReference<Collection<Long>> list = sharedObjects.add(new ArrayList<>());
        int n = 10000;
        env.setParallelism(100);
        env.fromSequence(0, n).map(i -> list.applySync(l -> l.add(i)));
        env.execute();
        assertEquals(n + 1, list.get().size());
        assertEquals(
                LongStream.rangeClosed(0, n).boxed().collect(Collectors.toList()),
                list.get().stream().sorted().collect(Collectors.toList()));
    }

    @Test
    public void testMigration() throws Exception {
        String outputPath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();
        String savepointBasePath = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        JobGraph streamingFileSinkJobGraph = createStreamingFileSinkJobGraph(outputPath);
        String savepointPath =
                executeAndTakeSavepoint(cfg, streamingFileSinkJobGraph, savepointBasePath);

        JobGraph fileSinkJobGraph = createFileSinkJobGraph(outputPath);
        loadSavepointAndExecute(cfg, fileSinkJobGraph, savepointPath);

        IntegerFileSinkTestDataUtils.checkIntegerSequenceSinkOutput(
                outputPath, NUM_RECORDS, NUM_BUCKETS, NUM_SOURCES);
    }

    private JobGraph createStreamingFileSinkJobGraph(String outputPath) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        StreamingFileSink<Integer> sink =
                StreamingFileSink.forRowFormat(
                                new Path(outputPath), new IntegerFileSinkTestDataUtils.IntEncoder())
                        .withBucketAssigner(
                                new IntegerFileSinkTestDataUtils.ModuloBucketAssigner(NUM_BUCKETS))
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .build();

        env.addSource(new StatefulSource(true, finalCheckpointLatch))
                .uid(SOURCE_UID)
                .setParallelism(NUM_SOURCES)
                .addSink(sink)
                .setParallelism(NUM_SINKS)
                .uid(SINK_UID);
        return env.getStreamGraph().getJobGraph();
    }

    private JobGraph createFileSinkJobGraph(String outputPath) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.enableCheckpointing(500, CheckpointingMode.EXACTLY_ONCE);

        FileSink<Integer> sink =
                FileSink.forRowFormat(
                                new Path(outputPath), new IntegerFileSinkTestDataUtils.IntEncoder())
                        .withBucketAssigner(
                                new IntegerFileSinkTestDataUtils.ModuloBucketAssigner(NUM_BUCKETS))
                        .withRollingPolicy(OnCheckpointRollingPolicy.build())
                        .build();

        env.addSource(new StatefulSource(false, finalCheckpointLatch))
                .uid(SOURCE_UID)
                .setParallelism(NUM_SOURCES)
                .sinkTo(sink)
                .setParallelism(NUM_SINKS)
                .uid(SINK_UID);
        return env.getStreamGraph().getJobGraph();
    }

    private String executeAndTakeSavepoint(
            MiniClusterConfiguration cfg, JobGraph jobGraph, String savepointBasePath)
            throws Exception {
        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            CompletableFuture<JobSubmissionResult> jobSubmissionResultFuture =
                    miniCluster.submitJob(jobGraph);
            JobID jobId = jobSubmissionResultFuture.get().getJobID();

            waitForAllTaskRunning(miniCluster, jobId, false);

            CompletableFuture<String> savepointResultFuture =
                    miniCluster.triggerSavepoint(jobId, savepointBasePath, true);
            return savepointResultFuture.get();
        }
    }

    private void loadSavepointAndExecute(
            MiniClusterConfiguration cfg, JobGraph jobGraph, String savepointPath)
            throws Exception {
        jobGraph.setSavepointRestoreSettings(
                SavepointRestoreSettings.forPath(savepointPath, false));

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }
    }

    private static class StatefulSource extends RichParallelSourceFunction<Integer>
            implements CheckpointedFunction, CheckpointListener {

        private final boolean takingSavepointMode;

        private SharedReference<CountDownLatch> finalCheckpointLatch;

        private ListState<Integer> nextValueState;

        private int nextValue;

        private volatile boolean snapshottedAfterAllRecordsOutput;

        private volatile boolean isWaitingCheckpointComplete;

        private volatile boolean isCanceled;

        public StatefulSource(
                boolean takingSavepointMode, SharedReference<CountDownLatch> finalCheckpointLatch) {
            this.takingSavepointMode = takingSavepointMode;
            this.finalCheckpointLatch = finalCheckpointLatch;
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            nextValueState =
                    context.getOperatorStateStore()
                            .getListState(new ListStateDescriptor<>("nextValue", Integer.class));

            if (nextValueState.get() != null && nextValueState.get().iterator().hasNext()) {
                nextValue = nextValueState.get().iterator().next();
            }
        }

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            if (takingSavepointMode) {
                sendRecordsUntil(NUM_RECORDS / 3, 0, ctx);
                sendRecordsUntil(NUM_RECORDS / 2, 100, ctx);

                while (true) {
                    Thread.sleep(5000);
                }
            } else {
                sendRecordsUntil(NUM_RECORDS, 0, ctx);

                // Wait the last checkpoint to commit all the pending records.
                isWaitingCheckpointComplete = true;
                finalCheckpointLatch.get().await();
            }
        }

        private void sendRecordsUntil(
                int targetNumber, int sleepInMillis, SourceContext<Integer> ctx)
                throws InterruptedException {
            while (!isCanceled && nextValue < targetNumber) {
                synchronized (ctx.getCheckpointLock()) {
                    ctx.collect(nextValue++);
                }

                if (sleepInMillis > 0) {
                    Thread.sleep(sleepInMillis);
                }
            }
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            nextValueState.update(Collections.singletonList(nextValue));

            if (isWaitingCheckpointComplete) {
                snapshottedAfterAllRecordsOutput = true;
            }
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            if (isWaitingCheckpointComplete && snapshottedAfterAllRecordsOutput) {
                finalCheckpointLatch.get().countDown();
            }
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }
}
