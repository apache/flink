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
import org.apache.flink.api.common.functions.IterationRuntimeContext;
import org.apache.flink.api.common.functions.RichFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
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
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.OnCheckpointRollingPolicy;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;
import org.apache.flink.util.TestLogger;

import org.junit.After;
import org.junit.Before;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.Serializable;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;

/**
 * Tests migrating from {@link StreamingFileSink} to {@link FileSink}. It trigger a savepoint for
 * the {@link StreamingFileSink} job and restore the {@link FileSink} job from the savepoint taken.
 */
public class FileSinkMigrationITCase extends TestLogger {

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    private static final String SOURCE_UID = "source";

    private static final String SINK_UID = "sink";

    private static final int NUM_SOURCES = 4;

    private static final int NUM_SINKS = 3;

    private static final int NUM_RECORDS = 10000;

    private static final int NUM_BUCKETS = 4;

    private static final Map<String, CountDownLatch> SAVEPOINT_LATCH_MAP =
            new ConcurrentHashMap<>();

    private static final Map<String, CountDownLatch> FINAL_CHECKPOINT_LATCH_MAP =
            new ConcurrentHashMap<>();

    private String latchId;

    @Before
    public void setup() {
        this.latchId = UUID.randomUUID().toString();
        SAVEPOINT_LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES));

        // We wait for two successful checkpoints in sources before shutting down. This ensures that
        // the sink can commit its data.
        // We need to keep a "static" latch here because all sources need to be kept running
        // while we're waiting for the required number of checkpoints. Otherwise, we would lock up
        // because we can only do checkpoints while all operators are running.
        FINAL_CHECKPOINT_LATCH_MAP.put(latchId, new CountDownLatch(NUM_SOURCES * 2));
    }

    @After
    public void teardown() {
        SAVEPOINT_LATCH_MAP.remove(latchId);
        FINAL_CHECKPOINT_LATCH_MAP.remove(latchId);
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

        env.addSource(new StatefulSource(true, latchId))
                .uid(SOURCE_UID)
                .setParallelism(NUM_SOURCES)
                .addSink(new WaitingRunningSink<>(latchId, sink))
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

        env.addSource(new StatefulSource(false, latchId))
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

            // wait till we can taking savepoint
            CountDownLatch latch = SAVEPOINT_LATCH_MAP.get(latchId);
            latch.await();

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

    private static class WaitingRunningSink<T>
            implements RichFunction,
                    Serializable,
                    SinkFunction<T>,
                    CheckpointedFunction,
                    CheckpointListener {
        private final String latchId;
        private final StreamingFileSink<T> streamingFileSink;

        /**
         * Creates a new {@code StreamingFileSink} that writes files to the given base directory
         * with the give buckets properties.
         */
        protected WaitingRunningSink(String latchId, StreamingFileSink<T> streamingFileSink) {
            this.latchId = latchId;
            this.streamingFileSink = streamingFileSink;
        }

        public void setRuntimeContext(RuntimeContext t) {
            streamingFileSink.setRuntimeContext(t);
        }

        public RuntimeContext getRuntimeContext() {
            return streamingFileSink.getRuntimeContext();
        }

        public IterationRuntimeContext getIterationRuntimeContext() {
            return streamingFileSink.getIterationRuntimeContext();
        }

        public void open(Configuration parameters) throws Exception {
            streamingFileSink.open(parameters);
        }

        public void close() throws Exception {
            streamingFileSink.close();
        }

        public void initializeState(FunctionInitializationContext context) throws Exception {
            streamingFileSink.initializeState(context);
        }

        public void notifyCheckpointComplete(long checkpointId) throws Exception {
            streamingFileSink.notifyCheckpointComplete(checkpointId);
        }

        public void notifyCheckpointAborted(long checkpointId) {
            streamingFileSink.notifyCheckpointAborted(checkpointId);
        }

        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            streamingFileSink.snapshotState(context);
        }

        @Override
        public void invoke(T value, Context context) throws Exception {
            SAVEPOINT_LATCH_MAP.get(latchId).countDown();

            streamingFileSink.invoke(value, context);
        }
    }

    private static class StatefulSource extends RichParallelSourceFunction<Integer>
            implements CheckpointedFunction, CheckpointListener {

        private final boolean takingSavepointMode;

        private final String latchId;

        private ListState<Integer> nextValueState;

        private int nextValue;

        private volatile boolean snapshottedAfterAllRecordsOutput;

        private volatile boolean isWaitingCheckpointComplete;

        private volatile boolean isCanceled;

        public StatefulSource(boolean takingSavepointMode, String latchId) {
            this.takingSavepointMode = takingSavepointMode;
            this.latchId = latchId;
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
                CountDownLatch latch = FINAL_CHECKPOINT_LATCH_MAP.get(latchId);
                latch.await();
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
                CountDownLatch latch = FINAL_CHECKPOINT_LATCH_MAP.get(latchId);
                latch.countDown();
            }
        }

        @Override
        public void cancel() {
            isCanceled = true;
        }
    }
}
