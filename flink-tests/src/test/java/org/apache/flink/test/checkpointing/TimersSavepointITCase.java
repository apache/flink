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

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.contrib.streaming.state.EmbeddedRocksDBStateBackend.PriorityQueueStateType;
import org.apache.flink.contrib.streaming.state.RocksDBOptions;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.SavepointRestoreSettings;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.DiscardingSink;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.util.Collector;

import org.apache.commons.io.FileUtils;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URL;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/** Tests for restoring {@link PriorityQueueStateType#HEAP} timers stored in raw operator state. */
public class TimersSavepointITCase {
    private static final int PARALLELISM = 4;

    private static final OneShotLatch savepointLatch = new OneShotLatch();
    private static final OneShotLatch resultLatch = new OneShotLatch();

    @ClassRule public static final TemporaryFolder TMP_FOLDER = new TemporaryFolder();

    // We use a single past Flink version as we verify heap timers stored in raw state
    // Starting from 1.13 we do not store heap timers in raw state, but we keep them in
    // managed state
    public static final String SAVEPOINT_FILE_NAME = "legacy-raw-state-heap-timers-rocks-db-1.12";

    /**
     * This test runs in either of two modes: 1) we want to generate the binary savepoint, i.e. we
     * have to run the checkpointing functions 2) we want to verify restoring, so we have to run the
     * checking functions.
     */
    public enum ExecutionMode {
        PERFORM_SAVEPOINT,
        VERIFY_SAVEPOINT
    }

    // TODO change this to PERFORM_SAVEPOINT to regenerate binary savepoints
    // TODO Note: You should generate the savepoint based on the release branch instead of the
    // master.
    private final ExecutionMode executionMode = ExecutionMode.VERIFY_SAVEPOINT;

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(1)
                            .setNumberSlotsPerTaskManager(PARALLELISM)
                            .build());

    @Test(timeout = 60_000)
    public void testSavepointWithTimers() throws Exception {
        try (ClusterClient<?> client = miniClusterResource.getClusterClient()) {
            if (executionMode == ExecutionMode.PERFORM_SAVEPOINT) {
                takeSavepoint("src/test/resources/" + SAVEPOINT_FILE_NAME, client);
            } else if (executionMode == ExecutionMode.VERIFY_SAVEPOINT) {
                verifySavepoint(getResourceFilename(SAVEPOINT_FILE_NAME), client);
            } else {
                throw new IllegalStateException("Unknown ExecutionMode " + executionMode);
            }
        }
    }

    private void verifySavepoint(String savepointPath, ClusterClient<?> client)
            throws IOException, InterruptedException, java.util.concurrent.ExecutionException {
        JobGraph jobGraph;

        jobGraph = getJobGraph(PriorityQueueStateType.HEAP);
        jobGraph.setSavepointRestoreSettings(SavepointRestoreSettings.forPath(savepointPath));
        client.submitJob(jobGraph).get();
        resultLatch.await();
    }

    private void takeSavepoint(String savepointPath, ClusterClient<?> client) throws Exception {
        JobGraph jobGraph = getJobGraph(PriorityQueueStateType.ROCKSDB);
        client.submitJob(jobGraph).get();
        savepointLatch.await();
        CompletableFuture<String> savepointPathFuture =
                client.triggerSavepoint(jobGraph.getJobID(), null);

        String jobmanagerSavepointPath = savepointPathFuture.get(2, TimeUnit.SECONDS);

        File jobManagerSavepoint = new File(new URI(jobmanagerSavepointPath).getPath());
        // savepoints were changed to be directories in Flink 1.3
        FileUtils.moveDirectory(jobManagerSavepoint, new File(savepointPath));
    }

    public JobGraph getJobGraph(PriorityQueueStateType priorityQueueStateType) throws IOException {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(PARALLELISM);
        env.addSource(new Source())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<Integer>forMonotonousTimestamps()
                                .withTimestampAssigner((i, p) -> i))
                .keyBy(i -> i)
                .process(new TimersProcessFunction())
                .addSink(new DiscardingSink<>());

        final Configuration config = new Configuration();
        config.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        config.set(
                CheckpointingOptions.CHECKPOINTS_DIRECTORY,
                TMP_FOLDER.newFolder().toURI().toString());
        config.set(
                CheckpointingOptions.SAVEPOINT_DIRECTORY,
                TMP_FOLDER.newFolder().toURI().toString());
        config.set(RocksDBOptions.TIMER_SERVICE_FACTORY, priorityQueueStateType);
        env.configure(config, this.getClass().getClassLoader());
        return env.getStreamGraph("Test", false).getJobGraph();
    }

    private static String getResourceFilename(String filename) {
        ClassLoader cl = TimersSavepointITCase.class.getClassLoader();
        URL resource = cl.getResource(filename);
        if (resource == null) {
            throw new NullPointerException("Missing snapshot resource.");
        }
        return resource.getFile();
    }

    private static class Source implements SourceFunction<Integer>, CheckpointedFunction {

        private volatile boolean running = true;
        private int emittedCount;
        private ListState<Integer> state;

        @Override
        public void run(SourceContext<Integer> ctx) throws Exception {
            while (running) {
                synchronized (ctx.getCheckpointLock()) {
                    if (emittedCount == 0) {
                        ctx.collect(0);
                        emittedCount = 1;
                    } else if (emittedCount == 1) {
                        ctx.collect(emittedCount);
                    } else {
                        ctx.collect(emittedCount++);
                    }
                }
                Thread.sleep(1);
            }
        }

        @Override
        public void cancel() {
            this.running = false;
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            state.add(emittedCount);
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            state =
                    context.getOperatorStateStore()
                            .getListState(
                                    new ListStateDescriptor<>(
                                            "emittedCount", IntSerializer.INSTANCE));
            if (context.isRestored()) {
                this.emittedCount = 2;
            }
        }
    }

    private static class TimersProcessFunction
            extends KeyedProcessFunction<Integer, Integer, Integer> {

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            if (value == 0) {
                ctx.timerService().registerEventTimeTimer(2L);
                savepointLatch.trigger();
            }
        }

        @Override
        public void onTimer(long timestamp, OnTimerContext ctx, Collector<Integer> out)
                throws Exception {
            out.collect(1);
            resultLatch.trigger();
        }
    }
}
