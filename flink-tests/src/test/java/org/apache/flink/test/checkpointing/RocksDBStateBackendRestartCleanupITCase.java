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

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;
import static org.assertj.core.api.Assertions.assertThat;

import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.StateBackendOptions;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.state.rocksdb.RocksDBOptions;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.util.RestartStrategyUtils;
import org.apache.flink.test.util.InfiniteIntegerSource;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.utils.TempDirUtils;
import org.apache.flink.util.Collector;
import org.apache.flink.util.TestLoggerExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/** IT case for RocksDB local directory cleanup after async state task restart. */
@ExtendWith(TestLoggerExtension.class)
class RocksDBStateBackendRestartCleanupITCase {

    private static final int NUM_TASK_MANAGERS = 1;
    private static final int NUM_TASK_SLOTS = 1;

    @TempDir private File temporaryFolder;

    private MiniClusterWithClientResource cluster;
    private File rocksDbLocalDir;

    @BeforeEach
    void setup() throws Exception {
        FailingStateFunction.reset();
        rocksDbLocalDir = TempDirUtils.newFolder(temporaryFolder.toPath());

        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
        configuration.set(RocksDBOptions.LOCAL_DIRECTORIES, rocksDbLocalDir.getAbsolutePath());

        cluster =
                new MiniClusterWithClientResource(
                        new MiniClusterResourceConfiguration.Builder()
                                .setConfiguration(configuration)
                                .setNumberTaskManagers(NUM_TASK_MANAGERS)
                                .setNumberSlotsPerTaskManager(NUM_TASK_SLOTS)
                                .build());
        cluster.before();
    }

    @AfterEach
    void teardown() {
        if (cluster != null) {
            cluster.after();
        }
    }

    @Test
    void testOldRocksDBDirectoryIsDeletedAfterAsyncStateTaskRestart() throws Exception {
        testOldRocksDBDirectoryIsDeletedAfterTaskRestart(StateMode.ASYNC);
    }

    @Test
    void testOldRocksDBDirectoryIsDeletedAfterSyncStateTaskRestart() throws Exception {
        testOldRocksDBDirectoryIsDeletedAfterTaskRestart(StateMode.SYNC);
    }

    private void testOldRocksDBDirectoryIsDeletedAfterTaskRestart(StateMode stateMode)
            throws Exception {
        MiniCluster miniCluster = cluster.getMiniCluster();
        JobGraph jobGraph = createJobGraph(rocksDbLocalDir.toPath(), stateMode);

        try {
            miniCluster.submitJob(jobGraph).get();

            waitUntilCondition(() -> FailingStateFunction.getFirstAttemptDirectory() != null);
            Path firstAttemptDirectory = Path.of(FailingStateFunction.getFirstAttemptDirectory());

            waitUntilCondition(() -> FailingStateFunction.getMaxAttemptNumber() >= 1);
            waitForAllTaskRunning(
                    () ->
                            miniCluster
                                    .getExecutionGraph(jobGraph.getJobID())
                                    .get(500, TimeUnit.SECONDS),
                    false);

            assertThat(firstAttemptDirectory).doesNotExist();
            assertThat(listRocksDBAttemptDirectories(rocksDbLocalDir.toPath())).hasSize(1);
        } finally {
            miniCluster.cancelJob(jobGraph.getJobID()).get();
        }
    }

    private static JobGraph createJobGraph(Path rocksDbLocalDir, StateMode stateMode) {
        Configuration configuration = new Configuration();
        configuration.set(StateBackendOptions.STATE_BACKEND, "rocksdb");
        configuration.set(CheckpointingOptions.INCREMENTAL_CHECKPOINTS, false);
        configuration.set(RocksDBOptions.LOCAL_DIRECTORIES, rocksDbLocalDir.toString());
        StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(configuration);
        RestartStrategyUtils.configureFixedDelayRestartStrategy(env, 1, 0L);

        KeyedStream<Integer, Integer> stream =
                env.addSource(new InfiniteIntegerSource()).setParallelism(1).keyBy(value -> value);

        if (stateMode == StateMode.ASYNC) {
            stream.enableAsyncState()
                    .process(new FailingAsyncStateFunction(rocksDbLocalDir))
                    .setParallelism(1)
                    .sinkTo(new DiscardingSink<>())
                    .setParallelism(1);
        } else {
            stream.process(new FailingSyncStateFunction(rocksDbLocalDir))
                    .setParallelism(1)
                    .sinkTo(new DiscardingSink<>())
                    .setParallelism(1);
        }

        return env.getStreamGraph().getJobGraph();
    }

    private static List<Path> listRocksDBAttemptDirectories(Path rocksDbLocalDir) throws Exception {
        if (!Files.exists(rocksDbLocalDir)) {
            return List.of();
        }
        try (Stream<Path> paths = Files.walk(rocksDbLocalDir, 2)) {
            return paths.filter(Files::isDirectory)
                    .filter(path -> path.getFileName().toString().startsWith("job_"))
                    .filter(path -> Files.isDirectory(path.resolve("db")))
                    .sorted(Comparator.comparing(Path::toString))
                    .collect(Collectors.toList());
        }
    }

    private enum StateMode {
        ASYNC,
        SYNC
    }

    private abstract static class FailingStateFunction
            extends KeyedProcessFunction<Integer, Integer, Integer> {

        private static final long serialVersionUID = 1L;

        private static final AtomicBoolean FAILED_ONCE = new AtomicBoolean();
        private static final AtomicInteger MAX_ATTEMPT_NUMBER = new AtomicInteger(-1);
        private static final AtomicReference<String> FIRST_ATTEMPT_DIRECTORY =
                new AtomicReference<>();

        private final String rocksDbLocalDir;

        private FailingStateFunction(Path rocksDbLocalDir) {
            this.rocksDbLocalDir = rocksDbLocalDir.toString();
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            MAX_ATTEMPT_NUMBER.updateAndGet(
                    current ->
                            Math.max(
                                    current, getRuntimeContext().getTaskInfo().getAttemptNumber()));
        }

        @Override
        public void processElement(Integer value, Context ctx, Collector<Integer> out)
                throws Exception {
            updateState();

            if (getRuntimeContext().getTaskInfo().getAttemptNumber() == 0
                    && FAILED_ONCE.compareAndSet(false, true)) {
                FIRST_ATTEMPT_DIRECTORY.compareAndSet(
                        null, waitForFirstRocksDBAttemptDirectory().toString());
                throw new ArtificialFailureException();
            }

            out.collect(value);
        }

        protected abstract void updateState() throws Exception;

        private Path waitForFirstRocksDBAttemptDirectory() throws Exception {
            for (int retry = 0; retry < 1_000; retry++) {
                List<Path> attemptDirectories =
                        listRocksDBAttemptDirectories(Path.of(rocksDbLocalDir));
                if (!attemptDirectories.isEmpty()) {
                    return attemptDirectories.get(0);
                }
                Thread.sleep(100);
            }
            throw new AssertionError("Timed out waiting for a RocksDB attempt directory.");
        }

        private static void reset() {
            FAILED_ONCE.set(false);
            MAX_ATTEMPT_NUMBER.set(-1);
            FIRST_ATTEMPT_DIRECTORY.set(null);
        }

        private static int getMaxAttemptNumber() {
            return MAX_ATTEMPT_NUMBER.get();
        }

        private static String getFirstAttemptDirectory() {
            return FIRST_ATTEMPT_DIRECTORY.get();
        }
    }

    private static class FailingAsyncStateFunction extends FailingStateFunction {

        private static final long serialVersionUID = 1L;

        private transient org.apache.flink.api.common.state.v2.ValueState<Integer> state;

        private FailingAsyncStateFunction(Path rocksDbLocalDir) {
            super(rocksDbLocalDir);
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            state =
                    ((StreamingRuntimeContext) getRuntimeContext())
                            .getValueState(
                                    new org.apache.flink.api.common.state.v2.ValueStateDescriptor<>(
                                            "async-count", Integer.class));
        }

        @Override
        protected void updateState() {
            state.asyncValue()
                    .thenCompose(current -> state.asyncUpdate(current == null ? 1 : current + 1));
        }
    }

    private static class FailingSyncStateFunction extends FailingStateFunction {

        private static final long serialVersionUID = 1L;

        private org.apache.flink.api.common.state.ValueState<Integer> state;

        private FailingSyncStateFunction(Path rocksDbLocalDir) {
            super(rocksDbLocalDir);
        }

        @Override
        public void open(OpenContext openContext) throws Exception {
            super.open(openContext);
            state =
                    getRuntimeContext()
                            .getState(
                                    new org.apache.flink.api.common.state.ValueStateDescriptor<>(
                                            "sync-count", Integer.class));
        }

        @Override
        protected void updateState() throws Exception {
            Integer current = state.value();
            state.update(current == null ? 1 : current + 1);
        }
    }

    private static class ArtificialFailureException extends Exception {}
}
