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

package org.apache.flink.test.streaming.runtime;

import org.apache.flink.api.common.JobStatus;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.connector.sink.Committer;
import org.apache.flink.api.connector.sink.GlobalCommitter;
import org.apache.flink.api.connector.sink.Sink;
import org.apache.flink.api.connector.sink.SinkWriter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.testutils.OneShotLatch;
import org.apache.flink.runtime.jobgraph.SavepointConfigOptions;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.test.junit5.MiniClusterExtension;
import org.apache.flink.testutils.junit.SharedObjectsExtension;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.TestLoggerExtension;

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;

import static org.assertj.core.api.Assertions.assertThat;

@ExtendWith(TestLoggerExtension.class)
class UnifiedSinkMigrationITCase {

    private static final Logger LOG = LoggerFactory.getLogger(UnifiedSinkMigrationITCase.class);

    private static final String SAVEPOINT_FOLDER_NAME = "unified-sink-migration-test";

    @RegisterExtension
    private static final MiniClusterExtension miniClusterExtension =
            new MiniClusterExtension(new MiniClusterResourceConfiguration.Builder().build());

    @RegisterExtension
    private final SharedObjectsExtension sharedObjects = SharedObjectsExtension.create();

    private static final int WRITER_STATE = 1;
    private static final int COMMITTER_STATE = 2;
    private static final int GLOBAL_COMMITTER_STATE = 3;
    private static final String SINK_UUID = "1c4ec0f9-2d96-46e9-99ea-45e8c3df5202";

    /**
     * You can enable this test to update the taken savepoint.
     *
     * <p>Be aware that the current savepoint points to snapshot taken before the introduction of
     * Sink V2 to test the migration.
     */
    @Disabled
    @Test
    void prepareSinkSavepoint() throws Exception {
        LOG.warn("Deleting the previous savepoints.");
        final Path basePath = Paths.get("src/test/resources/").resolve(SAVEPOINT_FOLDER_NAME);

        Files.walk(basePath)
                .skip(1)
                .sorted(Comparator.reverseOrder())
                .map(Path::toFile)
                .forEach(File::delete);

        final JobClient jobClient = executeJob(false);

        final CompletableFuture<String> savepointFuture =
                jobClient.stopWithSavepoint(
                        false, basePath.toString(), SavepointFormatType.CANONICAL);
        final String savepointPath = savepointFuture.get();
        LOG.info("Savepoint path: {}", savepointPath);
        assertThat(savepointPath).contains(basePath.toString());
    }

    /**
     * This test tries to restore a job with a complete Sink V1 sink
     * writer->committer->global-committer to ensure the state compatibility.
     */
    @Test
    void testRestoreSinkState() throws Exception {
        // Restore job and wait for one successful commit
        final JobClient jobClient = executeJob(true);

        // Await shutdown
        jobClient.cancel();
    }

    private JobClient executeJob(boolean restore) throws Exception {
        final Configuration conf = new Configuration();
        if (restore) {
            conf.set(SavepointConfigOptions.SAVEPOINT_PATH, findSavepointPath());
        }
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment(conf);
        final SharedReference<OneShotLatch> latch = sharedObjects.add(new OneShotLatch());
        final SharedReference<CountDownLatch> commitLatch =
                sharedObjects.add(new CountDownLatch(2));

        env.enableCheckpointing(100);
        env.setParallelism(1);
        env.setRestartStrategy(RestartStrategies.noRestart());
        env.fromSequence(1, Long.MAX_VALUE)
                .map(
                        (MapFunction<Long, Long>)
                                value -> {
                                    // Throttle the execution to prevent writing to many records
                                    Thread.sleep(10);
                                    return value;
                                })
                .sinkTo(new StateFulSinkV1(restore, latch, commitLatch))
                // Until FLINK-26358 is fixed
                .disableChaining()
                .uid(SINK_UUID);
        final JobClient jobClient = env.executeAsync();

        // Wait until a fist checkpoint has been taken successful
        latch.get().await();
        assertThat(jobClient.getJobStatus().get()).isEqualTo(JobStatus.RUNNING);
        return jobClient;
    }

    private File buildSavepointPath() throws URISyntaxException {
        return new File(getClass().getResource("/" + SAVEPOINT_FOLDER_NAME).toURI());
    }

    private String findSavepointPath() throws URISyntaxException {
        final File basePath = buildSavepointPath();
        LOG.info("Base path: {}", basePath.getAbsolutePath());
        final File[] savepointDirectories = basePath.listFiles(File::isDirectory);
        assertThat(savepointDirectories).isNotNull().hasSize(1);
        final File[] stateFiles = savepointDirectories[0].listFiles();
        assertThat(stateFiles).isNotNull().hasSize(1);
        return stateFiles[0].getAbsolutePath();
    }

    private static class StateFulSinkV1 implements Sink<Long, Integer, Integer, String> {

        private final boolean recovered;
        private final SharedReference<OneShotLatch> latch;
        private final SharedReference<CountDownLatch> commitLatch;

        StateFulSinkV1(
                boolean recovered,
                SharedReference<OneShotLatch> latch,
                SharedReference<CountDownLatch> commitLatch) {
            this.recovered = recovered;
            this.latch = latch;
            this.commitLatch = commitLatch;
        }

        @Override
        public SinkWriter<Long, Integer, Integer> createWriter(
                InitContext context, List<Integer> states) throws IOException {
            return new TestWriter(recovered, states);
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getWriterStateSerializer() {
            return Optional.of(new IntegerSerializer());
        }

        @Override
        public Optional<Committer<Integer>> createCommitter() throws IOException {
            return Optional.of(new TestCommitter(recovered, commitLatch));
        }

        @Override
        public Optional<GlobalCommitter<Integer, String>> createGlobalCommitter()
                throws IOException {
            return Optional.of(new TestGlobalCommitter(recovered, latch, commitLatch));
        }

        @Override
        public Optional<SimpleVersionedSerializer<Integer>> getCommittableSerializer() {
            return Optional.of(new IntegerSerializer());
        }

        @Override
        public Optional<SimpleVersionedSerializer<String>> getGlobalCommittableSerializer() {
            return Optional.of(new StringSerializer());
        }
    }

    private static class TestWriter implements SinkWriter<Long, Integer, Integer> {

        private final boolean recovered;
        private boolean emitted = false;

        TestWriter(boolean recovered, List<Integer> recoveredState) {
            this.recovered = recovered;
            if (recovered) {
                assertThat(recoveredState).containsExactly(WRITER_STATE);
            } else {
                assertThat(recoveredState).isEmpty();
            }
        }

        @Override
        public void write(Long element, Context context) throws IOException, InterruptedException {}

        @Override
        public List<Integer> prepareCommit(boolean flush) throws IOException, InterruptedException {
            if (emitted || recovered) {
                return Collections.emptyList();
            }
            emitted = true;
            return Arrays.asList(COMMITTER_STATE, GLOBAL_COMMITTER_STATE);
        }

        @Override
        public List<Integer> snapshotState(long checkpointId) throws IOException {
            return Collections.singletonList(WRITER_STATE);
        }

        @Override
        public void close() throws Exception {}
    }

    private static class TestCommitter implements Committer<Integer> {

        private final boolean recovered;
        private final SharedReference<CountDownLatch> commitLatch;
        boolean firstCommit = true;

        TestCommitter(boolean recovered, SharedReference<CountDownLatch> commitLatch) {
            this.recovered = recovered;
            this.commitLatch = commitLatch;
        }

        @Override
        public List<Integer> commit(List<Integer> committables)
                throws IOException, InterruptedException {
            if (firstCommit && !recovered) {
                assertThat(committables).containsExactly(COMMITTER_STATE, GLOBAL_COMMITTER_STATE);
            } else {
                assertThat(committables).containsExactly(COMMITTER_STATE);
            }
            LOG.info("Committing {}", committables);
            commitLatch.get().countDown();
            firstCommit = false;
            // Always retry to keep the state
            return Collections.singletonList(COMMITTER_STATE);
        }

        @Override
        public void close() throws Exception {}
    }

    private static class TestGlobalCommitter implements GlobalCommitter<Integer, String> {

        private final boolean recover;
        private final SharedReference<OneShotLatch> latch;
        private final SharedReference<CountDownLatch> commitLatch;
        private boolean firstCommitAfterRecover;

        TestGlobalCommitter(
                boolean recover,
                SharedReference<OneShotLatch> latch,
                SharedReference<CountDownLatch> commitLatch) {
            this.recover = recover;
            this.firstCommitAfterRecover = recover;
            this.latch = latch;
            this.commitLatch = commitLatch;
        }

        @Override
        public List<String> filterRecoveredCommittables(List<String> globalCommittables)
                throws IOException {
            if (recover) {
                assertThat(globalCommittables)
                        .containsExactly(String.valueOf(GLOBAL_COMMITTER_STATE));
            }
            return globalCommittables;
        }

        @Override
        public String combine(List<Integer> committables) throws IOException {
            assertThat(committables).hasSize(1);
            return String.valueOf(committables.get(0));
        }

        @Override
        public List<String> commit(List<String> globalCommittables)
                throws IOException, InterruptedException {
            LOG.info("Global committing {}", globalCommittables);
            LOG.info("Latch count: {}", commitLatch.get().getCount());
            if (!firstCommitAfterRecover && commitLatch.get().getCount() <= 0) {
                latch.get().trigger();
                // Always retry to keep the state
                assertThat(globalCommittables)
                        .containsExactly(String.valueOf(GLOBAL_COMMITTER_STATE));
            }
            firstCommitAfterRecover = false;
            return globalCommittables;
        }

        @Override
        public void endOfInput() throws IOException, InterruptedException {}

        @Override
        public void close() throws Exception {}
    }

    private static class StringSerializer implements SimpleVersionedSerializer<String> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(String obj) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(8);
            out.writeUTF(obj);
            return out.getCopyOfBuffer();
        }

        @Override
        public String deserialize(int version, byte[] serialized) throws IOException {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return in.readUTF();
        }
    }

    private static class IntegerSerializer implements SimpleVersionedSerializer<Integer> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Integer obj) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(8);
            out.writeInt(obj);
            return out.getCopyOfBuffer();
        }

        @Override
        public Integer deserialize(int version, byte[] serialized) throws IOException {
            final DataInputDeserializer in = new DataInputDeserializer(serialized);
            return in.readInt();
        }
    }
}
