/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.connector.source.lib.NumberSequenceSource;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.state.CheckpointStateOutputStream;
import org.apache.flink.runtime.state.CheckpointStateToolset;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageAccess;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.runtime.state.CheckpointStreamFactory;
import org.apache.flink.runtime.state.CheckpointedStateScope;
import org.apache.flink.runtime.state.CompletedCheckpointStorageLocation;
import org.apache.flink.runtime.state.StreamStateHandle;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStreamFactory.FsCheckpointStateOutputStream;
import org.apache.flink.runtime.state.storage.JobManagerCheckpointStorage;
import org.apache.flink.runtime.state.ttl.mock.MockKeyedStateBackend.MockSnapshotSupplier;
import org.apache.flink.runtime.state.ttl.mock.MockStateBackend;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.v2.DiscardingSink;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.apache.flink.testutils.junit.SharedObjects;
import org.apache.flink.testutils.junit.SharedReference;
import org.apache.flink.util.SerializedThrowable;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.singletonList;
import static org.apache.flink.api.common.JobStatus.RUNNING;
import static org.apache.flink.core.fs.Path.fromLocalFile;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForAllTaskRunning;
import static org.apache.flink.runtime.testutils.CommonTestUtils.waitForJobStatus;
import static org.apache.flink.util.ExceptionUtils.findThrowable;
import static org.apache.flink.util.ExceptionUtils.rethrow;

/**
 * Tests failure handling in channel state persistence.
 *
 * @see <a href="https://issues.apache.org/jira/browse/FLINK-24667">FLINK-24667</a>
 */
public class UnalignedCheckpointFailureHandlingITCase {

    private static final int PARALLELISM = 2;

    @Rule public final TemporaryFolder temporaryFolder = new TemporaryFolder();

    @Rule public final SharedObjects sharedObjects = SharedObjects.create();

    @Rule
    public final MiniClusterWithClientResource miniClusterResource =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration.Builder()
                            .setNumberTaskManagers(PARALLELISM)
                            .setNumberSlotsPerTaskManager(1)
                            .build());

    @Test
    public void testCheckpointSuccessAfterFailure() throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        TestCheckpointStorage storage =
                new TestCheckpointStorage(
                        new JobManagerCheckpointStorage(), sharedObjects, temporaryFolder);

        configure(env, storage);
        buildGraph(env);

        JobClient jobClient = env.executeAsync();
        JobID jobID = jobClient.getJobID();
        MiniCluster miniCluster = miniClusterResource.getMiniCluster();

        waitForJobStatus(jobClient, singletonList(RUNNING));
        waitForAllTaskRunning(miniCluster, jobID, false);

        triggerFailingCheckpoint(jobID, TestException.class, miniCluster);

        miniCluster.triggerCheckpoint(jobID).get();
    }

    private void configure(StreamExecutionEnvironment env, TestCheckpointStorage storage) {
        // enable checkpointing but only via API
        env.enableCheckpointing(Long.MAX_VALUE, CheckpointingMode.EXACTLY_ONCE);

        env.getCheckpointConfig().setCheckpointStorage(storage);

        // use non-snapshotting backend to test channel state persistence integration with
        // checkpoint storage
        env.setStateBackend(new MockStateBackend(MockSnapshotSupplier.EMPTY));

        env.getCheckpointConfig().enableUnalignedCheckpoints();

        env.getCheckpointConfig().setAlignedCheckpointTimeout(Duration.ZERO); // speed-up

        // failures are emitted by the storage
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(Integer.MAX_VALUE);

        // DoP > 1 is required for some barriers to lag
        env.setParallelism(PARALLELISM);

        // no chaining to have input channels (doesn't matter local or remote)
        env.disableOperatorChaining();
    }

    private void buildGraph(StreamExecutionEnvironment env) {
        // with zero alignment timeout some steps here are not strictly necessary currently, but
        // this may change in the future
        env.fromSource(
                        new NumberSequenceSource(0, Long.MAX_VALUE),
                        WatermarkStrategy.noWatermarks(),
                        "num-source")
                // source is not parallel, so keyBy to send to all down-streams
                .keyBy(value -> value)
                // exert back-pressure
                .map(
                        value -> {
                            Thread.sleep(1);
                            return value;
                        })
                .sinkTo(new DiscardingSink<>());
    }

    private void triggerFailingCheckpoint(
            JobID jobID, Class<TestException> expectedException, MiniCluster miniCluster)
            throws InterruptedException, ExecutionException {
        while (true) {
            Optional<Throwable> cpFailure =
                    miniCluster
                            .triggerCheckpoint(jobID)
                            .thenApply(ign -> Optional.empty())
                            .handle((ign, err) -> Optional.ofNullable(err))
                            .get();
            if (!cpFailure.isPresent()) {
                Thread.sleep(50); // trigger again - in case of no channel data was written
            } else if (isCausedBy(cpFailure.get(), expectedException)) {
                return;
            } else {
                rethrow(cpFailure.get());
            }
        }
    }

    private boolean isCausedBy(Throwable t, Class<TestException> expectedException) {
        return findThrowable(t, SerializedThrowable.class)
                .flatMap(
                        st -> {
                            Throwable deser = st.deserializeError(getClass().getClassLoader());
                            return findThrowable(deser, expectedException);
                        })
                .isPresent();
    }

    private static class TestCheckpointStorage implements CheckpointStorage {
        private final CheckpointStorage delegate;
        private final SharedReference<AtomicBoolean> failOnCloseRef;
        private final SharedReference<TemporaryFolder> tempFolderRef;

        private TestCheckpointStorage(
                CheckpointStorage delegate,
                SharedObjects sharedObjects,
                TemporaryFolder tempFolder) {
            this.delegate = delegate;
            this.failOnCloseRef = sharedObjects.add(new AtomicBoolean(true));
            this.tempFolderRef = sharedObjects.add(tempFolder);
        }

        @Override
        public CheckpointStorageAccess createCheckpointStorage(JobID jobId) throws IOException {
            return new TestCheckpointStorageAccess(
                    delegate.createCheckpointStorage(jobId),
                    failOnCloseRef.get(),
                    tempFolderRef.get().newFolder());
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
                throws IOException {
            return delegate.resolveCheckpoint(externalPointer);
        }
    }

    private static class TestCheckpointStorageAccess implements CheckpointStorageAccess {
        private final CheckpointStorageAccess delegate;
        private final AtomicBoolean failOnClose;
        private final File path;

        public TestCheckpointStorageAccess(
                CheckpointStorageAccess delegate, AtomicBoolean failOnClose, File file) {
            this.delegate = delegate;
            this.failOnClose = failOnClose;
            this.path = file;
        }

        @Override
        public CheckpointStreamFactory resolveCheckpointStorageLocation(
                long checkpointId, CheckpointStorageLocationReference reference) {
            return new CheckpointStreamFactory() {
                @Override
                public CheckpointStateOutputStream createCheckpointStateOutputStream(
                        CheckpointedStateScope scope) throws IOException {
                    return new FailingOnceFsCheckpointOutputStream(path, 100, 0, failOnClose);
                }

                @Override
                public boolean canFastDuplicate(
                        StreamStateHandle stateHandle, CheckpointedStateScope scope) {
                    return false;
                }

                @Override
                public List<StreamStateHandle> duplicate(
                        List<StreamStateHandle> stateHandles, CheckpointedStateScope scope)
                        throws IOException {
                    throw new UnsupportedEncodingException();
                }
            };
        }

        @Override
        public CheckpointStateOutputStream createTaskOwnedStateStream() throws IOException {
            return delegate.createTaskOwnedStateStream();
        }

        @Override
        public CheckpointStateToolset createTaskOwnedCheckpointStateToolset() {
            return delegate.createTaskOwnedCheckpointStateToolset();
        }

        @Override
        public boolean supportsHighlyAvailableStorage() {
            return delegate.supportsHighlyAvailableStorage();
        }

        @Override
        public boolean hasDefaultSavepointLocation() {
            return delegate.hasDefaultSavepointLocation();
        }

        @Override
        public CompletedCheckpointStorageLocation resolveCheckpoint(String externalPointer)
                throws IOException {
            return delegate.resolveCheckpoint(externalPointer);
        }

        @Override
        public void initializeBaseLocationsForCheckpoint() throws IOException {
            delegate.initializeBaseLocationsForCheckpoint();
        }

        @Override
        public CheckpointStorageLocation initializeLocationForCheckpoint(long checkpointId)
                throws IOException {
            return delegate.initializeLocationForCheckpoint(checkpointId);
        }

        @Override
        public CheckpointStorageLocation initializeLocationForSavepoint(
                long checkpointId, @Nullable String externalLocationPointer) throws IOException {
            return delegate.initializeLocationForSavepoint(checkpointId, externalLocationPointer);
        }
    }

    private static class FailingOnceFsCheckpointOutputStream extends FsCheckpointStateOutputStream {
        private final AtomicBoolean failOnClose;
        private volatile boolean failedCloseAndGetHandle = false;

        public FailingOnceFsCheckpointOutputStream(
                File path, int bufferSize, int localStateThreshold, AtomicBoolean failOnClose)
                throws IOException {
            super(
                    fromLocalFile(path.getAbsoluteFile()),
                    FileSystem.get(path.toURI()),
                    bufferSize,
                    localStateThreshold);
            this.failOnClose = failOnClose;
        }

        // called on write success
        @Override
        public StreamStateHandle closeAndGetHandle() throws IOException {
            if (failOnClose.get()) {
                failedCloseAndGetHandle = true;
                throw new TestException("failure from closeAndGetHandle");
            } else {
                return super.closeAndGetHandle();
            }
        }

        // called on no data and on failure (in particular of closeAndGetHandle)
        @Override
        public void close() {
            if (failedCloseAndGetHandle && failOnClose.compareAndSet(true, false)) {
                // the contract does allow IO exceptions to be thrown from close(),
                // but FsCheckpointStateOutputStream catches everything, which seems risky to change
                rethrow(new TestException("failure from close"));
            } else {
                super.close();
            }
        }
    }

    private static class TestException extends IOException {
        public TestException(String message) {
            super(message);
        }
    }
}
