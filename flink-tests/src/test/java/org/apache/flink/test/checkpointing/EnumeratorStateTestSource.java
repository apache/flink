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

package org.apache.flink.test.checkpointing;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.ReaderOutput;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SourceSplit;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.InputStatus;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.checkpoint.CheckpointStatsSnapshot;
import org.apache.flink.runtime.checkpoint.CompletedCheckpointStats;
import org.apache.flink.runtime.checkpoint.RestoredCheckpointStats;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;

import javax.annotation.Nullable;

import java.util.Collections;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.flink.runtime.testutils.CommonTestUtils.waitUntilCondition;

/**
 * An unbounded source that emits nothing and whose split enumerator checkpoints {@link
 * #ENUMERATOR_STATE}, for tests of how operator coordinator state is stored and restored. A
 * restored enumerator completes {@link #restoredEnumeratorState}, which {@link
 * #waitForRestoredEnumeratorState(MiniCluster, JobID)} waits for. If {@link
 * #FAIL_ENUMERATOR_AFTER_CHECKPOINT} is set, the enumerator fails once after a completed
 * checkpoint, which fails the job globally. If {@link #FAIL_READER_AFTER_CHECKPOINT} is set, the
 * reader fails once after a completed checkpoint, which fails only its task.
 *
 * <p>It implements the Source interfaces directly rather than through {@code
 * AbstractTestSourceBase}, which Flink 1.20, 2.0 and 2.1 lack, so that the savepoints of {@link
 * CoordinatorStateMigrationITCase} can be generated with those versions.
 */
class EnumeratorStateTestSource
        implements Source<Integer, EnumeratorStateTestSource.NoSplit, byte[]> {

    private static final long serialVersionUID = 1L;

    static final int ENUMERATOR_STATE_SIZE = 64 * 1024;

    static final byte[] ENUMERATOR_STATE = new byte[ENUMERATOR_STATE_SIZE];

    static final AtomicBoolean FAIL_ENUMERATOR_AFTER_CHECKPOINT = new AtomicBoolean();

    static final AtomicBoolean FAIL_READER_AFTER_CHECKPOINT = new AtomicBoolean();

    static volatile CompletableFuture<byte[]> restoredEnumeratorState = new CompletableFuture<>();

    static {
        new Random(42).nextBytes(ENUMERATOR_STATE);
    }

    /**
     * Waits until an enumerator of the given job is restored and returns the state it was restored
     * with. Fails if the job terminates first, or if it completes a checkpoint after its restore
     * without having restored an enumerator, as when a restore loses the coordinator state: the
     * coordinator takes part in a checkpoint only once its reset is done.
     */
    static byte[] waitForRestoredEnumeratorState(MiniCluster miniCluster, JobID jobId)
            throws Exception {
        final CompletableFuture<byte[]> restored = restoredEnumeratorState;
        waitUntilCondition(
                () -> {
                    if (restored.isDone()) {
                        return true;
                    }
                    final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
                    return graph.getState().isGloballyTerminalState()
                            || hasCompletedCheckpointAfterRestore(
                                    graph.getCheckpointStatsSnapshot());
                });
        if (!restored.isDone()) {
            final AccessExecutionGraph graph = miniCluster.getExecutionGraph(jobId).get();
            throw new AssertionError(
                    graph.getState().isGloballyTerminalState()
                            ? "The job terminated before its enumerator was restored"
                            : "The job completed a checkpoint after its restore without restoring its enumerator",
                    graph.getFailureInfo() == null ? null : graph.getFailureInfo().getException());
        }
        return restored.get();
    }

    static boolean hasCompletedCheckpointAfterRestore(@Nullable CheckpointStatsSnapshot snapshot) {
        if (snapshot == null) {
            return false;
        }
        final RestoredCheckpointStats restore = snapshot.getLatestRestoredCheckpoint();
        final CompletedCheckpointStats completed =
                snapshot.getHistory().getLatestCompletedCheckpoint();
        return restore != null
                && completed != null
                && completed.getCheckpointId() > restore.getCheckpointId();
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<Integer, NoSplit> createReader(SourceReaderContext readerContext) {
        return new Reader();
    }

    @Override
    public SplitEnumerator<NoSplit, byte[]> createEnumerator(
            SplitEnumeratorContext<NoSplit> enumContext) {
        return new Enumerator(ENUMERATOR_STATE);
    }

    @Override
    public SplitEnumerator<NoSplit, byte[]> restoreEnumerator(
            SplitEnumeratorContext<NoSplit> enumContext, byte[] checkpoint) {
        restoredEnumeratorState.complete(checkpoint);
        return new Enumerator(checkpoint);
    }

    @Override
    public SimpleVersionedSerializer<NoSplit> getSplitSerializer() {
        return new NoSplitSerializer();
    }

    @Override
    public SimpleVersionedSerializer<byte[]> getEnumeratorCheckpointSerializer() {
        return new BytesSerializer();
    }

    /** The split type of the source, which never creates a split. */
    static final class NoSplit implements SourceSplit {

        @Override
        public String splitId() {
            return "no-split";
        }
    }

    private static class Reader implements SourceReader<Integer, NoSplit> {

        @Override
        public void start() {}

        @Override
        public InputStatus pollNext(ReaderOutput<Integer> output) {
            return InputStatus.NOTHING_AVAILABLE;
        }

        @Override
        public List<NoSplit> snapshotState(long checkpointId) {
            return Collections.emptyList();
        }

        @Override
        public CompletableFuture<Void> isAvailable() {
            return new CompletableFuture<>();
        }

        @Override
        public void addSplits(List<NoSplit> splits) {}

        @Override
        public void notifyNoMoreSplits() {}

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (FAIL_READER_AFTER_CHECKPOINT.compareAndSet(true, false)) {
                throw new RuntimeException("Induced failure of the reader after a checkpoint");
            }
        }

        @Override
        public void close() {}
    }

    private static class Enumerator implements SplitEnumerator<NoSplit, byte[]> {

        private final byte[] state;

        Enumerator(byte[] state) {
            this.state = state;
        }

        @Override
        public void start() {}

        @Override
        public void handleSplitRequest(int subtaskId, @Nullable String requesterHostname) {}

        @Override
        public void addSplitsBack(List<NoSplit> splits, int subtaskId) {}

        @Override
        public void addReader(int subtaskId) {}

        @Override
        public byte[] snapshotState(long checkpointId) {
            return state;
        }

        @Override
        public void notifyCheckpointComplete(long checkpointId) {
            if (FAIL_ENUMERATOR_AFTER_CHECKPOINT.compareAndSet(true, false)) {
                throw new RuntimeException("Induced failure after a completed checkpoint");
            }
        }

        @Override
        public void close() {}
    }

    private static class NoSplitSerializer implements SimpleVersionedSerializer<NoSplit> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(NoSplit split) {
            return new byte[0];
        }

        @Override
        public NoSplit deserialize(int version, byte[] serialized) {
            return new NoSplit();
        }
    }

    private static class BytesSerializer implements SimpleVersionedSerializer<byte[]> {

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(byte[] obj) {
            return obj;
        }

        @Override
        public byte[] deserialize(int version, byte[] serialized) {
            return serialized;
        }
    }
}
