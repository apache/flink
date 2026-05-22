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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.api.common.JobID;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.jobgraph.JobVertexID;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.Assertions.assertThat;

class ChannelStateWriterImplAddInputDataFromSpillTest {

    private static final JobID JOB_ID = new JobID();
    private static final JobVertexID JOB_VERTEX_ID = new JobVertexID();
    private static final int SUBTASK_INDEX = 0;
    private static final long CHECKPOINT_ID = 7L;
    private static final String TASK_NAME = "test";

    @TempDir Path tempDir;

    @Test
    void testNonEmptySnapshotAsyncDemux() throws Exception {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer = newWriter(worker)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            InputChannelInfo c0 = new InputChannelInfo(0, 0);
            InputChannelInfo c1 = new InputChannelInfo(0, 1);

            FetchedChannelState state;
            try (TestSpillWriter spillWriter = new TestSpillWriter(tempDir)) {
                spillWriter.writeRecord(c0, new byte[] {1, 2, 3}, 3);
                spillWriter.writeRecord(c1, new byte[] {4, 5}, 2);
                spillWriter.writeRecord(c0, new byte[] {6}, 1);
                state = spillWriter.getChannelState();
            }
            FetchedChannelStateReader reader = state.reader();
            // Drop the handoff grant; the reader now holds the only outstanding grant.
            state.release();

            writer.addInputDataFromSpill(CHECKPOINT_ID, reader);
            // Request is queued but not yet processed — state must still be alive.
            assertThat(state.isClosed()).isFalse();

            worker.processAllRequests();
            // After processing, the reader is closed by the request, releasing the last grant.
            assertThat(state.isClosed()).isTrue();
        }
    }

    @Test
    void testEmptySnapshotStillSubmitted() throws Exception {
        // Empty readers are no longer short-circuited; they are submitted to the writer thread.
        QueueCountingExecutor worker = new QueueCountingExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer =
                new ChannelStateWriterImpl(
                        JOB_VERTEX_ID,
                        TASK_NAME,
                        SUBTASK_INDEX,
                        new ConcurrentHashMap<>(),
                        worker,
                        5)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            int submittedBefore = worker.submitCount.get();
            FetchedChannelState emptyState =
                    new FetchedChannelState(java.util.Collections.emptyList());
            FetchedChannelStateReader emptyReader = emptyState.reader();
            emptyState.release();

            writer.addInputDataFromSpill(CHECKPOINT_ID, emptyReader);

            assertThat(worker.submitCount.get())
                    .as("empty reader must still be submitted to the writer thread")
                    .isGreaterThan(submittedBefore);
        }
    }

    @Test
    void testSegmentsClosedOnSuccess() throws Exception {
        SyncChannelStateWriteRequestExecutor worker =
                new SyncChannelStateWriteRequestExecutor(JOB_ID);
        try (ChannelStateWriterImpl writer = newWriter(worker)) {
            worker.registerSubtask(JOB_VERTEX_ID, SUBTASK_INDEX);
            writer.start(CHECKPOINT_ID, CheckpointOptions.forCheckpointWithDefaultLocation());

            FetchedChannelState state;
            try (TestSpillWriter spillWriter = new TestSpillWriter(tempDir)) {
                spillWriter.writeRecord(new InputChannelInfo(0, 0), new byte[] {1}, 1);
                state = spillWriter.getChannelState();
            }
            FetchedChannelStateReader reader = state.reader();
            state.release();

            writer.addInputDataFromSpill(CHECKPOINT_ID, reader);
            worker.processAllRequests();

            // After processing, the last grant is released and the state is cleaned up.
            assertThat(state.isClosed()).isTrue();
        }
    }

    // -------------------------------------------------------------------------------------------
    // Helpers
    // -------------------------------------------------------------------------------------------

    private ChannelStateWriterImpl newWriter(SyncChannelStateWriteRequestExecutor worker) {
        return new ChannelStateWriterImpl(
                JOB_VERTEX_ID, TASK_NAME, SUBTASK_INDEX, new ConcurrentHashMap<>(), worker, 5);
    }

    // -------------------------------------------------------------------------------------------
    // Executor stubs
    // -------------------------------------------------------------------------------------------

    private static final class QueueCountingExecutor implements ChannelStateWriteRequestExecutor {

        final AtomicInteger submitCount = new AtomicInteger(0);

        QueueCountingExecutor(JobID jobID) {}

        @Override
        public void submit(ChannelStateWriteRequest e) {
            submitCount.incrementAndGet();
        }

        @Override
        public void submitPriority(ChannelStateWriteRequest e) {
            submitCount.incrementAndGet();
        }

        @Override
        public void start() throws IllegalStateException {}

        @Override
        public void registerSubtask(JobVertexID jobVertexID, int subtaskIndex) {}

        @Override
        public void releaseSubtask(JobVertexID jobVertexID, int subtaskIndex) {}
    }
}
