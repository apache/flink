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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.FetchedChannelStateReader;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveryCheckpointTrigger;
import org.apache.flink.runtime.checkpoint.channel.ResultSubpartitionInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;
import org.apache.flink.util.CloseableIterator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link ChannelState#onCheckpointStartedForAllInputs} dispatcher: call ordering and
 * feature-off no-op routing through the {@link RecoveryCheckpointTrigger#NO_OP} singleton.
 */
class ChannelStateTest {

    private static final long CHECKPOINT_ID = 7L;

    @Test
    void testStepOrderingFeatureOn() throws Exception {
        List<String> trace = new ArrayList<>();
        // An empty reader is sufficient to verify ordering.
        FetchedChannelStateReader snap = FetchedChannelStateReader.emptyReader();
        RecordingTrigger trigger = new RecordingTrigger(trace, snap);
        RecordingWriter writer = new RecordingWriter(trace);
        CheckpointableInput input1 = new RecordingInput(trace, "in1");
        CheckpointableInput input2 = new RecordingInput(trace, "in2");

        ChannelState state =
                new ChannelState(new CheckpointableInput[] {input1, input2}, trigger, writer);

        CheckpointBarrier barrier = newUnalignedBarrier();
        state.onCheckpointStartedForAllInputs(barrier);

        assertThat(trace)
                .containsExactly(
                        "trigger.snapshotAndInsertBarriers:" + CHECKPOINT_ID,
                        "in1.checkpointStarted:" + CHECKPOINT_ID,
                        "in2.checkpointStarted:" + CHECKPOINT_ID,
                        "writer.addInputDataFromSpill:" + CHECKPOINT_ID);
    }

    @Test
    void testStepOrderingFeatureOff() throws Exception {
        List<String> trace = new ArrayList<>();
        RecordingWriter writer = new RecordingWriter(trace);
        CheckpointableInput input = new RecordingInput(trace, "in1");

        ChannelState state =
                new ChannelState(
                        new CheckpointableInput[] {input}, RecoveryCheckpointTrigger.NO_OP, writer);

        state.onCheckpointStartedForAllInputs(newUnalignedBarrier());

        assertThat(trace)
                .containsExactly(
                        "in1.checkpointStarted:" + CHECKPOINT_ID,
                        "writer.addInputDataFromSpill:" + CHECKPOINT_ID);
        assertThat(writer.lastSnapshotWasEmpty.get()).isTrue();
    }

    @Test
    void testEmptySnapshotStillSubmitted() throws Exception {
        // Empty readers (no spill files) are no longer short-circuited; they still reach
        // addInputDataFromSpill on the writer thread.
        List<String> trace = new ArrayList<>();
        FetchedChannelStateReader emptySnap = FetchedChannelStateReader.emptyReader();
        RecordingTrigger trigger = new RecordingTrigger(trace, emptySnap);
        RecordingWriter writer = new RecordingWriter(trace);

        ChannelState state =
                new ChannelState(
                        new CheckpointableInput[] {new RecordingInput(trace, "in1")},
                        trigger,
                        writer);

        state.onCheckpointStartedForAllInputs(newUnalignedBarrier());

        // Empty reader must still reach the writer (no inline short-circuit).
        assertThat(writer.addInputDataFromSpillCalls.get()).isEqualTo(1);
        assertThat(writer.lastSnapshotWasEmpty.get()).isTrue();
    }

    private static CheckpointBarrier newUnalignedBarrier() {
        return new CheckpointBarrier(
                CHECKPOINT_ID,
                1000L,
                CheckpointOptions.unaligned(
                        CheckpointType.CHECKPOINT,
                        CheckpointStorageLocationReference.getDefault()));
    }

    private static final class RecordingTrigger implements RecoveryCheckpointTrigger {
        private final List<String> trace;
        private final FetchedChannelStateReader snapshot;

        RecordingTrigger(List<String> trace, FetchedChannelStateReader snapshot) {
            this.trace = trace;
            this.snapshot = snapshot;
        }

        @Override
        public FetchedChannelStateReader snapshotAndInsertBarriers(long checkpointId) {
            trace.add("trigger.snapshotAndInsertBarriers:" + checkpointId);
            return snapshot;
        }
    }

    private static final class RecordingWriter implements ChannelStateWriter {
        private final List<String> trace;
        final AtomicBoolean lastSnapshotWasEmpty = new AtomicBoolean(false);
        final AtomicLong lastCpId = new AtomicLong(-1L);
        final AtomicInteger addInputDataFromSpillCalls = new AtomicInteger(0);

        RecordingWriter(List<String> trace) {
            this.trace = trace;
        }

        @Override
        public void start(long checkpointId, CheckpointOptions checkpointOptions) {}

        @Override
        public void addInputData(
                long checkpointId,
                InputChannelInfo info,
                int startSeqNum,
                CloseableIterator<Buffer> data) {}

        @Override
        public void addOutputData(
                long checkpointId, ResultSubpartitionInfo info, int startSeqNum, Buffer... data) {}

        @Override
        public void addOutputDataFuture(
                long checkpointId,
                ResultSubpartitionInfo info,
                int startSeqNum,
                CompletableFuture<List<Buffer>> data) {}

        @Override
        public void finishInput(long checkpointId) {}

        @Override
        public void finishOutput(long checkpointId) {}

        @Override
        public void abort(long checkpointId, Throwable cause, boolean cleanup) {}

        @Override
        public ChannelStateWriteResult getAndRemoveWriteResult(long checkpointId) {
            return ChannelStateWriteResult.EMPTY;
        }

        @Override
        public void addInputDataFromSpill(long checkpointId, FetchedChannelStateReader reader) {
            trace.add("writer.addInputDataFromSpill:" + checkpointId);
            lastCpId.set(checkpointId);
            addInputDataFromSpillCalls.incrementAndGet();
            try {
                // Peek whether the reader has any segments by attempting the first advance.
                // The first nextSegment() call is exempt from the "previous body consumed" rule.
                lastSnapshotWasEmpty.set(reader.nextSegment().isEmpty());
                reader.close();
            } catch (Exception ignored) {
            }
        }

        @Override
        public void close() {}
    }

    private static final class RecordingInput implements CheckpointableInput {

        private final List<String> trace;
        private final String name;

        RecordingInput(List<String> trace, String name) {
            this.trace = trace;
            this.name = name;
        }

        @Override
        public void blockConsumption(InputChannelInfo channelInfo) {}

        @Override
        public void resumeConsumption(InputChannelInfo channelInfo) {}

        @Override
        public List<InputChannelInfo> getChannelInfos() {
            return Collections.emptyList();
        }

        @Override
        public int getNumberOfInputChannels() {
            return 0;
        }

        @Override
        public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {
            trace.add(name + ".checkpointStarted:" + barrier.getId());
        }

        @Override
        public void checkpointStopped(long cancelledCheckpointId) {}

        @Override
        public int getInputGateIndex() {
            return 0;
        }

        @Override
        public void convertToPriorityEvent(int channelIndex, int sequenceNumber) {}
    }
}
