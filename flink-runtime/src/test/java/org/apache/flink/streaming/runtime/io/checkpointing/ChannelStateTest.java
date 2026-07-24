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
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecoveryCheckpointTrigger;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;
import org.apache.flink.runtime.state.CheckpointStorageLocationReference;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Verifies the {@link ChannelState#onCheckpointStartedForAllInputs} dispatcher: call ordering and
 * feature-off no-op routing through the {@link RecoveryCheckpointTrigger#NO_OP} singleton.
 *
 * <p>FLINK-38544 transitional: this covers the 2-step dispatch (trigger, then per-input
 * notification); the spilling backend adds a third step handing the trigger's snapshot reader to
 * the channel-state writer and completes this test to cover all three steps.
 */
class ChannelStateTest {

    private static final long CHECKPOINT_ID = 7L;

    @Test
    void testStepOrderingFeatureOn() throws Exception {
        List<String> trace = new ArrayList<>();
        RecordingTrigger trigger = new RecordingTrigger(trace);
        CheckpointableInput input1 = new RecordingInput(trace, "in1");
        CheckpointableInput input2 = new RecordingInput(trace, "in2");

        ChannelState state = new ChannelState(new CheckpointableInput[] {input1, input2}, trigger);

        CheckpointBarrier barrier = newUnalignedBarrier();
        state.onCheckpointStartedForAllInputs(barrier);

        assertThat(trace)
                .containsExactly(
                        "trigger.snapshotAndInsertBarriers:" + CHECKPOINT_ID,
                        "in1.checkpointStarted:" + CHECKPOINT_ID,
                        "in2.checkpointStarted:" + CHECKPOINT_ID);
    }

    @Test
    void testStepOrderingFeatureOff() throws Exception {
        List<String> trace = new ArrayList<>();
        CheckpointableInput input = new RecordingInput(trace, "in1");

        ChannelState state =
                new ChannelState(
                        new CheckpointableInput[] {input}, RecoveryCheckpointTrigger.NO_OP);

        state.onCheckpointStartedForAllInputs(newUnalignedBarrier());

        assertThat(trace).containsExactly("in1.checkpointStarted:" + CHECKPOINT_ID);
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

        RecordingTrigger(List<String> trace) {
            this.trace = trace;
        }

        @Override
        public void snapshotAndInsertBarriers(long checkpointId) {
            trace.add("trigger.snapshotAndInsertBarriers:" + checkpointId);
        }
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
