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
import org.apache.flink.runtime.checkpoint.CheckpointMetaData;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.FinalizeBarrier;
import org.apache.flink.runtime.io.network.partition.consumer.CheckpointableInput;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedNoTimeout;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedWithTimeout;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.junit.Assert.assertEquals;

/*
 * Tests the behavior of {@link FinalizeBarrierComplementProcessor}.
 */
public class FinalizeBarrierComplementProcessorTest {
    private static final CheckpointOptions UNALIGNED = CheckpointOptions.unaligned(getDefault());
    private static final CheckpointOptions ALIGNED_WITH_TIMEOUT =
            alignedWithTimeout(getDefault(), 10);
    private static final CheckpointOptions ALIGNED_NO_TIMEOUT =
            alignedNoTimeout(CheckpointType.CHECKPOINT, getDefault());

    @Test
    public void testNotInsertOnFinalizeBarrierWithoutPendingCheckpoints() throws IOException {
        RecordingCheckpointableInput[] inputs = createInputs(2);
        FinalizeBarrierComplementProcessor processor =
                createFinalizeBarrierComplementProcessor(inputs);

        processor.processFinalBarrier(new FinalizeBarrier(5), new InputChannelInfo(0, 0));
        assertEquals(0, inputs[0].getInsertedBarriers(0).size());
    }

    @Test
    public void testInsertOnFinalizeBarrierForPendingCheckpoint() throws IOException {
        RecordingCheckpointableInput[] inputs = createInputs(2);
        FinalizeBarrierComplementProcessor processor =
                createFinalizeBarrierComplementProcessor(inputs);

        CheckpointBarrier[] barriers = {
            createBarrier(4, ALIGNED_NO_TIMEOUT),
            createBarrier(5, ALIGNED_WITH_TIMEOUT),
            createBarrier(6, UNALIGNED),
            createBarrier(9, ALIGNED_NO_TIMEOUT)
        };

        for (CheckpointBarrier barrier : barriers) {
            processor.onCheckpointAlignmentStart(barrier);
        }

        processor.processFinalBarrier(new FinalizeBarrier(5), new InputChannelInfo(0, 0));
        assertEquals(
                Arrays.asList(barriers[1], barriers[2], barriers[3]),
                inputs[0].getInsertedBarriers(0));
    }

    @Test
    public void testInsertBarriersOnCheckpointStart() throws IOException {
        RecordingCheckpointableInput[] inputs = createInputs(2);
        FinalizeBarrierComplementProcessor processor =
                createFinalizeBarrierComplementProcessor(inputs);

        processor.processFinalBarrier(new FinalizeBarrier(5), new InputChannelInfo(0, 0));
        processor.processFinalBarrier(new FinalizeBarrier(0), new InputChannelInfo(0, 1));
        processor.processFinalBarrier(new FinalizeBarrier(3), new InputChannelInfo(1, 0));

        CheckpointBarrier barrier = createBarrier(3, UNALIGNED);
        processor.onCheckpointAlignmentStart(barrier);

        for (InputChannelInfo channelInfo :
                Arrays.asList(new InputChannelInfo(0, 1), new InputChannelInfo(1, 0))) {
            assertEquals(
                    Collections.singletonList(barrier),
                    inputs[channelInfo.getGateIdx()].getInsertedBarriers(
                            channelInfo.getInputChannelIdx()));
        }
    }

    @Test
    public void testPendingCheckpointsRemovedOnCheckpointStop() throws IOException {
        RecordingCheckpointableInput[] inputs = createInputs(2);
        FinalizeBarrierComplementProcessor processor =
                createFinalizeBarrierComplementProcessor(inputs);

        CheckpointBarrier[] barriers = {
            createBarrier(4, ALIGNED_NO_TIMEOUT),
            createBarrier(5, ALIGNED_WITH_TIMEOUT),
            createBarrier(6, UNALIGNED),
            createBarrier(9, ALIGNED_NO_TIMEOUT)
        };

        for (CheckpointBarrier barrier : barriers) {
            processor.onCheckpointAlignmentStart(barrier);
        }

        // Which would clean all the checkpoints whose id <= 5.
        processor.onCheckpointAlignmentEnd(5);

        processor.processFinalBarrier(new FinalizeBarrier(0), new InputChannelInfo(0, 0));
        assertEquals(Arrays.asList(barriers[2], barriers[3]), inputs[0].getInsertedBarriers(0));
    }

    @Test
    public void testInsertBarriersOnRpcTrigger() throws IOException {
        RecordingCheckpointableInput[] inputs = createInputs(2);
        FinalizeBarrierComplementProcessor processor =
                createFinalizeBarrierComplementProcessor(inputs);

        processor.processFinalBarrier(new FinalizeBarrier(5), new InputChannelInfo(0, 0));
        processor.processFinalBarrier(new FinalizeBarrier(0), new InputChannelInfo(0, 1));
        processor.processFinalBarrier(new FinalizeBarrier(3), new InputChannelInfo(1, 0));

        CheckpointBarrier barrier = createBarrier(3, UNALIGNED);
        processor.onTriggeringCheckpoint(
                new CheckpointMetaData(barrier.getId(), barrier.getTimestamp()),
                barrier.getCheckpointOptions());

        for (InputChannelInfo channelInfo :
                Arrays.asList(new InputChannelInfo(0, 1), new InputChannelInfo(1, 0))) {
            assertEquals(
                    Collections.singletonList(barrier),
                    inputs[channelInfo.getGateIdx()].getInsertedBarriers(
                            channelInfo.getInputChannelIdx()));
        }
    }

    // ---------------------  Utilities -----------------------------------

    private CheckpointBarrier createBarrier(long checkpointId, CheckpointOptions options) {
        return new CheckpointBarrier(checkpointId, 101, options);
    }

    private FinalizeBarrierComplementProcessor createFinalizeBarrierComplementProcessor(
            RecordingCheckpointableInput... inputs) {
        FinalizeBarrierComplementProcessor processor =
                new FinalizeBarrierComplementProcessor(inputs);
        processor.setAllowComplementBarrier(true);
        return processor;
    }

    private RecordingCheckpointableInput[] createInputs(int numberOfInputs) {
        RecordingCheckpointableInput[] inputs = new RecordingCheckpointableInput[numberOfInputs];
        for (int i = 0; i < numberOfInputs; ++i) {
            inputs[i] = new RecordingCheckpointableInput();
        }

        return inputs;
    }

    private static class RecordingCheckpointableInput implements CheckpointableInput {
        private Map<Integer, List<CheckpointBarrier>> insertedBarriers = new HashMap<>();

        @Override
        public void insertBarrierBeforeEndOfPartition(int channelIndex, CheckpointBarrier barrier)
                throws IOException {
            insertedBarriers.computeIfAbsent(channelIndex, k -> new ArrayList<>()).add(barrier);
        }

        public List<CheckpointBarrier> getInsertedBarriers(int channelIndex) {
            return insertedBarriers.getOrDefault(channelIndex, new ArrayList<>());
        }

        public void reset() {
            insertedBarriers.clear();
        }

        @Override
        public void blockConsumption(InputChannelInfo channelInfo) {}

        @Override
        public void resumeConsumption(InputChannelInfo channelInfo) throws IOException {}

        @Override
        public List<InputChannelInfo> getChannelInfos() {
            return null;
        }

        @Override
        public int getNumberOfInputChannels() {
            return 0;
        }

        @Override
        public void checkpointStarted(CheckpointBarrier barrier) throws CheckpointException {}

        @Override
        public void checkpointStopped(long cancelledCheckpointId) {}

        @Override
        public int getInputGateIndex() {
            return 0;
        }

        @Override
        public void convertToPriorityEvent(int channelIndex, int sequenceNumber)
                throws IOException {}
    }
}
