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

package org.apache.flink.runtime.io.network.partition.consumer;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.channel.ChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;

import static org.apache.flink.runtime.io.network.buffer.BufferBuilderTestUtils.buildSomeBuffer;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** {@link ChannelStatePersister} test. */
class ChannelStatePersisterTest {

    @Test
    void testNewBarrierNotOverwrittenByStopPersisting() throws Exception {
        RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
        ChannelStatePersister persister =
                new ChannelStatePersister(channelStateWriter, channelInfo);

        long checkpointId = 1L;
        channelStateWriter.start(
                checkpointId, CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));

        persister.checkForBarrier(barrier(checkpointId));
        persister.startPersisting(checkpointId, Arrays.asList(buildSomeBuffer()));
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(1);

        persister.maybePersist(buildSomeBuffer());
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(1);

        // meanwhile, checkpoint coordinator timed out the 1st checkpoint and started the 2nd
        // now task thread is picking up the barrier and aborts the 1st:
        persister.checkForBarrier(barrier(checkpointId + 1));
        persister.maybePersist(buildSomeBuffer());
        persister.stopPersisting(checkpointId);
        persister.maybePersist(buildSomeBuffer());
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(1);

        assertThat(persister.hasBarrierReceived()).isTrue();
    }

    @Test
    void testNewBarrierNotOverwrittenByCheckForBarrier() throws Exception {
        ChannelStatePersister persister =
                new ChannelStatePersister(ChannelStateWriter.NO_OP, new InputChannelInfo(0, 0));

        persister.startPersisting(1L, Collections.emptyList());
        persister.startPersisting(2L, Collections.emptyList());

        assertThat(persister.checkForBarrier(barrier(1L))).isNotPresent();

        assertThat(persister.hasBarrierReceived()).isFalse();
    }

    @Test
    void testLateBarrierOnStartedAndCancelledCheckpoint() throws Exception {
        testLateBarrier(true, true);
    }

    @Test
    void testLateBarrierOnCancelledCheckpoint() throws Exception {
        testLateBarrier(false, true);
    }

    @Test
    void testLateBarrierOnNotYetCancelledCheckpoint() throws Exception {
        testLateBarrier(false, false);
    }

    private void testLateBarrier(
            boolean startCheckpointOnLateBarrier, boolean cancelCheckpointBeforeLateBarrier)
            throws Exception {
        RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);

        ChannelStatePersister persister =
                new ChannelStatePersister(channelStateWriter, channelInfo);

        long lateCheckpointId = 1L;
        long checkpointId = 2L;
        if (startCheckpointOnLateBarrier) {
            persister.startPersisting(lateCheckpointId, Collections.emptyList());
        }
        if (cancelCheckpointBeforeLateBarrier) {
            persister.stopPersisting(lateCheckpointId);
        }
        persister.checkForBarrier(barrier(lateCheckpointId));
        channelStateWriter.start(
                checkpointId, CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));
        persister.startPersisting(checkpointId, Arrays.asList(buildSomeBuffer()));
        persister.maybePersist(buildSomeBuffer());
        persister.checkForBarrier(barrier(checkpointId));
        persister.maybePersist(buildSomeBuffer());

        assertThat(persister.hasBarrierReceived()).isTrue();
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(2);
    }

    @Test
    void testLateBarrierTriggeringCheckpoint() throws Exception {
        ChannelStatePersister persister =
                new ChannelStatePersister(ChannelStateWriter.NO_OP, new InputChannelInfo(0, 0));

        long lateCheckpointId = 1L;
        long checkpointId = 2L;

        persister.checkForBarrier(barrier(checkpointId));
        assertThatThrownBy(
                        () -> persister.startPersisting(lateCheckpointId, Collections.emptyList()))
                .isInstanceOf(CheckpointException.class);
    }

    private static Buffer barrier(long id) throws IOException {
        return EventSerializer.toBuffer(
                new CheckpointBarrier(id, 1L, CheckpointOptions.forCheckpointWithDefaultLocation()),
                true);
    }
}
