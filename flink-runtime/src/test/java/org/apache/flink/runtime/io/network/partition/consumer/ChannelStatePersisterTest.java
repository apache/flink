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

    /**
     * Build a persister bound to a fresh {@link RecoveredBufferStore#EMPTY} so the test holds the
     * same store monitor that the persister asserts on. Tests that need a non-empty store pass one
     * explicitly to {@link #newPersister(ChannelStateWriter, InputChannelInfo,
     * RecoveredBufferStore)}.
     */
    private static ChannelStatePersister newPersister(
            ChannelStateWriter writer, InputChannelInfo channelInfo) {
        return newPersister(writer, channelInfo, RecoveredBufferStore.EMPTY);
    }

    private static ChannelStatePersister newPersister(
            ChannelStateWriter writer, InputChannelInfo channelInfo, RecoveredBufferStore store) {
        return new ChannelStatePersister(writer, channelInfo, store);
    }

    @Test
    void testNewBarrierNotOverwrittenByStopPersisting() throws Exception {
        RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
        RecoveredBufferStore store = RecoveredBufferStore.EMPTY;
        ChannelStatePersister persister = newPersister(channelStateWriter, channelInfo, store);

        long checkpointId = 1L;
        channelStateWriter.start(
                checkpointId, CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));

        synchronized (store) {
            persister.checkForBarrier(barrier(checkpointId));
            persister.startPersisting(checkpointId, Arrays.asList(buildSomeBuffer()));
        }
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(1);

        synchronized (store) {
            persister.maybePersist(buildSomeBuffer());
        }
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(1);

        // meanwhile, checkpoint coordinator timed out the 1st checkpoint and started the 2nd
        // now task thread is picking up the barrier and aborts the 1st:
        synchronized (store) {
            persister.checkForBarrier(barrier(checkpointId + 1));
            persister.maybePersist(buildSomeBuffer());
            persister.stopPersisting(checkpointId);
            persister.maybePersist(buildSomeBuffer());
        }
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(1);

        synchronized (store) {
            assertThat(persister.hasBarrierReceived()).isTrue();
        }
    }

    @Test
    void testNewBarrierNotOverwrittenByCheckForBarrier() throws Exception {
        RecoveredBufferStore store = RecoveredBufferStore.EMPTY;
        ChannelStatePersister persister =
                newPersister(ChannelStateWriter.NO_OP, new InputChannelInfo(0, 0), store);

        synchronized (store) {
            persister.startPersisting(1L, Collections.emptyList());
            persister.startPersisting(2L, Collections.emptyList());

            assertThat(persister.checkForBarrier(barrier(1L))).isNotPresent();

            assertThat(persister.hasBarrierReceived()).isFalse();
        }
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
        RecoveredBufferStore store = RecoveredBufferStore.EMPTY;

        ChannelStatePersister persister = newPersister(channelStateWriter, channelInfo, store);

        long lateCheckpointId = 1L;
        long checkpointId = 2L;
        synchronized (store) {
            if (startCheckpointOnLateBarrier) {
                persister.startPersisting(lateCheckpointId, Collections.emptyList());
            }
            if (cancelCheckpointBeforeLateBarrier) {
                persister.stopPersisting(lateCheckpointId);
            }
            persister.checkForBarrier(barrier(lateCheckpointId));
        }
        channelStateWriter.start(
                checkpointId, CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));
        synchronized (store) {
            persister.startPersisting(checkpointId, Arrays.asList(buildSomeBuffer()));
            persister.maybePersist(buildSomeBuffer());
            persister.checkForBarrier(barrier(checkpointId));
            persister.maybePersist(buildSomeBuffer());

            assertThat(persister.hasBarrierReceived()).isTrue();
        }
        assertThat(channelStateWriter.getAddedInput().get(channelInfo)).hasSize(2);
    }

    @Test
    void testStartPersistingRejectsNonEmptyStoreAndNonEmptyKnownBuffers() throws Exception {
        // Invariant: store non-empty and knownBuffers non-empty must not coexist. This is
        // guaranteed by UNALIGNED_RECOVER_OUTPUT_ON_DOWNSTREAM=true (upstream does not replay
        // output state) combined with RemoteInputChannel#getNextBuffer draining the store before
        // polling receivedBuffers. Violating this means one of those assumptions broke.
        RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);

        long checkpointId = 1L;
        channelStateWriter.start(
                checkpointId, CheckpointOptions.unaligned(CheckpointType.CHECKPOINT, getDefault()));

        RecoveredBufferStoreImpl nonEmptyStore = new RecoveredBufferStoreImpl(channelInfo);
        synchronized (nonEmptyStore) {
            nonEmptyStore.addBuffer(buildSomeBuffer());
        }
        ChannelStatePersister persister =
                newPersister(channelStateWriter, channelInfo, nonEmptyStore);
        assertThatThrownBy(
                        () -> {
                            synchronized (nonEmptyStore) {
                                persister.startPersisting(
                                        checkpointId, Arrays.asList(buildSomeBuffer()));
                            }
                        })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("Invariant violated");
    }

    @Test
    void testLateBarrierTriggeringCheckpoint() throws Exception {
        RecoveredBufferStore store = RecoveredBufferStore.EMPTY;
        ChannelStatePersister persister =
                newPersister(ChannelStateWriter.NO_OP, new InputChannelInfo(0, 0), store);

        long lateCheckpointId = 1L;
        long checkpointId = 2L;

        synchronized (store) {
            persister.checkForBarrier(barrier(checkpointId));
        }
        assertThatThrownBy(
                        () -> {
                            synchronized (store) {
                                persister.startPersisting(
                                        lateCheckpointId, Collections.emptyList());
                            }
                        })
                .isInstanceOf(CheckpointException.class);
    }

    private static Buffer barrier(long id) throws IOException {
        return EventSerializer.toBuffer(
                new CheckpointBarrier(id, 1L, CheckpointOptions.forCheckpointWithDefaultLocation()),
                true);
    }
}
