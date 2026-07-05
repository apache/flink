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

package org.apache.flink.runtime.checkpoint.channel;

import org.apache.flink.runtime.checkpoint.CheckpointException;
import org.apache.flink.runtime.checkpoint.CheckpointFailureReason;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.RecoverableInputChannel;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests for the {@link RecoveryCheckpointTrigger} singletons and the barrier-inserting {@link
 * InMemoryRecoveryCheckpointTrigger}.
 *
 * <p>FLINK-38544 transitional: deleted together with {@link InMemoryRecoveryCheckpointTrigger} when
 * the spilling backend lands.
 */
class InMemoryRecoveryCheckpointTriggerTest {

    private static final long CHECKPOINT_ID = 42L;

    @Test
    void testNoOpDoesNothing() {
        assertThatCode(
                        () ->
                                RecoveryCheckpointTrigger.NO_OP.snapshotAndInsertBarriers(
                                        CHECKPOINT_ID))
                .doesNotThrowAnyException();
    }

    @Test
    void testNotReadyDeclinesCheckpointAsTaskNotReady() {
        assertThatThrownBy(
                        () ->
                                RecoveryCheckpointTrigger.NOT_READY.snapshotAndInsertBarriers(
                                        CHECKPOINT_ID))
                .isInstanceOf(CheckpointException.class)
                .satisfies(
                        e ->
                                assertThat(((CheckpointException) e).getCheckpointFailureReason())
                                        .isEqualTo(
                                                CheckpointFailureReason
                                                        .CHECKPOINT_DECLINED_TASK_NOT_READY));
    }

    @Test
    void testInMemoryTriggerInsertsBarrierIntoEveryChannel() throws Exception {
        RecordingChannel channel1 = new RecordingChannel();
        RecordingChannel channel2 = new RecordingChannel();

        new InMemoryRecoveryCheckpointTrigger(Arrays.asList(channel1, channel2))
                .snapshotAndInsertBarriers(CHECKPOINT_ID);

        assertThat(channel1.insertedBarriers).containsExactly(CHECKPOINT_ID);
        assertThat(channel2.insertedBarriers).containsExactly(CHECKPOINT_ID);
    }

    @Test
    void testInMemoryTriggerWithoutChannelsIsNoOp() {
        assertThatCode(
                        () ->
                                new InMemoryRecoveryCheckpointTrigger(Collections.emptyList())
                                        .snapshotAndInsertBarriers(CHECKPOINT_ID))
                .doesNotThrowAnyException();
    }

    @Test
    void testInMemoryTriggerPropagatesInsertFailure() {
        RecordingChannel failing =
                new RecordingChannel() {
                    @Override
                    public void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId)
                            throws IOException {
                        throw new IOException("insert failed");
                    }
                };

        assertThatThrownBy(
                        () ->
                                new InMemoryRecoveryCheckpointTrigger(
                                                Collections.singletonList(failing))
                                        .snapshotAndInsertBarriers(CHECKPOINT_ID))
                .isInstanceOf(IOException.class)
                .hasMessage("insert failed");
    }

    private static class RecordingChannel implements RecoverableInputChannel {

        final List<Long> insertedBarriers = new ArrayList<>();

        @Override
        public InputChannelInfo getChannelInfo() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onRecoveredStateBuffer(Buffer buffer) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void finishRecoveredBufferDelivery() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void insertRecoveryCheckpointBarrierIfInRecovery(long checkpointId)
                throws IOException {
            insertedBarriers.add(checkpointId);
        }

        @Override
        public Buffer requestRecoveryBufferBlocking() {
            throw new UnsupportedOperationException();
        }

        @Override
        public void onRecoveredStateConsumed() {
            throw new UnsupportedOperationException();
        }
    }
}
