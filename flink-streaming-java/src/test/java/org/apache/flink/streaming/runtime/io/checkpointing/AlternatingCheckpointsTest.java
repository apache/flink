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

package org.apache.flink.streaming.runtime.io.checkpointing;

import org.apache.flink.core.execution.SavepointFormatType;
import org.apache.flink.runtime.checkpoint.CheckpointOptions;
import org.apache.flink.runtime.checkpoint.CheckpointType;
import org.apache.flink.runtime.checkpoint.SavepointType;
import org.apache.flink.runtime.checkpoint.SnapshotType;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.event.RuntimeEvent;
import org.apache.flink.runtime.io.network.api.CancelCheckpointMarker;
import org.apache.flink.runtime.io.network.api.CheckpointBarrier;
import org.apache.flink.runtime.io.network.api.EndOfPartitionEvent;
import org.apache.flink.runtime.io.network.api.EventAnnouncement;
import org.apache.flink.runtime.io.network.api.serialization.EventSerializer;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.partition.consumer.BufferOrEvent;
import org.apache.flink.runtime.io.network.partition.consumer.InputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.RemoteInputChannel;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGate;
import org.apache.flink.runtime.io.network.partition.consumer.SingleInputGateBuilder;
import org.apache.flink.runtime.io.network.partition.consumer.TestInputChannel;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.streaming.runtime.io.checkpointing.BarrierAlignmentUtil.Cancellable;
import org.apache.flink.streaming.util.TestCheckpointedInputGateBuilder;
import org.apache.flink.util.clock.Clock;
import org.apache.flink.util.clock.ManualClock;
import org.apache.flink.util.concurrent.FutureUtils;

import org.junit.jupiter.api.Test;

import javax.annotation.Nonnull;

import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

import static org.apache.flink.core.testutils.FlinkAssertions.assertThatFuture;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.AlignmentType;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedNoTimeout;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.alignedWithTimeout;
import static org.apache.flink.runtime.checkpoint.CheckpointOptions.unaligned;
import static org.apache.flink.runtime.checkpoint.CheckpointType.CHECKPOINT;
import static org.apache.flink.runtime.io.network.api.serialization.EventSerializer.toBuffer;
import static org.apache.flink.runtime.io.network.util.TestBufferFactory.createBuffer;
import static org.apache.flink.runtime.state.CheckpointStorageLocationReference.getDefault;
import static org.assertj.core.api.Assertions.assertThat;

/** Timing out aligned checkpoints tests. */
class AlternatingCheckpointsTest {

    private final ClockWithDelayedActions clock = new ClockWithDelayedActions();

    private TestBarrierHandlerFactory getTestBarrierHandlerFactory(
            ValidatingCheckpointHandler target) {
        return TestBarrierHandlerFactory.forTarget(target)
                .withActionRegistration(clock)
                .withClock(clock);
    }

    /**
     * Upon subsuming (or canceling) a checkpoint, channels should be notified regardless of whether
     * UC controller is currently being used or not. Otherwise, channels may not capture in-flight
     * buffers.
     */
    @Test
    void testChannelResetOnNewBarrier() throws Exception {
        RecordingChannelStateWriter stateWriter = new RecordingChannelStateWriter();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                2, getTestBarrierHandlerFactory(new ValidatingCheckpointHandler()))
                        .withChannelStateWriter(stateWriter)
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {

            sendBarrier(
                    0,
                    clock.relativeTimeMillis(),
                    SavepointType.savepoint(SavepointFormatType.CANONICAL),
                    gate,
                    0); // using AC because UC would require ordering in gate while polling
            ((RemoteInputChannel) gate.getChannel(0))
                    .onBuffer(createBuffer(1024), 1, 0, 0); // to be captured
            send(
                    toBuffer(
                            new CheckpointBarrier(
                                    1,
                                    clock.relativeTimeMillis(),
                                    unaligned(CheckpointType.CHECKPOINT, getDefault())),
                            true),
                    1,
                    gate);

            assertThat(stateWriter.getAddedInput().isEmpty()).isFalse();
        }
    }

    /**
     * If a checkpoint announcement was processed from one channel and then UC-barrier arrives on
     * another channel, this UC barrier should be processed by the UC controller.
     */
    @Test
    void testSwitchToUnalignedByUpstream() throws Exception {
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(2, getTestBarrierHandlerFactory(target))
                        .build()) {

            CheckpointBarrier aligned =
                    new CheckpointBarrier(
                            1,
                            clock.relativeTimeMillis(),
                            alignedWithTimeout(
                                    CheckpointType.CHECKPOINT, getDefault(), Integer.MAX_VALUE));

            send(
                    toBuffer(new EventAnnouncement(aligned, 0), true),
                    0,
                    gate); // process announcement but not the barrier
            assertThat(target.triggeredCheckpointCounter).isZero();
            send(
                    toBuffer(aligned.asUnaligned(), true),
                    1,
                    gate); // pretend it came from upstream before the first (AC) barrier was picked
            // up
            assertThat(target.triggeredCheckpointCounter).isOne();
        }
    }

    @Test
    void testCheckpointHandling() throws Exception {
        testBarrierHandling(CHECKPOINT);
    }

    @Test
    void testSavepointHandling() throws Exception {
        testBarrierHandling(SavepointType.savepoint(SavepointFormatType.CANONICAL));
    }

    @Test
    void testAlternation() throws Exception {
        int numBarriers = 123;
        int numChannels = 123;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .build()) {

            List<Long> barriers = new ArrayList<>();
            for (long barrier = 0; barrier < numBarriers; barrier++) {
                barriers.add(barrier);
                SnapshotType type =
                        barrier % 2 == 0
                                ? CHECKPOINT
                                : SavepointType.savepoint(SavepointFormatType.CANONICAL);
                for (int channel = 0; channel < numChannels; channel++) {
                    send(
                            barrier(
                                            barrier,
                                            clock.relativeTimeMillis(),
                                            alignedNoTimeout(type, getDefault()))
                                    .retainBuffer(),
                            channel,
                            gate);
                }
            }
            assertThat(target.triggeredCheckpoints).isEqualTo(barriers);
        }
    }

    @Test
    void testAlignedAfterTimedOut() throws Exception {
        int numChannels = 1;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        long alignedCheckpointTimeout = 100L;
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {

            Buffer barrier1 =
                    barrier(
                            1,
                            clock.relativeTimeMillis(),
                            alignedWithTimeout(
                                    CheckpointType.CHECKPOINT,
                                    getDefault(),
                                    alignedCheckpointTimeout));
            ((RemoteInputChannel) gate.getChannel(0)).onBuffer(barrier1.retainBuffer(), 0, 0, 0);
            assertAnnouncement(gate);
            clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
            assertBarrier(gate);

            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            Buffer barrier2 =
                    barrier(
                            2,
                            clock.relativeTimeMillis(),
                            alignedWithTimeout(
                                    CheckpointType.CHECKPOINT,
                                    getDefault(),
                                    alignedCheckpointTimeout));
            ((RemoteInputChannel) gate.getChannel(0)).onBuffer(barrier2.retainBuffer(), 1, 0, 0);
            assertAnnouncement(gate);
            assertBarrier(gate);

            assertThat(target.getTriggeredCheckpointCounter()).isEqualTo(2);
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(
                            unaligned(CheckpointType.CHECKPOINT, getDefault()),
                            alignedWithTimeout(
                                    CheckpointType.CHECKPOINT,
                                    getDefault(),
                                    alignedCheckpointTimeout));
        }
    }

    @Test
    void testAlignedNeverTimeoutableCheckpoint() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .build()) {

            Buffer neverTimeoutableCheckpoint = withTimeout(Integer.MAX_VALUE);
            send(neverTimeoutableCheckpoint, 0, gate);
            sendData(1000, 1, gate);
            assertThat(target.getTriggeredCheckpointCounter()).isZero();

            send(neverTimeoutableCheckpoint, 1, gate);
            assertThat(target.getTriggeredCheckpointCounter()).isOne();
        }
    }

    @Test
    void testTimeoutAlignment() throws Exception {
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(2, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {
            testTimeoutBarrierOnTwoChannels(target, gate, 10);
        }
    }

    @Test
    void testTimeoutAlignmentAfterProcessingBarrier() throws Exception {
        int numChannels = 3;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {

            send(
                    barrier(
                            1,
                            clock.relativeTimeMillis(),
                            alignedWithTimeout(
                                    CheckpointType.CHECKPOINT, getDefault(), Integer.MAX_VALUE)),
                    2,
                    gate);

            assertThat(target.getTriggeredCheckpointCounter()).isZero();

            testTimeoutBarrierOnTwoChannels(target, gate, Integer.MAX_VALUE);
        }
    }

    private void testTimeoutBarrierOnTwoChannels(
            ValidatingCheckpointHandler target,
            CheckpointedInputGate gate,
            long alignedCheckpointTimeout)
            throws Exception {
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        getChannel(gate, 0).onBuffer(dataBuffer(), 0, 0, 0);
        getChannel(gate, 0).onBuffer(dataBuffer(), 1, 0, 0);
        getChannel(gate, 0).onBuffer(checkpointBarrier.retainBuffer(), 2, 0, 0);
        getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
        getChannel(gate, 1).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);

        assertThat(target.getTriggeredCheckpointCounter()).isZero();
        assertAnnouncement(gate);
        clock.advanceTime(alignedCheckpointTimeout * 2, TimeUnit.MILLISECONDS);
        assertAnnouncement(gate);
        assertBarrier(gate);
        assertBarrier(gate);
        assertThat(target.getTriggeredCheckpointCounter()).isOne();
        assertThat(target.getTriggeredCheckpointOptions())
                .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
        // Followed by overtaken buffers
        assertData(gate);
        assertData(gate);
        assertData(gate);
    }

    private Buffer dataBuffer() {
        return createBuffer(100).retainBuffer();
    }

    /**
     * This test tries to make sure that the first time out happens after processing {@link
     * EventAnnouncement} but before/during processing the first {@link CheckpointBarrier}.
     */
    @Test
    void testTimeoutAlignmentOnFirstBarrier() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        for (int i = 0; i < numChannels; i++) {
            (getChannel(gate, i)).onBuffer(checkpointBarrier.retainBuffer(), 0, 0, 0);
        }

        assertThat(target.getTriggeredCheckpointCounter()).isZero();
        for (int i = 0; i < numChannels; i++) {
            assertAnnouncement(gate);
        }
        assertThat(target.getTriggeredCheckpointCounter()).isZero();

        clock.advanceTime(alignedCheckpointTimeout * 4, TimeUnit.MILLISECONDS);

        assertBarrier(gate);
        assertThat(target.getTriggeredCheckpointCounter()).isOne();
    }

    @Test
    void testTimeoutAlignmentBeforeFirstBarrier() throws Exception {
        // given: Local channels.
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withTestChannels()
                        .withMailboxExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        // when: Aligned checkpoint timeout expired before the first barrier received.
        Buffer checkpointBarrier = withTimeout(1, alignedCheckpointTimeout);
        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

        ((TestInputChannel) gate.getChannel(0)).read(checkpointBarrier.retainBuffer());

        // then: The UC is triggered as soon as the first barrier is received.
        assertBarrier(gate);
        assertThat(target.getTriggeredCheckpointCounter()).isOne();
    }

    @Test
    void testTimeoutAlignmentWhenLocalBarrierFirst() throws Exception {
        // given: Gate with remote and local channels.
        int numChannels = 3;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withMixedChannels(0)
                        .withMailboxExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(1, alignedCheckpointTimeout);

        // when: Execute the first checkpoint when announcement received first.
        ((TestInputChannel) gate.getChannel(0)).read(checkpointBarrier.retainBuffer());
        (getChannel(gate, 1)).onBuffer(checkpointBarrier.retainBuffer(), 0, 0, 0);
        (getChannel(gate, 2)).onBuffer(checkpointBarrier.retainBuffer(), 0, 0, 0);

        assertAnnouncement(gate);
        assertAnnouncement(gate);
        assertBarrier(gate);
        assertBarrier(gate);
        assertBarrier(gate);

        // then: The checkpoint executed successfully.
        assertThat(target.getTriggeredCheckpointCounter()).isOne();

        // given: The time in the future.
        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

        checkpointBarrier = withTimeout(2, alignedCheckpointTimeout);

        // when: Execute the second checkpoint when barrier from local channel without announcement
        // received first.
        ((TestInputChannel) gate.getChannel(0)).read(checkpointBarrier.retainBuffer());
        assertBarrier(gate);

        // then: Nothing happens because the alignment timeout should only start after this barrier.
        assertThat(target.getTriggeredCheckpointCounter()).isOne();

        // when: Receiving the barrier from second channel(with/without) announcement after time
        // more than alignment timeout.
        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
        (getChannel(gate, 1)).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);
        assertAnnouncement(gate);
        assertBarrier(gate);

        // then: The checkpoint should started as unaligned.
        assertThat(target.getTriggeredCheckpointCounter()).isEqualTo(2);
        List<CheckpointOptions> checkpointOptions = target.getTriggeredCheckpointOptions();
        assertThat(checkpointOptions.get(checkpointOptions.size() - 1).getAlignment())
                .isEqualTo(AlignmentType.UNALIGNED);
    }

    @Test
    void testActiveTimeoutAfterLocalBarrierPassiveTimeout() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withMixedChannels(0)
                        .withMailboxExecutor()
                        .build()) {
            long alignedCheckpointTimeout = 10;
            Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

            ((TestInputChannel) gate.getChannel(0)).read(checkpointBarrier.retainBuffer());
            ((TestInputChannel) gate.getChannel(0)).read(dataBuffer());
            assertBarrier(gate);

            getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 1).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);

            assertThat(target.getTriggeredCheckpointCounter()).isZero();
            clock.advanceTimeWithoutRunningCallables(
                    alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
            // the announcement should passively time out causing the barriers to overtake the data
            // buffers
            assertAnnouncement(gate);
            // we simulate active time out firing after the passive one
            clock.executeCallables();
            assertBarrier(gate);
            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
            // Followed by overtaken buffers
            assertData(gate);
            assertData(gate);
        }
    }

    /**
     * This test tries to make sure that the first time out happens after processing {@link
     * EventAnnouncement} but before/during processing the first {@link CheckpointBarrier} of at
     * least second checkpoint.
     */
    @Test
    void testTimeoutAlignmentOnAnnouncementForSecondCheckpoint() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        performFirstCheckpoint(numChannels, target, gate, alignedCheckpointTimeout);
        assertThat(target.getTriggeredCheckpointCounter()).isOne();

        Buffer checkpointBarrier = withTimeout(2, alignedCheckpointTimeout);

        for (int i = 0; i < numChannels; i++) {
            (getChannel(gate, i)).onBuffer(dataBuffer(), 1, 0, 0);
            (getChannel(gate, i)).onBuffer(checkpointBarrier.retainBuffer(), 2, 0, 0);
        }

        assertThat(target.getTriggeredCheckpointCounter()).isOne();
        for (int i = 0; i < numChannels; i++) {
            assertAnnouncement(gate);
        }
        assertThat(target.getTriggeredCheckpointCounter()).isOne();

        clock.advanceTime(alignedCheckpointTimeout * 4, TimeUnit.MILLISECONDS);
        // the barrier should overtake the data buffers
        assertBarrier(gate);
        assertThat(target.getTriggeredCheckpointCounter()).isEqualTo(2);
    }

    private void performFirstCheckpoint(
            int numChannels,
            ValidatingCheckpointHandler target,
            CheckpointedInputGate gate,
            long alignedCheckpointTimeout)
            throws IOException, InterruptedException {
        Buffer checkpointBarrier = withTimeout(1, alignedCheckpointTimeout);
        for (int i = 0; i < numChannels; i++) {
            (getChannel(gate, i)).onBuffer(checkpointBarrier.retainBuffer(), 0, 0, 0);
        }
        assertThat(target.getTriggeredCheckpointCounter()).isZero();
        for (int i = 0; i < numChannels; i++) {
            assertAnnouncement(gate);
        }
        for (int i = 0; i < numChannels; i++) {
            assertBarrier(gate);
        }
    }

    @Test
    void testPassiveTimeoutAlignmentOnAnnouncement() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        (getChannel(gate, 0)).onBuffer(checkpointBarrier.retainBuffer(), 0, 0, 0);
        assertThat(target.getTriggeredCheckpointCounter()).isZero();
        assertAnnouncement(gate);
        assertBarrier(gate);
        clock.advanceTimeWithoutRunningCallables(
                alignedCheckpointTimeout * 4, TimeUnit.MILLISECONDS);
        (getChannel(gate, 1)).onBuffer(checkpointBarrier.retainBuffer(), 0, 0, 0);
        assertAnnouncement(gate);

        assertBarrier(gate);
        assertThat(target.getTriggeredCheckpointCounter()).isOne();
    }

    @Test
    void testActiveTimeoutAlignmentOnFirstBarrier() throws Exception {
        int numberOfChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numberOfChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withSyncExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        send(checkpointBarrier, 0, gate);

        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
        assertThat(target.getTriggeredCheckpointOptions())
                .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
    }

    @Test
    void testAllChannelsUnblockedAfteralignedCheckpointTimeout() throws Exception {
        int numberOfChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numberOfChannels, getTestBarrierHandlerFactory(target))
                        .withTestChannels()
                        .withSyncExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        CheckpointBarrier checkpointBarrier =
                new CheckpointBarrier(
                        1,
                        clock.relativeTimeMillis(),
                        alignedWithTimeout(
                                CheckpointType.CHECKPOINT, getDefault(), alignedCheckpointTimeout));
        Buffer checkpointBarrierBuffer = toBuffer(checkpointBarrier, false);

        // we set timer on announcement and test channels do not produce announcements by themselves
        send(EventSerializer.toBuffer(new EventAnnouncement(checkpointBarrier, 0), true), 0, gate);
        // emulate blocking channels on aligned barriers
        ((TestInputChannel) gate.getChannel(0)).setBlocked(true);
        send(checkpointBarrierBuffer, 0, gate);

        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
        send(EventSerializer.toBuffer(new EventAnnouncement(checkpointBarrier, 0), true), 1, gate);
        // emulate blocking channels on aligned barriers
        ((TestInputChannel) gate.getChannel(1)).setBlocked(true);
        send(checkpointBarrierBuffer, 1, gate);

        assertThat(target.getTriggeredCheckpointOptions()).hasSize(1);
        assertThat(target.getTriggeredCheckpointOptions())
                .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
        assertThat(((TestInputChannel) gate.getChannel(0)).isBlocked()).isFalse();
        assertThat(((TestInputChannel) gate.getChannel(1)).isBlocked()).isFalse();
    }

    @Test
    void testNoActiveTimeoutAlignmentAfterLastBarrier() throws Exception {
        int numberOfChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numberOfChannels, getTestBarrierHandlerFactory(target))
                        .withTestChannels()
                        .withSyncExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        send(checkpointBarrier, 0, gate);
        send(checkpointBarrier, 1, gate);
        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

        assertThat(target.getTriggeredCheckpointOptions())
                .doesNotContain(unaligned(CheckpointType.CHECKPOINT, getDefault()));
    }

    @Test
    void testNoActiveTimeoutAlignmentAfterAbort() throws Exception {
        int numberOfChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numberOfChannels, getTestBarrierHandlerFactory(target))
                        .withTestChannels()
                        .withSyncExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        send(checkpointBarrier, 0, gate);
        send(toBuffer(new CancelCheckpointMarker(1L), true), 0, gate);
        send(toBuffer(new CancelCheckpointMarker(1L), true), 1, gate);
        clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

        assertThat(target.getTriggeredCheckpointOptions()).isEmpty();
    }

    @Test
    void testNoActiveTimeoutAlignmentAfterClose() throws Exception {
        int numberOfChannels = 2;
        ClockWithDelayedActions clockWithDelayedActions =
                new ClockWithDelayedActions() {
                    @Override
                    public Cancellable registerTask(Callable<?> callable, Duration delay) {
                        super.registerTask(callable, delay);
                        // do not unregister timers on cancel
                        return () -> {};
                    }
                };
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numberOfChannels,
                                TestBarrierHandlerFactory.forTarget(target)
                                        .withActionRegistration(clockWithDelayedActions)
                                        .withClock(clockWithDelayedActions))
                        .withRemoteChannels()
                        .withSyncExecutor()
                        .build();

        long alignedCheckpointTimeout = 100;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        send(checkpointBarrier, 0, gate);
        gate.close();
        clockWithDelayedActions.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

        assertThat(target.getTriggeredCheckpointOptions()).isEmpty();
    }

    @Test
    void testActiveTimeoutAlignmentOnAnnouncement() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {
            long alignedCheckpointTimeout = 10;
            Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

            getChannel(gate, 0).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 0).onBuffer(dataBuffer(), 1, 0, 0);
            getChannel(gate, 0).onBuffer(checkpointBarrier.retainBuffer(), 2, 0, 0);
            getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 1).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);

            assertThat(target.getTriggeredCheckpointCounter()).isZero();
            assertAnnouncement(gate);
            assertAnnouncement(gate);
            // the announcement should time out causing the barriers to overtake the data buffers
            clock.advanceTime(alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
            assertBarrier(gate);
            assertBarrier(gate);
            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
            // Followed by overtaken buffers
            assertData(gate);
            assertData(gate);
            assertData(gate);
        }
    }

    @Test
    void testActiveTimeoutAfterAnnouncementPassiveTimeout() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {
            long alignedCheckpointTimeout = 10;
            Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

            getChannel(gate, 0).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 0).onBuffer(dataBuffer(), 1, 0, 0);
            getChannel(gate, 0).onBuffer(checkpointBarrier.retainBuffer(), 2, 0, 0);
            getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 1).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);

            assertThat(target.getTriggeredCheckpointCounter()).isZero();
            assertAnnouncement(gate);
            clock.advanceTimeWithoutRunningCallables(
                    alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
            // the announcement should passively time out causing the barriers to overtake the data
            // buffers
            assertAnnouncement(gate);
            // we simulate active time out firing after the passive one
            clock.executeCallables();
            assertBarrier(gate);
            assertBarrier(gate);
            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
            // Followed by overtaken buffers
            assertData(gate);
            assertData(gate);
            assertData(gate);
        }
    }

    @Test
    void testActiveTimeoutBeforeFirstAnnouncementPassiveTimeout() throws Exception {
        // given: Two barriers from two channels.
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {
            long alignmentCheckpointTimeout = 10;
            Buffer checkpointBarrier = withTimeout(alignmentCheckpointTimeout);

            getChannel(gate, 0).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 0).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);
            getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 1).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);

            assertThat(target.getTriggeredCheckpointCounter()).isZero();

            // when: The receiving of the first announcement is delayed on more than alignment
            // checkpoint timeout.
            clock.advanceTimeWithoutRunningCallables(
                    alignmentCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

            assertAnnouncement(gate);
            // we simulate active time out firing after the passive one
            clock.executeCallables();

            // then: Barriers should be reprioritized and the UC should be triggered.
            assertAnnouncement(gate);
            assertBarrier(gate);
            assertBarrier(gate);
            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
            // Followed by overtaken buffers
            assertData(gate);
            assertData(gate);
        }
    }

    @Test
    void testActiveTimeoutAfterBarrierPassiveTimeout() throws Exception {
        int numChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {
            long alignedCheckpointTimeout = 10;
            Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

            getChannel(gate, 0).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 0).onBuffer(dataBuffer(), 1, 0, 0);
            getChannel(gate, 0).onBuffer(checkpointBarrier.retainBuffer(), 2, 0, 0);

            assertThat(target.getTriggeredCheckpointCounter()).isZero();
            assertAnnouncement(gate);
            // we simulate active time out firing after the passive one
            assertData(gate);
            assertData(gate);
            clock.advanceTimeWithoutRunningCallables(
                    alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);
            // the first barrier should passively time out causing the second barrier to overtake
            // the remaining data buffer
            assertBarrier(gate);
            clock.executeCallables();

            getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 1).onBuffer(checkpointBarrier.retainBuffer(), 1, 0, 0);
            assertAnnouncement(gate);
            assertBarrier(gate);

            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
            // Followed by overtaken buffers
            assertData(gate);
        }
    }

    /**
     * First we process aligned {@link CheckpointBarrier} and after that we receive an already
     * unaligned {@link CheckpointBarrier}, that has timed out on an upstream task.
     */
    @Test
    void testTimeoutAlignmentOnUnalignedCheckpoint() throws Exception {
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        RecordingChannelStateWriter channelStateWriter = new RecordingChannelStateWriter();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(3, getTestBarrierHandlerFactory(target))
                        .withChannelStateWriter(channelStateWriter)
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build();

        getChannel(gate, 0).onBuffer(withTimeout(Integer.MAX_VALUE).retainBuffer(), 0, 0, 0);

        assertAnnouncement(gate);
        assertBarrier(gate);

        getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
        getChannel(gate, 1).onBuffer(dataBuffer(), 1, 0, 0);
        getChannel(gate, 1)
                .onBuffer(
                        toBuffer(
                                        new CheckpointBarrier(
                                                1,
                                                clock.relativeTimeMillis(),
                                                unaligned(CheckpointType.CHECKPOINT, getDefault())),
                                        true)
                                .retainBuffer(),
                        2,
                        0,
                        0);

        assertBarrier(gate);

        assertThat(channelStateWriter.getAddedInput().get(getChannel(gate, 1).getChannelInfo()))
                .hasSize(2);
        assertThat(target.getTriggeredCheckpointCounter()).isOne();
    }

    @Test
    void testTimeoutAlignmentAfterReceivedEndOfPartition() throws Exception {
        int numChannels = 3;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        long alignedCheckpointTimeout = 100L;

        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {

            getChannel(gate, 0)
                    .onBuffer(
                            barrier(
                                    1,
                                    clock.relativeTimeMillis(),
                                    alignedWithTimeout(
                                            CheckpointType.CHECKPOINT,
                                            getDefault(),
                                            alignedCheckpointTimeout)),
                            0,
                            0,
                            0);
            assertAnnouncement(gate);
            assertBarrier(gate);

            // Advances time but do not execute the registered callable which would turns into
            // unaligned checkpoint.
            clock.advanceTimeWithoutRunningCallables(
                    alignedCheckpointTimeout + 1, TimeUnit.MILLISECONDS);

            // The EndOfPartition should convert the checkpoint into unaligned.
            getChannel(gate, 1).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 1).onBuffer(endOfPartition(), 1, 0, 0);
            assertData(gate);
            assertEvent(gate, EndOfPartitionEvent.class);

            getChannel(gate, 2).onBuffer(dataBuffer(), 0, 0, 0);
            getChannel(gate, 2).onBuffer(endOfPartition(), 1, 0, 0);
            assertData(gate);
            assertEvent(gate, EndOfPartitionEvent.class);

            assertThat(target.getTriggeredCheckpointCounter()).isOne();
            assertThat(target.getTriggeredCheckpointOptions())
                    .containsExactly(unaligned(CheckpointType.CHECKPOINT, getDefault()));
        }
    }

    /**
     * This test verifies a special case that the checkpoint handler starts the new checkpoint via
     * received barrier announcement from the first channel, then {@link EndOfPartitionEvent} from
     * the second channel and then the barrier from the first channel. In this case we should ensure
     * the {@link SingleCheckpointBarrierHandler#markAlignmentStart(long, long)} should be called.
     * More information is available in https://issues.apache.org/jira/browse/FLINK-24068.
     */
    @Test
    void testStartNewCheckpointViaAnnouncement() throws Exception {
        int numChannels = 3;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        long alignedCheckpointTimeout = 10000L;

        try (CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .withRemoteChannels()
                        .withMailboxExecutor()
                        .build()) {
            getChannel(gate, 0)
                    .onBuffer(
                            barrier(
                                    1,
                                    clock.relativeTimeMillis(),
                                    alignedWithTimeout(
                                            CheckpointType.CHECKPOINT,
                                            getDefault(),
                                            alignedCheckpointTimeout)),
                            0,
                            0,
                            0);
            getChannel(gate, 1).onBuffer(endOfPartition(), 0, 0, 0);

            // The barrier announcement would start the checkpoint.
            assertAnnouncement(gate);

            // When received the EndOfPartition from channel 1 markAlignmentStart should be called.
            assertEvent(gate, EndOfPartitionEvent.class);
            assertThat(gate.getCheckpointBarrierHandler().isDuringAlignment()).isTrue();

            // Received barrier from channel 0.
            assertBarrier(gate);

            // The last barrier from channel 2 finalize the checkpoint.
            getChannel(gate, 2)
                    .onBuffer(
                            barrier(
                                    1,
                                    clock.relativeTimeMillis(),
                                    alignedWithTimeout(
                                            CheckpointType.CHECKPOINT,
                                            getDefault(),
                                            alignedCheckpointTimeout)),
                            0,
                            0,
                            0);
            assertAnnouncement(gate);
            assertBarrier(gate);

            assertThat(target.triggeredCheckpoints).containsExactly(1L);
        }
    }

    private RemoteInputChannel getChannel(CheckpointedInputGate gate, int channelIndex) {
        return (RemoteInputChannel) gate.getChannel(channelIndex);
    }

    @Test
    void testMetricsAlternation() throws Exception {
        int numChannels = 2;
        int bufferSize = 1000;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .build();

        long startNanos = clock.relativeTimeNanos();
        long checkpoint1CreationTime = clock.relativeTimeMillis() - 10;
        sendBarrier(1, checkpoint1CreationTime, CHECKPOINT, gate, 0);
        sendData(bufferSize, 0, gate);
        sendData(bufferSize, 1, gate);

        clock.advanceTime(6, TimeUnit.MILLISECONDS);
        sendBarrier(1, checkpoint1CreationTime, CHECKPOINT, gate, 1);
        sendData(bufferSize, 0, gate);

        assertMetrics(
                target,
                gate.getCheckpointBarrierHandler(),
                1L,
                startNanos,
                6_000_000L,
                10_000_000L,
                bufferSize * 2);

        startNanos = clock.relativeTimeNanos();
        long checkpoint2CreationTime = clock.relativeTimeMillis() - 5;
        sendBarrier(
                2,
                checkpoint2CreationTime,
                SavepointType.savepoint(SavepointFormatType.CANONICAL),
                gate,
                0);
        sendData(bufferSize, 1, gate);

        assertMetrics(
                target,
                gate.getCheckpointBarrierHandler(),
                2L,
                startNanos,
                0L,
                5_000_000L,
                bufferSize * 2);
        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        sendBarrier(
                2,
                checkpoint2CreationTime,
                SavepointType.savepoint(SavepointFormatType.CANONICAL),
                gate,
                1);
        sendData(bufferSize, 0, gate);

        assertMetrics(
                target,
                gate.getCheckpointBarrierHandler(),
                2L,
                startNanos,
                5_000_000L,
                5_000_000L,
                bufferSize);

        startNanos = clock.relativeTimeNanos();
        long checkpoint3CreationTime = clock.relativeTimeMillis() - 7;
        send(
                barrier(
                        3,
                        checkpoint3CreationTime,
                        unaligned(CheckpointType.CHECKPOINT, getDefault())),
                0,
                gate);
        sendData(bufferSize, 0, gate);
        sendData(bufferSize, 1, gate);
        assertMetrics(
                target, gate.getCheckpointBarrierHandler(), 3L, startNanos, 0L, 7_000_000L, -1L);
        clock.advanceTime(10, TimeUnit.MILLISECONDS);
        send(
                barrier(
                        3,
                        checkpoint2CreationTime,
                        unaligned(CheckpointType.CHECKPOINT, getDefault())),
                1,
                gate);
        assertMetrics(
                target,
                gate.getCheckpointBarrierHandler(),
                3L,
                startNanos,
                10_000_000L,
                7_000_000L,
                bufferSize * 2);
    }

    @Test
    void testMetricsSingleChannel() throws Exception {
        int numChannels = 1;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numChannels, getTestBarrierHandlerFactory(target))
                        .build();

        long checkpoint1CreationTime = clock.relativeTimeMillis() - 10;
        long startNanos = clock.relativeTimeNanos();

        sendData(1000, 0, gate);
        sendBarrier(1, checkpoint1CreationTime, CHECKPOINT, gate, 0);
        sendData(1000, 0, gate);
        clock.advanceTime(6, TimeUnit.MILLISECONDS);
        assertMetrics(
                target, gate.getCheckpointBarrierHandler(), 1L, startNanos, 0L, 10_000_000L, 0);

        long checkpoint2CreationTime = clock.relativeTimeMillis() - 5;
        startNanos = clock.relativeTimeNanos();
        sendData(1000, 0, gate);
        sendBarrier(
                2,
                checkpoint2CreationTime,
                SavepointType.savepoint(SavepointFormatType.CANONICAL),
                gate,
                0);
        sendData(1000, 0, gate);
        clock.advanceTime(5, TimeUnit.MILLISECONDS);
        assertMetrics(
                target, gate.getCheckpointBarrierHandler(), 2L, startNanos, 0L, 5_000_000L, 0);
    }

    private void assertMetrics(
            ValidatingCheckpointHandler target,
            CheckpointBarrierHandler checkpointBarrierHandler,
            long latestCheckpointId,
            long alignmentDurationStartNanos,
            long alignmentDurationNanosMin,
            long startDelayNanos,
            long bytesProcessedDuringAlignment) {
        assertThat(checkpointBarrierHandler.getLatestCheckpointId()).isEqualTo(latestCheckpointId);
        long alignmentDurationNanos = checkpointBarrierHandler.getAlignmentDurationNanos();
        long expectedAlignmentDurationNanosMax =
                clock.relativeTimeNanos() - alignmentDurationStartNanos;
        assertThat(alignmentDurationNanos).isGreaterThanOrEqualTo(alignmentDurationNanosMin);
        assertThat(alignmentDurationNanos).isLessThanOrEqualTo(expectedAlignmentDurationNanosMax);
        assertThat(checkpointBarrierHandler.getCheckpointStartDelayNanos())
                .isGreaterThanOrEqualTo(startDelayNanos);
        assertThat(FutureUtils.getOrDefault(target.getLastBytesProcessedDuringAlignment(), -1L))
                .isEqualTo(bytesProcessedDuringAlignment);
    }

    @Test
    void testPreviousHandlerReset() throws Exception {
        SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
        TestInputChannel[] channels = {
            new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1)
        };
        inputGate.setInputChannels(channels);
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        SingleCheckpointBarrierHandler barrierHandler =
                getTestBarrierHandlerFactory(target).create(inputGate);

        for (int i = 0; i < 4; i++) {
            int channel = i % 2;
            SnapshotType type =
                    channel == 0
                            ? SavepointType.savepoint(SavepointFormatType.CANONICAL)
                            : CHECKPOINT;
            target.setNextExpectedCheckpointId(-1);

            if (type.isSavepoint()) {
                channels[channel].setBlocked(true);
            }
            barrierHandler.processBarrier(
                    new CheckpointBarrier(
                            i,
                            clock.relativeTimeMillis(),
                            new CheckpointOptions(type, getDefault())),
                    new InputChannelInfo(0, channel),
                    false);
            if (type.isSavepoint()) {
                assertThat(channels[channel].isBlocked()).isTrue();
                assertThat(channels[(channel + 1) % 2].isBlocked()).isFalse();
            } else {
                assertThat(channels[0].isBlocked()).isFalse();
                assertThat(channels[1].isBlocked()).isFalse();
            }

            assertThat(barrierHandler.isCheckpointPending()).isTrue();
            assertThat(barrierHandler.getAllBarriersReceivedFuture(i)).isNotDone();

            channels[0].setBlocked(false);
            channels[1].setBlocked(false);
        }
    }

    @Test
    void testHasInflightDataBeforeProcessBarrier() throws Exception {
        SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
        inputGate.setInputChannels(
                new TestInputChannel(inputGate, 0), new TestInputChannel(inputGate, 1));
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        SingleCheckpointBarrierHandler barrierHandler =
                getTestBarrierHandlerFactory(target).create(inputGate);

        final long id = 1;
        barrierHandler.processBarrier(
                new CheckpointBarrier(
                        id,
                        clock.relativeTimeMillis(),
                        new CheckpointOptions(CHECKPOINT, getDefault())),
                new InputChannelInfo(0, 0),
                false);

        assertThat(barrierHandler.getAllBarriersReceivedFuture(id)).isNotDone();
    }

    @Test
    void testOutOfOrderBarrier() throws Exception {
        SingleInputGate inputGate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
        TestInputChannel firstChannel = new TestInputChannel(inputGate, 0);
        TestInputChannel secondChannel = new TestInputChannel(inputGate, 1);
        inputGate.setInputChannels(firstChannel, secondChannel);
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        SingleCheckpointBarrierHandler barrierHandler =
                getTestBarrierHandlerFactory(target).create(inputGate);

        long checkpointId = 10;
        long outOfOrderSavepointId = 5;

        barrierHandler.processBarrier(
                new CheckpointBarrier(
                        checkpointId,
                        clock.relativeTimeMillis(),
                        new CheckpointOptions(CHECKPOINT, getDefault())),
                new InputChannelInfo(0, 0),
                false);
        secondChannel.setBlocked(true);
        barrierHandler.processBarrier(
                new CheckpointBarrier(
                        outOfOrderSavepointId,
                        clock.relativeTimeMillis(),
                        new CheckpointOptions(
                                SavepointType.savepoint(SavepointFormatType.CANONICAL),
                                getDefault())),
                new InputChannelInfo(0, 1),
                false);

        assertThat(barrierHandler.getLatestCheckpointId()).isEqualTo(checkpointId);
        assertThat(secondChannel.isBlocked()).isFalse();
    }

    @Test
    void testNextFirstCheckpointBarrierOvertakesCancellationBarrier() throws Exception {
        int numberOfChannels = 2;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        CheckpointedInputGate gate =
                new TestCheckpointedInputGateBuilder(
                                numberOfChannels, getTestBarrierHandlerFactory(target))
                        .withTestChannels()
                        .withSyncExecutor()
                        .build();

        long alignedCheckpointTimeout = 10000;
        Buffer checkpointBarrier = withTimeout(alignedCheckpointTimeout);

        send(checkpointBarrier, 0, gate);
        clock.advanceTime(Duration.ofSeconds(1));
        send(withTimeout(2, alignedCheckpointTimeout), 0, gate);
        clock.advanceTime(Duration.ofSeconds(1));
        send(toBuffer(new CancelCheckpointMarker(1L), true), 1, gate);
        clock.advanceTime(Duration.ofSeconds(1));
        send(withTimeout(2, alignedCheckpointTimeout), 1, gate);
        clock.advanceTime(Duration.ofSeconds(1));

        assertThatFuture(target.lastAlignmentDurationNanos)
                .eventuallySucceeds()
                .isEqualTo(Duration.ofSeconds(2).toNanos());
    }

    private void testBarrierHandling(SnapshotType checkpointType) throws Exception {
        final long barrierId = 123L;
        ValidatingCheckpointHandler target = new ValidatingCheckpointHandler();
        SingleInputGate gate = new SingleInputGateBuilder().setNumberOfChannels(2).build();
        TestInputChannel fast = new TestInputChannel(gate, 0, false, true);
        TestInputChannel slow = new TestInputChannel(gate, 1, false, true);
        gate.setInputChannels(fast, slow);
        SingleCheckpointBarrierHandler barrierHandler =
                getTestBarrierHandlerFactory(target).create(gate);
        CheckpointedInputGate checkpointedGate =
                new CheckpointedInputGate(gate, barrierHandler, new SyncMailboxExecutor());

        if (checkpointType.isSavepoint()) {
            fast.setBlocked(true);
            slow.setBlocked(true);
        }

        CheckpointOptions options =
                checkpointType.isSavepoint()
                        ? alignedNoTimeout(checkpointType, getDefault())
                        : unaligned(CheckpointType.CHECKPOINT, getDefault());
        Buffer barrier = barrier(barrierId, 1, options);
        send(barrier.retainBuffer(), fast, checkpointedGate);
        assertThat(target.triggeredCheckpoints.isEmpty()).isEqualTo(checkpointType.isSavepoint());
        send(barrier.retainBuffer(), slow, checkpointedGate);

        assertThat(target.triggeredCheckpoints).containsExactly(barrierId);
        if (checkpointType.isSavepoint()) {
            for (InputChannel channel : gate.inputChannels()) {
                assertThat(((TestInputChannel) channel).isBlocked())
                        .as("channel %d should be resumed", channel.getChannelIndex())
                        .isFalse();
            }
        }
    }

    private void sendBarrier(
            long barrierId,
            long barrierCreationTime,
            SnapshotType type,
            CheckpointedInputGate gate,
            int channelId)
            throws Exception {
        send(
                barrier(barrierId, barrierCreationTime, alignedNoTimeout(type, getDefault())),
                channelId,
                gate);
    }

    private void sendData(int dataSize, int channelId, CheckpointedInputGate gate)
            throws Exception {
        send(createBuffer(dataSize), channelId, gate);
    }

    private void send(Buffer buffer, int channelId, CheckpointedInputGate gate) throws Exception {
        send(buffer.retainBuffer(), gate.getChannel(channelId), gate);
    }

    private void send(Buffer buffer, InputChannel channel, CheckpointedInputGate checkpointedGate)
            throws IOException, InterruptedException {
        if (channel instanceof TestInputChannel) {
            ((TestInputChannel) channel).read(buffer, buffer.getDataType());
        } else if (channel instanceof RemoteInputChannel) {
            ((RemoteInputChannel) channel).onBuffer(buffer, 0, 0, 0);
        } else {
            throw new IllegalArgumentException("Unknown channel type: " + channel);
        }
        while (checkpointedGate.pollNext().isPresent()) {}
    }

    private Buffer withTimeout(long alignedCheckpointTimeout) throws IOException {
        return withTimeout(1, alignedCheckpointTimeout);
    }

    private Buffer withTimeout(int checkpointId, long alignedCheckpointTimeout) throws IOException {
        return barrier(
                checkpointId,
                clock.relativeTimeMillis(),
                alignedWithTimeout(
                        CheckpointType.CHECKPOINT, getDefault(), alignedCheckpointTimeout));
    }

    private Buffer barrier(long barrierId, long barrierTimestamp, CheckpointOptions options)
            throws IOException {
        CheckpointBarrier checkpointBarrier =
                new CheckpointBarrier(barrierId, barrierTimestamp, options);
        return toBuffer(
                checkpointBarrier,
                checkpointBarrier.getCheckpointOptions().isUnalignedCheckpoint());
    }

    private Buffer endOfPartition() throws IOException {
        return toBuffer(EndOfPartitionEvent.INSTANCE, false);
    }

    private static void assertAnnouncement(CheckpointedInputGate gate)
            throws IOException, InterruptedException {
        assertEvent(gate, EventAnnouncement.class);
    }

    private static void assertBarrier(CheckpointedInputGate gate)
            throws IOException, InterruptedException {
        assertEvent(gate, CheckpointBarrier.class);
    }

    private static <T extends RuntimeEvent> void assertEvent(
            CheckpointedInputGate gate, Class<T> clazz) throws IOException, InterruptedException {
        Optional<BufferOrEvent> bufferOrEvent = assertPoll(gate);
        assertThat(bufferOrEvent.get().isEvent())
                .as("expected event, got data buffer on " + bufferOrEvent.get().getChannelInfo())
                .isTrue();
        assertThat(bufferOrEvent.get().getEvent().getClass()).isEqualTo(clazz);
    }

    private static <T extends RuntimeEvent> void assertData(CheckpointedInputGate gate)
            throws IOException, InterruptedException {
        Optional<BufferOrEvent> bufferOrEvent = assertPoll(gate);
        assertThat(bufferOrEvent.get().isBuffer())
                .as(
                        "expected data, got "
                                + bufferOrEvent.get().getEvent()
                                + "  on "
                                + bufferOrEvent.get().getChannelInfo())
                .isTrue();
    }

    private static Optional<BufferOrEvent> assertPoll(CheckpointedInputGate gate)
            throws IOException, InterruptedException {
        Optional<BufferOrEvent> bufferOrEvent = gate.pollNext();
        assertThat(bufferOrEvent).as("empty gate").isPresent();
        return bufferOrEvent;
    }

    private static class CallableWithTimestamp {
        private final long timestamp;
        private final Callable<?> callable;

        private CallableWithTimestamp(long timestamp, @Nonnull Callable<?> callable) {
            this.timestamp = timestamp;
            this.callable = callable;
        }

        public long getTimestamp() {
            return timestamp;
        }

        public Callable<?> getCallable() {
            return callable;
        }
    }

    private static class ClockWithDelayedActions extends Clock
            implements BarrierAlignmentUtil.DelayableTimer {

        // must start at least at 100 ms, because ValidatingCheckpointHandler
        // expects barriers to have positive timestamps
        private final ManualClock clock = new ManualClock(100_000_000);
        private final PriorityQueue<CallableWithTimestamp> queue =
                new PriorityQueue<>(Comparator.comparingLong(CallableWithTimestamp::getTimestamp));

        @Override
        public Cancellable registerTask(Callable<?> callable, Duration delay) {
            CallableWithTimestamp callableWithTimestamp =
                    new CallableWithTimestamp(
                            clock.relativeTimeNanos() + delay.toNanos(), callable);
            this.queue.add(callableWithTimestamp);
            return () -> this.queue.remove(callableWithTimestamp);
        }

        public void advanceTime(long duration, TimeUnit timeUnit) throws Exception {
            clock.advanceTime(duration, timeUnit);
            executeCallables();
        }

        public void advanceTime(Duration duration) throws Exception {
            clock.advanceTime(duration);
            executeCallables();
        }

        public ManualClock getClock() {
            return clock;
        }

        public void advanceTimeWithoutRunningCallables(long duration, TimeUnit timeUnit) {
            clock.advanceTime(duration, timeUnit);
        }

        public void executeCallables() throws Exception {
            long currentTimestamp = clock.relativeTimeNanos();

            while (!queue.isEmpty() && queue.peek().getTimestamp() <= currentTimestamp) {
                queue.poll().getCallable().call();
            }
        }

        @Override
        public long absoluteTimeMillis() {
            return clock.absoluteTimeMillis();
        }

        @Override
        public long relativeTimeMillis() {
            return clock.relativeTimeMillis();
        }

        @Override
        public long relativeTimeNanos() {
            return clock.relativeTimeNanos();
        }
    }
}
