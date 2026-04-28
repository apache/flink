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

import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.checkpoint.channel.EntryPosition;
import org.apache.flink.runtime.checkpoint.channel.InputChannelInfo;
import org.apache.flink.runtime.checkpoint.channel.RecordingChannelStateWriter;
import org.apache.flink.runtime.checkpoint.channel.RecoveredBufferStoreCoordinator;
import org.apache.flink.runtime.io.network.buffer.Buffer;
import org.apache.flink.runtime.io.network.buffer.FreeingBufferRecycler;
import org.apache.flink.runtime.io.network.buffer.NetworkBuffer;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

import static org.assertj.core.api.Assertions.assertThat;

/** Tests for {@link RecoveredBufferStoreImpl}. */
class RecoveredBufferStoreTest {

    private static final InputChannelInfo DEFAULT_CHANNEL_INFO = new InputChannelInfo(0, 0);

    /** addBuffer / tryTake lifecycle. */
    @Test
    void testStoreLifecycle() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        // Query methods require holding the store monitor (locking contract).
        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
            assertThat(store.size()).isEqualTo(0);
            assertThat(store.peekNextDataType()).isEqualTo(Buffer.DataType.NONE);
        }

        NetworkBuffer buffer1 = createBuffer(new byte[] {1, 2, 3, 4});
        store.addBuffer(buffer1);

        Buffer taken;
        synchronized (store) {
            assertThat(store.isEmpty()).isFalse();
            assertThat(store.size()).isEqualTo(1);
            assertThat(store.peekNextDataType()).isEqualTo(Buffer.DataType.DATA_BUFFER);

            taken = store.tryTake();
            assertThat(taken).isNotNull();
            assertThat(store.isEmpty()).isTrue();
            assertThat(store.size()).isEqualTo(0);
        }
        taken.recycleBuffer();

        synchronized (store) {
            assertThat(store.tryTake()).isNull();
        }
    }

    /**
     * Checkpoint with ready buffers. Ready buffers should be retained and passed to the
     * ChannelStateWriter.
     */
    @Test
    void testCheckpointWithReadyBuffers() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        byte[] data = new byte[] {10, 20, 30, 40};
        NetworkBuffer buffer = createBuffer(data);
        store.addBuffer(buffer);

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        long checkpointId = 1L;
        writer.start(checkpointId, null);

        store.checkpoint(writer, checkpointId);

        assertThat(writer.getAddedInput().get(DEFAULT_CHANNEL_INFO)).hasSize(1);

        // The original buffer should still be in the store (retained, not consumed)
        synchronized (store) {
            assertThat(store.size()).isEqualTo(1);
        }

        // Clean up: recycle the buffer recorded by writer
        writer.getAddedInput().get(DEFAULT_CHANNEL_INFO).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    /** Concurrent access from two threads. One thread adds buffers and the other takes them. */
    @Test
    void testConcurrentCheckpointAndReplay() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int numBuffers = 100;
        CyclicBarrier barrier = new CyclicBarrier(2);
        AtomicReference<Throwable> error = new AtomicReference<>();

        // Producer thread: adds buffers
        Thread producer =
                new Thread(
                        () -> {
                            try {
                                barrier.await();
                                for (int i = 0; i < numBuffers; i++) {
                                    NetworkBuffer buf = createBuffer(new byte[] {(byte) i});
                                    store.addBuffer(buf);
                                }
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        });

        // Consumer thread: takes buffers
        CountDownLatch consumedAll = new CountDownLatch(1);
        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                barrier.await();
                                int consumed = 0;
                                while (consumed < numBuffers) {
                                    Buffer buf;
                                    synchronized (store) {
                                        buf = store.tryTake();
                                    }
                                    if (buf != null) {
                                        buf.recycleBuffer();
                                        consumed++;
                                    }
                                }
                                consumedAll.countDown();
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        });

        producer.start();
        consumer.start();
        producer.join(10_000);
        consumer.join(10_000);

        assertThat(error.get()).isNull();
        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
        }
    }

    /**
     * Simulate store transfer by adding buffers, then taking them in another "context" (simulating
     * conversion). Continue consuming after conversion.
     */
    @Test
    void testConsumptionAfterConversion() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        // Add buffers in "recovery" phase
        NetworkBuffer buf1 = createBuffer(new byte[] {1, 2});
        NetworkBuffer buf2 = createBuffer(new byte[] {3, 4});
        NetworkBuffer buf3 = createBuffer(new byte[] {5, 6});
        store.addBuffer(buf1);
        store.addBuffer(buf2);
        store.addBuffer(buf3);

        // Simulate partial consumption before conversion
        Buffer taken1;
        synchronized (store) {
            taken1 = store.tryTake();
        }
        assertThat(taken1).isNotNull();
        taken1.recycleBuffer();

        // After conversion, continue consuming remaining buffers
        Buffer taken2;
        Buffer taken3;
        synchronized (store) {
            taken2 = store.tryTake();
            assertThat(taken2).isNotNull();
            taken3 = store.tryTake();
            assertThat(taken3).isNotNull();
        }
        taken2.recycleBuffer();
        taken3.recycleBuffer();

        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
            assertThat(store.tryTake()).isNull();
        }
    }

    /** Verify releaseAll recycles all buffers and clears state. */
    @Test
    void testReleaseAll() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        NetworkBuffer buf1 = createBuffer(new byte[] {1});
        NetworkBuffer buf2 = createBuffer(new byte[] {2});
        store.addBuffer(buf1);
        store.addBuffer(buf2);

        store.releaseAll();

        assertThat(buf1.isRecycled()).isTrue();
        assertThat(buf2.isRecycled()).isTrue();
        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
            assertThat(store.size()).isEqualTo(0);
        }
    }

    /** Verify releaseAll notifies the registered coordinator with the bound channel info. */
    @Test
    void testReleaseAllNotifiesCoordinator() {
        InputChannelInfo channelInfo = new InputChannelInfo(3, 7);
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(channelInfo);

        RecordingCoordinator coordinator = new RecordingCoordinator();
        synchronized (store) {
            store.setCoordinator(coordinator);
        }

        // Add some in-memory and on-disk bookkeeping to make the release meaningful.
        store.addBuffer(createBuffer(new byte[] {1}));
        synchronized (store) {
            store.incrementPending();
        }

        store.releaseAll();

        assertThat(coordinator.released).containsExactly(channelInfo);
        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
            assertThat(store.size()).isEqualTo(0);
        }
    }

    /**
     * Verify the data-available listener is invoked OUTSIDE the store monitor.
     *
     * <p>If the listener fires while the store monitor is held, the listener's downstream lock
     * acquisition (e.g. {@code SingleInputGate.queueChannel} taking the gate's {@code
     * inputChannelsWithData} monitor) can deadlock against a task thread that already holds that
     * monitor and is trying to acquire the store monitor (via {@code peekNextDataType}). This is
     * the AB-BA deadlock pattern observed in JVM-level deadlock reports. The contract aligns with
     * {@link RecoveredBufferStoreImpl#checkpoint} / {@link RecoveredBufferStoreImpl#releaseAll} /
     * {@link RecoveredBufferStoreImpl#notifyCheckpointStopped}, all of which fire callbacks outside
     * the store lock.
     */
    @Test
    void testDataAvailableListenerFiresOutsideStoreMonitor() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        boolean[] storeMonitorHeldDuringCallback = {false};
        synchronized (store) {
            store.setDataAvailableListener(
                    () -> storeMonitorHeldDuringCallback[0] = Thread.holdsLock(store));
        }

        store.addBuffer(createBuffer(new byte[] {1}));

        assertThat(storeMonitorHeldDuringCallback[0])
                .as("dataAvailableListener must be invoked outside the store monitor")
                .isFalse();

        store.releaseAll();
    }

    /**
     * Reproduces the AB-BA deadlock pattern between {@link RecoveredBufferStoreImpl#addBuffer} and
     * a task thread that holds a downstream gate-side lock while reaching into the store.
     *
     * <p>Thread A (task): holds {@code gateLock}, then tries to acquire the store monitor via a
     * synchronized store method (mirrors {@link RecoveredBufferStoreImpl#peekNextDataType}). Thread
     * B (recovery): calls {@code addBuffer}, which under the broken contract fires the listener
     * while holding the store monitor; the listener tries to acquire {@code gateLock}, mirroring
     * {@code SingleInputGate.queueChannel}.
     *
     * <p>If {@code addBuffer} invokes the listener inside the synchronized block, this test
     * deadlocks. The fixed contract fires the listener outside the lock, so the test completes
     * within the timeout.
     */
    @Test
    void testAddBufferDoesNotDeadlockWithGateSideLock() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        Object gateLock = new Object();
        CountDownLatch taskHoldsGateLock = new CountDownLatch(1);
        CountDownLatch recoveryStartedAddBuffer = new CountDownLatch(1);
        AtomicReference<Throwable> error = new AtomicReference<>();

        // Listener mirrors SingleInputGate.queueChannel: must take gateLock to enqueue.
        synchronized (store) {
            store.setDataAvailableListener(
                    () -> {
                        synchronized (gateLock) {
                            // touch shared state under the gate lock to mirror real notify path
                        }
                    });
        }

        // Thread A (task): take gateLock first, then call into the store. This emulates
        // SingleInputGate holding inputChannelsWithData while reading peekNextDataType.
        Thread taskThread =
                new Thread(
                        () -> {
                            try {
                                synchronized (gateLock) {
                                    taskHoldsGateLock.countDown();
                                    // Wait until the recovery thread has entered addBuffer and (in
                                    // the broken contract) is holding the store monitor while
                                    // trying to grab gateLock.
                                    recoveryStartedAddBuffer.await();
                                    // Sleep a touch so the recovery thread is parked on gateLock
                                    // before we go grab the store monitor; without this, the test
                                    // could pass even with the broken contract because both
                                    // threads might serialise.
                                    Thread.sleep(50);
                                    // Now reach into the store under gateLock — this mirrors
                                    // peekNextDataType / size and would block on the store monitor
                                    // if addBuffer is still inside synchronized.
                                    synchronized (store) {
                                        store.peekNextDataType();
                                    }
                                }
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        },
                        "test-task-thread");

        // Thread B (recovery): wait until the task thread holds gateLock, then call addBuffer,
        // which will trigger the listener that wants gateLock.
        Thread recoveryThread =
                new Thread(
                        () -> {
                            try {
                                taskHoldsGateLock.await();
                                recoveryStartedAddBuffer.countDown();
                                store.addBuffer(createBuffer(new byte[] {1}));
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        },
                        "test-recovery-thread");

        taskThread.start();
        recoveryThread.start();

        // Generous timeout: deadlock would otherwise hang the test forever.
        taskThread.join(10_000);
        recoveryThread.join(10_000);

        assertThat(taskThread.isAlive())
                .as("task thread is still alive — likely deadlocked on store monitor")
                .isFalse();
        assertThat(recoveryThread.isAlive())
                .as("recovery thread is still alive — likely deadlocked on gate lock")
                .isFalse();
        assertThat(error.get()).isNull();

        store.releaseAll();
    }

    /** Verify data-available listener fires when buffer is added to empty store. */
    @Test
    void testDataAvailableListener() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int[] callbackCount = {0};
        synchronized (store) {
            store.setDataAvailableListener(() -> callbackCount[0]++);
        }

        // Add first buffer: should trigger listener (store was empty)
        store.addBuffer(createBuffer(new byte[] {1}));
        assertThat(callbackCount[0]).isEqualTo(1);

        // Add second buffer: should NOT trigger listener (store was not empty)
        store.addBuffer(createBuffer(new byte[] {2}));
        assertThat(callbackCount[0]).isEqualTo(1);

        // Drain both buffers
        synchronized (store) {
            store.tryTake().recycleBuffer();
            store.tryTake().recycleBuffer();
        }

        // Add buffer again to empty store: should trigger listener
        store.addBuffer(createBuffer(new byte[] {3}));
        assertThat(callbackCount[0]).isEqualTo(2);

        store.releaseAll();
    }

    /** Verify pending spill entry count tracking. */
    @Test
    void testPendingCount() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        synchronized (store) {
            store.incrementPending();

            // Store not empty when pending entries exist
            assertThat(store.isEmpty()).isFalse();
            // size() reports ready + pending so the channel-level backlog reflects on-disk data too
            assertThat(store.size()).isEqualTo(1);
        }

        // Drain the pending entry by handing back a buffer; addBuffer folds in the matching
        // pending decrement.
        store.addBuffer(createBuffer(new byte[] {1}));
        synchronized (store) {
            // pending consumed, buffer became ready — still size 1 but now in readyBuffers
            assertThat(store.size()).isEqualTo(1);
            store.tryTake().recycleBuffer();
            assertThat(store.isEmpty()).isTrue();
            assertThat(store.size()).isEqualTo(0);
        }
    }

    /** Verify size() aggregates ready buffers and pending on-disk entries. */
    @Test
    void testSizeAggregatesReadyAndPending() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);

        store.addBuffer(createBuffer(new byte[] {1}));
        synchronized (store) {
            store.incrementPending();
            store.incrementPending();

            assertThat(store.size()).isEqualTo(3);

            store.tryTake().recycleBuffer();
            assertThat(store.size()).isEqualTo(2);
        }

        // Drain both pending entries by handing back buffers; each addBuffer consumes one pending.
        store.addBuffer(createBuffer(new byte[] {2}));
        store.addBuffer(createBuffer(new byte[] {3}));
        synchronized (store) {
            assertThat(store.size()).isEqualTo(2);
        }

        store.releaseAll();
    }

    /**
     * Verify that the coordinator registered via setCoordinator receives onChannelCheckpointStarted
     * during checkpoint() after snapshotting ready buffers, with the correct checkpointId and
     * channelInfo.
     */
    @Test
    void testCheckpointNotifiesCoordinatorAfterSnapshot() throws Exception {
        InputChannelInfo channelInfo = new InputChannelInfo(0, 0);
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(channelInfo);

        RecordingCoordinator coordinator = new RecordingCoordinator();
        synchronized (store) {
            store.setCoordinator(coordinator);
        }

        store.addBuffer(createBuffer(new byte[] {1, 2}));

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        long checkpointId = 42L;
        writer.start(checkpointId, null);

        store.checkpoint(writer, checkpointId);

        // Coordinator must have been notified exactly once with correct args
        assertThat(coordinator.checkpointIds).containsExactly(42L);
        assertThat(coordinator.checkpointChannels).containsExactly(channelInfo);

        // Writer received the ready buffer before notification fired (snapshot happened first)
        assertThat(writer.getAddedInput().get(channelInfo)).hasSize(1);

        writer.getAddedInput().get(channelInfo).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    /** Verify checkpoint() without any ready buffers still notifies the coordinator. */
    @Test
    void testCheckpointNotifiesCoordinatorEvenWhenNoReadyBuffers() throws Exception {
        InputChannelInfo channelInfo = new InputChannelInfo(1, 2);
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(channelInfo);

        RecordingCoordinator coordinator = new RecordingCoordinator();
        synchronized (store) {
            store.setCoordinator(coordinator);
        }

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        writer.start(1L, null);
        store.checkpoint(writer, 1L);

        assertThat(coordinator.checkpointIds).containsExactly(1L);
    }

    /** Verify no notification is fired if setCoordinator was never called. */
    @Test
    void testCheckpointWithNoCoordinatorSetDoesNotThrow() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        store.addBuffer(createBuffer(new byte[] {1}));

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        writer.start(1L, null);
        // Should not throw even without a coordinator registered
        store.checkpoint(writer, 1L);

        writer.getAddedInput().get(DEFAULT_CHANNEL_INFO).forEach(Buffer::recycleBuffer);
        store.releaseAll();
    }

    /**
     * Verify notifyCheckpointStopped forwards the call to the registered coordinator with the bound
     * channel info.
     */
    @Test
    void testNotifyCheckpointStoppedNotifiesCoordinator() {
        InputChannelInfo channelInfo = new InputChannelInfo(2, 5);
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(channelInfo);

        RecordingCoordinator coordinator = new RecordingCoordinator();
        synchronized (store) {
            store.setCoordinator(coordinator);
        }

        store.notifyCheckpointStopped(11L);
        store.notifyCheckpointStopped(12L);

        assertThat(coordinator.stoppedCheckpointIds).containsExactly(11L, 12L);
        assertThat(coordinator.stoppedChannels).containsExactly(channelInfo, channelInfo);
    }

    /** Verify notifyCheckpointStopped is a safe no-op when no coordinator is registered. */
    @Test
    void testNotifyCheckpointStoppedWithoutCoordinatorIsNoOp() {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        // Should not throw without a coordinator registered
        store.notifyCheckpointStopped(7L);
    }

    /**
     * Verify setDataAvailableListener can be called through the RecoveredBufferStore interface
     * without instanceof casts.
     */
    @Test
    void testSetDataAvailableListenerViaInterface() {
        RecoveredBufferStore store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int[] callCount = {0};
        // Must compile and run without instanceof check
        synchronized (store) {
            store.setDataAvailableListener(() -> callCount[0]++);
        }

        ((RecoveredBufferStoreImpl) store).addBuffer(createBuffer(new byte[] {1}));
        assertThat(callCount[0]).isEqualTo(1);

        store.releaseAll();
    }

    /** Verify all methods of EMPTY return expected no-op / sentinel values. */
    @Test
    void testEmptySingletonBehavior() throws Exception {
        RecoveredBufferStore empty = RecoveredBufferStore.EMPTY;

        assertThat(empty.tryTake()).isNull();
        assertThat(empty.peekNextDataType()).isEqualTo(Buffer.DataType.NONE);
        assertThat(empty.isEmpty()).isTrue();
        assertThat(empty.size()).isEqualTo(0);
    }

    /** Verify checkpoint() on EMPTY is a no-op and does not write any channel state. */
    @Test
    void testEmptySingletonCheckpointIsNoOp() throws Exception {
        RecoveredBufferStore empty = RecoveredBufferStore.EMPTY;

        RecordingChannelStateWriter writer = new RecordingChannelStateWriter();
        writer.start(1L, null);
        empty.checkpoint(writer, 1L);

        // No data must have been written
        assertThat(writer.getAddedInput().isEmpty()).isTrue();
    }

    /** Verify releaseAll() on EMPTY does not throw. */
    @Test
    void testEmptySingletonReleaseAllIsNoOp() {
        RecoveredBufferStore.EMPTY.releaseAll();
    }

    /** Verify notifyCheckpointStopped() on EMPTY does not throw. */
    @Test
    void testEmptySingletonNotifyCheckpointStoppedIsNoOp() {
        RecoveredBufferStore.EMPTY.notifyCheckpointStopped(99L);
    }

    /** Verify all setters on EMPTY are no-ops (accept and discard without throwing). */
    @Test
    void testEmptySingletonSettersAreNoOp() {
        RecoveredBufferStore empty = RecoveredBufferStore.EMPTY;

        empty.setCoordinator(new RecordingCoordinator());
        empty.setDataAvailableListener(() -> {});
        // No exception == pass
    }

    /**
     * Calling a {@code @GuardedBy("this")} method without holding the store monitor must trip the
     * {@code assert Thread.holdsLock(this)} guard under {@code -ea}. This locks the contract in:
     * future refactors that accidentally drop the synchronized wrapper at a call site will fail
     * loudly in tests instead of silently producing torn reads.
     */
    @Test
    void testGuardedMethodsAssertHoldsLock() {
        // The AssertionError surfaces only with assertions enabled; flink test JVMs run with -ea
        // by default. Skip the test cleanly if for some reason this JVM was started without -ea
        // so the suite does not turn red on a JVM configuration issue.
        if (!RecoveredBufferStoreTest.class.desiredAssertionStatus()) {
            return;
        }

        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        try {
            assertThat(catchAssertion(() -> store.tryTake())).isTrue();
            assertThat(catchAssertion(() -> store.peekNextDataType())).isTrue();
            assertThat(catchAssertion(() -> store.isEmpty())).isTrue();
            assertThat(catchAssertion(() -> store.incrementPending())).isTrue();
            assertThat(catchAssertion(() -> store.setCoordinator(new RecordingCoordinator())))
                    .isTrue();
            assertThat(catchAssertion(() -> store.setDataAvailableListener(() -> {}))).isTrue();
            // size() is the deliberate exception — lock-free for metric / gate-bookkeeping paths.
            // Calling it without holding the monitor must NOT trip the assertion guard.
            assertThat(catchAssertion(() -> store.size())).isFalse();
        } finally {
            store.releaseAll();
        }
    }

    /**
     * Concurrent drain test: when the producer keeps appending and the consumer keeps polling, each
     * {@code tryTake + peekNextDataType} pair observed by the consumer must be self- consistent —
     * if {@code peekNextDataType()} returns {@code NONE} after a successful tryTake it must mean
     * the next tryTake on the same thread also returns null (modulo any further producer activity
     * that happened strictly after the peek), and if it returns a non-NONE type the next tryTake
     * must return a buffer with that type. The test guards against future regressions where someone
     * splits the take/peek pair across two store-lock acquisitions.
     */
    @Test
    void testTryTakePeekPairAtomicUnderConcurrency() throws Exception {
        RecoveredBufferStoreImpl store = new RecoveredBufferStoreImpl(DEFAULT_CHANNEL_INFO);
        int numBuffers = 500;
        CyclicBarrier startBarrier = new CyclicBarrier(2);
        AtomicReference<Throwable> error = new AtomicReference<>();

        Thread producer =
                new Thread(
                        () -> {
                            try {
                                startBarrier.await();
                                for (int i = 0; i < numBuffers; i++) {
                                    store.addBuffer(createBuffer(new byte[] {(byte) i}));
                                }
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        },
                        "atomic-pair-producer");

        Thread consumer =
                new Thread(
                        () -> {
                            try {
                                startBarrier.await();
                                int consumed = 0;
                                while (consumed < numBuffers) {
                                    Buffer taken;
                                    Buffer.DataType peekedType;
                                    synchronized (store) {
                                        taken = store.tryTake();
                                        peekedType = store.peekNextDataType();
                                    }
                                    if (taken == null) {
                                        // peeked type with no taken buffer must be NONE
                                        assertThat(peekedType).isEqualTo(Buffer.DataType.NONE);
                                        continue;
                                    }
                                    taken.recycleBuffer();
                                    consumed++;
                                }
                            } catch (Throwable t) {
                                error.set(t);
                            }
                        },
                        "atomic-pair-consumer");

        producer.start();
        consumer.start();
        producer.join(10_000);
        consumer.join(10_000);

        assertThat(error.get()).isNull();
        synchronized (store) {
            assertThat(store.isEmpty()).isTrue();
        }
    }

    private static boolean catchAssertion(Runnable r) {
        try {
            r.run();
            return false;
        } catch (AssertionError ae) {
            return true;
        }
    }

    /**
     * Test-only coordinator that records all coordinator notifications: started, stopped, and
     * released.
     */
    private static class RecordingCoordinator implements RecoveredBufferStoreCoordinator {
        final List<Long> checkpointIds = new ArrayList<>();
        final List<InputChannelInfo> checkpointChannels = new ArrayList<>();
        final List<EntryPosition> checkpointStartPositions = new ArrayList<>();
        final List<Long> stoppedCheckpointIds = new ArrayList<>();
        final List<InputChannelInfo> stoppedChannels = new ArrayList<>();
        final List<InputChannelInfo> released = new ArrayList<>();
        volatile EntryPosition currentDrainHead = EntryPosition.END;

        @Override
        public EntryPosition getCurrentDrainHead() {
            return currentDrainHead;
        }

        @Override
        public void onChannelCheckpointStarted(
                long checkpointId, InputChannelInfo channelInfo, EntryPosition startPos) {
            checkpointIds.add(checkpointId);
            checkpointChannels.add(channelInfo);
            checkpointStartPositions.add(startPos);
        }

        @Override
        public void onChannelCheckpointStopped(long checkpointId, InputChannelInfo channelInfo) {
            stoppedCheckpointIds.add(checkpointId);
            stoppedChannels.add(channelInfo);
        }

        @Override
        public void onChannelReleased(InputChannelInfo channelInfo) {
            released.add(channelInfo);
        }
    }

    private static NetworkBuffer createBuffer(byte[] data) {
        org.apache.flink.core.memory.MemorySegment segment =
                MemorySegmentFactory.allocateUnpooledSegment(data.length);
        segment.put(0, data, 0, data.length);
        NetworkBuffer buffer = new NetworkBuffer(segment, FreeingBufferRecycler.INSTANCE);
        buffer.setSize(data.length);
        return buffer;
    }
}
