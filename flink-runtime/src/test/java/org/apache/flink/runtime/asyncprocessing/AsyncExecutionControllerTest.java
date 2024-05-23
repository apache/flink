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

package org.apache.flink.runtime.asyncprocessing;

import org.apache.flink.api.common.operators.MailboxExecutor;
import org.apache.flink.api.common.state.v2.State;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.core.state.StateFutureImpl.AsyncFrameworkExceptionHandler;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.asyncprocessing.EpochManager.Epoch;
import org.apache.flink.runtime.asyncprocessing.EpochManager.ParallelMode;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutorService;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.StateBackendTestUtils;
import org.apache.flink.runtime.state.v2.InternalValueState;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.function.ThrowingRunnable;

import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link AsyncExecutionController}. */
class AsyncExecutionControllerTest {
    AsyncExecutionController aec;
    AtomicInteger output;
    TestValueState valueState;

    final Runnable userCode =
            () -> {
                valueState
                        .asyncValue()
                        .thenCompose(
                                val -> {
                                    int updated = (val == null ? 1 : (val + 1));
                                    return valueState
                                            .asyncUpdate(updated)
                                            .thenCompose(
                                                    o -> StateFutureUtils.completedFuture(updated));
                                })
                        .thenAccept(val -> output.set(val));
            };

    void setup(
            int batchSize,
            long timeout,
            int maxInFlight,
            MailboxExecutor mailboxExecutor,
            AsyncFrameworkExceptionHandler exceptionHandler,
            CloseableRegistry closeableRegistry)
            throws IOException {
        StateExecutor stateExecutor = new TestStateExecutor();
        ValueStateDescriptor<Integer> stateDescriptor =
                new ValueStateDescriptor<>("test-value-state", BasicTypeInfo.INT_TYPE_INFO);
        Supplier<State> stateSupplier =
                () -> new TestValueState(aec, new TestUnderlyingState(), stateDescriptor);
        StateBackend testAsyncStateBackend =
                StateBackendTestUtils.buildAsyncStateBackend(stateSupplier, stateExecutor);
        assertThat(testAsyncStateBackend.supportsAsyncKeyedStateBackend()).isTrue();
        AsyncKeyedStateBackend asyncKeyedStateBackend;
        try {
            asyncKeyedStateBackend = testAsyncStateBackend.createAsyncKeyedStateBackend(null);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        closeableRegistry.registerCloseable(asyncKeyedStateBackend);
        closeableRegistry.registerCloseable(asyncKeyedStateBackend::dispose);
        aec =
                new AsyncExecutionController<>(
                        mailboxExecutor,
                        exceptionHandler,
                        stateExecutor,
                        128,
                        batchSize,
                        timeout,
                        maxInFlight);
        asyncKeyedStateBackend.setup(aec);

        try {
            valueState = asyncKeyedStateBackend.createState(stateDescriptor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        output = new AtomicInteger();
    }

    @Test
    void testBasicRun() throws IOException {
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                100,
                10000L,
                1000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        // ============================ element1 ============================
        String record1 = "key1-r1";
        String key1 = "key1";
        // Simulate the wrapping in {@link RecordProcessorUtils#getRecordProcessor()}, wrapping the
        // record and key with RecordContext.
        RecordContext<String> recordContext1 = aec.buildContext(record1, key1);
        aec.setCurrentContext(recordContext1);
        // execute user code
        userCode.run();

        // Single-step run.
        // Firstly, the user code generates value get in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, the value update is in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update finishes.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(1);
        assertThat(recordContext1.getReferenceCount()).isEqualTo(0);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);

        // ============================ element 2 & 3 ============================
        String record2 = "key1-r2";
        String key2 = "key1";
        RecordContext<String> recordContext2 = aec.buildContext(record2, key2);
        aec.setCurrentContext(recordContext2);
        // execute user code
        userCode.run();

        String record3 = "key1-r3";
        String key3 = "key1";
        RecordContext<String> recordContext3 = aec.buildContext(record3, key3);
        aec.setCurrentContext(recordContext3);
        // execute user code
        userCode.run();

        // Single-step run.
        // Firstly, the user code for record2 generates value get in active buffer,
        // while user code for record3 generates value get in blocking buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(2);
        aec.triggerIfNeeded(true);
        // After running, the value update for record2 is in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(2);
        aec.triggerIfNeeded(true);
        // Value update for record2 finishes. The value get for record3 is migrated from blocking
        // buffer to active buffer actively.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(output.get()).isEqualTo(2);
        assertThat(recordContext2.getReferenceCount()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);

        // Let value get for record3 to run.
        aec.triggerIfNeeded(true);
        // After running, the value update for record3 is in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update for record3 finishes.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(3);
        assertThat(recordContext3.getReferenceCount()).isEqualTo(0);

        // ============================ element4 ============================
        String record4 = "key3-r3";
        String key4 = "key3";
        RecordContext<String> recordContext4 = aec.buildContext(record4, key4);
        aec.setCurrentContext(recordContext4);
        // execute user code
        userCode.run();

        // Single-step run for another key.
        // Firstly, the user code generates value get in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, the value update is in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update finishes.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(1);
        assertThat(recordContext4.getReferenceCount()).isEqualTo(0);

        resourceRegistry.close();
    }

    @Test
    void testRecordsRunInOrder() throws IOException {
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                100,
                10000L,
                1000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        // Record1 and record3 have the same key, record2 has a different key.
        // Record2 should be processed before record3.

        String record1 = "key1-r1";
        String key1 = "key1";
        RecordContext<String> recordContext1 = aec.buildContext(record1, key1);
        aec.setCurrentContext(recordContext1);
        // execute user code
        userCode.run();

        String record2 = "key2-r1";
        String key2 = "key2";
        RecordContext<String> recordContext2 = aec.buildContext(record2, key2);
        aec.setCurrentContext(recordContext2);
        // execute user code
        userCode.run();

        String record3 = "key1-r2";
        String key3 = "key1";
        RecordContext<String> recordContext3 = aec.buildContext(record3, key3);
        aec.setCurrentContext(recordContext3);
        // execute user code
        userCode.run();

        // Record1's value get and record2's value get are in active buffer
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(2);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(2);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(3);
        // Record3's value get is in blocking buffer
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, record1's value update and record2's value update are in active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(2);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(2);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(3);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Record1's value update and record2's value update finish, record3's value get migrates to
        // active buffer when record1's refCount reach 0.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(1);
        assertThat(recordContext1.getReferenceCount()).isEqualTo(0);
        assertThat(recordContext2.getReferenceCount()).isEqualTo(0);
        aec.triggerIfNeeded(true);
        //  After running, record3's value update is added to active buffer.
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        aec.triggerIfNeeded(true);
        assertThat(output.get()).isEqualTo(2);
        assertThat(recordContext3.getReferenceCount()).isEqualTo(0);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);

        resourceRegistry.close();
    }

    @Test
    void testInFlightRecordControl() throws IOException {
        int batchSize = 5;
        int maxInFlight = 10;
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                batchSize,
                10000L,
                maxInFlight,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        // For records with different keys, the in-flight records is controlled by batch size.
        for (int round = 0; round < 10; round++) {
            for (int i = 0; i < batchSize; i++) {
                String record =
                        String.format("key%d-r%d", round * batchSize + i, round * batchSize + i);
                String key = String.format("key%d", round * batchSize + i);
                RecordContext<String> recordContext = aec.buildContext(record, key);
                aec.setCurrentContext(recordContext);
                userCode.run();
            }
            assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);
            assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
            assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        }
        // For records with the same key, the in-flight records is controlled by max in-flight
        // records number.
        for (int i = 0; i < maxInFlight; i++) {
            String record = String.format("sameKey-r%d", i, i);
            String key = "sameKey";
            RecordContext<String> recordContext = aec.buildContext(record, key);
            aec.setCurrentContext(recordContext);
            userCode.run();
        }
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(maxInFlight);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(maxInFlight - 1);
        // In the following example, the batch size will degrade to 1, meaning that
        // each batch only have 1 state request.
        for (int i = maxInFlight; i < 10 * maxInFlight; i++) {
            String record = String.format("sameKey-r%d", i, i);
            String key = "sameKey";
            RecordContext<String> recordContext = aec.buildContext(record, key);
            aec.setCurrentContext(recordContext);
            userCode.run();
            assertThat(aec.inFlightRecordNum.get()).isEqualTo(maxInFlight + 1);
            assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
            assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(maxInFlight);
        }

        resourceRegistry.close();
    }

    @Test
    public void testSyncPoint() throws IOException {
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                1000,
                10000L,
                6000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        AtomicInteger counter = new AtomicInteger(0);

        // Test the sync point processing without a key occupied.
        RecordContext<String> recordContext = aec.buildContext("record", "key");
        aec.setCurrentContext(recordContext);
        recordContext.retain();
        aec.syncPointRequestWithCallback(counter::incrementAndGet);
        assertThat(counter.get()).isEqualTo(1);
        assertThat(recordContext.getReferenceCount()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        recordContext.release();
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);

        counter.set(0);
        // Test the sync point processing with a key occupied.
        RecordContext<String> recordContext1 = aec.buildContext("record1", "occupied");
        aec.setCurrentContext(recordContext1);
        userCode.run();

        RecordContext<String> recordContext2 = aec.buildContext("record2", "occupied");
        aec.setCurrentContext(recordContext2);
        aec.syncPointRequestWithCallback(counter::incrementAndGet);
        recordContext2.retain();
        assertThat(counter.get()).isEqualTo(0);
        assertThat(recordContext2.getReferenceCount()).isGreaterThan(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        assertThat(counter.get()).isEqualTo(0);
        assertThat(recordContext2.getReferenceCount()).isGreaterThan(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        assertThat(counter.get()).isEqualTo(1);
        assertThat(recordContext2.getReferenceCount()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        recordContext2.release();

        resourceRegistry.close();
    }

    @Test
    void testBufferTimeout() throws IOException {
        int batchSize = 5;
        int timeout = 1000;
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                batchSize,
                timeout,
                1000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        ManuallyTriggeredScheduledExecutorService scheduledExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        aec.stateRequestsBuffer.scheduledExecutor = scheduledExecutor;
        Runnable userCode = () -> valueState.asyncValue();

        // ------------ basic timeout -------------------
        for (int i = 0; i < batchSize - 1; i++) {
            String record = String.format("key%d-r%d", i, i);
            String key = String.format("key%d", batchSize + i);
            RecordContext<String> recordContext = aec.buildContext(record, key);
            aec.setCurrentContext(recordContext);
            userCode.run();
        }
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isDone()).isFalse();
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(batchSize - 1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(batchSize - 1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);

        // buffer timeout, trigger
        scheduledExecutor.triggerNonPeriodicScheduledTasks();
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isDone()).isTrue();
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(0);

        // ----------------- oldest state request timeout ------------------
        // r5 and r6 should be triggered due to r5 exceeding timeout
        String record5 = "key5-r5";
        String key5 = "key5";
        RecordContext<String> recordContext5 = aec.buildContext(record5, key5);
        aec.setCurrentContext(recordContext5);
        // execute user code
        userCode.run();
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isDone()).isFalse();
        ScheduledFuture<Void> scheduledFuture = aec.stateRequestsBuffer.currentScheduledFuture;
        String record6 = "key6-r6";
        String key6 = "key6";
        RecordContext<String> recordContext6 = aec.buildContext(record6, key6);
        aec.setCurrentContext(recordContext6);
        // execute user code
        userCode.run();
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(2);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(2);

        assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask().size()).isEqualTo(1);
        assertThat(scheduledExecutor.getAllNonPeriodicScheduledTask().size()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(1);
        scheduledExecutor.triggerNonPeriodicScheduledTasks();

        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(scheduledFuture).isEqualTo(aec.stateRequestsBuffer.currentScheduledFuture);
        assertThat(scheduledFuture.isDone()).isTrue();
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(2);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(1);

        resourceRegistry.close();
    }

    @Test
    void testBufferTimeoutSkip() throws IOException {
        int batchSize = 3;
        int timeout = 1000;
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                batchSize,
                timeout,
                1000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        ManuallyTriggeredScheduledExecutorService scheduledExecutor =
                new ManuallyTriggeredScheduledExecutorService();
        aec.stateRequestsBuffer.scheduledExecutor = scheduledExecutor;
        Runnable userCode = () -> valueState.asyncValue();

        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(-1);
        // register r1 timeout
        RecordContext<String> recordContext = aec.buildContext("record1", "key1");
        aec.setCurrentContext(recordContext);
        userCode.run();
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(0);

        // before r1 timeout execute, the active buffer size reach batch size.
        RecordContext<String> recordContext2 = aec.buildContext("record2", "key2");
        aec.setCurrentContext(recordContext2);
        userCode.run();
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(0);
        RecordContext<String> recordContext3 = aec.buildContext("record3", "key3");
        aec.setCurrentContext(recordContext3);
        userCode.run();
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(0);

        // r1 timeout executes, but r1 is already triggered in [r1,r2,r3], so r1 timeout should skip
        assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask().size()).isEqualTo(1);
        assertThat(scheduledExecutor.getAllNonPeriodicScheduledTask().size()).isEqualTo(1);
        scheduledExecutor.triggerNonPeriodicScheduledTask();
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isDone()).isTrue();
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(0);

        RecordContext<String> recordContext4 = aec.buildContext("record4", "key4");
        aec.setCurrentContext(recordContext4);
        userCode.run();

        // register r4 timeout, set new currentScheduledFuture
        assertThat(scheduledExecutor.getActiveNonPeriodicScheduledTask().size()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isDone()).isFalse();
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isCancelled()).isFalse();
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(1);

        // r4 timeout
        scheduledExecutor.triggerNonPeriodicScheduledTask();
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(0);
        assertThat(aec.stateRequestsBuffer.currentSeq.get()).isEqualTo(2);
        assertThat(aec.stateRequestsBuffer.scheduledSeq.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.currentScheduledFuture.isDone()).isTrue();

        resourceRegistry.close();
    }

    @Test
    void testUserCodeException() throws IOException {
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();
        TestMailboxExecutor testMailboxExecutor = new TestMailboxExecutor(false);
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(1000, 10000, 6000, testMailboxExecutor, exceptionHandler, resourceRegistry);
        Runnable userCode =
                () -> {
                    valueState
                            .asyncValue()
                            .thenAccept(
                                    val -> {
                                        throw new FlinkRuntimeException(
                                                "Artificial exception in user code");
                                    });
                };
        String record = "record";
        String key = "key";
        RecordContext<String> recordContext = aec.buildContext(record, key);
        aec.setCurrentContext(recordContext);
        userCode.run();
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        assertThat(exceptionHandler.exception).isNull();
        assertThat(exceptionHandler.message).isNull();
        aec.triggerIfNeeded(true);
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(0);
        assertThat(testMailboxExecutor.lastException).isInstanceOf(FlinkRuntimeException.class);
        assertThat(testMailboxExecutor.lastException.getMessage())
                .isEqualTo("Artificial exception in user code");
        assertThat(exceptionHandler.exception).isNull();
        assertThat(exceptionHandler.message).isNull();

        resourceRegistry.close();
    }

    @Test
    void testFrameworkException() throws IOException {
        TestAsyncFrameworkExceptionHandler exceptionHandler =
                new TestAsyncFrameworkExceptionHandler();
        TestMailboxExecutor testMailboxExecutor = new TestMailboxExecutor(true);
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(1000, 10000, 6000, testMailboxExecutor, exceptionHandler, resourceRegistry);
        Runnable userCode = () -> valueState.asyncValue().thenAccept(val -> {});
        String record = "record";
        String key = "key";
        RecordContext<String> recordContext = aec.buildContext(record, key);
        aec.setCurrentContext(recordContext);
        userCode.run();
        assertThat(aec.inFlightRecordNum.get()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.activeQueueSize()).isEqualTo(1);
        assertThat(aec.stateRequestsBuffer.blockingQueueSize()).isEqualTo(0);
        assertThat(exceptionHandler.exception).isNull();
        assertThat(exceptionHandler.message).isNull();
        aec.triggerIfNeeded(true);
        assertThat(testMailboxExecutor.lastException).isNull();
        assertThat(exceptionHandler.exception).isInstanceOf(RuntimeException.class);
        assertThat(exceptionHandler.exception.getMessage())
                .isEqualTo("java.lang.RuntimeException: Fail to execute.");
        assertThat(exceptionHandler.message)
                .isEqualTo("Caught exception when submitting StateFuture's callback.");

        resourceRegistry.close();
    }

    @Test
    void testEpochManager() throws Exception {
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        setup(
                1000,
                10000,
                6000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        AtomicInteger output = new AtomicInteger(0);
        Runnable userCode = () -> valueState.asyncValue().thenAccept(v -> output.incrementAndGet());

        String record1 = "key1-r1";
        String key1 = "key1";
        RecordContext<String> recordContext1 = aec.buildContext(record1, key1);
        Epoch epoch1 = recordContext1.getEpoch();
        aec.setCurrentContext(recordContext1);
        userCode.run();

        String record2 = "key2-r2";
        String key2 = "key2";
        RecordContext<String> recordContext2 = aec.buildContext(record2, key2);
        Epoch epoch2 = recordContext2.getEpoch();
        aec.setCurrentContext(recordContext2);
        userCode.run();

        assertThat(epoch1).isEqualTo(epoch2);
        assertThat(epoch1.ongoingRecordCount).isEqualTo(2);
        aec.processNonRecord(() -> output.incrementAndGet());

        assertThat(output.get()).isEqualTo(3);
        // SERIAL_BETWEEN_EPOCH mode would drain in-flight records on non-record arriving.
        assertThat(epoch1.ongoingRecordCount).isEqualTo(0);

        resourceRegistry.close();
    }

    @Test
    void testMixEpochMode() throws Exception {
        CloseableRegistry resourceRegistry = new CloseableRegistry();
        // epoch1(parallel mode) -> epoch2(parallel mode) -> epoch3(serial mode),
        // when epoch2 close, epoch1 is still in-flight.
        // when epoch3 close, all in-flight records should drain, epoch1 and epoch2 should finish.
        setup(
                1000,
                10000,
                6000,
                new SyncMailboxExecutor(),
                new TestAsyncFrameworkExceptionHandler(),
                resourceRegistry);
        AtomicInteger output = new AtomicInteger(0);
        Runnable userCode = () -> valueState.asyncValue().thenAccept(v -> output.incrementAndGet());

        String record1 = "key1-r1";
        String key1 = "key1";
        RecordContext<String> recordContext1 = aec.buildContext(record1, key1);
        Epoch epoch1 = recordContext1.getEpoch();
        aec.setCurrentContext(recordContext1);
        userCode.run();

        aec.epochManager.onNonRecord(
                () -> output.incrementAndGet(), ParallelMode.PARALLEL_BETWEEN_EPOCH);
        assertThat(epoch1.ongoingRecordCount).isEqualTo(1);

        String record2 = "key2-r2";
        String key2 = "key2";
        RecordContext<String> recordContext2 = aec.buildContext(record2, key2);
        Epoch epoch2 = recordContext2.getEpoch();
        aec.setCurrentContext(recordContext2);
        userCode.run();
        assertThat(epoch1.ongoingRecordCount).isEqualTo(1);
        assertThat(epoch2.ongoingRecordCount).isEqualTo(1);
        aec.epochManager.onNonRecord(
                () -> output.incrementAndGet(), ParallelMode.PARALLEL_BETWEEN_EPOCH);
        assertThat(epoch1.ongoingRecordCount).isEqualTo(1);
        assertThat(epoch2.ongoingRecordCount).isEqualTo(1);
        assertThat(output.get()).isEqualTo(0);

        String record3 = "key3-r3";
        String key3 = "key3";
        RecordContext<String> recordContext3 = aec.buildContext(record3, key3);
        Epoch epoch3 = recordContext3.getEpoch();
        aec.setCurrentContext(recordContext3);
        userCode.run();
        assertThat(epoch1.ongoingRecordCount).isEqualTo(1);
        assertThat(epoch2.ongoingRecordCount).isEqualTo(1);
        assertThat(epoch3.ongoingRecordCount).isEqualTo(1);
        aec.epochManager.onNonRecord(
                () -> output.incrementAndGet(), ParallelMode.SERIAL_BETWEEN_EPOCH);
        assertThat(epoch1.ongoingRecordCount).isEqualTo(0);
        assertThat(epoch2.ongoingRecordCount).isEqualTo(0);
        assertThat(epoch3.ongoingRecordCount).isEqualTo(0);
        assertThat(output.get()).isEqualTo(6);

        resourceRegistry.close();
    }

    /** Simulate the underlying state that is actually used to execute the request. */
    static class TestUnderlyingState {

        private final HashMap<String, Integer> hashMap;

        public TestUnderlyingState() {
            this.hashMap = new HashMap<>();
        }

        public Integer get(String key) {
            return hashMap.get(key);
        }

        public void update(String key, Integer val) {
            hashMap.put(key, val);
        }
    }

    static class TestValueState extends InternalValueState<String, Integer> {

        private final TestUnderlyingState underlyingState;

        public TestValueState(
                StateRequestHandler stateRequestHandler,
                TestUnderlyingState underlyingState,
                ValueStateDescriptor<Integer> stateDescriptor) {
            super(stateRequestHandler, stateDescriptor);
            this.underlyingState = underlyingState;
            assertThat(this.getValueSerializer()).isEqualTo(IntSerializer.INSTANCE);
        }
    }

    /**
     * A brief implementation of {@link StateExecutor}, to illustrate the interaction between AEC
     * and StateExecutor.
     */
    static class TestStateExecutor implements StateExecutor {

        public TestStateExecutor() {}

        @Override
        @SuppressWarnings({"unchecked", "rawtypes"})
        public CompletableFuture<Void> executeBatchRequests(
                StateRequestContainer stateRequestContainer) {
            Preconditions.checkArgument(stateRequestContainer instanceof MockStateRequestContainer);
            CompletableFuture<Void> future = new CompletableFuture<>();
            for (StateRequest request :
                    ((MockStateRequestContainer) stateRequestContainer).getStateRequestList()) {
                if (request.getRequestType() == StateRequestType.VALUE_GET) {
                    Preconditions.checkState(request.getState() != null);
                    TestValueState state = (TestValueState) request.getState();
                    Integer val =
                            state.underlyingState.get((String) request.getRecordContext().getKey());
                    request.getFuture().complete(val);
                } else if (request.getRequestType() == StateRequestType.VALUE_UPDATE) {
                    Preconditions.checkState(request.getState() != null);
                    TestValueState state = (TestValueState) request.getState();

                    state.underlyingState.update(
                            (String) request.getRecordContext().getKey(),
                            (Integer) request.getPayload());
                    request.getFuture().complete(null);
                } else {
                    throw new UnsupportedOperationException("Unsupported request type");
                }
            }
            future.complete(null);
            return future;
        }

        @Override
        public StateRequestContainer createStateRequestContainer() {
            return new MockStateRequestContainer();
        }

        @Override
        public void shutdown() {}
    }

    static class TestAsyncFrameworkExceptionHandler implements AsyncFrameworkExceptionHandler {
        String message = null;
        Throwable exception = null;

        public void handleException(String message, Throwable exception) {
            this.message = message;
            this.exception = exception;
        }
    }

    static class TestMailboxExecutor implements MailboxExecutor {
        Exception lastException = null;

        boolean failWhenExecute = false;

        public TestMailboxExecutor(boolean fail) {
            this.failWhenExecute = fail;
        }

        @Override
        public void execute(
                MailOptions mailOptions,
                ThrowingRunnable<? extends Exception> command,
                String descriptionFormat,
                Object... descriptionArgs) {
            if (failWhenExecute) {
                throw new RuntimeException("Fail to execute.");
            }
            try {
                command.run();
            } catch (Exception e) {
                this.lastException = e;
            }
        }

        @Override
        public void yield() throws InterruptedException, FlinkRuntimeException {}

        @Override
        public boolean tryYield() throws FlinkRuntimeException {
            return false;
        }

        @Override
        public boolean shouldInterrupt() {
            return false;
        }
    }
}
