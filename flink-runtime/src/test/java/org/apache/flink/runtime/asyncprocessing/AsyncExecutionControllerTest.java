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

import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.base.IntSerializer;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.state.AsyncKeyedStateBackend;
import org.apache.flink.runtime.state.CheckpointableKeyedStateBackend;
import org.apache.flink.runtime.state.OperatorStateBackend;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.v2.InternalValueState;
import org.apache.flink.runtime.state.v2.ValueStateDescriptor;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link AsyncExecutionController}. */
class AsyncExecutionControllerTest {
    AsyncExecutionController aec;
    TestUnderlyingState underlyingState;
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

    @BeforeEach
    void setup() {
        aec = new AsyncExecutionController<>(new SyncMailboxExecutor(), createStateExecutor());
        underlyingState = new TestUnderlyingState();
        valueState = new TestValueState(aec, underlyingState);
        output = new AtomicInteger();
    }

    @Test
    void testBasicRun() {
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
    }

    @Test
    void testRecordsRunInOrder() {
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
    }

    @Test
    void testInFlightRecordControl() {
        final int batchSize = 5;
        final int maxInFlight = 10;
        aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(), new TestStateExecutor(), batchSize, maxInFlight);
        valueState = new TestValueState(aec, underlyingState);

        AtomicInteger output = new AtomicInteger();
        Runnable userCode =
                () -> {
                    valueState
                            .asyncValue()
                            .thenCompose(
                                    val -> {
                                        int updated = (val == null ? 1 : (val + 1));
                                        return valueState
                                                .asyncUpdate(updated)
                                                .thenCompose(
                                                        o ->
                                                                StateFutureUtils.completedFuture(
                                                                        updated));
                                    })
                            .thenAccept(val -> output.set(val));
                };

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
    }

    @Test
    public void testSyncPoint() {
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
    }

    private StateExecutor createStateExecutor() {
        TestAsyncStateBackend testAsyncStateBackend = new TestAsyncStateBackend();
        assertThat(testAsyncStateBackend.supportsAsyncKeyedStateBackend()).isTrue();
        return testAsyncStateBackend.createAsyncKeyedStateBackend(null).createStateExecutor();
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
                AsyncExecutionController<String> aec, TestUnderlyingState underlyingState) {
            super(aec, new ValueStateDescriptor<>("test-value-state", BasicTypeInfo.INT_TYPE_INFO));
            this.underlyingState = underlyingState;
            assertThat(this.getValueSerializer()).isEqualTo(IntSerializer.INSTANCE);
        }
    }

    /**
     * A brief implementation of {@link StateBackend} which illustrates the interaction between AEC
     * and StateBackend.
     */
    static class TestAsyncStateBackend implements StateBackend {

        @Override
        public <K> CheckpointableKeyedStateBackend<K> createKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) throws Exception {
            throw new UnsupportedOperationException("Don't support createKeyedStateBackend yet");
        }

        @Override
        public OperatorStateBackend createOperatorStateBackend(
                OperatorStateBackendParameters parameters) throws Exception {
            throw new UnsupportedOperationException("Don't support createOperatorStateBackend yet");
        }

        @Override
        public boolean supportsAsyncKeyedStateBackend() {
            return true;
        }

        @Override
        public <K> AsyncKeyedStateBackend createAsyncKeyedStateBackend(
                KeyedStateBackendParameters<K> parameters) {
            return new AsyncKeyedStateBackend() {
                @Override
                public StateExecutor createStateExecutor() {
                    return new TestStateExecutor();
                }

                @Override
                public void dispose() {
                    // do nothing
                }
            };
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
        public CompletableFuture<Boolean> executeBatchRequests(
                Iterable<StateRequest<?, ?, ?>> processingRequests) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            for (StateRequest request : processingRequests) {
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
            future.complete(true);
            return future;
        }
    }
}
