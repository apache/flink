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
import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.core.state.StateFutureUtils;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link AsyncExecutionController}. */
class AsyncExecutionControllerTest {

    // TODO: this test is not well completed, cause buffering in AEC is not implemented.
    // Yet, just for illustrating the interaction between AEC and Async state API.
    @Test
    void testBasicRun() {
        TestAsyncExecutionController<String, String> aec =
                new TestAsyncExecutionController<>(
                        new SyncMailboxExecutor(), new TestStateExecutor());
        TestUnderlyingState underlyingState = new TestUnderlyingState();
        TestValueState valueState = new TestValueState(aec, underlyingState);
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

        // ============================ element1 ============================
        String record1 = "key1-r1";
        String key1 = "key1";
        // Simulate the wrapping in {@link RecordProcessorUtils#getRecordProcessor()}, wrapping the
        // record and key with RecordContext.
        RecordContext<String, String> recordContext1 = aec.buildContext(record1, key1);
        aec.setCurrentContext(recordContext1);
        // execute user code
        userCode.run();

        // Single-step run.
        // Firstly, the user code generates value get in active buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, the value update is in active buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update finishes.
        assertThat(aec.activeBuffer.size()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(1);
        assertThat(recordContext1.getReferenceCount()).isEqualTo(0);

        // ============================ element 2 & 3 ============================
        String record2 = "key1-r2";
        String key2 = "key1";
        RecordContext<String, String> recordContext2 = aec.buildContext(record2, key2);
        aec.setCurrentContext(recordContext2);
        // execute user code
        userCode.run();

        String record3 = "key1-r3";
        String key3 = "key1";
        RecordContext<String, String> recordContext3 = aec.buildContext(record3, key3);
        aec.setCurrentContext(recordContext3);
        // execute user code
        userCode.run();

        // Single-step run.
        // Firstly, the user code for record2 generates value get in active buffer,
        // while user code for record3 generates value get in blocking buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.blockingBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, the value update for record2 is in active buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.blockingBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update for record2 finishes. The value get for record3 is still in blocking status.
        assertThat(aec.activeBuffer.size()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(2);
        assertThat(recordContext2.getReferenceCount()).isEqualTo(0);
        assertThat(aec.blockingBuffer.size()).isEqualTo(1);

        aec.migrateBlockingToActive();
        // Value get for record3 is ready for run.

        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.blockingBuffer.size()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, the value update for record3 is in active buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update for record3 finishes.
        assertThat(aec.activeBuffer.size()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(3);
        assertThat(recordContext3.getReferenceCount()).isEqualTo(0);

        // ============================ element4 ============================
        String record4 = "key3-r3";
        String key4 = "key3";
        RecordContext<String, String> recordContext4 = aec.buildContext(record4, key4);
        aec.setCurrentContext(recordContext4);
        // execute user code
        userCode.run();

        // Single-step run for another key.
        // Firstly, the user code generates value get in active buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // After running, the value update is in active buffer.
        assertThat(aec.activeBuffer.size()).isEqualTo(1);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(1);
        aec.triggerIfNeeded(true);
        // Value update finishes.
        assertThat(aec.activeBuffer.size()).isEqualTo(0);
        assertThat(aec.keyAccountingUnit.occupiedCount()).isEqualTo(0);
        assertThat(output.get()).isEqualTo(1);
        assertThat(recordContext4.getReferenceCount()).isEqualTo(0);
    }

    /**
     * An AsyncExecutionController for testing purpose, which integrates with basic buffer
     * mechanism.
     */
    static class TestAsyncExecutionController<R, K> extends AsyncExecutionController<R, K> {

        LinkedList<StateRequest<K, ?, ?>> activeBuffer;

        LinkedList<StateRequest<K, ?, ?>> blockingBuffer;

        public TestAsyncExecutionController(
                MailboxExecutor mailboxExecutor, StateExecutor stateExecutor) {
            super(mailboxExecutor, stateExecutor);
            activeBuffer = new LinkedList<>();
            blockingBuffer = new LinkedList<>();
        }

        @Override
        <IN, OUT> void insertActiveBuffer(StateRequest<K, IN, OUT> request) {
            activeBuffer.push(request);
        }

        <IN, OUT> void insertBlockingBuffer(StateRequest<K, IN, OUT> request) {
            blockingBuffer.push(request);
        }

        void triggerIfNeeded(boolean force) {
            if (!force) {
                // Disable normal trigger, to perform single-step debugging and check.
                return;
            }
            LinkedList<StateRequest<?, ?, ?>> toRun = new LinkedList<>(activeBuffer);
            activeBuffer.clear();
            stateExecutor.executeBatchRequests(toRun);
        }

        @SuppressWarnings("unchecked")
        void migrateBlockingToActive() {
            Iterator<StateRequest<K, ?, ?>> blockingIter = blockingBuffer.iterator();
            while (blockingIter.hasNext()) {
                StateRequest<K, ?, ?> request = blockingIter.next();
                if (tryOccupyKey((RecordContext<R, K>) request.getRecordContext())) {
                    insertActiveBuffer(request);
                    blockingIter.remove();
                }
            }
        }
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

    static class TestValueState implements ValueState<Integer> {

        private final AsyncExecutionController<String, String> asyncExecutionController;

        private final TestUnderlyingState underlyingState;

        public TestValueState(
                AsyncExecutionController<String, String> aec, TestUnderlyingState underlyingState) {
            this.asyncExecutionController = aec;
            this.underlyingState = underlyingState;
        }

        @Override
        public StateFuture<Void> asyncClear() {
            return asyncExecutionController.handleRequest(this, StateRequestType.CLEAR, null);
        }

        @Override
        public StateFuture<Integer> asyncValue() {
            return asyncExecutionController.handleRequest(this, StateRequestType.VALUE_GET, null);
        }

        @Override
        public StateFuture<Void> asyncUpdate(Integer value) {
            return asyncExecutionController.handleRequest(
                    this, StateRequestType.VALUE_UPDATE, value);
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
