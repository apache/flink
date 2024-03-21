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

package org.apache.flink.runtime.taskprocessing;

import org.apache.flink.api.common.state.v2.StateFuture;
import org.apache.flink.api.common.state.v2.ValueState;
import org.apache.flink.core.state.InternalStateFuture;
import org.apache.flink.core.state.StateFutureImpl;
import org.apache.flink.runtime.mailbox.SyncMailboxExecutor;
import org.apache.flink.runtime.taskprocessing.ProcessingRequest.RequestType;
import org.apache.flink.util.Preconditions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/** Test for {@link AsyncExecutionController}. */
class AsyncExecutionControllerTest {

    // todo(20240330): this test is not completed, cause the order preservation is not implemented
    // yet, just for illustrating the interaction between AEC and Async state API.
    @Test
    void testStateOrder() {
        AsyncExecutionController aec =
                new AsyncExecutionController<>(
                        new SyncMailboxExecutor(), new TestStateExecutor(), 3);
        TestUnderlyingState underlyingState = new TestUnderlyingState();
        TestValueState valueState = new TestValueState(aec, underlyingState);
        AtomicInteger output = new AtomicInteger();
        Consumer<Void> userCode =
                empty ->
                        valueState
                                .asyncValue()
                                .thenAccept(
                                        val -> {
                                            if (val == null) {
                                                valueState
                                                        .asyncUpdate(1)
                                                        .thenAccept(o -> output.set(1));
                                            } else {
                                                valueState
                                                        .asyncUpdate(val + 1)
                                                        .thenAccept(o -> output.set(val + 1));
                                            }
                                        });

        // ============================ element1 ============================
        String record1 = "key1-r1";
        String key1 = "key1";
        // Simulate the wrapping in {@link RecordProcessorUtils#getRecordProcessor()}, wrapping the
        // record and key with RecordContext.
        RecordContext<String, String> recordContext1 = new RecordContext<>(record1, key1);
        valueState.setCurrentRecordCtx(recordContext1);
        // execute user code
        userCode.accept(null);
        recordContext1.release();
        assertThat(output.get()).isEqualTo(1);

        // ============================ element2 ============================
        String record2 = "key1-r2";
        String key2 = "key1";
        RecordContext<String, String> recordContext2 = new RecordContext<>(record2, key2);
        valueState.setCurrentRecordCtx(recordContext2);
        // execute user code
        userCode.accept(null);
        recordContext2.release();
        assertThat(output.get()).isEqualTo(2);

        // ============================ element3 ============================
        String record3 = "key3-r3";
        String key3 = "key3";
        RecordContext<String, String> recordContext3 = new RecordContext<>(record3, key3);
        valueState.setCurrentRecordCtx(recordContext3);
        // execute user code
        userCode.accept(null);
        recordContext3.release();
        assertThat(output.get()).isEqualTo(1);
    }

    class TestRequestParameter implements ProcessingRequest.Parameter<String> {
        private String key;
        private Integer value;

        public TestRequestParameter(String key) {
            this(key, null);
        }

        public TestRequestParameter(String key, Integer value) {
            this.key = key;
            this.value = value;
        }

        @Override
        public Optional<String> getKey() {
            return key == null ? Optional.empty() : Optional.of(key);
        }

        @Override
        public Optional<?> getValue() {
            return value == null ? Optional.empty() : Optional.of(value);
        }
    }

    /** Simulate the underlying state that is actually used to execute the request. */
    class TestUnderlyingState {

        private HashMap<String, Integer> hashMap;

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

    class TestProcessingRequest<OUT> implements ProcessingRequest<OUT> {

        private TestUnderlyingState underlyingState;

        private TestRequestParameter requestParameter;

        private RequestType requestType;

        private InternalStateFuture<OUT> stateFuture;

        public TestProcessingRequest(
                TestUnderlyingState underlyingState,
                TestRequestParameter parameter,
                RequestType requestType,
                InternalStateFuture<OUT> stateFuture) {
            this.underlyingState = underlyingState;
            this.requestParameter = parameter;
            this.requestType = requestType;
            this.stateFuture = stateFuture;
        }

        @Override
        public Optional<TestUnderlyingState> getUnderlyingState() {
            if (requestType == RequestType.SYNC) {
                return Optional.empty();
            }
            return Optional.of(underlyingState);
        }

        @Override
        public Parameter getParameter() {
            return requestParameter;
        }

        @Override
        public StateFuture<OUT> getFuture() {
            return stateFuture;
        }

        @Override
        public RequestType getRequestType() {
            return requestType;
        }
    }

    class TestValueState implements ValueState<Integer> {

        private AsyncExecutionController asyncExecutionController;

        private TestUnderlyingState underlyingState;

        private StateFutureImpl.CallbackRunner runner = Runnable::run;

        private RecordContext<String, String> currentRecordCtx;

        public TestValueState(AsyncExecutionController aec, TestUnderlyingState underlyingState) {
            this.asyncExecutionController = aec;
            this.underlyingState = underlyingState;
        }

        @Override
        public StateFuture<Void> asyncClear() {
            StateFutureImpl<Void> stateFuture = new StateFutureImpl<>(runner);
            TestRequestParameter parameter = new TestRequestParameter(currentRecordCtx.getKey());
            ProcessingRequest<Void> request =
                    new TestProcessingRequest<Void>(
                            underlyingState, parameter, RequestType.DELETE, stateFuture);
            asyncExecutionController.handleProcessingRequest(request, currentRecordCtx);
            return stateFuture;
        }

        @Override
        public StateFuture<Integer> asyncValue() {
            StateFutureImpl<Integer> stateFuture = new StateFutureImpl<>(runner);
            TestRequestParameter parameter = new TestRequestParameter(currentRecordCtx.getKey());
            ProcessingRequest<Integer> request =
                    new TestProcessingRequest<>(
                            underlyingState, parameter, RequestType.GET, stateFuture);
            asyncExecutionController.handleProcessingRequest(request, currentRecordCtx);
            return stateFuture;
        }

        @Override
        public StateFuture<Void> asyncUpdate(Integer value) {
            StateFutureImpl<Void> stateFuture = new StateFutureImpl<>(runner);
            TestRequestParameter parameter =
                    new TestRequestParameter(currentRecordCtx.getKey(), value);
            ProcessingRequest<Void> request =
                    new TestProcessingRequest<Void>(
                            underlyingState, parameter, RequestType.PUT, stateFuture);
            asyncExecutionController.handleProcessingRequest(request, currentRecordCtx);
            return stateFuture;
        }

        public void setCurrentRecordCtx(RecordContext<String, String> recordCtx) {
            this.currentRecordCtx = recordCtx;
        }
    }

    /**
     * A brief implementation of {@link StateExecutor}, to illustrate the interaction between AEC
     * and StateExecutor.
     */
    class TestStateExecutor implements StateExecutor {

        public TestStateExecutor() {}

        @Override
        public CompletableFuture<Boolean> executeBatchRequests(
                Iterable<ProcessingRequest<?>> processingRequests) {
            CompletableFuture<Boolean> future = new CompletableFuture<>();
            for (ProcessingRequest request : processingRequests) {
                if (request.getRequestType() == RequestType.GET) {
                    Preconditions.checkState(request.getUnderlyingState().isPresent());
                    TestUnderlyingState underlyingState =
                            (TestUnderlyingState) request.getUnderlyingState().get();
                    Integer val =
                            underlyingState.get(
                                    ((TestRequestParameter) request.getParameter()).getKey().get());
                    ((StateFutureImpl<Integer>) request.getFuture()).complete(val);
                } else if (request.getRequestType() == RequestType.PUT) {
                    Preconditions.checkState(request.getUnderlyingState().isPresent());
                    TestUnderlyingState underlyingState =
                            (TestUnderlyingState) request.getUnderlyingState().get();
                    TestRequestParameter parameter = (TestRequestParameter) request.getParameter();
                    underlyingState.update(
                            parameter.getKey().get(), (Integer) parameter.getValue().get());
                    ((StateFutureImpl<Void>) request.getFuture()).complete(null);
                } else {
                    throw new UnsupportedOperationException("Unsupported request type");
                }
            }
            future.complete(true);
            return future;
        }
    }
}
