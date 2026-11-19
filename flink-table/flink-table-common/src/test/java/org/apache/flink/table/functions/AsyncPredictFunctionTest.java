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

package org.apache.flink.table.functions;

import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link AsyncPredictFunction}. */
class AsyncPredictFunctionTest {

    @Test
    void testSuccessfulAsyncPrediction() throws Exception {
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.SUCCESS);
        function.open(new FunctionContext(null));

        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        function.eval(out, "input", 1);

        Collection<RowData> result = out.get();
        assertThat(result).hasSize(2);

        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isEqualTo(2L);
    }

    @Test
    void testAsyncPredictReturnsExceptionallyCompletedFuture() throws Exception {
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.FAIL_ASYNC);
        function.open(new FunctionContext(null));

        // Use a distinctive payload so we can assert it is NOT leaked into the exception message.
        String sensitivePayload = "SECRET_TOKEN_XYZ789";
        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        function.eval(out, sensitivePayload, 42);

        assertThatThrownBy(out::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to execute asynchronous prediction")
                .hasMessageContaining("arity=2")
                // Input payload must NOT be leaked into the exception message.
                .hasMessageNotContaining(sensitivePayload)
                .hasMessageNotContaining("42");

        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isZero();
        assertThat(metrics.getRequestsFailure().getCount()).isEqualTo(1L);
        assertThat(metrics.getRowsOutput().getCount()).isZero();
    }

    @Test
    void testAsyncPredictThrowsSynchronously() throws Exception {
        // A misbehaving subclass that throws before producing a future must still preserve
        // the request-accounting invariant (requests == success + failure).
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.FAIL_SYNC);
        function.open(new FunctionContext(null));

        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        function.eval(out, "input", 1);

        assertThatThrownBy(out::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlinkRuntimeException.class);

        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isZero();
        assertThat(metrics.getRequestsFailure().getCount()).isEqualTo(1L);
    }

    @Test
    void testAsyncPredictReturnsNullFuture() throws Exception {
        // A misbehaving subclass that returns a null CompletableFuture must be treated as a
        // failure, not as an NPE that escapes the metrics wrapper.
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.RETURN_NULL_FUTURE);
        function.open(new FunctionContext(null));

        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        function.eval(out, "input", 1);

        assertThatThrownBy(out::get)
                .isInstanceOf(ExecutionException.class)
                .hasCauseInstanceOf(FlinkRuntimeException.class);

        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isEqualTo(1L);
    }

    @Test
    void testAsyncPredictReturnsFutureCompletedWithNullCollection() throws Exception {
        // Future completes normally with null - treat as success with zero rows.
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.SUCCESS_NULL_RESULT);
        function.open(new FunctionContext(null));

        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        function.eval(out, "input", 1);
        assertThat(out.get()).isNull();

        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isZero();
    }

    @Test
    void testAsyncPredictReturnsEmptyCollection() throws Exception {
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.SUCCESS_EMPTY);
        function.open(new FunctionContext(null));

        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        function.eval(out, "input", 1);
        assertThat(out.get()).isEmpty();

        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isZero();
    }

    @Test
    void testRequestInvariantAcrossManyCalls() throws Exception {
        // Mixed workload: every other call fails asynchronously.
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.SUCCESS);
        function.open(new FunctionContext(null));

        int total = 10;
        for (int i = 0; i < total; i++) {
            function.setNextMode(i % 2 == 0 ? Mode.SUCCESS : Mode.FAIL_ASYNC);
            CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
            function.eval(out, "input", i);
            // drain
            out.whenComplete((r, t) -> {});
            try {
                out.get();
            } catch (ExecutionException ignored) {
                // expected for the failure branch
            }
        }

        PredictFunctionMetrics metrics = function.getMetrics();
        long requests = metrics.getRequests().getCount();
        long success = metrics.getRequestsSuccess().getCount();
        long failure = metrics.getRequestsFailure().getCount();
        assertThat(requests).isEqualTo(total);
        assertThat(success + failure).isEqualTo(requests);
        assertThat(success).isEqualTo(5L);
        assertThat(failure).isEqualTo(5L);
    }

    @Test
    void testEvalBeforeOpenThrowsIllegalState() {
        TestAsyncPredictFunction function = new TestAsyncPredictFunction(Mode.SUCCESS);
        CompletableFuture<Collection<RowData>> out = new CompletableFuture<>();
        assertThatThrownBy(() -> function.eval(out, "input", 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("open(FunctionContext) must be invoked before eval");
    }

    // ------------------------------------------------------------------------
    // Test Implementations
    // ------------------------------------------------------------------------

    private enum Mode {
        SUCCESS,
        SUCCESS_EMPTY,
        SUCCESS_NULL_RESULT,
        FAIL_ASYNC,
        FAIL_SYNC,
        RETURN_NULL_FUTURE
    }

    private static class TestAsyncPredictFunction extends AsyncPredictFunction {
        private volatile Mode mode;

        TestAsyncPredictFunction(Mode mode) {
            this.mode = mode;
        }

        void setNextMode(Mode mode) {
            this.mode = mode;
        }

        @Override
        public CompletableFuture<Collection<RowData>> asyncPredict(RowData inputRow) {
            switch (mode) {
                case SUCCESS:
                    RowData r1 =
                            GenericRowData.of(
                                    StringData.fromString("prediction1"), inputRow.getInt(1));
                    RowData r2 =
                            GenericRowData.of(
                                    StringData.fromString("prediction2"), inputRow.getInt(1) * 2);
                    return CompletableFuture.completedFuture(Arrays.asList(r1, r2));
                case SUCCESS_EMPTY:
                    return CompletableFuture.completedFuture(Collections.emptyList());
                case SUCCESS_NULL_RESULT:
                    return CompletableFuture.completedFuture(null);
                case FAIL_ASYNC:
                    CompletableFuture<Collection<RowData>> failed = new CompletableFuture<>();
                    failed.completeExceptionally(new RuntimeException("simulated async failure"));
                    return failed;
                case FAIL_SYNC:
                    throw new RuntimeException("simulated synchronous failure");
                case RETURN_NULL_FUTURE:
                    return null;
                default:
                    throw new IllegalStateException("unknown mode: " + mode);
            }
        }
    }
}
