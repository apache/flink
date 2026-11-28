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

import org.apache.flink.metrics.Histogram;
import org.apache.flink.metrics.HistogramStatistics;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.util.Collector;
import org.apache.flink.util.FlinkRuntimeException;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Tests for {@link PredictFunction}. */
class PredictFunctionTest {

    private TestPredictFunction predictFunction;
    private TestCollector testCollector;

    @BeforeEach
    void setUp() throws Exception {
        predictFunction = new TestPredictFunction();
        testCollector = new TestCollector();
        predictFunction.open(new FunctionContext(null));
        predictFunction.setCollector(testCollector);
    }

    @Test
    void testBasicPrediction() {
        predictFunction.eval("input1", 42);

        assertThat(testCollector.getCollectedResults()).hasSize(2);
        assertThat(testCollector.getCollectedResults().get(0).getArity()).isEqualTo(2);

        PredictFunctionMetrics metrics = predictFunction.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isEqualTo(2L);
    }

    @Test
    void testMultiplePredictions() {
        predictFunction.eval("input1", 1);
        predictFunction.eval("input2", 2);

        // Each prediction returns 2 results
        assertThat(testCollector.getCollectedResults()).hasSize(4);

        PredictFunctionMetrics metrics = predictFunction.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(2L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(2L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isEqualTo(4L);
    }

    @Test
    void testFailureHandling() throws Exception {
        TestPredictFunction failingFunction = new TestPredictFunction(true);
        TestCollector failCollector = new TestCollector();
        failingFunction.open(new FunctionContext(null));
        failingFunction.setCollector(failCollector);

        // Use a distinctive payload so we can assert it is NOT leaked into the exception message.
        String sensitivePayload = "SECRET_TOKEN_ABC123";
        assertThatThrownBy(() -> failingFunction.eval(sensitivePayload, 42))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to execute prediction")
                .hasMessageContaining("arity=2")
                // Input payload must NOT be leaked into the exception message.
                .hasMessageNotContaining(sensitivePayload)
                .hasMessageNotContaining("42");

        PredictFunctionMetrics metrics = failingFunction.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isZero();
        assertThat(metrics.getRequestsFailure().getCount()).isEqualTo(1L);
        assertThat(metrics.getRowsOutput().getCount()).isZero();
    }

    @Test
    void testErrorIsNotSwallowedAndStillCountsAsFailure() throws Exception {
        ErrorThrowingPredictFunction function = new ErrorThrowingPredictFunction();
        function.open(new FunctionContext(null));
        function.setCollector(new TestCollector());

        assertThatThrownBy(() -> function.eval("input", 1)).isInstanceOf(OutOfMemoryError.class);

        // Even though the Error is re-thrown as-is, the request-accounting invariant
        // (requests == success + failure) must hold.
        PredictFunctionMetrics metrics = function.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isZero();
        assertThat(metrics.getRequestsFailure().getCount()).isEqualTo(1L);
    }

    @Test
    void testNullResults() throws Exception {
        TestPredictFunction nullResultFunction = new TestPredictFunction(false, true);
        TestCollector nullCollector = new TestCollector();
        nullResultFunction.open(new FunctionContext(null));
        nullResultFunction.setCollector(nullCollector);

        nullResultFunction.eval("input", 1);
        assertThat(nullCollector.getCollectedResults()).isEmpty();

        // null return is treated as a successful request with zero output rows.
        PredictFunctionMetrics metrics = nullResultFunction.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isZero();
    }

    @Test
    void testEmptyResults() throws Exception {
        TestPredictFunction emptyResultFunction = new TestPredictFunction(false, false, true);
        TestCollector emptyCollector = new TestCollector();
        emptyResultFunction.open(new FunctionContext(null));
        emptyResultFunction.setCollector(emptyCollector);

        emptyResultFunction.eval("input", 1);
        assertThat(emptyCollector.getCollectedResults()).isEmpty();

        PredictFunctionMetrics metrics = emptyResultFunction.getMetrics();
        assertThat(metrics.getRequests().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsSuccess().getCount()).isEqualTo(1L);
        assertThat(metrics.getRequestsFailure().getCount()).isZero();
        assertThat(metrics.getRowsOutput().getCount()).isZero();
    }

    @Test
    void testCustomHistogramIsInvokedOnBothSuccessAndFailure() throws Exception {
        TestHistogram successHistogram = new TestHistogram();
        TestPredictFunctionWithHistogram successFunction =
                new TestPredictFunctionWithHistogram(successHistogram, false);
        successFunction.open(new FunctionContext(null));
        successFunction.setCollector(new TestCollector());
        successFunction.eval("input", 1);
        assertThat(successHistogram.getCount()).isEqualTo(1L);

        TestHistogram failureHistogram = new TestHistogram();
        TestPredictFunctionWithHistogram failingFunction =
                new TestPredictFunctionWithHistogram(failureHistogram, true);
        failingFunction.open(new FunctionContext(null));
        failingFunction.setCollector(new TestCollector());
        assertThatThrownBy(() -> failingFunction.eval("input", 1))
                .isInstanceOf(FlinkRuntimeException.class);
        assertThat(failureHistogram.getCount()).isEqualTo(1L);
    }

    @Test
    void testDefaultLatencyHistogramIsDisabled() throws Exception {
        // Default createLatencyHistogram returns null; recordLatency() must be a no-op and not
        // throw. The base test function exercises the success path already.
        assertThat(predictFunction.getMetrics().getLatency()).isNull();
        predictFunction.eval("input1", 1);
        // Sanity check: still succeeds.
        assertThat(predictFunction.getMetrics().getRequestsSuccess().getCount()).isEqualTo(1L);
    }

    @Test
    void testEvalBeforeOpenThrowsIllegalState() {
        TestPredictFunction unopened = new TestPredictFunction();
        unopened.setCollector(new TestCollector());
        assertThatThrownBy(() -> unopened.eval("input", 1))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("open(FunctionContext) must be invoked before eval");
    }

    // ------------------------------------------------------------------------
    // Test Implementations
    // ------------------------------------------------------------------------

    /** Test collector for capturing output. */
    private static class TestCollector implements Collector<RowData> {
        private final List<RowData> collectedResults = new ArrayList<>();

        @Override
        public void collect(RowData record) {
            collectedResults.add(record);
        }

        @Override
        public void close() {}

        List<RowData> getCollectedResults() {
            return collectedResults;
        }
    }

    /** Testing implementation of {@link PredictFunction}. */
    private static class TestPredictFunction extends PredictFunction {
        private final boolean shouldFail;
        private final boolean returnNull;
        private final boolean returnEmpty;

        TestPredictFunction() {
            this(false, false, false);
        }

        TestPredictFunction(boolean shouldFail) {
            this(shouldFail, false, false);
        }

        TestPredictFunction(boolean shouldFail, boolean returnNull) {
            this(shouldFail, returnNull, false);
        }

        TestPredictFunction(boolean shouldFail, boolean returnNull, boolean returnEmpty) {
            this.shouldFail = shouldFail;
            this.returnNull = returnNull;
            this.returnEmpty = returnEmpty;
        }

        @Override
        public Collection<RowData> predict(RowData inputRow) {
            if (shouldFail) {
                throw new RuntimeException("Simulated prediction failure");
            }
            if (returnNull) {
                return null;
            }
            if (returnEmpty) {
                return Collections.emptyList();
            }
            RowData result1 =
                    GenericRowData.of(StringData.fromString("prediction1"), inputRow.getInt(1));
            RowData result2 =
                    GenericRowData.of(StringData.fromString("prediction2"), inputRow.getInt(1) * 2);
            return Arrays.asList(result1, result2);
        }
    }

    /** Throws an {@link Error} (not an {@link Exception}) to verify the failure counter path. */
    private static class ErrorThrowingPredictFunction extends PredictFunction {
        @Override
        public Collection<RowData> predict(RowData inputRow) {
            throw new OutOfMemoryError("simulated OOM");
        }
    }

    /** Testing implementation of {@link PredictFunction} with a user-supplied histogram. */
    private static class TestPredictFunctionWithHistogram extends TestPredictFunction {
        private final TestHistogram histogram;

        TestPredictFunctionWithHistogram(TestHistogram histogram, boolean shouldFail) {
            super(shouldFail);
            this.histogram = histogram;
        }

        @Override
        protected Histogram createLatencyHistogram(MetricGroup metricGroup) {
            return metricGroup.histogram("latency", histogram);
        }
    }

    /** Minimal {@link Histogram} that counts update() invocations. */
    private static class TestHistogram implements Histogram {
        private long count = 0;

        @Override
        public void update(long value) {
            count++;
        }

        @Override
        public long getCount() {
            return count;
        }

        @Override
        public HistogramStatistics getStatistics() {
            // Return an empty statistics object instead of null so downstream reporters that
            // don't null-check don't NPE when reusing this helper.
            return new HistogramStatistics() {
                @Override
                public double getQuantile(double quantile) {
                    return 0.0;
                }

                @Override
                public long[] getValues() {
                    return new long[0];
                }

                @Override
                public int size() {
                    return 0;
                }

                @Override
                public double getMean() {
                    return 0.0;
                }

                @Override
                public double getStdDev() {
                    return 0.0;
                }

                @Override
                public long getMax() {
                    return 0L;
                }

                @Override
                public long getMin() {
                    return 0L;
                }
            };
        }
    }
}
