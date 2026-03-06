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
        // Test successful prediction
        predictFunction.eval("input1", 42);

        assertThat(testCollector.getCollectedResults()).hasSize(2);
        assertThat(testCollector.getCollectedResults().get(0).getArity()).isEqualTo(2);
    }

    @Test
    void testMultiplePredictions() {
        // Test multiple predictions work correctly
        predictFunction.eval("input1", 1);
        predictFunction.eval("input2", 2);

        // Each prediction returns 2 results
        assertThat(testCollector.getCollectedResults()).hasSize(4);
    }

    @Test
    void testFailureHandling() throws Exception {
        TestPredictFunction failingFunction = new TestPredictFunction(true);
        TestCollector failCollector = new TestCollector();
        failingFunction.open(new FunctionContext(null));
        failingFunction.setCollector(failCollector);

        assertThatThrownBy(() -> failingFunction.eval("input", 1))
                .isInstanceOf(FlinkRuntimeException.class)
                .hasMessageContaining("Failed to execute prediction");
    }

    @Test
    void testNullResults() throws Exception {
        TestPredictFunction nullResultFunction = new TestPredictFunction(false, true);
        TestCollector nullCollector = new TestCollector();
        nullResultFunction.open(new FunctionContext(null));
        nullResultFunction.setCollector(nullCollector);

        nullResultFunction.eval("input", 1);
        assertThat(nullCollector.getCollectedResults()).isEmpty();
    }

    @Test
    void testEmptyResults() throws Exception {
        TestPredictFunction emptyResultFunction = new TestPredictFunction(false, false, true);
        TestCollector emptyCollector = new TestCollector();
        emptyResultFunction.open(new FunctionContext(null));
        emptyResultFunction.setCollector(emptyCollector);

        emptyResultFunction.eval("input", 1);
        assertThat(emptyCollector.getCollectedResults()).isEmpty();
    }

    @Test
    void testCustomHistogram() throws Exception {
        TestPredictFunctionWithHistogram functionWithHistogram =
                new TestPredictFunctionWithHistogram();
        TestCollector histCollector = new TestCollector();
        functionWithHistogram.open(new FunctionContext(null));
        functionWithHistogram.setCollector(histCollector);

        functionWithHistogram.eval("input", 1);
        assertThat(histCollector.getCollectedResults()).hasSize(2);
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

            // Return two rows as prediction results
            RowData result1 =
                    GenericRowData.of(StringData.fromString("prediction1"), inputRow.getInt(1));
            RowData result2 =
                    GenericRowData.of(StringData.fromString("prediction2"), inputRow.getInt(1) * 2);

            return Arrays.asList(result1, result2);
        }
    }

    /** Testing implementation of {@link PredictFunction} with custom histogram. */
    private static class TestPredictFunctionWithHistogram extends TestPredictFunction {
        @Override
        protected Histogram createLatencyHistogram(MetricGroup metricGroup) {
            // Return a simple test histogram
            return metricGroup.histogram("latency", new TestHistogram());
        }
    }

    /** Testing implementation of {@link Histogram}. */
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
        public org.apache.flink.metrics.HistogramStatistics getStatistics() {
            return null;
        }
    }
}
